package io.github.eschizoid.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// Key-ordered dispatcher: records sharing a key process serially on a single virtual thread;
/// different keys process in parallel. Maintains an LRU map of active keys with a configurable
/// cap (default 10,000). Null-keyed records all serialize through a single sentinel queue.
///
/// **LRU eviction policy.** When the cap is reached and a new key needs a queue slot, we walk
/// the LRU oldest-first looking for a queue that is both empty AND has no worker running. We
/// never evict a non-empty queue (would break per-key ordering). If every queue at cap is
/// non-empty, the dispatch call holds — releasing and re-acquiring the lock, yielding the
/// consumer thread — until some queue drains. This is the implicit-backpressure mechanism for
/// KEY_ORDERED mode: a hot producer sending many distinct keys faster than they can process
/// will naturally stall the consumer thread's poll loop.
///
/// **Per-key worker lifecycle.** When a record arrives for a key with no active worker, the
/// dispatcher starts a virtual thread that drains the queue until empty, then exits. New
/// records arriving for the same key (after the worker has exited but the queue entry is
/// still in the LRU map) trigger a fresh worker. The empty queue stays in the LRU map until
/// evicted to make room for another key — that's how the LRU naturally retains "warm" keys.
///
/// **Concurrency.** All map mutations + queue mutations happen under a single
/// [ReentrantLock]. Task execution runs OUTSIDE the lock (the worker releases before calling
/// `task.run()`, reacquires for the next poll). This means the lock is held only for
/// microseconds at a time — enqueue, dequeue, LRU bookkeeping. Hot lock under very high key
/// cardinality is a known v1 limitation; striped locking or `Caffeine` are possible v2
/// optimizations.
///
/// **Atomic remove-if-empty.** The worker's exit is guarded under the lock — it polls from
/// the deque AND clears `workerActive` under the same lock acquisition. A concurrent
/// `dispatch()` either finds `workerActive=true` (in which case it just enqueues, the
/// existing worker picks it up) or finds `workerActive=false` (in which case it starts a new
/// worker for the same queue).
///
/// **Shutdown.** [KPipeConsumer#close] calls `waitForInFlightDrain(waitForMessagesTimeout)`
/// BEFORE invoking [#close()] on this dispatcher. By the time `close()` runs, all queues
/// should be empty and workers exited. This `close()` performs a defensive short wait for any
/// straggler then returns; in-progress records that exceed the timeout are abandoned to the
/// JVM (mirrors the existing `virtualThreadExecutor` shutdown behavior).
final class KeyOrderedDispatcher implements Dispatcher {

  private static final Logger LOGGER = System.getLogger(KeyOrderedDispatcher.class.getName());

  /// Default LRU cap on distinct keys held in memory simultaneously. Configurable via
  /// `KPipeConsumer.Builder.withKeyOrderedMaxKeys(int)`. Aliases the public single-source-of-
  /// truth constant so the default can't drift between this module and `kpipe-api`.
  static final int DEFAULT_MAX_KEYS = ProcessingMode.DEFAULT_KEY_ORDERED_MAX_KEYS;

  /// Sentinel used in place of `null` Kafka keys. Records with `null` keys all serialize
  /// through this single queue. Distinct by identity from any user-supplied key.
  private static final Object NULL_KEY = new Object();

  private static final Duration CLOSE_DRAIN_TIMEOUT = Duration.ofSeconds(5);

  private final int maxKeys;
  /// LRU map of active key queues. `accessOrder=true` means `get/put` move the entry to the
  /// most-recently-used end. Eviction scans from head (least recently used).
  private final LinkedHashMap<Object, KeyQueue> queues;
  private final ReentrantLock lock = new ReentrantLock();
  private final AtomicLong pending = new AtomicLong(0);
  /// Set once on the first dispatch-stall (cap saturated, every queue non-empty) so the WARN
  /// log fires exactly once per dispatcher instance. Repeating the warning on every stall
  /// would flood logs under sustained saturation; one signal is enough to tell an operator
  /// to bump `KPipeConsumer.Builder.withKeyOrderedMaxKeys(int)`.
  private final AtomicBoolean stallWarningEmitted = new AtomicBoolean(false);

  /// Set by [#close()] to break out of the saturation yield-loop in [#allocateNewQueue]. If
  /// every queue is full and workers are stuck (e.g. user pipeline deadlock), the consumer
  /// thread would otherwise spin past close until `KPipeConsumer.threadTerminationTimeout`
  /// expires. With this flag, close() causes the in-flight dispatch to abandon — the
  /// untracked record stays uncommitted in the offset manager so Kafka redelivers it on the
  /// next consumer start (correct at-least-once semantics).
  private volatile boolean closed = false;

  /// Live set of currently-running per-key worker VTs. Each worker registers itself on entry
  /// and removes itself on exit (both inside the runnable, so the lifecycle is race-free).
  /// [#close()] iterates this set to interrupt stuck workers after the drain timeout — same
  /// shutdown semantics as `ParallelDispatcher`'s `executor.shutdownNow()`.
  private final Set<Thread> activeWorkers = ConcurrentHashMap.newKeySet();

  /// @param maxKeys LRU cap on distinct keys (must be positive)
  KeyOrderedDispatcher(final int maxKeys) {
    if (maxKeys <= 0) throw new IllegalArgumentException("maxKeys must be positive, got " + maxKeys);
    this.maxKeys = maxKeys;
    this.queues = new LinkedHashMap<>(16, 0.75f, true);
  }

  @Override
  public void dispatch(final ConsumerRecord<byte[], byte[]> record, final Runnable processTask, final Runnable onComplete) {
    // No `if (closed) return` early-exit here — that would orphan records mid-batch
    // (TrackOffset already enqueued in KPipeConsumer.processRecords, but no
    // MarkOffsetProcessed since we skipped). The `closed` flag is checked ONLY inside the
    // saturation yield-loop in allocateNewQueue, so a normally-arriving dispatch always
    // enqueues its task and lets the worker drain it.
    final var key = normalizeKey(record.key());
    pending.incrementAndGet();

    // Hold `(processTask, onComplete)` together as a single small record rather than wrapping
    // them in a closure that also captures `pending` and `key`. `drain()` runs each half
    // directly with its own try/finally, eliminating the per-record wrapper Runnable allocation.
    final var queued = new QueuedTask(processTask, onComplete);

    lock.lock();
    try {
      var queue = queues.get(key);
      if (queue == null) {
        queue = allocateNewQueue(key);
        if (queue == null) {
          // allocateNewQueue returned null because close() fired during the saturation stall.
          // Roll back the pending increment so totalInFlight() doesn't get stuck.
          pending.decrementAndGet();
          return;
        }
      }
      queue.tasks.addLast(queued);
      if (!queue.workerActive) {
        queue.workerActive = true;
        try {
          startWorker(key, queue);
        } catch (final RuntimeException e) {
          // Worker failed to start (e.g. the JVM can't create the thread). Undo the enqueue so
          // this key isn't wedged with workerActive=true and an undrained task, and pending
          // doesn't leak (which would stall drain/backpressure). Re-throw to fail fast.
          queue.workerActive = false;
          queue.tasks.removeLast();
          pending.decrementAndGet();
          throw e;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /// Called under `lock`. Walks the LRU map until an empty + idle queue is found, evicts it,
  /// and allocates a new queue for `key`. Stalls (releasing + reacquiring the lock with a 1ms
  /// park) only when NO queue is evictable — the implicit-backpressure mechanism. Returns
  /// `null` if shutdown is signalled (or the thread is interrupted) *while stalled*, so the
  /// caller can roll back the pending counter and abandon the dispatch.
  ///
  /// The shutdown/interrupt check lives in the stall branch, not at the top of the loop:
  /// during shutdown we still want to evict an idle queue and process the record if we can,
  /// rather than abandoning it (which would orphan its already-tracked offset and force
  /// avoidable reprocessing on restart). We only give up when stalling is the only option.
  private KeyQueue allocateNewQueue(final Object key) {
    while (queues.size() >= maxKeys) {
      final var evictable = findEvictable();
      if (evictable != null) {
        queues.remove(evictable);
      } else {
        // No evictable queue → the only options are stall or give up. If shutting down or
        // interrupted, give up (return null → caller abandons). Checking interrupt here
        // rather than after Thread.sleep also prevents a set interrupt flag from making the
        // sleep throw every iteration, which would turn the 1ms backoff into a tight spin.
        if (closed || Thread.currentThread().isInterrupted()) return null;
        if (stallWarningEmitted.compareAndSet(false, true)) {
          LOGGER.log(
            Level.WARNING,
            "KEY_ORDERED dispatch stalled: all {0} key queues are non-empty and the LRU cap is saturated. " +
              "The consumer thread will hold until a queue drains. If this is sustained, raise the cap via " +
              "withKeyOrderedMaxKeys(...) (on the Builder, Stream, or MultiBuilder). This warning fires once " +
              "per dispatcher instance.",
            maxKeys
          );
        }
        lock.unlock();
        try {
          // 1ms park, not Thread.yield, so sustained saturation doesn't peg a CPU core on
          // the consumer thread. Worst-case latency is one sleep tick after a queue drains.
          Thread.sleep(1);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          lock.lock();
        }
      }
    }
    final var q = new KeyQueue();
    queues.put(key, q);
    return q;
  }

  /// Called under `lock`. Returns the LRU-oldest key whose queue is empty AND has no running
  /// worker, or `null` if none. Iteration is from LRU (oldest) to MRU (newest).
  private Object findEvictable() {
    for (final var entry : queues.entrySet()) {
      final var q = entry.getValue();
      if (q.tasks.isEmpty() && !q.workerActive) return entry.getKey();
    }
    return null;
  }

  /// Called under `lock`. Starts a new virtual thread that drains the queue until empty.
  /// Registers the worker in [#activeWorkers] BEFORE starting it — not from inside the
  /// runnable — so [#close()]'s interrupt loop can't miss a worker that has been started but
  /// hasn't run its first statement yet. (Registering inside the runnable left a window where
  /// close() could time out and interrupt the set before the new worker added itself, leaving
  /// it un-interrupted and able to run after shutdown closed the offset manager / producer.)
  /// The runnable removes itself on exit; if `start()` throws, we remove it as a fallback
  /// since the finally would never run.
  private void startWorker(final Object key, final KeyQueue queue) {
    final var worker = Thread.ofVirtual()
      .name("kpipe-key-worker-" + System.identityHashCode(key))
      .unstarted(() -> {
        try {
          drain(queue);
        } finally {
          activeWorkers.remove(Thread.currentThread());
        }
      });
    activeWorkers.add(worker);
    try {
      worker.start();
    } catch (final RuntimeException e) {
      activeWorkers.remove(worker);
      throw e;
    }
  }

  /// Inner drain loop for [#startWorker]. Pulls tasks under the lock; runs them outside.
  /// `pending.decrement` and `onComplete` were previously folded into a wrapper [Runnable]
  /// allocated per-dispatch; they now happen here, removing that allocation from the hot path.
  private void drain(final KeyQueue queue) {
    while (true) {
      QueuedTask task;
      lock.lock();
      try {
        task = queue.tasks.pollFirst();
        if (task == null) {
          queue.workerActive = false;
          return;
        }
      } finally {
        lock.unlock();
      }
      // Run OUTSIDE the lock. Outer try/finally guarantees pending is decremented and
      // onComplete is invoked even if processTask throws; the outer catch keeps the drain
      // loop alive on any Throwable, mirroring the previous wrapper's safety net.
      try {
        try {
          task.processTask.run();
        } finally {
          pending.decrementAndGet();
          try {
            task.onComplete.run();
          } catch (final RuntimeException e) {
            LOGGER.log(Level.WARNING, "onComplete callback threw", e);
          }
        }
      } catch (final Throwable t) {
        LOGGER.log(Level.ERROR, "Per-key worker task threw; continuing drain", t);
      }
    }
  }

  @Override
  public long pendingCount() {
    return pending.get();
  }

  /// Snapshot of the top `n` keys by current queue depth, deepest-first. Walks every queue
  /// under the lock; intended for ad-hoc diagnostics, not hot-path use. The null-key queue is
  /// returned with a `null` entry key. `byte[]` keys are internally wrapped in a `ByteBuffer`
  /// for content-based identity; the snapshot returns a defensive `byte[]` copy of the
  /// underlying bytes so callers can't mutate the buffer's position/limit or the backing
  /// array and corrupt the dispatcher's internal map.
  @Override
  public List<Map.Entry<byte[], Integer>> topKeyQueueDepths(final int n) {
    if (n <= 0) throw new IllegalArgumentException("n must be positive, got " + n);
    final List<Map.Entry<byte[], Integer>> snapshot;
    lock.lock();
    try {
      // queues is a plain LinkedHashMap mutated only under lock — size it here, not before.
      snapshot = new ArrayList<>(queues.size());
      for (final var entry : queues.entrySet()) {
        snapshot.add(
          new AbstractMap.SimpleImmutableEntry<>(toPublicKey(entry.getKey()), entry.getValue().tasks.size())
        );
      }
    } finally {
      lock.unlock();
    }
    snapshot.sort(Comparator.comparingInt((Map.Entry<byte[], Integer> e) -> e.getValue()).reversed());
    return snapshot.size() <= n ? snapshot : new ArrayList<>(snapshot.subList(0, n));
  }

  /// Converts the dispatcher's internal LRU key into the form returned to diagnostic callers:
  /// the null sentinel becomes `null`, and the `ByteBuffer` wrapper becomes a defensive copy
  /// of the underlying bytes (so callers can't mutate position/limit or the backing array on
  /// our live map key).
  private static byte[] toPublicKey(final Object internalKey) {
    if (internalKey == NULL_KEY) return null;
    final var bb = (ByteBuffer) internalKey;
    final var copy = new byte[bb.remaining()];
    bb.duplicate().get(copy);
    return copy;
  }

  /// Normalizes the raw `byte[]` Kafka key into something the LRU map can index correctly. The
  /// `LinkedHashMap` uses `Object.equals`/`hashCode` to identify entries, but `byte[]` has
  /// reference-based equality — two arrays with identical content would be treated as
  /// distinct keys and break per-key serialization — so keys are wrapped in a `ByteBuffer` to
  /// get content-based equality.
  ///
  /// The wrapper holds a **defensive copy** of the bytes (via `bytes.clone()`) so the LRU
  /// map's key identity cannot be invalidated by a caller mutating the original `byte[]`
  /// after dispatch. Without the copy, post-dispatch mutation would change the buffer's
  /// `equals`/`hashCode` and the map entry would become unreachable, leaking its queue.
  ///
  /// `null` keys all collapse to a single sentinel so they serialize through one queue.
  private static Object normalizeKey(final byte[] rawKey) {
    if (rawKey == null) return NULL_KEY;
    return ByteBuffer.wrap(rawKey.clone());
  }

  @Override
  public void signalShutdown() {
    // Non-blocking. Set by KPipeConsumer.close() BEFORE waitForInFlightDrain so a consumer
    // thread stuck in allocateNewQueue's saturation yield-loop can escape promptly and let
    // the drain wait + thread.join make progress. Workers already inside task.run() are not
    // interrupted — they finish naturally or get abandoned at JVM exit (VTs are daemon).
    closed = true;
  }

  @Override
  public void close() {
    // Safety net: re-signal in case close() was called directly without signalShutdown()
    // (e.g. from a test or non-standard shutdown path). Idempotent — volatile write.
    closed = true;
    // `KPipeConsumer.close()` already calls `waitForInFlightDrain(waitForMessagesTimeout)`
    // BEFORE invoking us, so `pending` should typically be 0 by now. Short defensive wait
    // for any straggler in-flight record. Records that don't finish within the timeout are
    // abandoned to the JVM (same behavior as the prior `virtualThreadExecutor.shutdownNow`
    // path for PARALLEL mode).
    final var deadline = System.nanoTime() + CLOSE_DRAIN_TIMEOUT.toNanos();
    while (pending.get() > 0 && System.nanoTime() < deadline) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    if (pending.get() > 0) {
      LOGGER.log(
        Level.WARNING,
        "Closing KeyOrderedDispatcher with {0} in-flight records; interrupting {1} active worker(s)",
        pending.get(),
        activeWorkers.size()
      );
      // Matches ParallelDispatcher's `executor.shutdownNow()` semantics — stuck workers get
      // interrupted so they don't outlive the consumer's offset manager + producer cleanup
      // inside a long-running JVM. Interrupt-aware user pipelines exit cleanly; CPU-bound
      // ones still run to completion (same caveat as PARALLEL).
      for (final var w : activeWorkers) w.interrupt();
    }
  }

  /// Per-key state: a serial FIFO queue of pending tasks plus a flag that records whether a
  /// virtual-thread worker is currently draining it. Mutated only under
  /// [KeyOrderedDispatcher#lock].
  private static final class KeyQueue {

    final ArrayDeque<QueuedTask> tasks = new ArrayDeque<>();
    boolean workerActive = false;
  }

  /// Pairs the per-record `processTask` with its `onComplete` callback. Replaces the
  /// per-dispatch wrapper [Runnable] that previously captured both plus `pending` and `key`.
  private record QueuedTask(Runnable processTask, Runnable onComplete) {}
}
