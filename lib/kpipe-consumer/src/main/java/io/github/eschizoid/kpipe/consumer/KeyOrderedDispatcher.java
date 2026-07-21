package io.github.eschizoid.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// Key-ordered dispatcher: records sharing a key process serially on a single virtual thread;
/// different keys process in parallel. Maintains a bounded map of active keys with a
/// configurable cap (default 10,000). Null-keyed records all serialize through a single
/// sentinel queue.
///
/// **Eviction policy.** When the cap is reached and a new key needs a queue slot, we scan for
/// any queue that is both empty AND has no worker running, and evict it. We never evict a
/// non-empty queue (would break per-key ordering). If every queue at cap is non-empty, the
/// dispatch call holds — sleeping briefly and retrying — until some queue drains. This is the
/// implicit-backpressure mechanism for KEY_ORDERED mode: a hot producer sending many distinct
/// keys faster than they can process will naturally stall the consumer thread's poll loop.
/// (The v1 design kept an access-ordered LRU and evicted coldest-first; ordering was never a
/// correctness requirement — only empty+idle queues are ever reclaimed — so v2 trades the
/// coldest-first preference for lock-free reads on the hot path.)
///
/// **Per-key worker lifecycle.** When a record arrives for a key with no active worker, the
/// dispatcher starts a virtual thread that drains the queue until empty, then exits. New
/// records arriving for the same key (after the worker has exited but the queue entry is
/// still in the map) trigger a fresh worker. The empty queue stays in the map until evicted
/// to make room for another key.
///
/// **Concurrency.** v2 of this class: the single global `ReentrantLock` (the measured
/// throughput ceiling at high key cardinality) is replaced by a `ConcurrentHashMap` plus one
/// monitor per key queue. The hot path — enqueue by the consumer thread, dequeue by that
/// key's worker — contends only on that key's monitor; workers for different keys never
/// contend on monitors (they still share the `pending` counter and the `activeWorkers`
/// set). Map membership changes (insert on first dispatch for a key,
/// remove on eviction) go through the `ConcurrentHashMap`, and eviction's
/// empty-and-idle check runs inside `computeIfPresent` while holding the queue's monitor, so
/// removal is atomic with respect to both enqueuers and the draining worker. Lock ordering is
/// map-bin → queue monitor (eviction); dispatch and drain acquire the queue monitor only
/// after any map operation completes, so no cycle exists.
///
/// **Evict-vs-enqueue race.** A dispatcher may obtain a queue reference just before eviction
/// removes that queue from the map. Eviction marks the queue `dead` (under its monitor)
/// atomically with the removal; the dispatcher re-checks `dead` under the same monitor before
/// enqueuing and retries with a fresh map lookup if it lost the race. A worker cannot race
/// eviction at all: eviction requires `workerActive == false`, which is cleared only under
/// the queue's monitor (worker exit after observing an empty queue, or the start-failure
/// rollback) — never while a worker is mid-drain.
///
/// **Shutdown.** [KPipeConsumer#close] calls `waitForInFlightDrain(waitForMessagesTimeout)`
/// BEFORE invoking [#close()] on this dispatcher. By the time `close()` runs, all queues
/// should be empty and workers exited. This `close()` performs a defensive short wait for
/// any straggler, then interrupts remaining workers and returns; records still in flight
/// after that are abandoned to the JVM (mirrors the `virtualThreadExecutor` shutdown
/// behavior in PARALLEL mode).
final class KeyOrderedDispatcher implements Dispatcher {

  private static final Logger LOGGER = System.getLogger(KeyOrderedDispatcher.class.getName());

  /// Default cap on distinct keys held in memory simultaneously. Configurable via
  /// `KPipeConsumerBuilder.withKeyOrderedMaxKeys(int)`. Aliases the public single-source-of-
  /// truth constant so the default can't drift between this module and `kpipe-api`.
  static final int DEFAULT_MAX_KEYS = ProcessingMode.DEFAULT_KEY_ORDERED_MAX_KEYS;

  /// Sentinel used in place of `null` Kafka keys. Records with `null` keys all serialize
  /// through this single queue. Distinct by identity from any user-supplied key.
  private static final Object NULL_KEY = new Object();

  private static final Duration CLOSE_DRAIN_TIMEOUT = Duration.ofSeconds(5);

  private final int maxKeys;
  /// Active key queues. Concurrent map so workers, diagnostics, and (in tests) concurrent
  /// dispatchers can operate without a global lock; per-queue state is guarded by each
  /// [KeyQueue]'s monitor.
  private final ConcurrentHashMap<Object, KeyQueue> queues = new ConcurrentHashMap<>();
  private final AtomicLong pending = new AtomicLong(0);
  /// Set once on the first dispatch-stall (cap saturated, every queue non-empty) so the WARN
  /// log fires once per saturation episode: it re-arms when a stalled dispatch eventually
  /// succeeds, so a NEW saturation hours later warns again, while a sustained stall still
  /// emits a single line instead of flooding. The signal tells an operator to bump
  /// `KPipeConsumerBuilder.withKeyOrderedMaxKeys(int)`.
  private final AtomicBoolean stallWarningEmitted = new AtomicBoolean(false);

  /// One-shot flag for the shutdown-abandon WARN in [#dispatch] so a close() landing on a
  /// saturated consumer logs the first abandoned record (with coordinates) instead of either
  /// silence or one line per abandoned record in the tail of the poll batch.
  private final AtomicBoolean abandonWarningEmitted = new AtomicBoolean(false);

  /// Counts dead-tombstone retries in [#dispatch] — the cold path where a dispatcher's queue
  /// reference was evicted between lookup and monitor entry. Package-private so stress tests
  /// can observe that the window was actually reached (the retry outcome is invisible from
  /// the public surface: the record processes normally either way).
  final AtomicLong tombstoneRetries = new AtomicLong(0);

  /// Set by [#close()] to break out of the saturation stall-loop in [#reserveCapacity]. If
  /// every queue is full and workers are stuck (e.g. user pipeline deadlock), the consumer
  /// thread would otherwise spin past close until `KPipeConsumer.threadTerminationTimeout`
  /// expires. With this flag, close() causes the in-flight dispatch to abandon — the record
  /// stays tracked-but-unmarked in the offset manager, so the commit frontier never passes
  /// it and Kafka redelivers it on the next consumer start (correct at-least-once
  /// semantics).
  private volatile boolean closed = false;

  /// Live set of currently-running per-key worker VTs. The dispatching thread registers a
  /// worker BEFORE `start()` (so close()'s interrupt loop can't miss a started-but-not-yet-
  /// running worker); the runnable removes itself in its `finally`, with a fallback removal
  /// if `start()` throws. [#close()] iterates this set to interrupt stuck workers after the
  /// drain timeout — same shutdown semantics as `ParallelDispatcher`'s
  /// `executor.shutdownNow()`.
  private final Set<Thread> activeWorkers = ConcurrentHashMap.newKeySet();

  /// @param maxKeys cap on distinct keys (must be positive)
  KeyOrderedDispatcher(final int maxKeys) {
    if (maxKeys <= 0) throw new IllegalArgumentException("maxKeys must be positive, got " + maxKeys);
    this.maxKeys = maxKeys;
  }

  @Override
  public void dispatch(
    final ConsumerRecord<byte[], byte[]> record,
    final Runnable processTask,
    final Runnable onComplete
  ) {
    // No `if (closed) return` early-exit here — that would orphan records mid-batch
    // (TrackOffset already enqueued in KPipeConsumer.processRecords, but no
    // MarkOffsetProcessed since we skipped). The `closed` flag is checked ONLY inside the
    // saturation stall-loop in reserveCapacity, so a normally-arriving dispatch always
    // enqueues its task and lets the worker drain it.
    final var key = normalizeKey(record.key());
    pending.incrementAndGet();

    // Hold `(processTask, onComplete)` together as a single small record rather than wrapping
    // them in a closure that also captures `pending` and `key`. `drain()` runs each half
    // directly with its own try/finally, eliminating the per-record wrapper Runnable allocation.
    final var queued = new QueuedTask(processTask, onComplete);

    var retries = 0L;
    while (true) {
      var queue = queues.get(key);
      if (queue == null) {
        if (!reserveCapacity()) {
          // reserveCapacity returned false because close() fired during the saturation stall.
          // Roll back the pending increment so totalInFlight() doesn't get stuck; the record
          // stays tracked-but-unmarked, so it is redelivered on restart.
          pending.decrementAndGet();
          if (abandonWarningEmitted.compareAndSet(false, true)) {
            LOGGER.log(
              Level.WARNING,
              "Abandoning dispatch during shutdown (cap saturated, close() signalled): {0}-{1}@{2}. " +
                "This and any further abandoned records in the batch stay uncommitted and will be " +
                "redelivered on restart. Further abandons are not logged individually.",
              record.topic(),
              record.partition(),
              record.offset()
            );
          }
          return;
        }
        final var fresh = new KeyQueue();
        final var raced = queues.putIfAbsent(key, fresh);
        queue = raced != null ? raced : fresh;
      }
      synchronized (queue) {
        // Re-check under the monitor: eviction may have removed this queue from the map
        // between our lookup and here. `dead` is set under this monitor atomically with the
        // removal, so the retry loop eventually observes the removal (a retry may
        // transiently re-read the dead entry before the unlink publishes, but `dead` is
        // permanent so the loop cannot enqueue into it).
        if (queue.dead) {
          tombstoneRetries.incrementAndGet();
          if (++retries == 1_000) {
            // Production has a single dispatching thread, which cannot race its own
            // eviction — sustained retries mean concurrent dispatchers are starving this
            // one. Log so the spin is diagnosable instead of silent CPU burn.
            LOGGER.log(
              Level.WARNING,
              "dispatch retried {0} times against evicted queues for one record; concurrent " +
                "dispatchers are contending with eviction (cap {1} may be too small)",
              retries,
              maxKeys
            );
          }
          continue;
        }
        queue.tasks.addLast(queued);
        if (!queue.workerActive) {
          queue.workerActive = true;
          try {
            startWorker(key, queue);
          } catch (final RuntimeException e) {
            // Worker failed to start (e.g. the JVM can't create the thread). Undo the enqueue
            // so this key isn't wedged with workerActive=true and an undrained task, and
            // pending doesn't leak (which would stall drain/backpressure). Re-throw to fail
            // fast.
            queue.workerActive = false;
            queue.tasks.removeLast();
            pending.decrementAndGet();
            throw e;
          }
        }
      }
      return;
    }
  }

  /// Ensures there is room in the map for one more key, evicting an empty + idle queue if the
  /// cap is reached. Stalls (with a 1ms park) only when NO queue is evictable — the
  /// implicit-backpressure mechanism. Returns `false` if shutdown is signalled (or the thread
  /// is interrupted) *while stalled*, so the caller can roll back the pending counter and
  /// abandon the dispatch.
  ///
  /// The shutdown/interrupt check lives in the stall branch, not at the top of the loop:
  /// during shutdown we still want to evict an idle queue and process the record if we can,
  /// rather than abandoning it (which would orphan its already-tracked offset and force
  /// avoidable reprocessing on restart). We only give up when stalling is the only option.
  ///
  /// Under concurrent dispatch (a test-only scenario; production has a single consumer
  /// thread) the size check and insert are not one atomic step, so the map can transiently
  /// exceed the cap by the number of concurrent dispatchers minus one. The bound is exact in
  /// production use.
  private boolean reserveCapacity() {
    var stalled = false;
    while (queues.mappingCount() >= maxKeys) {
      if (evictOneIdle()) continue;
      stalled = true;
      // No evictable queue → the only options are stall or give up. If shutting down or
      // interrupted, give up (return false → caller abandons). Checking interrupt here
      // rather than after Thread.sleep also prevents a set interrupt flag from making the
      // sleep throw every iteration, which would turn the 1ms backoff into a tight spin.
      if (closed || Thread.currentThread().isInterrupted()) return false;
      if (stallWarningEmitted.compareAndSet(false, true)) {
        LOGGER.log(
          Level.WARNING,
          "KEY_ORDERED dispatch stalled: all {0} key queues are non-empty and the key cap is saturated. " +
            "The consumer thread will hold until a queue drains. If this is sustained, raise the cap via " +
            "withKeyOrderedMaxKeys(...) (on the Builder, Stream, or MultiBuilder). This warning fires once " +
            "per saturation episode.",
          maxKeys
        );
      }
      try {
        // 1ms park, not Thread.yield, so sustained saturation doesn't peg a CPU core on
        // the consumer thread. Worst-case latency is one sleep tick after a queue drains.
        //noinspection BusyWait — intentional bounded backpressure park, not a spin
        Thread.sleep(1);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    // Re-arm the stall warning once the episode clears, so the NEXT saturation (possibly
    // hours later) gets its own signal while a sustained stall still logs a single line.
    if (stalled) stallWarningEmitted.set(false);
    return true;
  }

  /// Scans the map for a queue that is empty AND has no running worker, and removes it.
  /// The check-and-remove is atomic: `computeIfPresent` holds the map bin while the queue's
  /// monitor confirms it is reclaimable, and `dead` is set inside that same monitor section,
  /// so a dispatcher holding a stale reference to the evicted queue will observe `dead` and
  /// retry, and a worker can never be mid-drain (that would mean `workerActive == true`).
  /// Returns `true` if a queue was evicted.
  private boolean evictOneIdle() {
    for (final var key : queues.keySet()) {
      final var removed = new AtomicBoolean(false);
      queues.computeIfPresent(key, (k, q) -> {
        synchronized (q) {
          if (q.tasks.isEmpty() && !q.workerActive) {
            q.dead = true;
            removed.set(true);
            return null;
          }
          return q;
        }
      });
      if (removed.get()) return true;
    }
    return false;
  }

  /// Starts a new virtual thread that drains the queue until empty. Called while holding the
  /// queue's monitor (the new worker's first drain step re-acquires it, so it simply blocks
  /// until the dispatching thread releases). Registers the worker in [#activeWorkers] BEFORE
  /// starting it — not from inside the runnable — so [#close()]'s interrupt loop can't miss a
  /// worker that has been started but hasn't run its first statement yet. (Registering inside
  /// the runnable left a window where close() could time out and interrupt the set before the
  /// new worker added itself, leaving it un-interrupted and able to run after shutdown closed
  /// the offset manager / producer.) The runnable removes itself on exit; if `start()`
  /// throws, we remove it as a fallback since the finally would never run.
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

  /// Inner drain loop for [#startWorker]. Pulls tasks under the queue's monitor; runs them
  /// outside it. The exit decision (queue empty → `workerActive = false` → return) happens in
  /// the same monitor section as the failed poll, so an enqueue that lands while the worker
  /// is deciding either is seen by this worker's next poll or finds `workerActive == false`
  /// and starts a fresh worker — a task can never be stranded.
  private void drain(final KeyQueue queue) {
    while (true) {
      QueuedTask task;
      synchronized (queue) {
        task = queue.tasks.pollFirst();
        if (task == null) {
          queue.workerActive = false;
          return;
        }
      }
      // Run OUTSIDE the monitor. Outer try/finally guarantees pending is decremented and
      // onComplete is invoked even if processTask throws; the outer catch keeps the drain
      // loop alive on any Throwable.
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
        // Restore the interrupt flag if the task surfaced an interrupt (close() interrupts
        // stuck workers; swallowing the flag here would defeat that signal for the rest of
        // the drain). Kafka's InterruptException restores it in its own constructor.
        if (t instanceof InterruptedException) Thread.currentThread().interrupt();
        LOGGER.log(Level.ERROR, "Per-key worker task threw; continuing drain", t);
      }
    }
  }

  @Override
  public long activeCount() {
    return pending.get();
  }

  /// Snapshot of the top `n` keys by current queue depth, deepest-first. Weakly consistent:
  /// the map is iterated without a global lock (entries may be added or evicted mid-walk) and
  /// each queue's depth is read under its own monitor. Intended for ad-hoc diagnostics, not
  /// hot-path use. The null-key queue is returned with a `null` entry key. `byte[]` keys are
  /// internally wrapped in a `ByteBuffer` for content-based identity; the snapshot returns a
  /// defensive `byte[]` copy of the underlying bytes so callers can't mutate the buffer's
  /// position/limit or the backing array and corrupt the dispatcher's internal map.
  @Override
  public List<Map.Entry<byte[], Integer>> topKeyQueueDepths(final int n) {
    if (n <= 0) throw new IllegalArgumentException("n must be positive, got " + n);
    final List<Map.Entry<byte[], Integer>> snapshot = new ArrayList<>();
    for (final var entry : queues.entrySet()) {
      final var q = entry.getValue();
      final int depth;
      synchronized (q) {
        depth = q.tasks.size();
      }
      snapshot.add(new AbstractMap.SimpleImmutableEntry<>(toPublicKey(entry.getKey()), depth));
    }
    snapshot.sort(Comparator.comparingInt((Map.Entry<byte[], Integer> e) -> e.getValue()).reversed());
    return snapshot.size() <= n ? snapshot : new ArrayList<>(snapshot.subList(0, n));
  }

  /// Converts the dispatcher's internal map key into the form returned to diagnostic callers:
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

  /// Normalizes the raw `byte[]` Kafka key into something the map can index correctly. The
  /// map uses `Object.equals`/`hashCode` to identify entries, but `byte[]` has
  /// reference-based equality — two arrays with identical content would be treated as
  /// distinct keys and break per-key serialization — so keys are wrapped in a `ByteBuffer` to
  /// get content-based equality.
  ///
  /// The wrapper holds a **defensive copy** of the bytes (via `bytes.clone()`) so the map's
  /// key identity cannot be invalidated by a caller mutating the original `byte[]` after
  /// dispatch. Without the copy, post-dispatch mutation would change the buffer's
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
    // thread stuck in reserveCapacity's saturation stall-loop can escape promptly and let
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
        //noinspection BusyWait — deadline-bounded drain wait during close, not a spin
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

  /// Per-key state: a serial FIFO queue of pending tasks, a flag that records whether a
  /// virtual-thread worker is currently draining it, and a tombstone set by eviction. All
  /// three fields are guarded by this object's monitor.
  private static final class KeyQueue {

    final ArrayDeque<QueuedTask> tasks = new ArrayDeque<>();
    boolean workerActive = false;
    /// Set (under the monitor, atomically with removal from the map) when eviction reclaims
    /// this queue. A dispatcher that obtained its reference before the removal observes the
    /// tombstone under the monitor and retries against the live map.
    boolean dead = false;
  }

  /// Pairs the per-record `processTask` with its `onComplete` callback. Replaces the
  /// per-dispatch wrapper [Runnable] that previously captured both plus `pending` and `key`.
  private record QueuedTask(Runnable processTask, Runnable onComplete) {}
}
