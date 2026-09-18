package io.github.eschizoid.kpipe.consumer;

import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchResult;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// Internal per-topic batch buffer. Owns a queue of `(record, value)` pairs and flushes either on
/// size or on age (whichever fires first), or on shutdown drain. The user [BatchSink] returns a
/// [BatchResult] naming per-record outcomes; succeeded records have their offsets marked
/// processed, failed records are routed through the caller-supplied [BatchCallbacks#onBatchFailure]
/// hook (DLQ + error handler). A failed record's offset is marked only once it is durably parked in
/// the DLQ (or no DLQ is configured); a failed DLQ send leaves it pending for reprocessing.
///
/// **Thread-safety.** A single [ReentrantLock] guards the buffer.
/// `enqueue` may be called from many virtual-thread workers at once
/// (parallel and key-ordered modes), or serialized on the consumer
/// thread (sequential mode); the lock makes both safe. `tick` runs on
/// the shared scheduler and `close` runs from the shutdown path. All
/// paths serialize through the same lock, so the buffer, the
/// `oldestEnqueueNanos` timestamp and the `bufferedCount` stay coherent.
///
/// **Backpressure participation.** The wrapper exposes a [#bufferedCount] gauge that the owning
/// consumer adds to its in-flight count when the in-flight backpressure strategy is active.
/// Without that addition, buffered records would be invisible to the watermark check in parallel
/// mode — a slow batch sink could let the buffer grow unbounded while the consumer kept polling.
/// `bufferedCount` increments on each [#enqueue] call and decrements after every flush by the
/// snapshot size.
///
/// @param <T> deserialized value type
final class BatchPipelineWrapper<T> implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(BatchPipelineWrapper.class.getName());

  /// Hooks back into the owning consumer for offset bookkeeping and failure handling. Decouples
  /// the wrapper from `KPipeConsumer`'s package-private state for testability.
  interface BatchCallbacks {
    /// Mark a record's offset as successfully processed (after a successful batch flush).
    void markProcessed(ConsumerRecord<byte[], byte[]> record);

    /// Handle a record that was part of a failed batch: increment error metrics, route to the DLQ
    /// if one is configured, and invoke the error handler. The offset is marked processed only if
    /// the record reaches a durable terminal state — successfully parked in the DLQ, or no DLQ is
    /// configured (log-and-advance). A failed DLQ send leaves the offset pending so the record is
    /// reprocessed on restart rather than silently dropped.
    void onBatchFailure(ConsumerRecord<byte[], byte[]> record, Exception cause);
  }

  private final String topic;
  private final MessagePipeline<T> pipeline;
  private final BatchSink<T> sink;
  private final BatchPolicy policy;
  private final ScheduledExecutorService scheduler;
  private final BatchCallbacks callbacks;

  private final ReentrantLock lock = new ReentrantLock();
  private final List<Entry<T>> buffer = new ArrayList<>();
  private final AtomicLong bufferedCount = new AtomicLong(0);

  /// Dispatches handed out by [#flushLocked] that have not finished running. Decremented when the
  /// dispatch ends, whatever the outcome.
  ///
  /// The increment happens while the flush lock is still held, not when the dispatch starts. That
  /// is what makes a `close()` arriving after a tick released the lock — but before its dispatch
  /// began — still see the work outstanding. Moving it into the dispatch itself narrows the window
  /// rather than closing it, and the window is too narrow for a racing test to enter reliably:
  /// `BatchCloseDrainFrayTest` schedules it deliberately instead.
  private final AtomicInteger dispatchesInFlight = new AtomicInteger();

  private final ReentrantLock quiesceLock = new ReentrantLock();
  private final Condition dispatchesQuiesced = quiesceLock.newCondition();
  private long oldestEnqueueNanos;
  private ScheduledFuture<?> tickFuture;

  BatchPipelineWrapper(
    final String topic,
    final MessagePipeline<T> pipeline,
    final BatchSink<T> sink,
    final BatchPolicy policy,
    final ScheduledExecutorService scheduler,
    final BatchCallbacks callbacks
  ) {
    this.topic = topic;
    this.pipeline = pipeline;
    this.sink = sink;
    this.policy = policy;
    this.scheduler = scheduler;
    this.callbacks = callbacks;
  }

  MessagePipeline<T> pipeline() {
    return pipeline;
  }

  /// Schedules the periodic age-trigger tick. The tick fires at half the configured `maxAge`
  /// to bound flush-latency overshoot to ~50% of the policy.
  void start() {
    final var tickMs = Math.max(50L, policy.maxAge().toMillis() / 2);
    tickFuture = scheduler.scheduleWithFixedDelay(this::tick, tickMs, tickMs, TimeUnit.MILLISECONDS);
  }

  /// Adds `(record, value)` to the buffer. If the new size meets the policy threshold, flushes
  /// inline. The caller must already have driven the pipeline (`pipeline.process(...)`) and only
  /// enqueue the `Passed` value; filtered records skip enqueueing entirely.
  ///
  /// `bufferedCount` is incremented before the lock is released so the in-flight backpressure
  /// strategy observes the new buffered record on its next check. The matching decrement happens
  /// in [#flushLocked] after the user sink returns.
  void enqueue(final ConsumerRecord<byte[], byte[]> record, final T value) {
    Runnable dispatch = null;
    lock.lock();
    try {
      if (buffer.isEmpty()) oldestEnqueueNanos = System.nanoTime();
      buffer.add(new Entry<>(record, value));
      bufferedCount.incrementAndGet();
      if (buffer.size() >= policy.maxSize()) dispatch = flushLocked();
    } finally {
      lock.unlock();
    }
    runDispatch(dispatch);
  }

  /// Returns the current count of records buffered in this wrapper across both completed and
  /// pending flushes. Used by [KPipeConsumer]'s in-flight backpressure strategy to include
  /// buffered records in the watermark check.
  long bufferedCount() {
    return bufferedCount.get();
  }

  private void tick() {
    try {
      Runnable dispatch = null;
      lock.lock();
      try {
        if (buffer.isEmpty()) return;
        final var ageNanos = System.nanoTime() - oldestEnqueueNanos;
        if (ageNanos >= policy.maxAge().toNanos()) dispatch = flushLocked();
      } finally {
        lock.unlock();
      }
      // Outside the lock, but inside this try: an Error escaping the dispatch has to reach the
      // log below just as one from the flush does, or the operator loses the only line that
      // says this topic stopped age-flushing.
      runDispatch(dispatch);
    } catch (final Throwable t) {
      // In practice only an Error reaches here. The RuntimeException paths through flushLocked are
      // closed: the sink call and both callback loops catch Exception, a null BatchResult is
      // handled explicitly, and BatchResult's canonical constructor rejects null collections and
      // null elements while still inside the user's sink call. "In practice" because System.Logger
      // is pluggable through the LoggerFinder SPI, so a throwing implementation would arrive here
      // from the log calls themselves — not defensible, and not worth defending against.
      //
      // The age comparison above is a subtler case. BatchPolicy does not bound maxAge, and
      // Duration.toNanos() overflows past roughly 292 years, so a large enough policy would throw
      // ArithmeticException on that line. What prevents it is that start() derives the tick period
      // from the same maxAge via toMillis(), which overflows three orders of magnitude later: any
      // value big enough to break toNanos() schedules the first tick centuries out, so this code
      // never runs. That coupling is load-bearing — a fixed tick period would open the path.
      //
      // The log is what an operator sees. The rethrow is a separate decision: a periodic task that
      // throws is cancelled and never rescheduled, so this permanently stops the age trigger for
      // this topic, and records then flush only on the size threshold and on shutdown drain. That
      // is the intended trade: the JVM has thrown an Error, and continuing to schedule flushes on
      // it is worse than letting a low-volume topic miss its age deadline.
      LOGGER.log(Level.ERROR, "Batch tick failed for topic {0}: {1}", topic, t.getMessage(), t);
      throw t;
    }
  }

  /// Caller must hold `lock`. Snapshots the buffer's values into a single pre-sized list, clears
  /// the buffer, drives the sink, and returns the per-record outcome dispatch for the caller to
  /// run **after releasing the lock** (or `null` when there was nothing to flush).
  ///
  /// The split is the point. `sink.apply` runs here, under the lock, because `BatchSink`'s
  /// contract promises implementers that flushes never overlap and so need not be thread-safe.
  /// The outcome dispatch carries no such promise and is the expensive half: `onBatchFailure`
  /// reaches a synchronous DLQ produce that waits for a broker ack, and a whole-batch failure
  /// performs one per record, serially. Holding the lock across that blocked every other worker
  /// enqueueing to this topic for the whole dispatch — measured at 221ms for a 20-record batch
  /// against a 10ms-per-record DLQ, and scaling with batch size times the produce timeout.
  ///
  /// `bufferedCount` still comes down only once the dispatch has run, so the gauge keeps counting
  /// records this wrapper still owns rather than dropping them the moment the sink returns.
  ///
  /// The buffer itself is reused — `ArrayList.clear()` keeps the backing array, so steady-state
  /// flushes don't reallocate the buffer. Only the per-flush snapshot list (used to walk records
  /// after `flush` has finished iterating values) is allocated fresh per cycle.
  private Runnable flushLocked() {
    final var size = buffer.size();
    if (size == 0) return null;
    final var snapshot = new ArrayList<>(buffer);
    final var values = new ArrayList<T>(size);
    for (final var entry : snapshot) values.add(entry.value());
    buffer.clear();

    final Runnable dispatch;
    try {
      dispatch = flush(snapshot, values);
    } catch (final Throwable t) {
      // `flush` catches every Exception the sink can raise, so reaching here means an Error. The
      // records have already left the buffer, so the gauge has to come down even though no dispatch
      // will run for them.
      bufferedCount.addAndGet(-size);
      throw t;
    }
    dispatchesInFlight.incrementAndGet();
    return () -> {
      try {
        dispatch.run();
      } finally {
        bufferedCount.addAndGet(-size);
        dispatchFinished();
      }
    };
  }

  /// Runs a dispatch returned by [#flushLocked]. Callers invoke this after releasing `lock`.
  private void runDispatch(final Runnable dispatch) {
    if (dispatch != null) dispatch.run();
  }

  private void dispatchFinished() {
    if (dispatchesInFlight.decrementAndGet() > 0) return;
    quiesceLock.lock();
    try {
      dispatchesQuiesced.signalAll();
    } finally {
      quiesceLock.unlock();
    }
  }

  /// Blocks until no dispatch is running. Only `close()` calls this, and only after running its own
  /// flush, so what it waits for is a dispatch started by the age tick — `tickFuture.cancel(false)`
  /// does not stop a tick already in progress, and the tick releases the flush lock before running
  /// its dispatch, so the lock alone no longer holds `close()` back the way it did when the
  /// dispatch ran inside it.
  ///
  /// The wait is unbounded, which is what it was before the dispatch moved out of the lock: back
  /// then `close()` blocked on `lock.lock()` for as long as the dispatch took. It is interruptible
  /// now, which that version was not.
  private void awaitDispatchesQuiesced() {
    quiesceLock.lock();
    try {
      while (dispatchesInFlight.get() > 0) dispatchesQuiesced.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      // Returning here leaves callbacks running while the consumer tears down around them, which
      // is worth a line in the log rather than a silent early return — the dispatcher logs the
      // same situation the same way.
      LOGGER.log(
        Level.WARNING,
        "Interrupted while draining batch outcome dispatches for topic {0}; {1} still in flight",
        topic,
        dispatchesInFlight.get()
      );
    } finally {
      quiesceLock.unlock();
    }
  }

  /// Calls the sink and classifies the [BatchResult], then returns the dispatch that will walk the
  /// snapshot exactly once. Logging any out-of-range indexes and building the synthetic failure for
  /// uncovered positions happen here, with the sink, because they are pure computation over the
  /// result — only the callbacks themselves are deferred. Sink-throw and null-result both fall back
  /// to whole-batch failure with a clear log line.
  private Runnable flush(final List<Entry<T>> snapshot, final List<T> values) {
    final var size = snapshot.size();

    final BatchResult result;
    try {
      result = sink.apply(values);
    } catch (final Exception e) {
      logBatchFailure("threw", size, e);
      return failAll(snapshot, e);
    }
    if (result == null) {
      final var cause = new IllegalStateException(
        "BatchSink returned null BatchResult for topic " + topic + " (" + size + " records)"
      );
      logBatchFailure("returned null", size, cause);
      return failAll(snapshot, cause);
    }

    // BitSet is constant-time `set` / `get` on primitive int indexes — no boxing on the hot path,
    // unlike `HashSet<Integer>`. Sized exactly to the batch so all sets land in word 0 for small
    // batches.
    final var succeeded = new BitSet(size);
    for (final var i : result.succeededIndexes()) {
      if (i != null && i >= 0 && i < size) succeeded.set(i);
      else logOutOfRange("succeeded", i, size);
    }
    final var failedByIndex = result.failedByIndex();
    for (final var i : failedByIndex.keySet()) {
      if (i == null || i < 0 || i >= size) logOutOfRange("failed", i, size);
    }

    IllegalStateException violation = null;
    // First pass: count missing indexes without allocating a list. Build the list only on the
    // contract-violation path (rare); the common success path stays allocation-free.
    var missingCount = 0;
    for (int i = 0; i < size; i++) {
      if (!succeeded.get(i) && !failedByIndex.containsKey(i)) missingCount++;
    }
    if (missingCount > 0) {
      final var missing = new ArrayList<Integer>(missingCount);
      for (int i = 0; i < size; i++) {
        if (!succeeded.get(i) && !failedByIndex.containsKey(i)) missing.add(i);
      }
      violation = new IllegalStateException(
        "BatchSink for topic " +
          topic +
          " did not account for indexes " +
          missing +
          " in a batch of " +
          size +
          " — treating as failures to avoid silent data loss"
      );
      LOGGER.log(Level.WARNING, violation.getMessage(), violation);
    }
    final var coverageViolation = violation;

    return () -> {
      for (int i = 0; i < size; i++) {
        final var record = snapshot.get(i).record();
        // Per-iteration guard: a throwing callback (a custom OffsetManager in markProcessed, or a
        // buggy hook) must not abort the loop and strand the REMAINING records with no outcome at
        // all — every record in the batch gets its callback attempt. The failing record's own
        // outcome is lost (logged at ERROR); its offset stays unmarked, so it is reprocessed rather
        // than dropped.
        try {
          if (succeeded.get(i)) {
            callbacks.markProcessed(record);
            continue;
          }
          final var perRecordCause = failedByIndex.get(i);
          callbacks.onBatchFailure(
            record,
            perRecordCause != null
              ? perRecordCause
              : (coverageViolation != null
                  ? coverageViolation
                  : new IllegalStateException("BatchSink contract violation at index " + i + " for topic " + topic))
          );
        } catch (final Exception callbackEx) {
          if (callbackEx instanceof InterruptedException) Thread.currentThread().interrupt();
          LOGGER.log(
            Level.ERROR,
            "Batch outcome callback threw for offset " +
              record.offset() +
              " on topic " +
              topic +
              "; continuing with the remaining records (offset stays unmarked, record will be reprocessed)",
            callbackEx
          );
        }
      }
    };
  }

  private Runnable failAll(final List<Entry<T>> snapshot, final Exception cause) {
    return () -> {
      for (final var entry : snapshot) {
        try {
          callbacks.onBatchFailure(entry.record(), cause);
        } catch (final Exception callbackEx) {
          if (callbackEx instanceof InterruptedException) Thread.currentThread().interrupt();
          LOGGER.log(
            Level.ERROR,
            "Batch failure callback threw for offset " +
              entry.record().offset() +
              " on topic " +
              topic +
              "; continuing with the remaining records",
            callbackEx
          );
        }
      }
    };
  }

  private void logOutOfRange(final String kind, final Integer index, final int batchSize) {
    LOGGER.log(
      Level.WARNING,
      "BatchSink returned out-of-range {0} index {1} for topic {2} (batchSize={3})",
      kind,
      index,
      topic,
      batchSize
    );
  }

  private void logBatchFailure(final String kind, final int size, final Throwable cause) {
    LOGGER.log(
      Level.WARNING,
      "Batch sink {0} for topic {1} ({2} records); falling back to whole-batch failure: {3}",
      kind,
      topic,
      size,
      cause.getMessage()
    );
  }

  /// Must not be called from inside an outcome callback. The drain below waits on a counter that
  /// only the calling dispatch can decrement, so a re-entrant close blocks itself — where the old
  /// lock-held dispatch would simply have re-entered the reentrant lock on the same thread.
  @Override
  public void close() {
    if (tickFuture != null) tickFuture.cancel(false);
    Runnable dispatch = null;
    lock.lock();
    try {
      dispatch = flushLocked();
    } finally {
      lock.unlock();
    }
    // Both lines matter for the drain. The first runs this flush's own dispatch; the second waits
    // out any dispatch the age tick started, which the flush lock no longer holds close() back for.
    runDispatch(dispatch);
    awaitDispatchesQuiesced();
  }

  record Entry<T>(ConsumerRecord<byte[], byte[]> record, T value) {}
}
