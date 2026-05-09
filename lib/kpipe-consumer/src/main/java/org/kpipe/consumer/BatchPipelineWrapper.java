package org.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.kpipe.registry.MessagePipeline;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.BatchResult;
import org.kpipe.sink.BatchSink;

/// Internal per-topic batch buffer. Owns a queue of `(record, value)` pairs and flushes either on
/// size or on age (whichever fires first), or on shutdown drain. The user [BatchSink] returns a
/// [BatchResult] naming per-record outcomes; succeeded records have their offsets marked
/// processed, failed records are routed through the caller-supplied [BatchCallbacks#onBatchFailure]
/// hook (DLQ + error handler) and still marked processed so the consumer does not loop on a
/// poison batch.
///
/// **Thread-safety.** A single [ReentrantLock] guards the buffer. `enqueue` may be called from
/// many virtual-thread workers concurrently (parallel mode) or serialized on the consumer thread
/// (sequential mode); the lock makes both safe. `tick` runs on the shared scheduler; `close`
/// runs from the shutdown path. All paths serialize through the same lock so mutations to the
/// buffer, the `oldestEnqueueNanos` timestamp, and the `bufferedCount` are coherent.
///
/// **Backpressure participation.** The wrapper exposes a [#bufferedCount] gauge that the owning
/// consumer adds to its in-flight count when the in-flight backpressure strategy is active.
/// Without that addition, buffered records would be invisible to the watermark check in parallel
/// mode — a slow batch sink could let the buffer grow unbounded while the consumer kept polling.
/// `bufferedCount` increments on each [#enqueue] call and decrements after every flush by the
/// snapshot size.
///
/// @param <K> Kafka record key type
/// @param <T> deserialized value type
final class BatchPipelineWrapper<K, T> implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(BatchPipelineWrapper.class.getName());

  /// Hooks back into the owning consumer for offset bookkeeping and failure handling. Decouples
  /// the wrapper from `KPipeConsumer`'s package-private state for testability.
  ///
  /// @param <K> Kafka record key type
  interface BatchCallbacks<K> {
    /// Mark a record's offset as successfully processed (after a successful batch flush).
    void markProcessed(ConsumerRecord<K, byte[]> record);

    /// Handle a record that was part of a failed batch — DLQ + error handler + mark processed.
    void onBatchFailure(ConsumerRecord<K, byte[]> record, Exception cause);
  }

  private final String topic;
  private final MessagePipeline<T> pipeline;
  private final BatchSink<T> sink;
  private final BatchPolicy policy;
  private final ScheduledExecutorService scheduler;
  private final BatchCallbacks<K> callbacks;

  private final ReentrantLock lock = new ReentrantLock();
  private final List<Entry<K, T>> buffer = new ArrayList<>();
  private final AtomicLong bufferedCount = new AtomicLong(0);
  private long oldestEnqueueNanos;
  private ScheduledFuture<?> tickFuture;

  BatchPipelineWrapper(
    final String topic,
    final MessagePipeline<T> pipeline,
    final BatchSink<T> sink,
    final BatchPolicy policy,
    final ScheduledExecutorService scheduler,
    final BatchCallbacks<K> callbacks
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
  /// inline. The caller must already have invoked `pipeline.processToValue(...)` with `value`
  /// non-null (filtered records skip enqueueing entirely).
  ///
  /// `bufferedCount` is incremented before the lock is released so the in-flight backpressure
  /// strategy observes the new buffered record on its next check. The matching decrement happens
  /// in [#flushLocked] after the user sink returns.
  void enqueue(final ConsumerRecord<K, byte[]> record, final T value) {
    lock.lock();
    try {
      if (buffer.isEmpty()) oldestEnqueueNanos = System.nanoTime();
      buffer.add(new Entry<>(record, value));
      bufferedCount.incrementAndGet();
      if (buffer.size() >= policy.maxSize()) flushLocked();
    } finally {
      lock.unlock();
    }
  }

  /// Returns the current count of records buffered in this wrapper across both completed and
  /// pending flushes. Used by [KPipeConsumer]'s in-flight backpressure strategy to include
  /// buffered records in the watermark check.
  long bufferedCount() {
    return bufferedCount.get();
  }

  private void tick() {
    lock.lock();
    try {
      if (buffer.isEmpty()) return;
      final var ageNanos = System.nanoTime() - oldestEnqueueNanos;
      if (ageNanos >= policy.maxAge().toNanos()) flushLocked();
    } catch (final Throwable t) {
      // Scheduler swallows uncaught exceptions and silently cancels the task — log and rethrow
      // so an operator sees the failure if a flush ever throws non-Exception.
      LOGGER.log(Level.ERROR, "Batch tick failed for topic {0}: {1}", topic, t.getMessage(), t);
      throw t;
    } finally {
      lock.unlock();
    }
  }

  /// Caller must hold `lock`. Snapshots the buffer, clears it, invokes the user sink, then
  /// dispatches per-record outcomes. `bufferedCount` is decremented in `finally` by the snapshot
  /// size so it tracks records still owned by the wrapper even if dispatch throws.
  private void flushLocked() {
    if (buffer.isEmpty()) return;
    final var snapshot = List.copyOf(buffer);
    buffer.clear();

    final var values = new ArrayList<T>(snapshot.size());
    for (final var entry : snapshot) values.add(entry.value());

    try {
      flush(snapshot, values);
    } finally {
      bufferedCount.addAndGet(-snapshot.size());
    }
  }

  private void flush(final List<Entry<K, T>> snapshot, final List<T> values) {
    final var size = snapshot.size();
    final BatchResult result;
    try {
      result = sink.apply(values);
    } catch (final Exception e) {
      // Sink itself blew up — fall back to whole-batch behavior so no record gets silently
      // dropped. Every record goes to the DLQ with the thrown exception as the cause.
      logBatchFailure("threw", size, e);
      for (final var entry : snapshot) callbacks.onBatchFailure(entry.record(), e);
      return;
    }

    if (result == null) {
      final var cause = new IllegalStateException(
        "BatchSink returned null BatchResult for topic " + topic + " (" + size + " records)"
      );
      logBatchFailure("returned null", size, cause);
      for (final var entry : snapshot) callbacks.onBatchFailure(entry.record(), cause);
      return;
    }

    final var succeededSet = collectIndexes(result.succeededIndexes(), size, "succeeded");
    for (final var key : result.failedByIndex().keySet()) validateIndex(key, size, "failed");

    final var coverageViolation = checkCoverage(succeededSet, result.failedByIndex().keySet(), size);
    for (int i = 0; i < size; i++) {
      final var record = snapshot.get(i).record();
      if (succeededSet.contains(i)) {
        callbacks.markProcessed(record);
        continue;
      }
      final var perRecordCause = result.failedByIndex().get(i);
      if (perRecordCause != null) {
        callbacks.onBatchFailure(record, perRecordCause);
        continue;
      }
      // Uncovered index — synthetic failure so operators can attribute it back to a sink bug.
      callbacks.onBatchFailure(
        record,
        coverageViolation != null
          ? coverageViolation
          : new IllegalStateException("BatchSink contract violation at index " + i + " for topic " + topic)
      );
    }
  }

  private HashSet<Integer> collectIndexes(final List<Integer> indexes, final int batchSize, final String kind) {
    final var set = new HashSet<Integer>(indexes.size() * 2);
    for (final var i : indexes) if (validateIndex(i, batchSize, kind)) set.add(i);
    return set;
  }

  private boolean validateIndex(final Integer i, final int batchSize, final String kind) {
    if (i == null || i < 0 || i >= batchSize) {
      LOGGER.log(
        Level.WARNING,
        "BatchSink returned out-of-range {0} index {1} for topic {2} (batchSize={3})",
        kind,
        i,
        topic,
        batchSize
      );
      return false;
    }
    return true;
  }

  /// Builds the synthetic failure that uncovered indexes are attributed to. Returns `null` when
  /// coverage is complete (every position 0..size-1 is named in either set). The §12 "no silent
  /// failures" doctrine says a contract violation must be observable, so missing positions are
  /// flagged via a real exception rather than silently marked processed.
  private IllegalStateException checkCoverage(
    final HashSet<Integer> succeeded,
    final java.util.Set<Integer> failed,
    final int size
  ) {
    final var covered = new HashSet<Integer>(size * 2);
    covered.addAll(succeeded);
    for (final var i : failed) if (i != null && i >= 0 && i < size) covered.add(i);
    if (covered.size() == size) return null;
    final var missing = new ArrayList<Integer>();
    for (int i = 0; i < size; i++) if (!covered.contains(i)) missing.add(i);
    final var violation = new IllegalStateException(
      "BatchSink for topic " +
      topic +
      " did not account for indexes " +
      missing +
      " in a batch of " +
      size +
      " — treating as failures to avoid silent data loss"
    );
    LOGGER.log(Level.WARNING, violation.getMessage());
    return violation;
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

  @Override
  public void close() {
    if (tickFuture != null) tickFuture.cancel(false);
    lock.lock();
    try {
      flushLocked();
    } finally {
      lock.unlock();
    }
  }

  record Entry<K, T>(ConsumerRecord<K, byte[]> record, T value) {}
}
