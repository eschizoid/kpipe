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
import org.kpipe.sink.PartialBatchSink;

/// Internal per-topic batch buffer. Owns a queue of `(record, value)` pairs and flushes either on
/// size or on age (whichever fires first), or on shutdown drain. Offsets are marked processed
/// only after the user's [BatchSink] returns successfully — failed batches go through the
/// caller-supplied [BatchCallbacks#onBatchFailure] hook (DLQ + error handler) and are still
/// marked processed so the consumer does not loop on a poison batch.
///
/// **Two flavors.** A wrapper is constructed with either a [BatchSink] (the v1 whole-batch
/// flavor — the entire batch is treated as a single success-or-failure unit) or a
/// [PartialBatchSink] (returns a [BatchResult] naming per-record outcomes). The lock/buffer/tick
/// machinery is shared; only the flush leaf differs.
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

  /// Strategy interface that owns the actual sink call. The two implementations bind either to a
  /// [BatchSink] (whole-batch) or a [PartialBatchSink] (per-record). Kept private to the
  /// wrapper.
  private interface Flusher<K, T> {
    /// Invoked under the wrapper's lock with a snapshot of the buffered records and a parallel
    /// list of their deserialized values. Implementations dispatch to the user sink and call
    /// `callbacks.markProcessed` / `callbacks.onBatchFailure` for each record.
    void flush(List<Entry<K, T>> snapshot, List<T> values, BatchCallbacks<K> callbacks);
  }

  private final String topic;
  private final MessagePipeline<T> pipeline;
  private final BatchPolicy policy;
  private final ScheduledExecutorService scheduler;
  private final BatchCallbacks<K> callbacks;
  private final Flusher<K, T> flusher;

  private final ReentrantLock lock = new ReentrantLock();
  private final List<Entry<K, T>> buffer = new ArrayList<>();
  private final AtomicLong bufferedCount = new AtomicLong(0);
  private long oldestEnqueueNanos;
  private ScheduledFuture<?> tickFuture;

  /// Constructs a whole-batch wrapper backed by a [BatchSink]. Equivalent to v1 behavior — a
  /// thrown sink fails the entire batch, a returning sink succeeds the entire batch.
  BatchPipelineWrapper(
    final String topic,
    final MessagePipeline<T> pipeline,
    final BatchSink<T> batchSink,
    final BatchPolicy policy,
    final ScheduledExecutorService scheduler,
    final BatchCallbacks<K> callbacks
  ) {
    this(topic, pipeline, policy, scheduler, callbacks, wholeBatchFlusher(topic, batchSink));
  }

  /// Constructs a partial-batch wrapper backed by a [PartialBatchSink]. The sink reports
  /// per-record success/failure via a [BatchResult]; failed records go to the DLQ and succeeded
  /// records are marked processed. A thrown sink falls back to whole-batch failure semantics
  /// (every record routed to the DLQ with the thrown exception as the cause).
  BatchPipelineWrapper(
    final String topic,
    final MessagePipeline<T> pipeline,
    final PartialBatchSink<T> partialSink,
    final BatchPolicy policy,
    final ScheduledExecutorService scheduler,
    final BatchCallbacks<K> callbacks
  ) {
    this(topic, pipeline, policy, scheduler, callbacks, partialBatchFlusher(topic, partialSink));
  }

  private BatchPipelineWrapper(
    final String topic,
    final MessagePipeline<T> pipeline,
    final BatchPolicy policy,
    final ScheduledExecutorService scheduler,
    final BatchCallbacks<K> callbacks,
    final Flusher<K, T> flusher
  ) {
    this.topic = topic;
    this.pipeline = pipeline;
    this.policy = policy;
    this.scheduler = scheduler;
    this.callbacks = callbacks;
    this.flusher = flusher;
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
  /// buffered records in the watermark calculation when batch sinks run in parallel mode.
  ///
  /// @return the current buffered-record count (always &gt;= 0)
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

  /// Caller must hold `lock`. Snapshots the buffer, clears it, then delegates to the bound
  /// [Flusher] for the actual sink call + per-record offset bookkeeping. `bufferedCount` is
  /// decremented in `finally` by the snapshot size so it tracks records still owned by the
  /// wrapper even if the flusher throws — under parallel mode this is what keeps the in-flight
  /// metric stable across slow flushes (the records are no longer "in the buffer" but they have
  /// already been counted out of `inFlightCount` in the consumer).
  private void flushLocked() {
    if (buffer.isEmpty()) return;
    final var snapshot = List.copyOf(buffer);
    buffer.clear();

    final var values = new ArrayList<T>(snapshot.size());
    for (final var entry : snapshot) values.add(entry.value());

    try {
      flusher.flush(snapshot, values, callbacks);
    } finally {
      bufferedCount.addAndGet(-snapshot.size());
    }
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

  private static <K, T> Flusher<K, T> wholeBatchFlusher(final String topic, final BatchSink<T> sink) {
    return (snapshot, values, callbacks) -> {
      Exception failure = null;
      try {
        sink.accept(values);
      } catch (final Exception e) {
        failure = e;
        LOGGER.log(
          Level.WARNING,
          "Batch sink failed for topic {0} ({1} records): {2}",
          topic,
          snapshot.size(),
          e.getMessage()
        );
      }

      for (final var entry : snapshot) {
        if (failure == null) callbacks.markProcessed(entry.record());
        else callbacks.onBatchFailure(entry.record(), failure);
      }
    };
  }

  private static <K, T> Flusher<K, T> partialBatchFlusher(final String topic, final PartialBatchSink<T> sink) {
    return (snapshot, values, callbacks) -> {
      final var size = snapshot.size();
      BatchResult<T> result;
      try {
        result = sink.apply(values);
      } catch (final Exception e) {
        // Sink itself blew up — fall back to v1 whole-batch behavior so no record gets silently
        // dropped. Every record goes to the DLQ with the thrown exception as the cause.
        LOGGER.log(
          Level.WARNING,
          "Partial batch sink threw for topic {0} ({1} records); falling back to whole-batch failure: {2}",
          topic,
          size,
          e.getMessage()
        );
        for (final var entry : snapshot) callbacks.onBatchFailure(entry.record(), e);
        return;
      }

      if (result == null) {
        // Defensive — a `null` BatchResult is a contract violation; treat as whole-batch failure.
        final var cause = new IllegalStateException(
          "PartialBatchSink returned null BatchResult for topic " + topic + " (" + size + " records)"
        );
        LOGGER.log(Level.WARNING, cause.getMessage());
        for (final var entry : snapshot) callbacks.onBatchFailure(entry.record(), cause);
        return;
      }

      // Build a coverage set so we can detect indexes the sink did not account for. Iterating
      // through the snapshot once at the end keeps offset-marking order stable (succeeded
      // records first as they appear, then failures) — matters for `markOffsetProcessed`
      // observability when records arrive out of partition order.
      final var covered = new HashSet<Integer>(size * 2);
      final var succeededSet = new HashSet<Integer>(result.succeededIndexes().size() * 2);
      for (final var i : result.succeededIndexes()) {
        if (i == null || i < 0 || i >= size) {
          LOGGER.log(
            Level.WARNING,
            "PartialBatchSink returned out-of-range succeeded index {0} for topic {1} (batchSize={2})",
            i,
            topic,
            size
          );
          continue;
        }
        succeededSet.add(i);
        covered.add(i);
      }
      for (final var entry : result.failedByIndex().entrySet()) {
        final var i = entry.getKey();
        if (i == null || i < 0 || i >= size) {
          LOGGER.log(
            Level.WARNING,
            "PartialBatchSink returned out-of-range failed index {0} for topic {1} (batchSize={2})",
            i,
            topic,
            size
          );
          continue;
        }
        covered.add(i);
      }

      // Detect uncovered indexes BEFORE walking the snapshot, so we can reuse the same loop for
      // dispatch. Uncovered indexes are treated as failures — silently marking them processed
      // would mask sink bugs (the §12 "no silent failures" doctrine: a contract violation must
      // be observable).
      IllegalStateException coverageViolation = null;
      if (covered.size() != size) {
        final var missing = new ArrayList<Integer>();
        for (int i = 0; i < size; i++) if (!covered.contains(i)) missing.add(i);
        coverageViolation = new IllegalStateException(
          "PartialBatchSink for topic " +
          topic +
          " did not account for indexes " +
          missing +
          " in a batch of " +
          size +
          " — treating as failures to avoid silent data loss"
        );
        LOGGER.log(Level.WARNING, coverageViolation.getMessage());
      }

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
        // Index neither in succeeded nor in failed map — coverage violation. Use the synthetic
        // exception so operators can attribute failed records back to the contract bug.
        callbacks.onBatchFailure(
          record,
          coverageViolation != null
            ? coverageViolation
            : new IllegalStateException("PartialBatchSink contract violation at index " + i + " for topic " + topic)
        );
      }
    };
  }

  record Entry<K, T>(ConsumerRecord<K, byte[]> record, T value) {}
}
