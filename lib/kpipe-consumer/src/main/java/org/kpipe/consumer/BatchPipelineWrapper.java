package org.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.kpipe.registry.MessagePipeline;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.BatchSink;

/// Internal per-topic batch buffer. Owns a queue of `(record, value)` pairs and flushes either on
/// size or on age (whichever fires first), or on shutdown drain. Offsets are marked processed
/// only after the user's [BatchSink] returns successfully — failed batches go through the
/// caller-supplied [BatchCallbacks#onBatchFailure] hook (DLQ + error handler) and are still
/// marked processed so the consumer does not loop on a poison batch.
///
/// Thread-safety: a single [ReentrantLock] guards the buffer. `enqueue` is called from the
/// consumer thread (sequential mode is enforced upstream); `tick` runs on the shared scheduler;
/// `close` runs from the shutdown path. All three serialize through the same lock.
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
  private final BatchSink<T> batchSink;
  private final BatchPolicy policy;
  private final ScheduledExecutorService scheduler;
  private final BatchCallbacks<K> callbacks;

  private final ReentrantLock lock = new ReentrantLock();
  private final List<Entry<K, T>> buffer = new ArrayList<>();
  private long oldestEnqueueNanos;
  private ScheduledFuture<?> tickFuture;

  BatchPipelineWrapper(
    final String topic,
    final MessagePipeline<T> pipeline,
    final BatchSink<T> batchSink,
    final BatchPolicy policy,
    final ScheduledExecutorService scheduler,
    final BatchCallbacks<K> callbacks
  ) {
    this.topic = topic;
    this.pipeline = pipeline;
    this.batchSink = batchSink;
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
  void enqueue(final ConsumerRecord<K, byte[]> record, final T value) {
    lock.lock();
    try {
      if (buffer.isEmpty()) oldestEnqueueNanos = System.nanoTime();
      buffer.add(new Entry<>(record, value));
      if (buffer.size() >= policy.maxSize()) flushLocked();
    } finally {
      lock.unlock();
    }
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

  /// Caller must hold `lock`. Snapshots the buffer, clears it, invokes the user sink with the
  /// values, and marks each record's offset processed (success or failure — failed batches go
  /// to DLQ via [BatchCallbacks#onBatchFailure] but still commit so the consumer does not loop).
  private void flushLocked() {
    if (buffer.isEmpty()) return;
    final var snapshot = List.copyOf(buffer);
    buffer.clear();

    final var values = new ArrayList<T>(snapshot.size());
    for (final var entry : snapshot) values.add(entry.value());

    Exception failure = null;
    try {
      batchSink.accept(values);
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

  private record Entry<K, T>(ConsumerRecord<K, byte[]> record, T value) {}
}
