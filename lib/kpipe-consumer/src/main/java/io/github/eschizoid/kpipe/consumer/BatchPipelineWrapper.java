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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// Internal per-topic batch buffer. Owns a queue of `(record, value)` pairs and flushes either on
/// size or on age (whichever fires first), or on shutdown drain. The user [BatchSink] returns a
/// [BatchResult] naming per-record outcomes; succeeded records have their offsets marked
/// processed, failed records are routed through the caller-supplied [BatchCallbacks#onBatchFailure]
/// hook (DLQ + error handler). A failed record's offset is marked only once it is durably parked in
/// the DLQ (or no DLQ is configured); a failed DLQ send leaves it pending for reprocessing.
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

    /// Handle a record that was part of a failed batch: increment error metrics, route to the DLQ
    /// if one is configured, and invoke the error handler. The offset is marked processed only if
    /// the record reaches a durable terminal state — successfully parked in the DLQ, or no DLQ is
    /// configured (log-and-advance). A failed DLQ send leaves the offset pending so the record is
    /// reprocessed on restart rather than silently dropped.
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
  /// inline. The caller must already have driven the pipeline (`pipeline.process(...)`) and only
  /// enqueue the `Passed` value; filtered records skip enqueueing entirely.
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

  /// Caller must hold `lock`. Snapshots the buffer's values into a single pre-sized list, clears
  /// the buffer, then drives the sink + dispatches per-record outcomes off the same snapshot.
  /// `bufferedCount` is decremented in `finally` by the snapshot size so it tracks records still
  /// owned by the wrapper even if dispatch throws.
  ///
  /// The buffer itself is reused — `ArrayList.clear()` keeps the backing array, so steady-state
  /// flushes don't reallocate the buffer. Only the per-flush snapshot list (used to walk records
  /// after `flush` has finished iterating values) is allocated fresh per cycle.
  private void flushLocked() {
    final var size = buffer.size();
    if (size == 0) return;
    final var snapshot = new ArrayList<>(buffer);
    final var values = new ArrayList<T>(size);
    for (final var entry : snapshot) values.add(entry.value());
    buffer.clear();

    try {
      flush(snapshot, values);
    } finally {
      bufferedCount.addAndGet(-size);
    }
  }

  /// All the dispatch lives in one place: call the sink, classify the [BatchResult], log any
  /// out-of-range indexes, build a synthetic failure for uncovered positions so a misreported
  /// batch result can't silently mark records processed, then walk the snapshot exactly once.
  /// Sink-throw and null-result both fall back to whole-batch failure with a clear log line.
  private void flush(final List<Entry<K, T>> snapshot, final List<T> values) {
    final var size = snapshot.size();

    final BatchResult result;
    try {
      result = sink.apply(values);
    } catch (final Exception e) {
      logBatchFailure("threw", size, e);
      failAll(snapshot, e);
      return;
    }
    if (result == null) {
      final var cause = new IllegalStateException(
        "BatchSink returned null BatchResult for topic " + topic + " (" + size + " records)"
      );
      logBatchFailure("returned null", size, cause);
      failAll(snapshot, cause);
      return;
    }

    // BitSet is constant-time `set` / `get` on primitive int indexes — no boxing on the hot path,
    // unlike `HashSet<Integer>`. Sized exactly to the batch so all sets land in word 0 for small
    // batches.
    final var succeeded = new BitSet(size);
    for (final var i : result.succeededIndexes()) {
      if (i != null && i >= 0 && i < size) succeeded.set(i);
      else logOutOfRange("succeeded", i, size);
    }
    for (final var i : result.failedByIndex().keySet()) {
      if (i == null || i < 0 || i >= size) logOutOfRange("failed", i, size);
    }

    IllegalStateException coverageViolation = null;
    // First pass: count missing indexes without allocating a list. Build the list only on the
    // contract-violation path (rare); the common success path stays allocation-free.
    var missingCount = 0;
    for (int i = 0; i < size; i++) {
      if (!succeeded.get(i) && !result.failedByIndex().containsKey(i)) missingCount++;
    }
    if (missingCount > 0) {
      final var missing = new ArrayList<Integer>(missingCount);
      for (int i = 0; i < size; i++) {
        if (!succeeded.get(i) && !result.failedByIndex().containsKey(i)) missing.add(i);
      }
      coverageViolation = new IllegalStateException(
        "BatchSink for topic " +
          topic +
          " did not account for indexes " +
          missing +
          " in a batch of " +
          size +
          " — treating as failures to avoid silent data loss"
      );
      LOGGER.log(Level.WARNING, coverageViolation.getMessage(), coverageViolation);
    }

    for (int i = 0; i < size; i++) {
      final var record = snapshot.get(i).record();
      if (succeeded.get(i)) {
        callbacks.markProcessed(record);
        continue;
      }
      final var perRecordCause = result.failedByIndex().get(i);
      callbacks.onBatchFailure(
        record,
        perRecordCause != null
          ? perRecordCause
          : (coverageViolation != null
              ? coverageViolation
              : new IllegalStateException("BatchSink contract violation at index " + i + " for topic " + topic))
      );
    }
  }

  private void failAll(final List<Entry<K, T>> snapshot, final Exception cause) {
    for (final var entry : snapshot) callbacks.onBatchFailure(entry.record(), cause);
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
