package io.github.eschizoid.kpipe.consumer;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// Per-mode dispatch strategy for [KPipeConsumer]. Each [ProcessingMode] maps to one
/// `Dispatcher` implementation; the choice is made in the [KPipeConsumer] constructor and
/// never changes for the life of a consumer.
///
/// The dispatcher owns:
///
/// - Whatever in-flight counter the mode uses (Sequential: 0 or 1 — incremented around the
///   inline `processTask.run()` so `inFlight` metrics + drain reporting are accurate;
///   Parallel: an atomic counter incremented at submit, decremented at completion; KeyOrdered:
///   an atomic count of records dispatched but not yet finished — both per-key queued records
///   and the currently executing one).
/// - Whatever executor / worker threads / per-key queues are needed.
/// - Lifecycle. The dispatcher's [#close()] runs **before** `offsetManager.close()` so that
///   buffered records get their offsets marked before the offset manager flushes its final
///   commit.
///
/// [KPipeConsumer] funnels every record through `dispatch(record, processTask, onComplete)`.
/// `processTask` calls `KPipeConsumer.processRecord(record)` (the per-record pipeline +
/// retry + DLQ logic). `onComplete` runs after `processTask`, regardless of throws, on
/// whichever thread executed the task — it's used to unpark the consumer thread when
/// backpressure is held.
///
/// @param <K> the Kafka record key type
sealed interface Dispatcher<K>
  extends AutoCloseable
  permits SequentialDispatcher, ParallelDispatcher, KeyOrderedDispatcher
{
  /// Dispatches a record. The dispatcher decides when and on which thread `processTask` runs;
  /// after `processTask` finishes (success or throw), the dispatcher runs `onComplete`.
  ///
  /// @param record       the Kafka record being dispatched (for diagnostics, key extraction)
  /// @param processTask  the per-record pipeline call (typically `() -> processRecord(record)`)
  /// @param onComplete   bookkeeping the consumer wants run after every record completes
  ///                     (typically an unpark of the consumer thread if backpressure-held)
  void dispatch(ConsumerRecord<K, byte[]> record, Runnable processTask, Runnable onComplete);

  /// Returns the number of records currently dispatched but not yet finished. Fed into
  /// `KPipeConsumer.totalInFlight()` so the in-flight backpressure strategy sees the
  /// dispatcher's buffered + active records.
  ///
  /// Sequential mode returns 0 or 1 (one inline record at a time). Lag-based backpressure
  /// doesn't consult this value, but it's still tracked so `inFlight` metrics and
  /// `shutdownGracefully(timeout)` drain reporting are accurate.
  long pendingCount();

  /// Snapshot of the top-`n` keys by current queue depth, ordered deepest-first. Intended for
  /// ad-hoc diagnostics (heap-dump replacement, REPL/JMX inspection) — not for continuous
  /// metric emission, since per-key cardinality is unbounded.
  ///
  /// Defaults to an empty list for dispatchers that have no per-key structure (Sequential,
  /// Parallel). Only [KeyOrderedDispatcher] returns meaningful data. The null-keyed queue
  /// appears with a `null` entry key. `byte[]` Kafka keys are returned as a defensive
  /// `byte[]` copy — the dispatcher's internal `ByteBuffer` wrapper (used for content-based
  /// identity) is an implementation detail and is not exposed in the snapshot.
  ///
  /// @param n maximum number of entries to return (must be positive)
  /// @return ordered list of `(key, queueDepth)` entries; never null, may be empty
  default List<Map.Entry<Object, Integer>> topKeyQueueDepths(final int n) {
    if (n <= 0) throw new IllegalArgumentException("n must be positive, got " + n);
    return List.of();
  }

  /// Non-blocking shutdown signal. Called by [KPipeConsumer#close] BEFORE
  /// `waitForInFlightDrain` and `thread.join` so the consumer thread can escape any in-flight
  /// stall (e.g. [KeyOrderedDispatcher]'s saturation yield-loop) and let drain + join proceed
  /// promptly. Implementations that have no such stall override default to a no-op.
  /// Idempotent — `close()` may also call this internally as a safety net.
  default void signalShutdown() {}

  /// Signals shutdown. Implementations should drain or terminate their work and release
  /// resources. Runs before `offsetManager.close()` so buffered records get their offsets
  /// marked before the offset manager flushes.
  @Override
  void close();
}
