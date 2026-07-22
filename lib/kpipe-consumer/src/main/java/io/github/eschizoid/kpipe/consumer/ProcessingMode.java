package io.github.eschizoid.kpipe.consumer;

/// Selects how a [KPipeConsumer] dispatches records into its processing pipeline.
///
/// Three modes are supported:
///
/// - [#SEQUENTIAL] — records are processed one at a time on the consumer thread, in offset
///   order per partition. The pipeline runs synchronously inside `processRecords`; the next
///   poll does not happen until the current record finishes. Backpressure is measured by Kafka
///   lag (`Σ endOffset - position`) — the only metric that's meaningful when one record runs
///   at a time.
///
/// - [#PARALLEL] — every record is dispatched to its own virtual thread via
///   `Executors.newVirtualThreadPerTaskExecutor()`. Unbounded parallelism, no ordering
///   guarantees across or within keys. Backpressure is measured by total in-flight count.
///
/// - [#KEY_ORDERED] — records with the same key process serially on a per-key virtual thread;
///   records with different keys process in parallel. The dispatcher maintains a bounded map
///   of active keys with a configurable cap (default 10,000 — see
///   `Builder.withKeyOrderedMaxKeys`). Records with `null` keys all serialize through a single
///   sentinel queue. When the cap is reached and every active key still has pending work, the
///   consumer thread holds dispatch until a queue drains — this acts as implicit backpressure.
///   Backpressure is measured by total in-flight count (same as PARALLEL). Eviction reclaims
///   any queue that is both empty AND has no active worker — non-empty queues are never
///   evicted, which would break per-key ordering.
///
/// `PARALLEL` is the default. Older versions of KPipe used a `withSequentialProcessing(boolean)`
/// setter — removed in this release per the no-deprecation policy; callers migrate to
/// `withProcessingMode(...)` directly.
public enum ProcessingMode {
  /// One record at a time per partition, in offset order. Backpressure on Kafka lag.
  SEQUENTIAL,
  /// Virtual thread per record. No ordering. Backpressure on in-flight count.
  PARALLEL,
  /// Per-key virtual thread with a distinct-key cap. Same-key records serialize; different
  /// keys parallelize. Backpressure on in-flight count.
  KEY_ORDERED;

  /// Default cap on distinct in-flight keys for [#KEY_ORDERED]. Single source of truth
  /// shared by the dispatcher (`KeyOrderedDispatcher`), the fluent facade (`DefaultStream`),
  /// and `MultiBuilder` so the default can't drift across modules. Override per-consumer via
  /// `KPipeConsumerBuilder.withKeyOrderedMaxKeys(int)` /
  /// `Stream.withKeyOrderedMaxKeys(int)` / `MultiBuilder.withKeyOrderedMaxKeys(int)`.
  public static final int DEFAULT_KEY_ORDERED_MAX_KEYS = 10_000;
}
