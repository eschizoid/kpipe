package org.kpipe.sink;

import java.util.List;
import java.util.function.Consumer;

/// Terminal sink that consumes batches of processed messages rather than one at a time. Useful for
/// destinations whose write-throughput dominates per-call overhead — JDBC bulk inserts, HTTP bulk
/// endpoints, S3 multi-part uploads, etc.
///
/// **Lifecycle and offset semantics.** When a stream terminates with a `BatchSink<T>`, the
/// consumer buffers `(ConsumerRecord, T)` pairs and flushes when the [BatchPolicy] threshold is
/// hit (size or age, whichever fires first) or when the consumer shuts down. Offsets are marked
/// processed only after [#accept(List)] returns successfully — a thrown exception causes the
/// whole batch to be sent to the DLQ if one is configured (or logged otherwise) and the offsets
/// are still marked processed so the consumer does not loop on a poison batch.
///
/// **Threading.** All flushes for a given batch pipeline run sequentially on a shared scheduled
/// executor. Implementations do not need to be thread-safe themselves but must not block
/// indefinitely — long flushes delay subsequent batches and stall offset commits.
///
/// **Implementations should:**
/// - Be idempotent if at-least-once delivery is unacceptable downstream.
/// - Throw on partial-batch failure to opt into the whole-batch DLQ path. v1 does not surface
///   per-record failures; if you need per-record DLQ, write a per-record [MessageSink] instead.
///
/// @param <T> the type of the processed object
@FunctionalInterface
public interface BatchSink<T> extends Consumer<List<T>> {}
