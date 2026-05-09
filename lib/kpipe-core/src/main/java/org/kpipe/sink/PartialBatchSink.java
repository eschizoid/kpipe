package org.kpipe.sink;

import java.util.List;
import java.util.function.Function;

/// Terminal sink that consumes batches of processed messages and reports per-record success or
/// failure rather than treating the batch as an atomic unit. Counterpart to [BatchSink] for
/// downstream systems that partially succeed — JDBC bulk inserts where one row violates a
/// unique constraint while the other 99 commit, HTTP bulk endpoints that return per-element
/// status codes, S3 multi-object writes, etc.
///
/// **Lifecycle and offset semantics.** When a stream terminates with a `PartialBatchSink<T>`,
/// the consumer buffers `(ConsumerRecord, T)` pairs and flushes when the [BatchPolicy] threshold
/// is hit (size or age, whichever fires first) or when the consumer shuts down. The sink returns
/// a [BatchResult] that names the indexes that succeeded and the indexes that failed (with their
/// per-record exception). The consumer then:
///
/// - Marks each succeeded record's offset processed.
/// - Sends each failed record (with its per-record exception) to the configured DLQ and invokes
///   the consumer's error handler, then marks the offset processed so the consumer does not
///   loop on a poison batch.
/// - If the [BatchResult] does not cover every input position, the missing indexes are treated
///   as failures (using a synthetic [IllegalStateException] explaining the contract violation)
///   and routed to the DLQ. This protects against silent data loss when a sink's bookkeeping is
///   buggy.
/// - If the sink itself throws, the failure falls back to whole-batch behavior — every record
///   in the input batch is sent to the DLQ with the thrown exception as the cause.
///
/// **Threading.** All flushes for a given batch pipeline run sequentially on a shared scheduled
/// executor. Implementations do not need to be thread-safe themselves but must not block
/// indefinitely — long flushes delay subsequent batches and stall offset commits.
///
/// **Implementations should:**
/// - Be idempotent: a failed batch may be retried wholesale by an external orchestrator after
///   DLQ replay.
/// - Use the [BatchResult#allSucceeded] / [BatchResult#allFailed] factories when the downstream
///   system reports an atomic outcome (transactional commit success/abort) — they're equivalent
///   to using a [BatchSink] for that batch.
/// - Throw only for unrecoverable infrastructure errors (the connection-pool died, the auth
///   credentials expired). Returning a [BatchResult] with per-record failures is the preferred
///   path for "the call ran, here's what happened to each record."
///
/// @param <T> the type of the processed object
@FunctionalInterface
public interface PartialBatchSink<T> extends Function<List<T>, BatchResult<T>> {}
