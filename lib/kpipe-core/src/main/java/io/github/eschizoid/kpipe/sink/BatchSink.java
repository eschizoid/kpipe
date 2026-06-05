package io.github.eschizoid.kpipe.sink;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/// Terminal sink that consumes batches of processed messages and reports per-record success or
/// failure as a [BatchResult]. Useful for destinations whose write-throughput dominates per-call
/// overhead — JDBC bulk inserts, HTTP bulk endpoints, S3 multi-part uploads — where the call
/// "succeeded" or "failed" can be more nuanced than a single boolean (one row violated a
/// constraint while the other 99 committed; an HTTP bulk endpoint returned per-element status
/// codes).
///
/// **Lifecycle and offset semantics.** When a stream terminates with a `BatchSink<T>`, the
/// consumer buffers `(ConsumerRecord, T)` pairs and flushes when the [BatchPolicy] threshold is
/// hit (size or age, whichever fires first) or when the consumer shuts down. The sink returns
/// a [BatchResult] naming the indexes that succeeded and the indexes that failed (with their
/// per-record exceptions). The consumer then:
///
/// - Marks each succeeded record's offset processed.
/// - Sends each failed record (with its per-record exception) to the configured DLQ and invokes
///   the consumer's error handler, then marks the offset processed so the consumer does not
///   loop on a poison batch.
/// - If the [BatchResult] does not cover every input position, the missing indexes are treated
///   as failures (using a synthetic [IllegalStateException] explaining the contract violation)
///   and routed to the DLQ. This protects against silent data loss when sink bookkeeping is
///   buggy.
/// - If the sink itself throws, the failure falls back to whole-batch behavior — every record
///   is sent to the DLQ with the thrown exception as the cause.
///
/// **Threading.** All flushes for a given batch pipeline run sequentially on a shared scheduled
/// executor. Implementations do not need to be thread-safe themselves but must not block
/// indefinitely — long flushes delay subsequent batches and stall offset commits.
///
/// **Implementations should:**
/// - Be idempotent: a failed batch may be retried wholesale by an external orchestrator after
///   DLQ replay.
/// - Use the [BatchResult#allSucceeded] / [BatchResult#allFailed] factories when the downstream
///   system reports an atomic outcome (transactional commit success/abort).
/// - Throw only for unrecoverable infrastructure errors (the connection-pool died, the auth
///   credentials expired). Returning a [BatchResult] with per-record failures is the preferred
///   path for "the call ran, here's what happened to each record."
///
/// **Void-style helper.** Wrap a `Consumer<List<T>>` that throws on failure via [#ofVoid] —
/// success becomes `BatchResult.allSucceeded(size)`, exception becomes `BatchResult.allFailed`.
/// Useful when the downstream system has no per-record outcome (a fire-and-forget call that
/// either succeeds or rolls back the whole batch).
///
/// @param <T> the type of the processed object
@FunctionalInterface
public interface BatchSink<T> extends Function<List<T>, BatchResult> {
  /// Adapts a void-returning [Consumer] into a [BatchSink] that returns
  /// `BatchResult.allSucceeded(size)` on normal completion and `BatchResult.allFailed(size, e)`
  /// when the consumer throws. Use this when the downstream system reports atomic
  /// success/failure for the whole batch (a transactional commit, a fire-and-forget HTTP POST).
  ///
  /// @param consumer the void-returning batch consumer (must not be null)
  /// @param <T> the type of the processed object
  /// @return a [BatchSink] that translates the consumer's exceptions into whole-batch failures
  /// @throws NullPointerException if `consumer` is null
  static <T> BatchSink<T> ofVoid(final Consumer<List<T>> consumer) {
    Objects.requireNonNull(consumer, "consumer cannot be null");
    return batch -> {
      try {
        consumer.accept(batch);
        return BatchResult.allSucceeded(batch.size());
      } catch (final Exception e) {
        return BatchResult.allFailed(batch.size(), e);
      }
    };
  }
}
