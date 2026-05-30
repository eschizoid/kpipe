package org.kpipe;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.registry.Result;
import org.kpipe.registry.SchemaResolver;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.BatchSink;
import org.kpipe.sink.MessageSink;

/// Narrower fluent stream for per-route configurators inside [MultiBuilder]. Lacks consumer-wide
/// settings (`withProcessingMode`, `withKeyOrderedMaxKeys`, backpressure, poll timeout, metrics,
/// tracer, circuit breaker) because those live on the [MultiBuilder] itself — one [KPipeConsumer]
/// runs in exactly one mode with one set of those facilities, and the per-route lambda has no
/// business setting them. Trying to call them inside a route configurator is a compile error.
///
/// For the single-topic [KPipe] entry points, see [Stream] (which extends this interface and
/// adds the consumer-wide methods).
///
/// **Immutability contract:** every builder method returns a NEW instance carrying the updated
/// configuration. The original is never mutated.
///
/// @param <T> the deserialized message type flowing through the pipeline
public interface RouteStream<T> {
  /// Returns a new stream with `op` appended to the pipeline.
  ///
  /// @param op the operator to append (must not be null). Returning `null` from the operator
  ///     is treated as intentional filtering — downstream operators are skipped for that
  ///     message (see [org.kpipe.registry.MessagePipeline] error semantics).
  /// @return a new stream with `op` appended
  /// @throws NullPointerException if `op` is null
  RouteStream<T> pipe(final UnaryOperator<T> op);

  /// Returns a new stream with a filter operator appended. Messages for which `keep.test(msg)`
  /// returns false are dropped (the operator returns null, short-circuiting downstream).
  /// Equivalent to `pipe(Operators.filter(keep))`.
  ///
  /// @param keep predicate; messages for which it returns true are passed downstream
  /// @return a new stream with the filter appended
  /// @throws NullPointerException if `keep` is null
  RouteStream<T> filter(final Predicate<T> keep);

  /// Returns a new stream that runs `sideEffect` on each message and passes it through
  /// unchanged. Useful for logging, metrics, or other observation. Equivalent to
  /// `pipe(Operators.peek(sideEffect))`.
  ///
  /// @param sideEffect the side-effect to run on each message
  /// @return a new stream with the side-effect appended
  /// @throws NullPointerException if `sideEffect` is null
  RouteStream<T> peek(final Consumer<T> sideEffect);

  /// Returns a new stream with a conditional-branch operator appended. Each message is sent
  /// through `ifTrue` when `cond.test(msg)` is true, otherwise through `ifFalse`.
  ///
  /// @param cond the condition predicate
  /// @param ifTrue operator applied when `cond.test(msg)` is true
  /// @param ifFalse operator applied when `cond.test(msg)` is false
  /// @return a new stream with the branch appended
  /// @throws NullPointerException if any argument is null
  RouteStream<T> when(final Predicate<T> cond, final UnaryOperator<T> ifTrue, final UnaryOperator<T> ifFalse);

  /// Returns a new stream with retry behavior configured. After a processing exception, the
  /// pipeline retries up to `maxRetries` times, waiting `backoff` between attempts. When
  /// `maxRetries` is 0 (the default) the message goes straight to the error path / DLQ.
  ///
  /// @param maxRetries maximum number of retry attempts (must be ≥ 0)
  /// @param backoff duration to wait between attempts (must be non-null)
  /// @return a new stream with the retry policy configured
  /// @throws IllegalArgumentException if `maxRetries` is negative
  /// @throws NullPointerException if `backoff` is null
  RouteStream<T> withRetry(final int maxRetries, final Duration backoff);

  /// Returns a new stream that invokes `observer` whenever a record is intentionally filtered
  /// (an operator returned `null`). The observer is for visibility — it does **not** affect
  /// pipeline flow, suppress the filter, or reroute the record. Use [#withErrorHandler] or
  /// [#withDeadLetterTopic] for real failure routing; this is purely a hook for logging and
  /// metrics.
  ///
  /// Runs on the consumer thread immediately after the pipeline returns `Filtered`. If the
  /// observer throws, the exception is logged at WARNING and swallowed so observer bugs cannot
  /// crash the pipeline. Calling this method more than once replaces the previous observer —
  /// for fan-out, compose externally.
  ///
  /// @param observer a side-effect invoked when a record is filtered (must not be null)
  /// @return a new stream with the observer attached
  /// @throws NullPointerException if `observer` is null
  RouteStream<T> onFiltered(final Runnable observer);

  /// Returns a new stream that invokes `observer` with the captured cause whenever the pipeline
  /// returns `Failed`. The observer is for visibility — it does **not** suppress the failure or
  /// stop the failure from reaching retry / DLQ / `withErrorHandler` logic.
  ///
  /// Runs on the consumer thread immediately after the pipeline returns `Failed`. If the
  /// observer throws, the exception is logged at WARNING and swallowed. Calling this method
  /// more than once replaces the previous observer.
  ///
  /// **Use this for visibility into silently-failed records.** When `messagesProcessed` rises
  /// but the sink stays at 0, attach `onFailed(cause -> log.warn("pipeline failed", cause))`
  /// to surface what's actually being lost.
  ///
  /// @param observer a side-effect invoked with the failure cause (must not be null)
  /// @return a new stream with the observer attached
  /// @throws NullPointerException if `observer` is null
  RouteStream<T> onFailed(final Consumer<Throwable> observer);

  /// Returns a new stream that invokes `observer` with every [Result] the pipeline produces —
  /// `Passed`, `Filtered`, or `Failed`. The lowest-level observer hook; use this for full
  /// pattern-matching diagnostic taps. Does **not** affect pipeline flow.
  ///
  /// Runs on the consumer thread before any downstream sink invocation. If the observer throws,
  /// the exception is logged at WARNING and swallowed. Calling this method more than once
  /// replaces the previous observer.
  ///
  /// @param observer a side-effect invoked with every pipeline outcome (must not be null)
  /// @return a new stream with the observer attached
  /// @throws NullPointerException if `observer` is null
  RouteStream<T> peekResult(final Consumer<Result<T>> observer);

  /// Returns a new stream with a custom error handler. The handler is invoked after retries
  /// are exhausted (or immediately when `maxRetries == 0`) with the failing record, the
  /// exception, and the retry count. The default handler logs at WARNING.
  ///
  /// @param handler the error handler (must not be null)
  /// @return a new stream with the error handler attached
  /// @throws NullPointerException if `handler` is null
  RouteStream<T> withErrorHandler(final Consumer<KPipeConsumer.ProcessingError<byte[]>> handler);

  /// Returns a new stream with a dead-letter topic configured. Records that fail processing
  /// after retries are exhausted are forwarded to `dlqTopic` (using a `KPipeProducer` derived
  /// from the consumer properties) before the error handler is invoked.
  ///
  /// @param dlqTopic the dead-letter topic name (must be non-blank)
  /// @return a new stream with the DLQ configured
  /// @throws IllegalArgumentException if `dlqTopic` is null or blank
  RouteStream<T> withDeadLetterTopic(final String dlqTopic);

  /// Returns a new stream that skips the first `n` bytes of every payload before
  /// deserialization. Useful for stripping wire-format prefixes — e.g. Confluent Schema
  /// Registry's 5-byte envelope (1 magic + 4 schema id) for Avro, or 6-byte envelope (5 + 1
  /// message-index varint) for the single-top-level-message Protobuf case.
  ///
  /// Prefer [#withSchemaRegistry(SchemaResolver)] for Avro on Confluent SR — it consumes the
  /// envelope and resolves the correct writer schema per record, which is also what makes
  /// schema-evolution-correct decoding work. `skipBytes(5)` only strips the prefix; it does
  /// not fix the static-reader-schema correctness hole.
  ///
  /// @param n the number of leading bytes to skip (must be ≥ 0)
  /// @return a new stream with the skip configured
  /// @throws IllegalArgumentException if `n` is negative
  RouteStream<T> skipBytes(final int n);

  /// Switches the stream's format into Schema-Registry mode. Only supported when the
  /// originating format honours the [SchemaResolver] SPI — at the time of writing,
  /// `kpipe-format-avro`. Each record's Confluent wire envelope (1-byte magic + 4-byte schema
  /// ID) is consumed inside the format, the writer schema is resolved via `resolver` (cache it
  /// with `CachedSchemaResolver` from `kpipe-schema-registry-confluent`), and the remaining
  /// bytes are decoded against that schema. Do NOT combine with `skipBytes(5)` — the format
  /// already strips the envelope.
  ///
  /// **Protobuf is not yet supported** through this entry point. Protobuf SR auto-lookup
  /// requires runtime `.proto` text compilation (protoc-jar) which is out of scope for the
  /// current release; use `kpipe-format-protobuf` with `skipBytes(6)` and a single descriptor
  /// for now.
  ///
  /// @param resolver the schema resolver (typically a `CachedSchemaResolver` wrapping a
  ///     `ConfluentSchemaResolver`)
  /// @return a new stream with the registry-backed format wired in
  /// @throws NullPointerException if `resolver` is null
  /// @throws UnsupportedOperationException if the underlying format does not support
  ///     registry-backed mode
  RouteStream<T> withSchemaRegistry(final SchemaResolver resolver);

  /// Terminates the stream with a format-appropriate console sink and returns a [Sink] ready
  /// to start. For Avro streams this requires that a default schema has been registered; see
  /// [KPipe#avro] for details.
  ///
  /// @return a [Sink] ready to start
  /// @throws IllegalStateException if the stream's format requires schema/config that has not
  ///     been registered (e.g. Avro without a default schema)
  Sink<T> toConsole();

  /// Terminates the stream with a user-provided sink and returns a [Sink] ready to start.
  ///
  /// @param sink the terminal sink (must not be null)
  /// @return a [Sink] ready to start
  /// @throws NullPointerException if `sink` is null
  Sink<T> toCustom(final MessageSink<T> sink);

  /// Terminates the stream with a buffering [BatchSink], flushing in chunks of
  /// `policy.maxSize()` or whenever `policy.maxAge()` elapses since the oldest buffered
  /// record. The sink returns a [org.kpipe.sink.BatchResult] naming per-record outcomes:
  /// succeeded records have their offsets marked processed; failed records are routed to the
  /// configured DLQ. For void-style consumers that report whole-batch success or failure
  /// (e.g. a transactional commit), wrap with [BatchSink#ofVoid] — exceptions translate to
  /// `BatchResult.allFailed`.
  ///
  /// **Coverage contract.** The returned [org.kpipe.sink.BatchResult] must account for every
  /// position in the input batch. Indexes that are not named in either the success list or
  /// the failure map are treated as failures (with a synthetic [IllegalStateException] as the
  /// cause) — silently marking them processed would mask sink bugs and risk data loss. If the
  /// sink itself throws, every record is sent to the DLQ with the thrown exception
  /// (whole-batch fallback).
  ///
  /// Under the in-flight-backpressure modes (`PARALLEL` and `KEY_ORDERED`), buffered records
  /// count toward the in-flight metric so a slow sink cannot let the buffer grow unbounded.
  ///
  /// @param sink the batch sink invoked on each flush (must not be null)
  /// @param policy the size/age flush thresholds (must not be null)
  /// @return a [Sink] ready to start
  /// @throws NullPointerException if any argument is null
  Sink<T> toBatch(final BatchSink<T> sink, final BatchPolicy policy);

  /// Terminates the stream by fanning out to multiple sinks. Each delivered message is
  /// dispatched to every sink; an exception in one sink is **logged at ERROR but suppressed**
  /// so the remaining sinks still receive the message (see
  /// [org.kpipe.sink.CompositeMessageSink]).
  ///
  /// **Best-effort delivery, NOT at-least-once-per-sink.** Because exceptions are suppressed,
  /// a sink that consistently fails will silently drop messages while the consumer continues
  /// to mark them as processed and commit offsets. The consumer's error path / DLQ is not
  /// invoked for per-sink failures — only the per-sink ERROR log.
  ///
  /// If you need guaranteed delivery to every sink, do not use `toMulti`. Wire each sink as
  /// its own pipeline through the explicit [org.kpipe.registry.MessageProcessorRegistry] API
  /// and drive failure handling through the consumer's `errorHandler` / DLQ configuration.
  ///
  /// @param sinks the sinks to broadcast to (must contain at least one element)
  /// @return a [Sink] ready to start
  /// @throws IllegalArgumentException if `sinks` is empty
  /// @throws NullPointerException if `sinks` is null
  @SuppressWarnings("unchecked")
  Sink<T> toMulti(final MessageSink<T>... sinks);
}
