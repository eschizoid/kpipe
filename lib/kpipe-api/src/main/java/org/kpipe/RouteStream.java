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
/// For the single-topic [KPipe] entry points, see [Stream] (which extends this interface and adds
/// the consumer-wide methods).
///
/// @param <T> the deserialized message type flowing through the pipeline
public interface RouteStream<T> {
  /// Returns a new stream with `op` appended to the pipeline.
  RouteStream<T> pipe(final UnaryOperator<T> op);

  /// Returns a new stream with a filter operator appended.
  RouteStream<T> filter(final Predicate<T> keep);

  /// Returns a new stream that runs `sideEffect` on each message and passes it through unchanged.
  RouteStream<T> peek(final Consumer<T> sideEffect);

  /// Returns a new stream with a conditional-branch operator appended.
  RouteStream<T> when(final Predicate<T> cond, final UnaryOperator<T> ifTrue, final UnaryOperator<T> ifFalse);

  /// Returns a new stream with retry behavior configured.
  RouteStream<T> withRetry(final int maxRetries, final Duration backoff);

  /// Returns a new stream that invokes `observer` whenever a record is intentionally filtered.
  RouteStream<T> onFiltered(final Runnable observer);

  /// Returns a new stream that invokes `observer` with the captured cause whenever the pipeline
  /// returns `Failed`.
  RouteStream<T> onFailed(final Consumer<Throwable> observer);

  /// Returns a new stream that invokes `observer` with every [Result] the pipeline produces.
  RouteStream<T> peekResult(final Consumer<Result<T>> observer);

  /// Returns a new stream with a custom error handler.
  RouteStream<T> withErrorHandler(final Consumer<KPipeConsumer.ProcessingError<byte[]>> handler);

  /// Returns a new stream with a dead-letter topic configured.
  RouteStream<T> withDeadLetterTopic(final String dlqTopic);

  /// Returns a new stream that skips the first `n` bytes of every payload before deserialization.
  RouteStream<T> skipBytes(final int n);

  /// Switches the stream's format into Schema-Registry mode.
  RouteStream<T> withSchemaRegistry(final SchemaResolver resolver);

  /// Terminates the stream with a format-appropriate console sink.
  Sink<T> toConsole();

  /// Terminates the stream with a user-provided sink.
  Sink<T> toCustom(final MessageSink<T> sink);

  /// Terminates the stream with a buffering [BatchSink], flushing in chunks of `policy.maxSize()`
  /// or whenever `policy.maxAge()` elapses since the oldest buffered record.
  Sink<T> toBatch(final BatchSink<T> sink, final BatchPolicy policy);

  /// Terminates the stream by fanning out to multiple sinks (best-effort delivery; see
  /// [Stream#toMulti] for the full caveat).
  @SuppressWarnings("unchecked")
  Sink<T> toMulti(final MessageSink<T>... sinks);
}
