package org.kpipe;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.kpipe.consumer.CircuitBreakerController;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.consumer.ProcessingMode;
import org.kpipe.metrics.ConsumerMetrics;
import org.kpipe.producer.tracing.Tracer;
import org.kpipe.registry.Result;
import org.kpipe.registry.SchemaResolver;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.BatchSink;
import org.kpipe.sink.MessageSink;

/// Full fluent stream-composition type for the [KPipe] single-topic entry points. Extends
/// [RouteStream] with consumer-wide settings — processing mode, key cap, backpressure, poll
/// timeout, metrics, tracer, circuit breaker — that only make sense when this is THE stream for
/// the whole consumer (not one of several routes inside a [MultiBuilder]).
///
/// A `Stream<T>` accumulates configuration against a logical Kafka topic + format and is
/// terminated by a `to*` method that returns a [Sink] ready to `start()`.
///
/// **Immutability contract:** every builder method returns a NEW `Stream<T>` carrying the updated
/// configuration. The original instance is never mutated. This makes branching from a common root
/// safe:
///
/// ```java
/// final var root = KPipe.json("events", props);
/// final var enriched = root.pipe(addTimestamp);   // root remains untouched
/// final var sanitized = root.pipe(removeFields("password")); // independent of `enriched`
/// ```
///
/// @param <T> the deserialized message type flowing through the pipeline
public interface Stream<T> extends RouteStream<T> {
  @Override
  Stream<T> pipe(final UnaryOperator<T> op);

  @Override
  Stream<T> filter(final Predicate<T> keep);

  @Override
  Stream<T> peek(final Consumer<T> sideEffect);

  @Override
  Stream<T> when(final Predicate<T> cond, final UnaryOperator<T> ifTrue, final UnaryOperator<T> ifFalse);

  @Override
  Stream<T> withRetry(final int maxRetries, final Duration backoff);

  /// Returns a new stream with backpressure enabled using default watermarks (pause at 10,000
  /// in-flight messages, resume at 7,000). Consumer-wide setting.
  Stream<T> withBackpressure();

  /// Returns a new stream with backpressure enabled using explicit watermarks. Consumer-wide
  /// setting; one consumer = one backpressure controller.
  Stream<T> withBackpressure(final long high, final long low);

  /// Returns a new stream with the processing mode set. Consumer-wide setting.
  Stream<T> withProcessingMode(final ProcessingMode mode);

  /// Sets the LRU cap on distinct keys held in-memory when using
  /// [ProcessingMode#KEY_ORDERED]. Consumer-wide setting.
  Stream<T> withKeyOrderedMaxKeys(final int maxKeys);

  /// Attaches a [ConsumerMetrics] implementation. Consumer-wide setting.
  Stream<T> withMetrics(final ConsumerMetrics metrics);

  /// Attaches a [Tracer]. Consumer-wide setting.
  Stream<T> withTracer(final Tracer tracer);

  /// Attaches a circuit breaker. Consumer-wide setting.
  Stream<T> withCircuitBreaker(final double failureThreshold, final int windowSize, final Duration openDuration);

  /// Attaches a pre-built [CircuitBreakerController]. Consumer-wide setting.
  Stream<T> withCircuitBreaker(final CircuitBreakerController controller);

  @Override
  Stream<T> onFiltered(final Runnable observer);

  @Override
  Stream<T> onFailed(final Consumer<Throwable> observer);

  @Override
  Stream<T> peekResult(final Consumer<Result<T>> observer);

  @Override
  Stream<T> withErrorHandler(final Consumer<KPipeConsumer.ProcessingError<byte[]>> handler);

  @Override
  Stream<T> withDeadLetterTopic(final String dlqTopic);

  /// Returns a new stream with a custom Kafka poll timeout. Consumer-wide setting.
  Stream<T> withPollTimeout(final Duration timeout);

  @Override
  Stream<T> skipBytes(final int n);

  @Override
  Stream<T> withSchemaRegistry(final SchemaResolver resolver);

  @Override
  Sink<T> toConsole();

  @Override
  Sink<T> toCustom(final MessageSink<T> sink);

  @Override
  Sink<T> toBatch(final BatchSink<T> sink, final BatchPolicy policy);

  /// Terminates the stream by fanning out to multiple sinks. Each delivered message is dispatched
  /// to every sink; an exception in one sink is **logged at ERROR but suppressed** so the
  /// remaining sinks still receive the message (see [org.kpipe.sink.CompositeMessageSink]).
  ///
  /// **Best-effort delivery, NOT at-least-once-per-sink.** Because exceptions are suppressed,
  /// a sink that consistently fails will silently drop messages while the consumer continues to
  /// mark them as processed and commit offsets.
  @Override
  @SuppressWarnings("unchecked")
  Sink<T> toMulti(final MessageSink<T>... sinks);
}
