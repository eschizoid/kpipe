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
/// **Immutability contract:** every builder method returns a NEW `Stream<T>` carrying the
/// updated configuration. The original instance is never mutated. This makes branching from a
/// common root safe:
///
/// ```java
/// final var root = KPipe.json("events", props);
/// final var enriched = root.pipe(addTimestamp);   // root remains untouched
/// final var sanitized = root.pipe(removeFields("password")); // independent of `enriched`
/// ```
///
/// Standard fluent usage chains the calls so the intermediate instances are not visible:
///
/// ```java
/// KPipe.json("events", kafkaProps)
///     .pipe(msg -> { msg.put("ts", System.currentTimeMillis()); return msg; })
///     .filter(msg -> "active".equals(msg.get("status")))
///     .toConsole()
///     .start();
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
  /// in-flight messages, resume at 7,000). The strategy is in-flight in parallel mode and
  /// consumer-lag in sequential mode.
  ///
  /// Backpressure is enabled by default; calling this method is only required when you want
  /// to override the watermarks via [#withBackpressure(long, long)].
  ///
  /// @return a new stream with backpressure configured
  Stream<T> withBackpressure();

  /// Returns a new stream with backpressure enabled using explicit watermarks. Hysteresis:
  /// the consumer pauses Kafka polling when the metric reaches `high` and resumes when it
  /// drops to or below `low`.
  ///
  /// @param high pause threshold (must be > 0)
  /// @param low resume threshold (must be ≥ 0 and strictly less than `high`)
  /// @return a new stream with the backpressure thresholds configured
  /// @throws IllegalArgumentException if the watermarks are invalid
  Stream<T> withBackpressure(final long high, final long low);

  /// Returns a new stream with the processing mode set. See [ProcessingMode] for semantics:
  ///
  /// - `SEQUENTIAL`: one message at a time per partition, in offset order. Lag-based
  ///   backpressure.
  /// - `PARALLEL` (default): virtual thread per record. No ordering guarantees. In-flight
  ///   backpressure.
  /// - `KEY_ORDERED`: per-key serial processing on a virtual thread per key, with an LRU cap
  ///   (default 10,000 keys; override with [#withKeyOrderedMaxKeys]). Records with the same
  ///   key process in order; different keys process in parallel. In-flight backpressure.
  ///
  /// @param mode the processing mode (must be non-null)
  /// @return a new stream with the processing mode configured
  /// @throws NullPointerException if `mode` is null
  Stream<T> withProcessingMode(final ProcessingMode mode);

  /// Sets the LRU cap on distinct keys held in-memory simultaneously when this stream uses
  /// [ProcessingMode#KEY_ORDERED]. Default 10,000. Has no effect for `SEQUENTIAL` or
  /// `PARALLEL` modes (a WARNING is logged at build time when set without `KEY_ORDERED`).
  ///
  /// @param maxKeys LRU cap (must be positive)
  /// @return a new stream with the key cap configured
  /// @throws IllegalArgumentException if `maxKeys` is non-positive
  Stream<T> withKeyOrderedMaxKeys(final int maxKeys);

  /// Returns a new stream with an OpenTelemetry-backed (or any custom) [ConsumerMetrics]
  /// implementation attached. Use `new OtelConsumerMetrics(otel, "consumer-name")` from
  /// `kpipe-metrics-otel` for the standard OTel wiring.
  ///
  /// @param metrics the metrics implementation (must not be null)
  /// @return a new stream with metrics attached
  /// @throws NullPointerException if `metrics` is null
  Stream<T> withMetrics(final ConsumerMetrics metrics);

  /// Returns a new stream with a [Tracer] attached for W3C trace-context propagation. The
  /// tracer starts a consumer span around each record (extracting upstream context from
  /// headers) and injects the active context into DLQ and producer-sink outbound records.
  ///
  /// Use `new OtelTracer(openTelemetry, "my-pipeline")` from `kpipe-tracing-otel` for
  /// OpenTelemetry wiring. Pass [Tracer#noop] to disable tracing explicitly (also the default
  /// when this method is not called).
  ///
  /// @param tracer the tracer implementation (must not be null; use `Tracer.noop()` to disable)
  /// @return a new stream with tracing attached
  /// @throws NullPointerException if `tracer` is null
  Stream<T> withTracer(final Tracer tracer);

  /// Returns a new stream with a circuit breaker attached. Watches per-record outcomes; once
  /// the failure rate over the sliding window crosses `failureThreshold`, pauses Kafka polling
  /// until `openDuration` elapses, then resumes and treats the next record as a probe (success
  /// returns to closed, failure trips again and restarts the timer).
  ///
  /// **Why CB and not just retries.** Retries handle transient per-record failures. CB
  /// handles sustained outages — DB down, downstream 503-storm — by stopping work entirely
  /// until the dependency recovers. Without it, every record during an outage exhausts
  /// `maxRetries` and floods the DLQ.
  ///
  /// @param failureThreshold the failure rate (0.0..1.0) at which to trip
  /// @param windowSize the rolling sample size — must be positive
  /// @param openDuration how long to stay in OPEN before probing
  /// @return a new stream with the breaker configured
  /// @throws IllegalArgumentException for invalid arguments
  Stream<T> withCircuitBreaker(final double failureThreshold, final int windowSize, final Duration openDuration);

  /// Returns a new stream with an explicit [CircuitBreakerController]. Use this form when you
  /// need to share a pre-built controller across consumers or when constructing it from
  /// configuration. For inline use prefer the three-argument overload.
  ///
  /// @param controller the controller (must not be null)
  /// @return a new stream with the breaker configured
  /// @throws NullPointerException if `controller` is null
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

  /// Returns a new stream with a custom Kafka poll timeout. The default is 100ms; longer
  /// timeouts reduce CPU usage on idle topics, shorter timeouts make backpressure / shutdown
  /// commands more responsive.
  ///
  /// @param timeout the poll timeout (must be non-null and non-negative)
  /// @return a new stream with the poll timeout configured
  /// @throws NullPointerException if `timeout` is null
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

  @Override
  @SuppressWarnings("unchecked")
  Sink<T> toMulti(final MessageSink<T>... sinks);
}
