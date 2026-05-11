package org.kpipe.tracing.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.kpipe.producer.tracing.Tracer;

/// OpenTelemetry-backed [Tracer] — W3C `traceparent` propagation across the Kafka boundary.
///
/// Wire via the consumer builder:
///
/// ```java
/// final var otel = GlobalOpenTelemetry.get();
/// KPipeConsumer.<byte[]>builder()
///   .withProperties(props)
///   .withTracer(new OtelTracer(otel, "my-pipeline"))
///   .build();
/// ```
///
/// When OpenTelemetry is not configured, prefer [Tracer#noop()] from `kpipe-producer` to avoid
/// this module's classpath dependency entirely.
///
/// **Scope of v1:** uses `otel.getPropagators().getTextMapPropagator()` which defaults to W3C
/// trace context. B3, Datadog, and custom propagators are out of scope for v1 (configure your
/// SDK with `W3CTraceContextPropagator` if not already the default).
public final class OtelTracer implements Tracer {

  private static final Logger LOGGER = System.getLogger(OtelTracer.class.getName());

  /// Default instrumentation name used when constructing the tracer without an explicit name.
  public static final String DEFAULT_INSTRUMENTATION_NAME = "org.kpipe.consumer";

  private static final String SPAN_NAME = "kpipe.consumer.process";
  private static final AttributeKey<String> KAFKA_TOPIC_KEY = AttributeKey.stringKey("messaging.kafka.topic");
  private static final AttributeKey<Long> KAFKA_PARTITION_KEY = AttributeKey.longKey("messaging.kafka.partition");
  private static final AttributeKey<Long> KAFKA_OFFSET_KEY = AttributeKey.longKey("messaging.kafka.offset");
  private static final AttributeKey<String> MESSAGING_SYSTEM_KEY = AttributeKey.stringKey("messaging.system");
  private static final AttributeKey<String> MESSAGING_OPERATION_KEY = AttributeKey.stringKey("messaging.operation");

  private final io.opentelemetry.api.trace.Tracer otelTracer;
  private final TextMapPropagator propagator;

  /// Creates an [OtelTracer] using the supplied [OpenTelemetry] entry point and an explicit
  /// instrumentation name (typically a pipeline / app name).
  ///
  /// @param openTelemetry the OTel entry point (e.g. `GlobalOpenTelemetry.get()`)
  /// @param instrumentationName the tracer name (`otel.getTracer(name)`)
  public OtelTracer(final OpenTelemetry openTelemetry, final String instrumentationName) {
    Objects.requireNonNull(openTelemetry, "openTelemetry");
    Objects.requireNonNull(instrumentationName, "instrumentationName");
    this.otelTracer = openTelemetry.getTracer(instrumentationName);
    this.propagator = openTelemetry.getPropagators().getTextMapPropagator();
  }

  /// Creates an [OtelTracer] with the default instrumentation name.
  ///
  /// @param openTelemetry the OTel entry point
  public OtelTracer(final OpenTelemetry openTelemetry) {
    this(openTelemetry, DEFAULT_INSTRUMENTATION_NAME);
  }

  @Override
  public SpanScope startConsumerSpan(final ConsumerRecord<?, byte[]> record) {
    // Extract any upstream context from the record's headers (W3C `traceparent` + tracestate).
    final Context parent;
    try {
      parent = propagator.extract(Context.current(), record.headers(), HeadersTextMapGetter.INSTANCE);
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Failed to extract trace context from headers: {0}", e.getMessage());
      return SpanScope.noop();
    }

    final Span span;
    try {
      span = otelTracer
        .spanBuilder(SPAN_NAME)
        .setParent(parent)
        .setSpanKind(SpanKind.CONSUMER)
        .setAttribute(MESSAGING_SYSTEM_KEY, "kafka")
        .setAttribute(MESSAGING_OPERATION_KEY, "process")
        .setAttribute(KAFKA_TOPIC_KEY, record.topic())
        .setAttribute(KAFKA_PARTITION_KEY, (long) record.partition())
        .setAttribute(KAFKA_OFFSET_KEY, record.offset())
        .startSpan();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Failed to start span: {0}", e.getMessage());
      return SpanScope.noop();
    }

    final Scope scope;
    try {
      scope = span.makeCurrent();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Failed to make span current: {0}", e.getMessage());
      span.end();
      return SpanScope.noop();
    }

    return new OtelSpanScope(span, scope);
  }

  @Override
  public void injectContextInto(final Headers headers) {
    if (headers == null) return;
    try {
      propagator.inject(Context.current(), headers, HeadersTextMapSetter.INSTANCE);
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Failed to inject trace context into headers: {0}", e.getMessage());
    }
  }

  /// Span scope returned by [#startConsumerSpan]. Closing the scope detaches the context from the
  /// current thread AND ends the span.
  private static final class OtelSpanScope implements SpanScope {

    private final Span span;
    private final Scope scope;
    private boolean closed = false;

    OtelSpanScope(final Span span, final Scope scope) {
      this.span = span;
      this.scope = scope;
    }

    @Override
    public void recordException(final Throwable t) {
      if (closed || t == null) return;
      try {
        span.recordException(t);
        span.setStatus(StatusCode.ERROR, t.getClass().getSimpleName());
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Failed to record exception on span: {0}", e.getMessage());
      }
    }

    @Override
    public void close() {
      if (closed) return;
      closed = true;
      // Always detach the Context (scope) before ending the span; otherwise a subsequent
      // injection on the same carrier thread would read a stale span.
      try {
        scope.close();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Failed to close scope: {0}", e.getMessage());
      }
      try {
        span.end();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Failed to end span: {0}", e.getMessage());
      }
    }
  }
}
