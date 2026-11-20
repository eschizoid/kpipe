package io.github.eschizoid.kpipe.tracing.otel;

import io.github.eschizoid.kpipe.tracing.Tracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/// A [Tracer] on OpenTelemetry. Each record gets a `CONSUMER` span named `process <topic>`, a child
/// of the trace in its headers, with the OpenTelemetry messaging attributes:
/// `messaging.system` (`kafka`), `messaging.operation.type` and `messaging.operation.name`
/// (`process`), `messaging.destination.name`, `messaging.destination.partition.id`,
/// `messaging.kafka.offset` and, when the consumer has a group, `messaging.consumer.group.name`.
/// A failed attempt adds a `kpipe.attempt.failed` event; a record that fails for good records its
/// exception, sets `error.type` and marks the span as an error.
///
/// Context travels in the headers through the `OpenTelemetry` instance's text-map propagator, so
/// that instance needs one configured, such as W3C trace context. With none, nothing propagates,
/// and the constructor logs a warning.
///
/// ```java
/// KPipe.json("orders", props)
///   .withTracer(new OtelTracer(openTelemetry))
///   .toKafka("orders-enriched");
/// ```
public final class OtelTracer implements Tracer {

  private static final Logger LOGGER = System.getLogger(OtelTracer.class.getName());

  /// The instrumentation scope this tracer reports under.
  public static final String INSTRUMENTATION_SCOPE = "io.github.eschizoid.kpipe";

  private static final AttributeKey<String> SYSTEM = AttributeKey.stringKey("messaging.system");
  private static final AttributeKey<String> OPERATION_TYPE = AttributeKey.stringKey("messaging.operation.type");
  private static final AttributeKey<String> OPERATION_NAME = AttributeKey.stringKey("messaging.operation.name");
  private static final AttributeKey<String> DESTINATION = AttributeKey.stringKey("messaging.destination.name");
  private static final AttributeKey<String> PARTITION = AttributeKey.stringKey("messaging.destination.partition.id");
  private static final AttributeKey<Long> OFFSET = AttributeKey.longKey("messaging.kafka.offset");
  private static final AttributeKey<String> GROUP = AttributeKey.stringKey("messaging.consumer.group.name");
  private static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error.type");
  private static final AttributeKey<Long> ATTEMPT = AttributeKey.longKey("kpipe.attempt");
  private static final AttributeKey<String> EXCEPTION_TYPE = AttributeKey.stringKey("exception.type");
  private static final AttributeKey<String> EXCEPTION_MESSAGE = AttributeKey.stringKey("exception.message");

  private final io.opentelemetry.api.trace.Tracer tracer;
  private final TextMapPropagator propagator;

  /// @param openTelemetry where spans go and which propagator carries their context
  public OtelTracer(final OpenTelemetry openTelemetry) {
    Objects.requireNonNull(openTelemetry, "openTelemetry cannot be null");
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    this.propagator = openTelemetry.getPropagators().getTextMapPropagator();
    if (propagator == TextMapPropagator.noop()) LOGGER.log(
      Level.WARNING,
      "The OpenTelemetry instance has no text-map propagator, so no trace context reaches or leaves the records; " +
        "configure one, such as W3CTraceContextPropagator"
    );
  }

  @Override
  public SpanScope startConsumerSpan(final ConsumerRecord<?, byte[]> record, final String consumerGroup) {
    try {
      final var parent =
        record.headers() == null
          ? Context.root()
          : propagator.extract(Context.root(), record.headers(), HeadersGetter.INSTANCE);
      final var builder = tracer
        .spanBuilder("process " + record.topic())
        .setParent(parent)
        .setSpanKind(SpanKind.CONSUMER)
        .setAttribute(SYSTEM, "kafka")
        .setAttribute(OPERATION_TYPE, "process")
        .setAttribute(OPERATION_NAME, "process")
        .setAttribute(DESTINATION, record.topic())
        .setAttribute(PARTITION, Integer.toString(record.partition()))
        .setAttribute(OFFSET, record.offset());
      if (consumerGroup != null) builder.setAttribute(GROUP, consumerGroup);
      final var span = builder.startSpan();
      return new OtelSpanScope(span, parent.with(span), propagator);
    } catch (final RuntimeException e) {
      LOGGER.log(Level.WARNING, "Failed to start a span for the record", e);
      return SpanScope.noop();
    }
  }

  /// One record's span and the context it continues, so [#inject] writes this record's trace on any
  /// thread.
  private static final class OtelSpanScope implements SpanScope {

    private final Span span;
    private final Context context;
    private final TextMapPropagator propagator;
    private final AtomicBoolean closed = new AtomicBoolean();

    OtelSpanScope(final Span span, final Context context, final TextMapPropagator propagator) {
      this.span = span;
      this.context = context;
      this.propagator = propagator;
    }

    @Override
    public Activation activate() {
      try {
        return context.makeCurrent()::close;
      } catch (final RuntimeException e) {
        LOGGER.log(Level.WARNING, "Failed to make the span current", e);
        return Activation.NONE;
      }
    }

    @Override
    public void inject(final Headers headers) {
      if (headers == null) return;
      try {
        propagator.inject(context, headers, HeadersSetter.INSTANCE);
      } catch (final RuntimeException e) {
        LOGGER.log(Level.WARNING, "Failed to write the trace context into headers", e);
      }
    }

    @Override
    public void attemptFailed(final int attempt, final Throwable failure) {
      if (closed.get() || failure == null) return;
      try {
        final var attributes = Attributes.builder()
          .put(ATTEMPT, (long) attempt)
          .put(EXCEPTION_TYPE, failure.getClass().getName());
        if (failure.getMessage() != null) attributes.put(EXCEPTION_MESSAGE, failure.getMessage());
        span.addEvent("kpipe.attempt.failed", attributes.build());
      } catch (final RuntimeException e) {
        LOGGER.log(Level.WARNING, "Failed to record a failed attempt", e);
      }
    }

    @Override
    public void recordException(final Throwable failure) {
      if (closed.get() || failure == null) return;
      try {
        span.recordException(failure);
        span.setAttribute(ERROR_TYPE, failure.getClass().getName());
        span.setStatus(StatusCode.ERROR, failure.getClass().getSimpleName());
      } catch (final RuntimeException e) {
        LOGGER.log(Level.WARNING, "Failed to record an exception on the span", e);
      }
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true)) return;
      try {
        span.end();
      } catch (final RuntimeException e) {
        LOGGER.log(Level.WARNING, "Failed to end the span", e);
      }
    }
  }

  /// Reads UTF-8 header values for the propagator.
  private static final class HeadersGetter implements TextMapGetter<Headers> {

    static final HeadersGetter INSTANCE = new HeadersGetter();

    private HeadersGetter() {}

    @Override
    public Iterable<String> keys(final Headers carrier) {
      if (carrier == null) return List.of();
      final var keys = new ArrayList<String>();
      for (final Header h : carrier) keys.add(h.key());
      return keys;
    }

    @Override
    public String get(final Headers carrier, final String key) {
      if (carrier == null) return null;
      final var header = carrier.lastHeader(key);
      if (header == null || header.value() == null) return null;
      return new String(header.value(), StandardCharsets.UTF_8);
    }
  }

  /// Writes UTF-8 header values for the propagator, replacing a header of the same name, so a
  /// record that copies its source's headers carries one, current `traceparent`.
  private static final class HeadersSetter implements TextMapSetter<Headers> {

    static final HeadersSetter INSTANCE = new HeadersSetter();

    private HeadersSetter() {}

    @Override
    public void set(final Headers carrier, final String key, final String value) {
      if (carrier == null) return;
      carrier.remove(key);
      carrier.add(key, value.getBytes(StandardCharsets.UTF_8));
    }
  }
}
