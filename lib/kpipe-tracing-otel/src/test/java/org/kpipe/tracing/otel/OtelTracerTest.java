package org.kpipe.tracing.otel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Unit test for [OtelTracer] backed by the in-memory SDK exporter.
///
/// Core invariant: a record arriving with a W3C `traceparent` header is processed under a span
/// whose parent is the same trace ID as the inbound header. That's distributed-trace continuity
/// across the Kafka boundary.
class OtelTracerTest {

  // Known parent context — trace-id and parent span-id encoded into a `traceparent` header.
  private static final String PARENT_TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
  private static final String PARENT_SPAN_ID = "b7ad6b7169203331";
  private static final String TRACEPARENT_HEADER = "00-" + PARENT_TRACE_ID + "-" + PARENT_SPAN_ID + "-01";

  private InMemorySpanExporter spanExporter;
  private OpenTelemetrySdk sdk;
  private OtelTracer tracer;

  @BeforeEach
  void setUp() {
    spanExporter = InMemorySpanExporter.create();
    sdk = OpenTelemetrySdk.builder()
      .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spanExporter)).build())
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .build();
    tracer = new OtelTracer(sdk, "test-pipeline");
  }

  @AfterEach
  void tearDown() {
    if (sdk != null) sdk.close();
  }

  @Test
  void startConsumerSpanInheritsParentTraceFromTraceparentHeader() {
    final var headers = new RecordHeaders();
    headers.add("traceparent", TRACEPARENT_HEADER.getBytes(StandardCharsets.UTF_8));
    final var record = new ConsumerRecord<>("source-topic", 0, 42L, null, new byte[] { 1, 2, 3 });
    // Attach the headers post-construction (ConsumerRecord exposes a mutable Headers).
    for (final var h : headers) record.headers().add(h.key(), h.value());

    try (final var scope = tracer.startConsumerSpan(record)) {
      // Span is started + made current. End the scope to flush the span to the exporter.
      assertNotNull(scope);
    }

    final var spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size(), "Expected exactly one finished span");
    final var span = spans.getFirst();

    // CORE INVARIANT — propagation works: the started span lives inside the upstream trace.
    assertEquals(PARENT_TRACE_ID, span.getTraceId(), "Span must inherit the inbound trace-id");
    assertEquals(PARENT_SPAN_ID, span.getParentSpanId(), "Span's parent must be the inbound span-id");
    // The new span's own span-id is fresh (different from the parent's).
    assertNotEquals(PARENT_SPAN_ID, span.getSpanId());

    // Sanity checks on enrichment attributes — names follow OTel messaging semantic conventions.
    assertEquals(
      "source-topic",
      span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.kafka.topic"))
    );
    assertEquals(
      "kafka",
      span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.system"))
    );
  }

  @Test
  void startConsumerSpanWithoutParentHeaderCreatesNewTrace() {
    final var record = new ConsumerRecord<>("source-topic", 0, 99L, null, new byte[] { 9 });

    try (final var scope = tracer.startConsumerSpan(record)) {
      assertNotNull(scope);
    }

    final var spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());
    final var span = spans.getFirst();
    // No parent → fresh trace id (not the known constant).
    assertNotEquals(PARENT_TRACE_ID, span.getTraceId());
    assertTrue(span.getParentSpanContext() == null || !span.getParentSpanContext().isValid());
  }

  @Test
  void injectContextIntoWritesTraceparentDerivedFromActiveSpan() {
    final var inbound = new ConsumerRecord<>("source-topic", 0, 1L, null, new byte[] { 0 });
    inbound.headers().add("traceparent", TRACEPARENT_HEADER.getBytes(StandardCharsets.UTF_8));

    final var outbound = new RecordHeaders();
    try (final var scope = tracer.startConsumerSpan(inbound)) {
      tracer.injectContextInto(outbound);
    }

    final var injected = outbound.lastHeader("traceparent");
    assertNotNull(injected, "Outbound headers should contain `traceparent` after injection");
    final var injectedStr = new String(injected.value(), StandardCharsets.UTF_8);
    // The injected traceparent must carry the SAME trace-id (still the same distributed trace).
    assertTrue(
      injectedStr.contains(PARENT_TRACE_ID),
      "Injected traceparent (" + injectedStr + ") must carry the upstream trace-id " + PARENT_TRACE_ID
    );
  }

  @Test
  void recordExceptionMarksSpanStatusErrored() {
    final var record = new ConsumerRecord<>("source-topic", 0, 7L, null, new byte[] { 0 });
    final var boom = new RuntimeException("kaboom");

    try (final var scope = tracer.startConsumerSpan(record)) {
      scope.recordException(boom);
    }

    final var spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());
    assertEquals(StatusCode.ERROR, spans.getFirst().getStatus().getStatusCode());
  }

  @Test
  void noopTracerInjectionAndExtractionAreSilentNoops() {
    // Sanity — the noop default doesn't allocate, doesn't throw, and produces no span.
    final var noop = org.kpipe.producer.tracing.Tracer.noop();
    final var record = new ConsumerRecord<>("t", 0, 0L, (Object) null, new byte[0]);
    try (final var scope = noop.startConsumerSpan(record)) {
      noop.injectContextInto(new RecordHeaders());
      scope.recordException(new RuntimeException("ignored"));
    }
    assertEquals(0, spanExporter.getFinishedSpanItems().size());
  }
}
