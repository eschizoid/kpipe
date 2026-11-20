package io.github.eschizoid.kpipe.otel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.tracing.Tracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Unit test for [OtelTracer] backed by the in-memory SDK exporter.
///
/// A record arriving with a W3C `traceparent` header is processed under a span in the same trace,
/// and every record written on its behalf carries that span's context.
class OtelTracerTest {

  // Known parent context — trace-id and parent span-id encoded into a `traceparent` header.
  private static final String PARENT_TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
  private static final String PARENT_SPAN_ID = "b7ad6b7169203331";
  private static final String TRACEPARENT_HEADER = "00-" + PARENT_TRACE_ID + "-" + PARENT_SPAN_ID + "-01";

  private InMemorySpanExporter spanExporter;
  private OpenTelemetrySdk sdk;
  private Tracer tracer;

  @BeforeEach
  void setUp() {
    spanExporter = InMemorySpanExporter.create();
    sdk = OpenTelemetrySdk.builder()
      .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spanExporter)).build())
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .build();
    tracer = KPipeOtel.tracer(sdk);
  }

  @AfterEach
  void tearDown() {
    if (sdk != null) sdk.close();
  }

  @Test
  void aSpanContinuesTheTraceInTheRecordsHeadersWithMessagingAttributes() {
    final var record = new ConsumerRecord<>("source-topic", 3, 42L, null, new byte[] { 1, 2, 3 });
    record.headers().add("traceparent", TRACEPARENT_HEADER.getBytes(StandardCharsets.UTF_8));

    try (final var scope = tracer.startConsumerSpan(record, "billing")) {
      assertNotNull(scope);
    }

    final var spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());
    final var span = spans.getFirst();
    assertEquals(PARENT_TRACE_ID, span.getTraceId(), "the span continues the inbound trace");
    assertEquals(PARENT_SPAN_ID, span.getParentSpanId());
    assertEquals("process source-topic", span.getName());
    assertEquals(SpanKind.CONSUMER, span.getKind());
    final var attributes = span.getAttributes();
    assertEquals("kafka", attributes.get(AttributeKey.stringKey("messaging.system")));
    assertEquals("process", attributes.get(AttributeKey.stringKey("messaging.operation.type")));
    assertEquals("source-topic", attributes.get(AttributeKey.stringKey("messaging.destination.name")));
    assertEquals("3", attributes.get(AttributeKey.stringKey("messaging.destination.partition.id")));
    assertEquals(42L, attributes.get(AttributeKey.longKey("messaging.kafka.offset")));
    assertEquals("billing", attributes.get(AttributeKey.stringKey("messaging.consumer.group.name")));
  }

  @Test
  void aRecordWithoutATraceStartsANewOne() {
    final var record = new ConsumerRecord<>("source-topic", 0, 99L, null, new byte[] { 9 });

    try (final var scope = tracer.startConsumerSpan(record, null)) {
      assertNotNull(scope);
    }

    final var span = spanExporter.getFinishedSpanItems().getFirst();
    assertNotEquals(PARENT_TRACE_ID, span.getTraceId());
    assertFalse(span.getParentSpanContext().isValid());
    assertNull(span.getAttributes().get(AttributeKey.stringKey("messaging.consumer.group.name")));
  }

  /// The span writes its own context into headers on any thread, active there or not, which is
  /// what lets a batch flush write each record's trace.
  @Test
  void injectWritesTheSpansOwnContextFromAnotherThread() throws Exception {
    final var inbound = new ConsumerRecord<>("source-topic", 0, 1L, null, new byte[] { 0 });
    inbound.headers().add("traceparent", TRACEPARENT_HEADER.getBytes(StandardCharsets.UTF_8));
    final var outbound = new RecordHeaders();

    try (final var scope = tracer.startConsumerSpan(inbound, null)) {
      final var other = Thread.ofPlatform().start(() -> scope.inject(outbound));
      other.join();
    }

    final var span = spanExporter.getFinishedSpanItems().getFirst();
    final var injected = new String(outbound.lastHeader("traceparent").value(), StandardCharsets.UTF_8);
    assertEquals("00-" + PARENT_TRACE_ID + "-" + span.getSpanId() + "-01", injected);
  }

  @Test
  void activateMakesTheSpanCurrentUntilClosed() {
    final var record = new ConsumerRecord<>("source-topic", 0, 1L, null, new byte[] { 0 });
    try (final var scope = tracer.startConsumerSpan(record, null)) {
      try (var _ = scope.activate()) {
        assertTrue(Span.current().getSpanContext().isValid(), "the span is current while activated");
      }
      assertFalse(Span.current().getSpanContext().isValid(), "closing the activation restores the previous context");
    }
  }

  @Test
  void aFailedAttemptAddsAnEventAndAFinalFailureMarksTheSpan() {
    final var record = new ConsumerRecord<>("source-topic", 0, 7L, null, new byte[] { 0 });

    try (final var scope = tracer.startConsumerSpan(record, null)) {
      scope.attemptFailed(0, new IllegalStateException("first"));
      scope.recordException(new IllegalArgumentException("last"));
    }

    final var span = spanExporter.getFinishedSpanItems().getFirst();
    final var attempt = span
      .getEvents()
      .stream()
      .filter(e -> e.getName().equals("kpipe.attempt.failed"))
      .findFirst()
      .orElseThrow();
    assertEquals(0L, attempt.getAttributes().get(AttributeKey.longKey("kpipe.attempt")));
    assertEquals(
      IllegalStateException.class.getName(),
      attempt.getAttributes().get(AttributeKey.stringKey("exception.type"))
    );
    assertEquals(StatusCode.ERROR, span.getStatus().getStatusCode());
    assertEquals(
      IllegalArgumentException.class.getName(),
      span.getAttributes().get(AttributeKey.stringKey("error.type"))
    );
  }

  @Test
  void theNoopTracerRecordsNothing() {
    final var noop = Tracer.noop();
    final var headers = new RecordHeaders();
    try (final var scope = noop.startConsumerSpan(new ConsumerRecord<>("t", 0, 0L, null, new byte[0]), "g")) {
      scope.inject(headers);
      scope.recordException(new RuntimeException("ignored"));
    }
    assertEquals(0, spanExporter.getFinishedSpanItems().size());
    assertFalse(headers.iterator().hasNext());
    assertSame(
      Tracer.SpanScope.noop(),
      noop.startConsumerSpan(new ConsumerRecord<>("t", 0, 0L, null, new byte[0]), null)
    );
  }

  @Test
  void anOpenTelemetryWithoutAPropagatorIsReportedAtConstruction() {
    final var logger = Logger.getLogger(OtelTracer.class.getName());
    final var warnings = new ArrayList<String>();
    final var handler = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        if (record.getLevel() == Level.WARNING) warnings.add(record.getMessage());
      }

      @Override
      public void flush() {}

      @Override
      public void close() {}
    };
    logger.addHandler(handler);
    try {
      KPipeOtel.tracer(OpenTelemetry.noop());
      assertEquals(1, warnings.size());
      assertTrue(warnings.getFirst().contains("no text-map propagator"));

      KPipeOtel.tracer(sdk);
      assertEquals(1, warnings.size(), "a configured propagator is not reported");
    } finally {
      logger.removeHandler(handler);
    }
  }

  @Test
  void aClosedSpanEndsOnceAndIgnoresLaterFailures() {
    final var record = new ConsumerRecord<>("source-topic", 0, 7L, null, new byte[] { 0 });

    final var scope = tracer.startConsumerSpan(record, null);
    scope.attemptFailed(1, new IllegalStateException());
    scope.close();
    scope.close();
    scope.attemptFailed(2, new IllegalStateException("late"));
    scope.recordException(new IllegalStateException("late"));

    final var spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size(), "closing twice ends the span once");
    final var span = spans.getFirst();
    assertEquals(1, span.getEvents().size());
    assertNull(span.getEvents().getFirst().getAttributes().get(AttributeKey.stringKey("exception.message")));
    assertEquals(StatusCode.UNSET, span.getStatus().getStatusCode());
  }
}
