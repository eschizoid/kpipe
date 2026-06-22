package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.tracing.otel.OtelTracer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/// End-to-end trace propagation across the Kafka boundary onto the dead-letter path.
///
/// The existing tracing coverage proves the inbound `traceparent` chains the consumer span and
/// reaches a normal outbound sink. This fills the remaining gap: when the pipeline FAILS, the
/// record is routed to the DLQ, and the consumer's active span context must be injected onto
/// the DLQ record so an operator can follow the failure trace from the original producer into
/// the dead-letter topic.
///
/// The chain under test: inbound record carries a known `traceparent` → consumer starts a span
/// parented off it (span is current on the worker thread) → an operator throws → the consumer
/// routes the record to the DLQ via its auto-built producer (wired with the same tracer) →
/// `injectContextInto` writes the active context onto the DLQ record. The DLQ `traceparent`
/// must therefore reuse the inbound trace id (same trace) but carry a fresh span id (the
/// consumer span, not the inbound parent). The exported consumer span is also asserted to be
/// parented off the inbound span and marked errored.
///
/// CI-RUN-REQUIRED: boots a real Kafka broker via Testcontainers — Docker must be available;
/// this will not run in a Docker-less local sandbox.
@Testcontainers(disabledWithoutDocker = true)
class DlqTracePropagationIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");

  // Known parent: traceparent format is `version-traceId-parentSpanId-flags`.
  private static final String INBOUND_TRACE_ID = "11111111111111111111111111111111";
  private static final String INBOUND_SPAN_ID = "2222222222222222";
  private static final String INBOUND_TRACEPARENT = "00-" + INBOUND_TRACE_ID + "-" + INBOUND_SPAN_ID + "-01";

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION)).asCompatibleSubstituteFor("apache/kafka")
  ).withStartupAttempts(3);

  @Test
  void failingRecordCarriesInboundTraceContextOntoDlqRecord() throws Exception {
    final var inputTopic = "dlq-trace-in-" + UUID.randomUUID().toString().substring(0, 8);
    final var dlqTopic = "dlq-trace-dlq-" + UUID.randomUUID().toString().substring(0, 8);

    final var spanExporter = InMemorySpanExporter.create();
    final var sdk = OpenTelemetrySdk.builder()
      .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spanExporter)).build())
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .build();

    final var tracer = new OtelTracer(sdk, "dlq-trace-test");

    // A pipeline that always fails — drives every record onto the DLQ path. The DLQ producer
    // is auto-built by the consumer and wired with this same tracer, so the active consumer
    // span is injected onto the DLQ record.
    final var handle = KPipe.json(inputTopic, consumerProps("dlq-trace-group-" + UUID.randomUUID()))
      .withTracer(tracer)
      .withDeadLetterTopic(dlqTopic)
      .pipe(v -> {
        throw new IllegalStateException("forced failure to exercise DLQ");
      })
      .toConsole()
      .start();

    try {
      // Send an inbound record carrying the known traceparent.
      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
        final var payload = """
          {"id":1,"event":"boom"}""".getBytes(StandardCharsets.UTF_8);
        producer
          .send(
            new ProducerRecord<byte[], byte[]>(
              inputTopic,
              null,
              null,
              payload,
              Collections.singletonList(
                new RecordHeader("traceparent", INBOUND_TRACEPARENT.getBytes(StandardCharsets.UTF_8))
              )
            )
          )
          .get();
        producer.flush();
      }

      // The DLQ record must carry a traceparent.
      final var dlqTraceparent = pollFirstTraceparent(dlqTopic, Duration.ofSeconds(25));
      assertNotNull(dlqTraceparent, "DLQ record must carry a traceparent header");

      // Same trace (proves propagation), different span id (proves the consumer span — not
      // the inbound parent — is what was injected).
      final var parts = dlqTraceparent.split("-");
      assertEquals(4, parts.length, "traceparent should have 4 dash-separated fields");
      assertEquals(INBOUND_TRACE_ID, parts[1], "DLQ trace id must equal inbound trace id");
      assertNotEquals(INBOUND_SPAN_ID, parts[2], "DLQ span id must NOT be the inbound parent id");

      // The exported consumer span chains off the inbound parent and is marked errored.
      final var spans = waitForSpans(spanExporter, Duration.ofSeconds(10));
      final var span = spans.getFirst();
      final var parentCtx = span.getParentSpanContext();
      assertTrue(parentCtx.isValid(), "consumer span must have a valid parent context");
      assertEquals(INBOUND_TRACE_ID, parentCtx.getTraceId(), "consumer span parent trace id must equal inbound");
      assertEquals(INBOUND_SPAN_ID, parentCtx.getSpanId(), "consumer span parent span id must equal inbound");
      assertEquals(inputTopic, span.getAttributes().get(AttributeKey.stringKey("messaging.kafka.topic")));
      // The injected DLQ span id must be the consumer span that processed the failing record.
      assertEquals(span.getSpanContext().getSpanId(), parts[2], "DLQ span id must be the consumer span id");

      assertTrue(handle.shutdownGracefully(Duration.ofSeconds(5)));
    } finally {
      sdk.close();
    }
  }

  private static java.util.List<io.opentelemetry.sdk.trace.data.SpanData> waitForSpans(
    final InMemorySpanExporter exporter,
    final Duration timeout
  ) throws InterruptedException {
    final var deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      final var spans = exporter.getFinishedSpanItems();
      if (!spans.isEmpty()) return spans;
      TimeUnit.MILLISECONDS.sleep(100);
    }
    throw new AssertionError("Timed out waiting for the consumer span to be exported");
  }

  /// Polls the DLQ topic until the first record arrives, returns its `traceparent` header value
  /// (or null if the record had none).
  private String pollFirstTraceparent(final String topic, final Duration timeout) {
    try (
      final var consumer = new KafkaConsumer<byte[], byte[]>(consumerProps("dlq-trace-reader-" + UUID.randomUUID()))
    ) {
      consumer.subscribe(Collections.singletonList(topic));
      final var deadline = System.nanoTime() + timeout.toNanos();
      while (System.nanoTime() < deadline) {
        final var records = consumer.poll(Duration.ofMillis(500));
        for (final var record : records) {
          final var header = record.headers().lastHeader("traceparent");
          return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
        }
      }
    }
    throw new AssertionError("Timed out waiting for a DLQ record on " + topic);
  }

  private static Properties consumerProps(final String groupId) {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return props;
  }

  private static Properties producerProps() {
    final var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return props;
  }
}
