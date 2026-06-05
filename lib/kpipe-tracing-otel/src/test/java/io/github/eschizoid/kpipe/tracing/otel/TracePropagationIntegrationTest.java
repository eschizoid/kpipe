package io.github.eschizoid.kpipe.tracing.otel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.KPipe;
import io.github.eschizoid.kpipe.producer.sink.KafkaMessageSink;
import io.opentelemetry.api.trace.SpanContext;
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
import org.apache.kafka.clients.producer.Producer;
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

/// End-to-end test for W3C trace context propagation across the Kafka boundary.
///
/// Boots a real Kafka container, sends an inbound record with a known `traceparent` header,
/// runs it through a KPipe consumer that produces to a downstream topic via [KafkaMessageSink],
/// then reads the downstream record and asserts the propagated `traceparent` carries the SAME
/// trace id as the inbound — proving the consumer span chained off the producer's parent and
/// the outbound injection put the active context back onto the wire.
@Testcontainers(disabledWithoutDocker = true)
class TracePropagationIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.2.0");

  // Known parent: traceparent format is `version-traceId-parentSpanId-flags` (W3C §3.2).
  private static final String INBOUND_TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
  private static final String INBOUND_SPAN_ID = "b7ad6b7169203331";
  private static final String INBOUND_TRACEPARENT = "00-" + INBOUND_TRACE_ID + "-" + INBOUND_SPAN_ID + "-01";

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION)).asCompatibleSubstituteFor("apache/kafka")
  ).withStartupAttempts(3);

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
    throw new AssertionError(
      "Timed out waiting for %d spans, got %d".formatted(1, exporter.getFinishedSpanItems().size())
    );
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

  @Test
  void traceContextPropagatesFromInboundHeaderToOutboundSink() throws Exception {
    final var inputTopic = "trace-input-" + UUID.randomUUID().toString().substring(0, 8);
    final var outputTopic = "trace-output-" + UUID.randomUUID().toString().substring(0, 8);

    // Build an OTel SDK with W3C propagator + in-memory span sink.
    final var spanExporter = InMemorySpanExporter.create();
    final var sdk = OpenTelemetrySdk.builder()
      .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spanExporter)).build())
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .build();

    final var tracer = new OtelTracer(sdk, "trace-propagation-test");

    // The KPipe consumer produces to the output topic via KafkaMessageSink (with the same tracer
    // so it injects the active context into outbound headers).
    final Producer<byte[], byte[]> sinkProducer = new KafkaProducer<>(producerProps());
    final var sink = KafkaMessageSink.<byte[]>of(sinkProducer, outputTopic, v -> v, tracer);

    final var handle = KPipe.bytes(inputTopic, consumerProps("trace-prop-group-" + UUID.randomUUID()))
      .withTracer(tracer)
      .toCustom(sink)
      .start();

    try {
      // Send an inbound record with the known traceparent.
      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
        final var record = new ProducerRecord<byte[], byte[]>(
          inputTopic,
          null,
          null,
          "payload".getBytes(StandardCharsets.UTF_8),
          Collections.singletonList(
            new RecordHeader("traceparent", INBOUND_TRACEPARENT.getBytes(StandardCharsets.UTF_8))
          )
        );
        producer.send(record).get();
        producer.flush();
      }

      // Read the outbound record from the downstream topic.
      final var downstreamHeaders = pollFirstHeaders(outputTopic, Duration.ofSeconds(20));
      assertNotNull(downstreamHeaders.outboundTraceparent, "outbound record must carry a traceparent header");

      // The outbound traceparent must reuse the inbound trace id (proves propagation worked) but
      // have a DIFFERENT span id (proves we started a fresh span chained off the parent).
      final var parts = downstreamHeaders.outboundTraceparent.split("-");
      assertEquals(4, parts.length, "traceparent should have 4 dash-separated fields");
      assertEquals(INBOUND_TRACE_ID, parts[1], "outbound trace id must equal inbound trace id");
      assertNotEquals(INBOUND_SPAN_ID, parts[2], "outbound span id must NOT be the inbound parent id");

      // Verify the captured span has the inbound parent.
      final var spans = waitForSpans(spanExporter, Duration.ofSeconds(10));
      final var span = spans.getFirst();
      final SpanContext parentCtx = span.getParentSpanContext();
      assertTrue(parentCtx.isValid(), "captured span must have a valid parent context");
      assertEquals(INBOUND_TRACE_ID, parentCtx.getTraceId(), "span parent trace id must equal inbound trace id");
      assertEquals(INBOUND_SPAN_ID, parentCtx.getSpanId(), "span parent span id must equal inbound parent id");
      assertEquals(
        inputTopic,
        span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.kafka.topic"))
      );
    } finally {
      try {
        handle.shutdownGracefully(Duration.ofSeconds(5));
      } finally {
        sinkProducer.close(Duration.ofSeconds(5));
        sdk.close();
      }
    }
  }

  /// Polls the downstream topic until the first record is available, returns its `traceparent`
  /// header.
  private OutboundHeaders pollFirstHeaders(final String topic, final Duration timeout) {
    try (
      final var consumer = new KafkaConsumer<byte[], byte[]>(
        consumerProps("trace-prop-downstream-" + UUID.randomUUID())
      )
    ) {
      consumer.subscribe(Collections.singletonList(topic));
      final var deadline = System.nanoTime() + timeout.toNanos();
      while (System.nanoTime() < deadline) {
        final var records = consumer.poll(Duration.ofMillis(500));
        for (final var record : records) {
          final var header = record.headers().lastHeader("traceparent");
          return new OutboundHeaders(header == null ? null : new String(header.value(), StandardCharsets.UTF_8));
        }
      }
    }
    throw new AssertionError("Timed out waiting for downstream record on " + topic);
  }

  private record OutboundHeaders(String outboundTraceparent) {}
}
