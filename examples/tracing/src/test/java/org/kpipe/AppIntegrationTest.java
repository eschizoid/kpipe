package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.kpipe.sink.MessageSink;
import org.kpipe.tracing.otel.OtelTracer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class AppIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.2.0");

  // Known parent: traceparent format is `version-traceId-parentSpanId-flags` (W3C §3.2).
  private static final String INBOUND_TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
  private static final String INBOUND_SPAN_ID = "b7ad6b7169203331";
  private static final String INBOUND_TRACEPARENT = "00-" + INBOUND_TRACE_ID + "-" + INBOUND_SPAN_ID + "-01";

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION))
                   .asCompatibleSubstituteFor("apache/kafka")
  ).withStartupAttempts(3);

  @Test
  void consumerSpanChainsOffInboundTraceparent() throws Exception {
    final var topic = "trace-ex-" + UUID.randomUUID().toString().substring(0, 8);
    final var captured = new CopyOnWriteArrayList<Map<String, Object>>();

    // Wire an OTel SDK with W3C propagator + an InMemorySpanExporter we can assert against.
    final var spanExporter = InMemorySpanExporter.create();
    final var sdk = OpenTelemetrySdk.builder()
      .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spanExporter)).build())
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .build();

    final var tracer = new OtelTracer(sdk, "tracing-example-test");
    final MessageSink<Map<String, Object>> capturingSink = captured::add;

    try (
      final var handle = KPipe
        .json(topic, consumerProps())
        .withTracer(tracer)
        .toCustom(capturingSink)
        .start()
    ) {
      // Produce a record carrying a known traceparent.
      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
        final var payload = """
          {"id":1,"event":"hello"}""".getBytes(StandardCharsets.UTF_8);
        producer.send(
          new ProducerRecord<byte[], byte[]>(
            topic,
            null,
            null,
            payload,
            Collections.singletonList(new RecordHeader("traceparent", INBOUND_TRACEPARENT.getBytes(StandardCharsets.UTF_8)))
          )
        ).get();
      }

      // Wait for the record to be processed and the span exported.
      final var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      while (System.nanoTime() < deadline && (captured.isEmpty() || spanExporter.getFinishedSpanItems().isEmpty())) {
        TimeUnit.MILLISECONDS.sleep(100);
      }

      assertEquals(1, captured.size(), "consumer should have received the produced record");
      final var spans = spanExporter.getFinishedSpanItems();
      assertEquals(1, spans.size(), "exactly one consumer span should have been exported");

      final var span = spans.getFirst();
      final var parentCtx = span.getParentSpanContext();
      assertTrue(parentCtx.isValid(), "captured span must have a valid parent context");
      assertEquals(INBOUND_TRACE_ID, parentCtx.getTraceId(), "span parent trace id should equal inbound trace id");
      assertEquals(INBOUND_SPAN_ID, parentCtx.getSpanId(), "span parent span id should equal inbound parent id");
      assertEquals(topic, span.getAttributes().get(AttributeKey.stringKey("messaging.kafka.topic")));

      assertTrue(handle.shutdownGracefully(Duration.ofSeconds(5)));
    } finally {
      sdk.close();
    }
  }

  private static Properties consumerProps() {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "trace-ex-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
