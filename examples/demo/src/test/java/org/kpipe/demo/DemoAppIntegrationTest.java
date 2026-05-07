package org.kpipe.demo;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.format.avro.AvroFormat;
import org.kpipe.format.protobuf.ProtobufFormat;
import org.kpipe.producer.config.KafkaProducerConfig;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class DemoAppIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.2.0");

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION))
                   .asCompatibleSubstituteFor("apache/kafka")
  ).withStartupAttempts(3);

  @BeforeEach
  void registerSchemasFromTestResources() throws IOException {
    AvroFormat.INSTANCE.clearSchemas();
    AvroFormat.INSTANCE.addSchema("1", "com.kpipe.customer", loadAvroSchema());
    AvroFormat.INSTANCE.withDefaultSchema("1");

    ProtobufFormat.INSTANCE.addDescriptor("customer", DemoApp.buildCustomerDescriptor());
    ProtobufFormat.INSTANCE.withDefaultDescriptor("customer");
  }

  @Test
  void testJsonPipelineEndToEnd() throws Exception {
    final var config = new DemoConfig(
      kafka.getBootstrapServers(),
      "test-group",
      "http://localhost:8081", // unused — schemas pre-registered above
      "json-test-topic",
      "avro-test-topic",
      "proto-test-topic",
      Duration.ofMillis(100),
      Duration.ofSeconds(5),
      Duration.ofSeconds(60)
    );

    try (final var _ = new DemoApp(config)) {
      final var appThread = Thread.ofVirtual().start(() -> {});
      TimeUnit.SECONDS.sleep(3);

      final var producerProps = KafkaProducerConfig.createProducerConfig(kafka.getBootstrapServers());
      final var message = """
        {"id":1,"name":"Test User","email":"test@example.com"}""".getBytes(StandardCharsets.UTF_8);

      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
        final var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
          producer.send(new ProducerRecord<>("json-test-topic", message)).get();
          TimeUnit.MILLISECONDS.sleep(500);
        }
      }

      appThread.join(Duration.ofSeconds(10).toMillis());
      assertFalse(appThread.isAlive(), "App should exit after processing all messages");
    }
  }

  private static String loadAvroSchema() throws IOException {
    try (final InputStream in = DemoAppIntegrationTest.class.getResourceAsStream("/avro/customer.avsc")) {
      if (in == null) throw new IOException("avro/customer.avsc not found on test classpath");
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
