package org.kpipe.demo;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class DemoAppIntegrationTest {

  private static final Logger log = System.getLogger(DemoAppIntegrationTest.class.getName());
  private static final String CONFLUENT_PLATFORM_VERSION = System.getProperty("confluentPlatformVersion", "8.2.0");

  @Container
  static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:%s".formatted(CONFLUENT_PLATFORM_VERSION))
  ).withStartupAttempts(3);

  @Test
  void testJsonPipelineEndToEnd() throws Exception {
    System.setProperty("kpipe.test.mode", "true");
    final var config = new DemoConfig(
      kafka.getBootstrapServers(),
      "test-group",
      "http://localhost:8081", // not used for JSON
      "json-test-topic",
      "avro-test-topic",
      "proto-test-topic",
      Duration.ofMillis(100),
      Duration.ofSeconds(5),
      Duration.ofSeconds(60)
    );

    // We test the JSON pipeline only (Avro/Protobuf require Schema Registry)
    try (final var app = new DemoApp(config)) {
      // Start in a virtual thread
      final var appThread = Thread.ofVirtual().start(() -> {
        try {
          app.start();
        } catch (final Exception e) {
          log.log(Level.ERROR, "App error", e);
        }
      });

      // Allow time for consumer to subscribe
      TimeUnit.SECONDS.sleep(3);

      // Produce a JSON message
      final var producerProps = new Properties();
      producerProps.putAll(
        Map.of(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
          kafka.getBootstrapServers(),
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          ByteArraySerializer.class.getName(),
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          ByteArraySerializer.class.getName()
        )
      );

      final var message = """
        {"id":1,"name":"Test User","email":"test@example.com"}""".getBytes(StandardCharsets.UTF_8);

      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
        final var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
          producer.send(new ProducerRecord<>("json-test-topic", message)).get();
          TimeUnit.MILLISECONDS.sleep(500);
        }
      }

      // Wait for the app to finish processing and exit cleanly
      appThread.join(Duration.ofSeconds(10).toMillis());
      assertFalse(appThread.isAlive(), "App should exit after processing all messages");
    }
  }
}
