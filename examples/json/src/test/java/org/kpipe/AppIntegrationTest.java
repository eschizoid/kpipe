package org.kpipe;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.kpipe.consumer.config.AppConfig;
import org.kpipe.processor.JsonMessageProcessor;
import org.kpipe.registry.RegistryKey;
import org.kpipe.sink.MessageSink;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class AppIntegrationTest {

  private static final Logger log = System.getLogger(AppIntegrationTest.class.getName());
  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.2.0");

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION))
                   .asCompatibleSubstituteFor("apache/kafka")
  ).withStartupAttempts(3);

  @Test
  void testJsonAppEndToEnd() throws Exception {
    final var topic = "json-topic";
    final var config = new AppConfig(
      kafka.getBootstrapServers(),
      "test-group",
      topic,
      "json-app",
      Duration.ofMillis(100),
      Duration.ofSeconds(1),
      Duration.ofSeconds(5),
      List.of("addSource", "markProcessed", "addTimestamp")
    );

    final var capturingSink = new CapturingSink();

    try (final var app = new App(config)) {
      final var registry = app.getProcessorRegistry();
      registry.sinkRegistry().register(RegistryKey.json("jsonLogging"), capturingSink);

      // Set up the processor registry
      registry.register(RegistryKey.json("addSource"), JsonMessageProcessor.addFieldOperator("source", "test-app"));
      registry.register(
        RegistryKey.json("markProcessed"),
        JsonMessageProcessor.addFieldOperator("status", "processed")
      );
      registry.register(RegistryKey.json("addTimestamp"), JsonMessageProcessor.addTimestampOperator("processedAt"));

      // Start the app in a virtual thread
      final var appThread = Thread.ofVirtual().start(() -> {
        try {
          app.start();
          app.awaitShutdown();
        } catch (final Exception e) {
          log.log(Level.ERROR, "App error", e);
        }
      });

      // Produce a message
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
        {"id":1,"message":"Hello JSON"}""".getBytes(StandardCharsets.UTF_8);
      produceUntilConsumed(message, producerProps, capturingSink, Duration.ofSeconds(10));

      // Verify the app is still running
      assertTrue(appThread.isAlive());

      // Validate the captured message
      final var received = capturingSink.getMessages();
      assertFalse(received.isEmpty(), "Should have received at least one message");

      final var processedMap = received.getFirst();
      assertAll(
        "Verify processed message fields",
        () -> assertEquals(1.0, ((Number) processedMap.get("id")).doubleValue()),
        () -> assertEquals("Hello JSON", processedMap.get("message")),
        () -> assertEquals("test-app", processedMap.get("source")),
        () -> assertEquals("processed", processedMap.get("status")),
        () -> assertTrue(processedMap.containsKey("processedAt"))
      );
    }
  }

  private static void produceUntilConsumed(
    final byte[] payload,
    final Properties producerProps,
    final CapturingSink sink,
    final Duration timeout
  ) throws Exception {
    final var deadline = System.nanoTime() + timeout.toNanos();
    try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
      while (System.nanoTime() < deadline) {
        producer.send(new ProducerRecord<>("json-topic", payload)).get();
        if (sink.size() >= 1) return;
        TimeUnit.MILLISECONDS.sleep(250);
      }
    }
    throw new AssertionError("Timed out waiting for consumer to receive produced message(s)");
  }

  private static class CapturingSink implements MessageSink<Map<String, Object>> {

    private final List<Map<String, Object>> messages = new ArrayList<>();

    @Override
    public synchronized void accept(final Map<String, Object> processedValue) {
      messages.add(processedValue);
    }

    public synchronized List<Map<String, Object>> getMessages() {
      return new ArrayList<>(messages);
    }

    public synchronized int size() {
      return messages.size();
    }
  }
}
