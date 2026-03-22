package org.kpipe;

import static org.junit.jupiter.api.Assertions.*;

import com.dslplatform.json.DslJson;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.kpipe.config.AppConfig;
import org.kpipe.registry.MessageSinkRegistry;
import org.kpipe.sink.MessageSink;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class AppIntegrationTest {

  private static final Logger log = System.getLogger(AppIntegrationTest.class.getName());
  private static final String CONFLUENT_PLATFORM_VERSION = System.getProperty("confluentPlatformVersion", "8.2.0");

  @Container
  static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:%s".formatted(CONFLUENT_PLATFORM_VERSION))
  )
    .withStartupAttempts(3);

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
      // Register the capturing sink
      app.getSinkRegistry().register(MessageSinkRegistry.JSON_LOGGING, byte[].class, capturingSink);

      // Start the app in a virtual thread
      final var appThread = Thread
        .ofVirtual()
        .start(() -> {
          try {
            app.start();
            app.awaitShutdown();
          } catch (final Exception e) {
            log.log(Level.ERROR, "App error", e);
          }
        });

      // Produce a message
      final var producerProps = new Properties();
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      final var message = "{\"id\":1,\"message\":\"Hello JSON\"}".getBytes(StandardCharsets.UTF_8);
      produceUntilConsumed(message, producerProps, capturingSink, Duration.ofSeconds(10));

      // Verify the app is still running
      assertTrue(appThread.isAlive());

      // Validate the captured message
      final var received = capturingSink.getMessages();
      assertFalse(received.isEmpty(), "Should have received at least one message");

      final var processedBytes = received.getFirst();
      final var dslJson = new DslJson<Map<String, Object>>();
      final Map<String, Object> processedMap;
      try (final var input = new java.io.ByteArrayInputStream(processedBytes)) {
        processedMap = dslJson.deserialize(Map.class, input);
      }

      assertEquals(1.0, ((Number) processedMap.get("id")).doubleValue());
      assertEquals("Hello JSON", processedMap.get("message"));
      assertTrue(processedMap.containsKey("source"), "Should have 'source' field added by addSource");
      assertTrue(processedMap.containsKey("processed"), "Should have 'processed' field added by markProcessed");
      assertTrue(processedMap.containsKey("timestamp"), "Should have 'timestamp' field added by addTimestamp");
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

  private static class CapturingSink implements MessageSink<byte[], byte[]> {

    private final List<byte[]> messages = new ArrayList<>();

    @Override
    public synchronized void send(ConsumerRecord<byte[], byte[]> record, byte[] processedValue) {
      messages.add(processedValue);
    }

    public synchronized List<byte[]> getMessages() {
      return new ArrayList<>(messages);
    }

    public synchronized int size() {
      return messages.size();
    }
  }
}
