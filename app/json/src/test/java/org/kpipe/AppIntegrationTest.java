package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.kpipe.sink.MessageSink;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class AppIntegrationTest {

  private static final Logger log = System.getLogger(AppIntegrationTest.class.getName());

  @Container
  static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

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
      app.getSinkRegistry().register("capturingSink", capturingSink);

      // Start the app in a virtual thread
      final var appThread = Thread
        .ofVirtual()
        .start(() -> {
          try {
            app.start();
            app.awaitShutdown();
          } catch (Exception e) {
            log.log(Level.ERROR, "App error", e);
          }
        });

      // Produce a message
      final var producerProps = new Properties();
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
        final var message = "{\"id\":1,\"message\":\"Hello JSON\"}";
        producer.send(new ProducerRecord<>(topic, message.getBytes(StandardCharsets.UTF_8))).get();
      }

      // Wait a bit for processing
      TimeUnit.SECONDS.sleep(5);

      // Verify the app is still running
      assertTrue(appThread.isAlive());

      // Validate the captured message
      final var received = capturingSink.getMessages();
      assertEquals(1, received.size(), "Should have received exactly one message");

      final var processedBytes = received.get(0);
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

  private static class CapturingSink implements MessageSink<byte[], byte[]> {

    private final List<byte[]> messages = new ArrayList<>();

    @Override
    public synchronized void send(ConsumerRecord<byte[], byte[]> record, byte[] processedValue) {
      messages.add(processedValue);
    }

    public synchronized List<byte[]> getMessages() {
      return new ArrayList<>(messages);
    }
  }
}
