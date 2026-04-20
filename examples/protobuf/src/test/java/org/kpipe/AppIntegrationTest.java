package org.kpipe;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
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
import org.kpipe.processor.ProtobufMessageProcessor;
import org.kpipe.registry.RegistryKey;
import org.kpipe.sink.MessageSink;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class AppIntegrationTest {

  private static final Logger log = System.getLogger(AppIntegrationTest.class.getName());
  private static final String CONFLUENT_PLATFORM_VERSION = System.getProperty("confluentPlatformVersion", "8.2.0");
  private static final String PROTOBUF_TOPIC = "protobuf-topic";

  @Container
  static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:%s".formatted(CONFLUENT_PLATFORM_VERSION))
  ).withStartupAttempts(3);

  @Test
  void testProtobufAppEndToEnd() throws Exception {
    final var config = new AppConfig(
      kafka.getBootstrapServers(),
      "test-group",
      PROTOBUF_TOPIC,
      "protobuf-app",
      Duration.ofMillis(100),
      Duration.ofSeconds(1),
      Duration.ofSeconds(5),
      List.of("addSource", "markProcessed")
    );

    final var capturingSink = new CapturingSink();

    try (final var app = new App(config)) {
      final var registry = app.getProcessorRegistry();

      // Register processors
      registry.register(
        RegistryKey.protobuf("addSource"),
        ProtobufMessageProcessor.addFieldOperator("name", "processed-by-kpipe")
      );
      registry.register(
        RegistryKey.protobuf("markProcessed"),
        ProtobufMessageProcessor.addFieldOperator("active", true)
      );

      // Register the capturing sink
      registry.sinkRegistry().register(RegistryKey.protobuf("protobufLogging"), capturingSink);

      // Start the app in a virtual thread
      final var appThread = Thread.ofVirtual().start(() -> {
        try {
          app.start();
          app.awaitShutdown();
        } catch (final Exception e) {
          log.log(Level.ERROR, "App error", e);
        }
      });

      // Build and produce a Protobuf message
      final var descriptor = App.buildCustomerDescriptor();
      final var message = DynamicMessage.newBuilder(descriptor)
        .setField(descriptor.findFieldByName("id"), 1L)
        .setField(descriptor.findFieldByName("name"), "Test User")
        .setField(descriptor.findFieldByName("email"), "test@example.com")
        .setField(descriptor.findFieldByName("active"), false)
        .setField(descriptor.findFieldByName("registration_date"), System.currentTimeMillis())
        .build();

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

      produceUntilConsumed(message.toByteArray(), producerProps, capturingSink, Duration.ofSeconds(10));

      // Verify the app is still running
      assertTrue(appThread.isAlive());

      // Validate the captured message
      final var received = capturingSink.getMessages();
      assertFalse(received.isEmpty(), "Should have received at least one message");

      final var processedMessage = received.getFirst();
      final var desc = processedMessage.getDescriptorForType();

      assertAll(
        "Verify processed Protobuf message fields",
        () -> assertEquals(1L, processedMessage.getField(desc.findFieldByName("id"))),
        // name was overwritten by the addSource processor
        () -> assertEquals("processed-by-kpipe", processedMessage.getField(desc.findFieldByName("name"))),
        () -> assertEquals("test@example.com", processedMessage.getField(desc.findFieldByName("email"))),
        // active was set to true by the markProcessed processor
        () -> assertEquals(true, processedMessage.getField(desc.findFieldByName("active")))
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
        producer.send(new ProducerRecord<>(PROTOBUF_TOPIC, payload)).get();
        if (sink.size() >= 1) return;
        TimeUnit.MILLISECONDS.sleep(250);
      }
    }
    throw new AssertionError("Timed out waiting for consumer to receive produced message(s)");
  }

  private static class CapturingSink implements MessageSink<Message> {

    private final List<Message> messages = new ArrayList<>();

    @Override
    public synchronized void accept(final Message processedValue) {
      messages.add(processedValue);
    }

    public synchronized List<Message> getMessages() {
      return new ArrayList<>(messages);
    }

    public synchronized int size() {
      return messages.size();
    }
  }
}
