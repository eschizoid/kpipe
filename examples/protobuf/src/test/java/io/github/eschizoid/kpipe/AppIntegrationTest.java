package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.producer.config.KafkaProducerConfig;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class AppIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION)).asCompatibleSubstituteFor("apache/kafka")
  ).withStartupAttempts(3);

  private ProtobufFormat format;

  @BeforeEach
  void buildFormat() {
    format = new ProtobufFormat(App.buildCustomerDescriptor());
  }

  @Test
  void testProtobufAppEndToEnd() throws Exception {
    final var topic = "protobuf-topic-" + UUID.randomUUID().toString().substring(0, 8);
    final var captured = new CopyOnWriteArrayList<Message>();
    final MessageSink<Message> capturingSink = captured::add;

    final var descriptor = format.descriptor();

    try (
      final var handle = KPipe.protobuf(topic, consumerProps(), format)
        .pipe(msg ->
          msg.toBuilder().setField(msg.getDescriptorForType().findFieldByName("name"), "processed-by-kpipe").build()
        )
        .pipe(msg -> msg.toBuilder().setField(msg.getDescriptorForType().findFieldByName("active"), true).build())
        .toCustom(capturingSink)
        .start()
    ) {
      assertTrue(handle.isHealthy(), "Handle should be healthy after start()");

      final var payload = DynamicMessage.newBuilder(descriptor)
        .setField(descriptor.findFieldByName("id"), 1L)
        .setField(descriptor.findFieldByName("name"), "Test User")
        .setField(descriptor.findFieldByName("email"), "test@example.com")
        .setField(descriptor.findFieldByName("active"), false)
        .setField(descriptor.findFieldByName("registration_date"), System.currentTimeMillis())
        .build()
        .toByteArray();

      produceUntilConsumed(topic, payload, captured, Duration.ofSeconds(15));

      final var processed = captured.getFirst();
      final var desc = processed.getDescriptorForType();
      assertAll(
        () -> assertEquals(1L, processed.getField(desc.findFieldByName("id"))),
        () -> assertEquals("processed-by-kpipe", processed.getField(desc.findFieldByName("name"))),
        () -> assertEquals("test@example.com", processed.getField(desc.findFieldByName("email"))),
        () -> assertEquals(true, processed.getField(desc.findFieldByName("active")))
      );

      assertTrue(handle.shutdownGracefully(Duration.ofSeconds(5)));
      assertFalse(handle.isHealthy());
    }
  }

  private static Properties consumerProps() {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }

  private static void produceUntilConsumed(
    final String topic,
    final byte[] payload,
    final List<?> sink,
    final Duration timeout
  ) throws Exception {
    final var producerProps = KafkaProducerConfig.createProducerConfig(kafka.getBootstrapServers());
    final var deadline = System.nanoTime() + timeout.toNanos();
    try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
      while (System.nanoTime() < deadline) {
        producer.send(new ProducerRecord<>(topic, payload)).get();
        if (!sink.isEmpty()) return;
        TimeUnit.MILLISECONDS.sleep(250);
      }
    }
    throw new AssertionError("Timed out waiting for consumer to receive produced message(s)");
  }
}
