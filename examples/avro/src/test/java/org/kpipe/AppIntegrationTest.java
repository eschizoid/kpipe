package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.format.avro.AvroFormat;
import org.kpipe.producer.config.KafkaProducerConfig;
import org.kpipe.sink.MessageSink;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class AppIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.2.0");

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION)).asCompatibleSubstituteFor("apache/kafka")
  ).withStartupAttempts(3);

  private Schema schema;
  private AvroFormat format;

  @BeforeEach
  void loadSchemaFromTestResource() throws Exception {
    try (final var in = getClass().getClassLoader().getResourceAsStream("avro/customer.avsc")) {
      assertNotNull(in, "avro/customer.avsc not found on test classpath");
      schema = new Schema.Parser().parse(in);
    }
    format = new AvroFormat(schema);
  }

  @Test
  void testAvroAppEndToEnd() throws Exception {
    final var topic = "avro-topic-" + UUID.randomUUID().toString().substring(0, 8);
    final var captured = new CopyOnWriteArrayList<GenericRecord>();
    final MessageSink<GenericRecord> capturingSink = captured::add;

    try (final var handle = KPipe.avro(format, topic, consumerProps()).skipBytes(5).toCustom(capturingSink).start()) {
      assertTrue(handle.isHealthy(), "Handle should be healthy after start()");

      produceUntilConsumed(topic, createConfluentWirePayload(), captured, Duration.ofSeconds(15));

      final var processed = captured.getFirst();
      assertAll(
        () -> assertEquals(1L, processed.get("id")),
        () -> assertEquals("Test User", processed.get("name").toString()),
        () -> assertEquals("test.user@example.com", processed.get("email").toString()),
        () -> assertEquals(true, processed.get("active"))
      );

      assertTrue(handle.shutdownGracefully(Duration.ofSeconds(5)));
      assertFalse(handle.isHealthy());
    }
  }

  private byte[] createConfluentWirePayload() throws Exception {
    final var record = new GenericData.Record(schema);
    record.put("id", 1L);
    record.put("name", "Test User");
    record.put("email", "test.user@example.com");
    record.put("active", true);
    record.put("registrationDate", System.currentTimeMillis());
    record.put("address", null);
    record.put("tags", new GenericData.Array<>(schema.getField("tags").schema(), Collections.emptyList()));
    record.put("preferences", Collections.emptyMap());

    final var out = new ByteArrayOutputStream();
    out.write(0); // Confluent magic byte
    out.write(ByteBuffer.allocate(4).putInt(1).array()); // schema id 1

    final var encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
    encoder.flush();
    return out.toByteArray();
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
