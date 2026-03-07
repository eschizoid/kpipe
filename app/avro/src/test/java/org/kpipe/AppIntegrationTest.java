package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.kpipe.config.AppConfig;
import org.kpipe.sink.MessageSink;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class AppIntegrationTest {

  private static final Logger log = System.getLogger(AppIntegrationTest.class.getName());

  static Network network = Network.newNetwork();

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:7.7.1").asCompatibleSubstituteFor("apache/kafka")
  )
    .withNetwork(network)
    .withNetworkAliases("kafka")
    .waitingFor(Wait.forListeningPort());

  @Container
  static GenericContainer<?> schemaRegistry = new GenericContainer<>(
    DockerImageName
      .parse("confluentinc/cp-schema-registry:7.7.1")
      .asCompatibleSubstituteFor("confluentinc/cp-schema-registry")
  )
    .withNetwork(network)
    .withNetworkAliases("schema-registry")
    .withExposedPorts(8081)
    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
    .dependsOn(kafka)
    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));

  @Test
  void testAvroAppEndToEnd() throws Exception {
    final var topic = "avro-topic";
    final var srUrl = "http://%s:%d".formatted(schemaRegistry.getHost(), schemaRegistry.getMappedPort(8081));

    final var config = new AppConfig(
      kafka.getBootstrapServers(),
      "test-group",
      topic,
      "avro-app",
      Duration.ofMillis(100),
      Duration.ofSeconds(1),
      Duration.ofSeconds(5),
      List.of("addSource_1", "addTimestamp_1")
    );

    final var capturingSink = new CapturingSink();

    try (final var app = new App(config)) {
      // Register the capturing sink
      app.getSinkRegistry().register("capturingSink", capturingSink);

      // Start the app
      final var appThread = Thread
        .ofVirtual()
        .start(() -> {
          try {
            System.setProperty("SCHEMA_REGISTRY_URL", srUrl);
            app.start();
            app.awaitShutdown();
          } catch (Exception e) {
            log.log(Level.ERROR, "App error", e);
          }
        });

      // Load schema
      final Schema schema;
      try (InputStream is = getClass().getClassLoader().getResourceAsStream("avro/customer.avsc")) {
        assertNotNull(is, "Schema file not found");
        schema = new Schema.Parser().parse(is);
      }

      // Produce an Avro message with Confluent Wire Format (Magic Byte 0 + Schema ID)
      final var record = new GenericData.Record(schema);
      record.put("id", 1L);
      record.put("name", "Test User");
      record.put("active", true);
      record.put("registrationDate", System.currentTimeMillis());

      final var out = new ByteArrayOutputStream();
      out.write(0); // Magic byte
      out.write(ByteBuffer.allocate(4).putInt(1).array()); // Schema ID 1

      final var encoder = EncoderFactory.get().binaryEncoder(out, null);
      final var writer = new GenericDatumWriter<GenericRecord>(schema);
      writer.write(record, encoder);
      encoder.flush();

      final var producerProps = new Properties();
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
        producer.send(new ProducerRecord<>(topic, out.toByteArray())).get();
      }

      // Wait for processing
      TimeUnit.SECONDS.sleep(5);

      // Verify
      assertTrue(appThread.isAlive());

      final var received = capturingSink.getMessages();
      assertEquals(1, received.size(), "Should have received exactly one message");

      final var processedBytes = received.get(0);
      final var decoder = DecoderFactory.get().binaryDecoder(processedBytes, null);
      final var reader = new GenericDatumReader<GenericRecord>(schema);
      final var processedRecord = reader.read(null, decoder);

      assertEquals(1L, processedRecord.get("id"));
      assertEquals("Test User", processedRecord.get("name").toString());
      assertNotNull(processedRecord.get("source"), "Should have 'source' field added");
      assertNotNull(processedRecord.get("timestamp"), "Should have 'timestamp' field added");
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
