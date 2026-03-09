package org.kpipe;

import static org.junit.jupiter.api.Assertions.*;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayOutputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
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
    DockerImageName.parse("confluentinc/cp-schema-registry:7.8.7")
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

    // Load and register schema before App construction (App fetches latest schema during init).
    final Schema schema;
    try (final var is = getClass().getClassLoader().getResourceAsStream("avro/customer.avsc")) {
      assertNotNull(is, "Schema file not found");
      schema = new Schema.Parser().parse(is);
    }
    registerSchema(srUrl, "com.kpipe.customer", schema.toString());

    final var config = new AppConfig(
      kafka.getBootstrapServers(),
      "test-group",
      topic,
      "avro-app",
      Duration.ofMillis(100),
      Duration.ofSeconds(1),
      Duration.ofSeconds(5),
      List.of()
    );

    final var capturingSink = new CapturingSink();

    try (final var app = new App(config, srUrl)) {
      // Register the capturing sink
      app.getSinkRegistry().register("avroLogging", capturingSink);

      // Start the app
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

      // Produce an Avro message with Confluent Wire Format (Magic Byte 0 + Schema ID)
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

      // Retry produce during the warm-up window so we do not miss messages while the consumer
      // finishes initial group assignment.
      produceUntilConsumed(out.toByteArray(), producerProps, capturingSink, Duration.ofSeconds(10));

      // Verify
      assertTrue(appThread.isAlive());

      final var received = capturingSink.getMessages();
      assertFalse(received.isEmpty(), "Should have received at least one message");

      final var processedBytes = received.getFirst();
      final var decoder = DecoderFactory.get().binaryDecoder(processedBytes, null);
      final var reader = new GenericDatumReader<GenericRecord>(schema);
      final var processedRecord = reader.read(null, decoder);

      assertEquals(1L, processedRecord.get("id"));
      assertEquals("Test User", processedRecord.get("name").toString());
      assertEquals("test.user@example.com", processedRecord.get("email").toString());
      assertEquals(true, processedRecord.get("active"));
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
        producer.send(new ProducerRecord<>("avro-topic", payload)).get();
        if (sink.size() >= 1) return;
        TimeUnit.MILLISECONDS.sleep(250);
      }
    }
    throw new AssertionError("Timed out waiting for consumer to receive produced message(s)");
  }

  private static void registerSchema(final String schemaRegistryUrl, final String subject, final String schemaJson)
    throws Exception {
    final var json = new DslJson<>();
    final var payloadMap = Collections.singletonMap("schema", schemaJson);
    final byte[] payload;
    try (final var out = new ByteArrayOutputStream()) {
      json.serialize(payloadMap, out);
      payload = out.toByteArray();
    }

    final var request = HttpRequest
      .newBuilder()
      .uri(URI.create("%s/subjects/%s/versions".formatted(schemaRegistryUrl, subject)))
      .header("Content-Type", "application/vnd.schemaregistry.v1+json")
      .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
      .build();

    final var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    assertTrue(
      response.statusCode() == 200 || response.statusCode() == 409,
      "Schema registration failed: HTTP %d body=%s".formatted(response.statusCode(), response.body())
    );
  }

  private static class CapturingSink implements MessageSink<byte[], byte[]> {

    private final List<byte[]> messages = new ArrayList<>();

    @Override
    public synchronized void send(final ConsumerRecord<byte[], byte[]> record, byte[] processedValue) {
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
