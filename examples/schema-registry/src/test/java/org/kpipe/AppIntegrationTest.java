package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.kpipe.format.avro.AvroFormat;
import org.kpipe.schemaregistry.confluent.ConfluentSchemaResolver;
import org.kpipe.sink.MessageSink;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/// End-to-end demonstration: SR resolves a schema at startup, the consumer registers it with
/// `AvroFormat`, and a producer publishes Confluent-wire-format-wrapped records that the
/// pipeline decodes via `.skipBytes(5)`.
@Testcontainers(disabledWithoutDocker = true)
class AppIntegrationTest {

  private static final String CONFLUENT_VERSION = System.getProperty("confluentPlatformVersion", "8.2.0");
  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.2.0");
  private static final String SUBJECT = "user-value";
  private static final String SCHEMA_JSON = """
    {"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}""";

  private static final Network NETWORK = Network.newNetwork();

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION))
                   .asCompatibleSubstituteFor("apache/kafka")
  )
    .withNetwork(NETWORK)
    .withListener("kafka:19092")
    .withStartupAttempts(3);

  @Container
  static GenericContainer<?> schemaRegistry = new GenericContainer<>(
    DockerImageName.parse("soldevelo/schema-registry:%s".formatted(CONFLUENT_VERSION))
  )
    .withNetwork(NETWORK)
    .withNetworkAliases("schema-registry")
    .withExposedPorts(8081)
    .withEnv("SCHEMA_REGISTRY_ADVERTISED_HOSTNAME", "schema-registry")
    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
    .withEnv("SCHEMA_REGISTRY_KAFKA_BROKERS", "PLAINTEXT://kafka:19092")
    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC", "_schemas")
    .dependsOn(kafka)
    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
    .withStartupTimeout(Duration.ofMinutes(2));

  @Test
  void resolvesSchemaAndConsumesAvroRecords() throws Exception {
    final var topic = "users-" + UUID.randomUUID().toString().substring(0, 8);
    final var schemaId = registerSchema();
    final var schema = new Schema.Parser().parse(SCHEMA_JSON);

    // Fetch via the resolver and register with AvroFormat — exactly what App.java does at startup.
    try (final var resolver = new ConfluentSchemaResolver(schemaRegistryUrl())) {
      final var fetched = resolver.lookupBySubjectVersion(SUBJECT, "latest");
      assertEquals(SCHEMA_JSON, fetched);
      AvroFormat.INSTANCE.addSchema(SUBJECT, fetched);
      AvroFormat.INSTANCE.withDefaultSchema(SUBJECT);
    }

    final var captured = new CopyOnWriteArrayList<GenericRecord>();
    final MessageSink<GenericRecord> capturingSink = captured::add;

    try (
      final var handle = KPipe
        .avro(topic, consumerProps())
        .skipBytes(5)
        .toCustom(capturingSink)
        .start()
    ) {
      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
        producer.send(new ProducerRecord<>(topic, encodeConfluentWireFormat(schema, schemaId, 1L, "alice"))).get();
        producer.send(new ProducerRecord<>(topic, encodeConfluentWireFormat(schema, schemaId, 2L, "bob"))).get();
      }

      final var deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      while (System.nanoTime() < deadline && captured.size() < 2) TimeUnit.MILLISECONDS.sleep(100);

      assertEquals(2, captured.size(), "consumer should have received both Avro records");
      assertEquals(1L, captured.get(0).get("id"));
      assertEquals("alice", captured.get(0).get("name").toString());
      assertEquals(2L, captured.get(1).get("id"));
      assertEquals("bob", captured.get(1).get("name").toString());

      assertTrue(handle.shutdownGracefully(Duration.ofSeconds(5)));
    }
  }

  /// Wraps an Avro-encoded record in the Confluent wire envelope: 1 magic byte (0x00) +
  /// 4-byte big-endian schema id + Avro binary payload.
  private static byte[] encodeConfluentWireFormat(final Schema schema, final int schemaId, final long id, final String name)
    throws Exception {
    final var record = new GenericData.Record(schema);
    record.put("id", id);
    record.put("name", name);

    final var payload = new ByteArrayOutputStream();
    final var encoder = EncoderFactory.get().binaryEncoder(payload, null);
    new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
    encoder.flush();

    final var envelope = ByteBuffer.allocate(5 + payload.size());
    envelope.put((byte) 0x00).putInt(schemaId).put(payload.toByteArray());
    return envelope.array();
  }

  private static int registerSchema() throws Exception {
    final var payload = """
      {"schema":%s}""".formatted(jsonStringEscape(SCHEMA_JSON));
    try (final var client = HttpClient.newHttpClient()) {
      final var request = HttpRequest.newBuilder()
        .uri(URI.create("%s/subjects/%s/versions".formatted(schemaRegistryUrl(), SUBJECT)))
        .timeout(Duration.ofSeconds(10))
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8))
        .build();

      final var response = client.send(request, HttpResponse.BodyHandlers.ofString());
      assertEquals(200, response.statusCode(), "SR registration must return 200, body: " + response.body());

      final var body = response.body();
      final var idMarker = "\"id\":";
      final var i = body.indexOf(idMarker);
      assertTrue(i >= 0, "expected 'id' field in SR response: " + body);
      int j = i + idMarker.length();
      while (j < body.length() && (body.charAt(j) == ' ' || body.charAt(j) == '\t')) j++;
      int k = j;
      while (k < body.length() && Character.isDigit(body.charAt(k))) k++;
      return Integer.parseInt(body.substring(j, k));
    }
  }

  private static String schemaRegistryUrl() {
    return "http://%s:%d".formatted(schemaRegistry.getHost(), schemaRegistry.getMappedPort(8081));
  }

  private static String jsonStringEscape(final String s) {
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  private static Properties consumerProps() {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "sr-ex-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }

  private static Properties producerProps() {
    final var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return props;
  }
}
