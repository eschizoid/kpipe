package org.kpipe;

import static org.junit.jupiter.api.Assertions.*;

import com.dslplatform.json.DslJson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.kpipe.consumer.config.AppConfig;
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
  private static final String AVRO_TOPIC = "avro-topic";
  private static final String SCHEMA_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";
  private static final String SCHEMA_SUBJECT = "com.kpipe.customer";
  private static final String SCHEMA_VERSIONS_PATH = "/subjects/" + SCHEMA_SUBJECT + "/versions";
  private static final String SCHEMA_LATEST_PATH = SCHEMA_VERSIONS_PATH + "/latest";
  private static final String SUBJECTS_EMPTY_JSON = """
    []
    """;
  private static final String REGISTERED_SCHEMA_RESPONSE_JSON = """
    {"id":1}
    """;
  private static final String SCHEMA_NOT_FOUND_JSON = """
    {"error_code":40403,"message":"Schema not found"}
    """;
  private static final String ROUTE_NOT_FOUND_JSON = """
    {"message":"Not found"}
    """;
  private static final DslJson<Object> JSON = new DslJson<>();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static volatile String latestSchema;
  private static HttpServer schemaRegistryStub;

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION))
                   .asCompatibleSubstituteFor("apache/kafka")
  ).withStartupAttempts(3);

  @BeforeAll
  static void startSchemaRegistryStub() throws Exception {
    schemaRegistryStub = HttpServer.create(new InetSocketAddress(0), 0);
    schemaRegistryStub.createContext("/subjects", AppIntegrationTest::handleSchemaStubRequest);
    schemaRegistryStub.start();
  }

  @AfterAll
  static void stopSchemaRegistryStub() {
    if (schemaRegistryStub != null) schemaRegistryStub.stop(0);
    latestSchema = null;
  }

  @Test
  void testAvroAppEndToEnd() throws Exception {
    final var srUrl = "http://localhost:%d".formatted(schemaRegistryStub.getAddress().getPort());

    // Load and register schema before App construction (App fetches latest schema during init).
    final Schema schema;
    try (final var is = getClass().getClassLoader().getResourceAsStream("avro/customer.avsc")) {
      assertNotNull(is, "Schema file not found");
      schema = new Schema.Parser().parse(is);
    }
    registerSchema(srUrl, schema.toString());

    final var config = new AppConfig(
      kafka.getBootstrapServers(),
      "test-group",
      AVRO_TOPIC,
      "avro-app",
      Duration.ofMillis(100),
      Duration.ofSeconds(1),
      Duration.ofSeconds(5),
      List.of()
    );

    final var capturingSink = new CapturingSink();

    try (final var app = new App(config, srUrl)) {
      final var registry = app.getProcessorRegistry();
      // Register the capturing sink
      registry.sinkRegistry().register(RegistryKey.avro("avroLogging"), capturingSink);

      // Start the app
      final var appThread = Thread.ofVirtual().start(() -> {
        try {
          app.start();
          app.awaitShutdown();
        } catch (final Exception e) {
          log.log(Level.ERROR, "App error", e);
        }
      });

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

      // Retry produce during the warm-up window so we do not miss messages while the consumer
      // finishes initial group assignment.
      produceUntilConsumed(createConfluentWirePayload(schema), producerProps, capturingSink, Duration.ofSeconds(10));

      // Verify
      assertTrue(appThread.isAlive());

      final var received = capturingSink.getMessages();
      assertFalse(received.isEmpty(), "Should have received at least one message");

      final var processedRecord = received.getFirst();

      assertAll(
        () -> assertEquals(1L, processedRecord.get("id")),
        () -> assertEquals("Test User", processedRecord.get("name").toString()),
        () -> assertEquals("test.user@example.com", processedRecord.get("email").toString()),
        () -> assertEquals(true, processedRecord.get("active"))
      );
    }
  }

  private static byte[] createConfluentWirePayload(final Schema schema) throws Exception {
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
    return out.toByteArray();
  }

  private static void handleSchemaStubRequest(final HttpExchange exchange) {
    try (exchange) {
      final var method = exchange.getRequestMethod();
      final var path = exchange.getRequestURI().getPath();

      if ("GET".equals(method) && "/subjects".equals(path)) {
        writeJson(exchange, 200, SUBJECTS_EMPTY_JSON);
        return;
      }

      if ("POST".equals(method) && SCHEMA_VERSIONS_PATH.equals(path)) {
        final var bodyBytes = exchange.getRequestBody().readAllBytes();
        final var payload = JSON.deserialize(Map.class, new ByteArrayInputStream(bodyBytes));
        latestSchema = payload != null && payload.get("schema") != null ? payload.get("schema").toString() : null;
        writeJson(exchange, 200, REGISTERED_SCHEMA_RESPONSE_JSON);
        return;
      }

      if ("GET".equals(method) && SCHEMA_LATEST_PATH.equals(path)) {
        if (latestSchema == null) {
          writeJson(exchange, 404, SCHEMA_NOT_FOUND_JSON);
          return;
        }

        final var response = new HashMap<String, Object>();
        response.put("subject", SCHEMA_SUBJECT);
        response.put("version", 1);
        response.put("id", 1);
        response.put("schema", latestSchema);

        final byte[] json;
        try (final var out = new ByteArrayOutputStream()) {
          JSON.serialize(response, out);
          json = out.toByteArray();
        }

        exchange.getResponseHeaders().set("Content-Type", SCHEMA_CONTENT_TYPE);
        exchange.sendResponseHeaders(200, json.length);
        exchange.getResponseBody().write(json);
        return;
      }

      writeJson(exchange, 404, ROUTE_NOT_FOUND_JSON);
    } catch (final Exception e) {
      throw new RuntimeException(e);
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
        producer.send(new ProducerRecord<>(AVRO_TOPIC, payload)).get();
        if (sink.size() >= 1) return;
        TimeUnit.MILLISECONDS.sleep(250);
      }
    }
    throw new AssertionError("Timed out waiting for consumer to receive produced message(s)");
  }

  private static void registerSchema(final String schemaRegistryUrl, final String schemaJson) throws Exception {
    final var payloadMap = Collections.singletonMap("schema", schemaJson);
    final byte[] payload;
    try (final var out = new ByteArrayOutputStream()) {
      JSON.serialize(payloadMap, out);
      payload = out.toByteArray();
    }

    final var request = HttpRequest.newBuilder()
      .uri(URI.create("%s%s".formatted(schemaRegistryUrl, SCHEMA_VERSIONS_PATH)))
      .header("Content-Type", SCHEMA_CONTENT_TYPE)
      .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
      .build();

    final var response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    assertTrue(
      response.statusCode() == 200 || response.statusCode() == 409,
      "Schema registration failed: HTTP %d body=%s".formatted(response.statusCode(), response.body())
    );
  }

  private static void writeJson(final HttpExchange exchange, final int status, final String body) {
    try {
      final var payload = body.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", SCHEMA_CONTENT_TYPE);
      exchange.sendResponseHeaders(status, payload.length);
      exchange.getResponseBody().write(payload);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class CapturingSink implements MessageSink<GenericRecord> {

    private final List<GenericRecord> messages = new ArrayList<>();

    @Override
    public synchronized void accept(GenericRecord processedValue) {
      messages.add(processedValue);
    }

    public synchronized List<GenericRecord> getMessages() {
      return new ArrayList<>(messages);
    }

    public synchronized int size() {
      return messages.size();
    }
  }
}
