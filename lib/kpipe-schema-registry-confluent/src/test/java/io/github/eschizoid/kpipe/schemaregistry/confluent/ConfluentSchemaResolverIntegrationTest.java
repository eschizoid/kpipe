package io.github.eschizoid.kpipe.schemaregistry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/// End-to-end test for [ConfluentSchemaResolver] against a real `soldevelo/schema-registry`
/// container. Registers a schema via SR's REST API, then exercises both lookup shapes.
///
/// **Networking note.** Schema Registry runs in a sibling container and needs to reach the
/// broker via the shared docker network. `KafkaContainer.getBootstrapServers()` returns
/// a `localhost:<mappedPort>` address that's only reachable from the host — peers on the docker
/// network can't resolve it. `withListener("kafka:19092")` adds an additional advertised listener
/// reachable as `PLAINTEXT://kafka:19092` from any sibling on the same `Network`.
@Testcontainers(disabledWithoutDocker = true)
class ConfluentSchemaResolverIntegrationTest {

  private static final String CONFLUENT_VERSION = System.getProperty("confluentPlatformVersion", "8.2.0");
  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.2.0");
  private static final String SUBJECT = "kpipe-test-schema";
  private static final String SCHEMA_JSON = """
    {"type":"record","name":"KPipeTest","fields":[{"name":"id","type":"long"}]}""";

  private static final Network NETWORK = Network.newNetwork();

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION)).asCompatibleSubstituteFor("apache/kafka")
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

  private String baseUrl() {
    return "http://%s:%d".formatted(schemaRegistry.getHost(), schemaRegistry.getMappedPort(8081));
  }

  private int registerSchema() throws Exception {
    final var payload = "{\"schema\":%s}".formatted(jsonStringEscape(SCHEMA_JSON));
    try (final var client = HttpClient.newHttpClient()) {
      final var request = HttpRequest.newBuilder()
        .uri(URI.create("%s/subjects/%s/versions".formatted(baseUrl(), SUBJECT)))
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

  private static String jsonStringEscape(final String s) {
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  @Test
  void resolverFetchesByIdAndBySubjectVersion() throws Exception {
    final var schemaId = registerSchema();

    try (final var resolver = new ConfluentSchemaResolver(baseUrl())) {
      final var bySubject = resolver.lookupBySubjectVersion(SUBJECT, "latest");
      assertEquals(SCHEMA_JSON, bySubject, "lookupBySubjectVersion must return the unwrapped schema JSON");

      final var byId = resolver.lookupById(schemaId);
      assertEquals(SCHEMA_JSON, byId, "lookupById must return the same JSON as lookupBySubjectVersion");
    }
  }

  @Test
  void lookupOfMissingSubjectThrowsWith404() {
    try (final var resolver = new ConfluentSchemaResolver(baseUrl())) {
      final var thrown = assertThrows(RuntimeException.class, () ->
        resolver.lookupBySubjectVersion("does-not-exist", "latest")
      );
      assertNotNull(thrown.getMessage());
      assertTrue(thrown.getMessage().contains("404"), "exception should mention 404, got: " + thrown.getMessage());
    }
  }
}
