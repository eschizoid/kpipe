package io.github.eschizoid.kpipe.schemaregistry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Contract tests for [ConfluentSchemaResolver] against an in-JDK [HttpServer] standing in for
/// Confluent Schema Registry. Verifies the two URL shapes (`/schemas/ids/{id}` and
/// `/subjects/{subject}/versions/{version}`) and the JSON-envelope unwrap that strips the
/// `{"schema":"..."}` wrapper.
class ConfluentSchemaResolverTest {

  private HttpServer server;
  private String baseUrl;
  private final AtomicReference<String> lastPath = new AtomicReference<>();
  private final AtomicReference<String> responseBody = new AtomicReference<>();
  private final AtomicReference<Integer> responseStatus = new AtomicReference<>(200);

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.start();
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private void handle(final HttpExchange exchange) throws IOException {
    lastPath.set(exchange.getRequestURI().getPath());
    final var body = responseBody.get();
    final var bytes = body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(responseStatus.get(), bytes.length);
    try (final var os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  @Test
  void lookupByIdUnwrapsSchemaField() {
    responseBody.set(
      """
      {"schema":"{\\"type\\":\\"record\\",\\"name\\":\\"Foo\\"}","subject":"foo"}"""
    );

    try (final var resolver = new ConfluentSchemaResolver(baseUrl)) {
      final var schema = resolver.lookupById(42);

      assertEquals("/schemas/ids/42", lastPath.get(), "expected SR id endpoint");
      assertEquals(
        """
        {"type":"record","name":"Foo"}""",
        schema,
        "envelope should be stripped, escapes decoded"
      );
    }
  }

  @Test
  void lookupBySubjectVersionUsesCorrectEndpoint() {
    responseBody.set(
      """
      {"schema":"\\"inline-string-schema\\"","id":7}"""
    );

    try (final var resolver = new ConfluentSchemaResolver(baseUrl)) {
      final var schema = resolver.lookupBySubjectVersion("com.kpipe.customer", "latest");

      assertEquals("/subjects/com.kpipe.customer/versions/latest", lastPath.get());
      assertEquals("\"inline-string-schema\"", schema);
    }
  }

  @Test
  void nonNumericVersionIsUrlEncoded() {
    responseBody.set(
      """
      {"schema":"{\\"type\\":\\"int\\"}"}"""
    );

    try (final var resolver = new ConfluentSchemaResolver(baseUrl)) {
      resolver.lookupBySubjectVersion("with spaces", "v 1");

      assertEquals("/subjects/with+spaces/versions/v+1", lastPath.get(), "subject and version must be URL-encoded");
    }
  }

  @Test
  void non2xxResponseThrows() {
    responseStatus.set(404);
    responseBody.set("Not found");

    try (final var resolver = new ConfluentSchemaResolver(baseUrl)) {
      final var thrown = assertThrows(RuntimeException.class, () -> resolver.lookupById(99));
      assertTrue(thrown.getMessage().contains("404"), "exception should surface the status code");
    }
  }

  @Test
  void responseMissingSchemaFieldThrows() {
    responseBody.set(
      """
      {"id":42,"subject":"foo"}"""
    );

    try (final var resolver = new ConfluentSchemaResolver(baseUrl)) {
      final var thrown = assertThrows(RuntimeException.class, () -> resolver.lookupById(42));
      assertTrue(thrown.getMessage().contains("Schema field not found"), thrown.getMessage());
    }
  }

  @Test
  void emptyResponseBodyThrows() {
    responseBody.set("");

    try (final var resolver = new ConfluentSchemaResolver(baseUrl)) {
      final var thrown = assertThrows(RuntimeException.class, () -> resolver.lookupById(1));
      assertTrue(thrown.getMessage().contains("Empty schema registry response"), thrown.getMessage());
    }
  }

  @Test
  void negativeSchemaIdRejected() {
    try (final var resolver = new ConfluentSchemaResolver(baseUrl)) {
      assertThrows(IllegalArgumentException.class, () -> resolver.lookupById(-1));
    }
  }

  @Test
  void trailingSlashInBaseUrlIsStripped() {
    responseBody.set(
      """
      {"schema":"\\"x\\""}"""
    );

    try (final var resolver = new ConfluentSchemaResolver(baseUrl + "/")) {
      final var result = resolver.lookupById(1);
      assertNotNull(result);
      assertEquals("/schemas/ids/1", lastPath.get(), "no double slash should appear in the URL");
    }
  }
}
