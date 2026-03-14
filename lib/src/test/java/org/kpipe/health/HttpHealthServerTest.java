package org.kpipe.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.kpipe.config.AppConfig;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class HttpHealthServerTest {

  @Test
  void shouldNormalizePathAndDefaultAppName() throws Exception {
    final var server = new HttpHealthServer("127.0.0.1", 0, "health", () -> true, null);
    try (server) {
      server.start();
      final var response = sendRequest(server, "GET");
      assertEquals(200, response.statusCode());
      assertEquals("OK", response.body());
    }
  }

  @Test
  void shouldReturnOkWhenHealthy() throws Exception {
    try (HttpHealthServer server = newServer(() -> true)) {
      server.start();

      final var response = sendRequest(server, "GET");
      assertEquals(200, response.statusCode());
      assertEquals("OK", response.body());
    }
  }

  @Test
  void shouldReturnUnhealthyWhenSupplierFalse() throws Exception {
    try (HttpHealthServer server = newServer(() -> false)) {
      server.start();

      final var response = sendRequest(server, "GET");
      assertEquals(503, response.statusCode());
      assertEquals("UNHEALTHY", response.body());
    }
  }

  @Test
  void shouldReturnUnhealthyWhenSupplierThrows() throws Exception {
    final BooleanSupplier supplier = () -> {
      throw new RuntimeException("boom");
    };

    try (HttpHealthServer server = newServer(supplier)) {
      server.start();

      final var response = sendRequest(server, "GET");
      assertEquals(503, response.statusCode());
      assertEquals("UNHEALTHY", response.body());
    }
  }

  @Test
  void shouldReturnMethodNotAllowedForNonGet() throws Exception {
    try (HttpHealthServer server = newServer(() -> true)) {
      server.start();

      final var response = sendRequest(server, "POST");
      assertEquals(405, response.statusCode());
      assertEquals("Method Not Allowed", response.body());
    }
  }

  @Test
  void shouldReturnEmptyWhenDisabledFromEnv() {
    try (MockedStatic<AppConfig> mocked = Mockito.mockStatic(AppConfig.class)) {
      mocked.when(() -> AppConfig.getEnvOrDefault(HttpHealthServer.ENV_ENABLED, "true")).thenReturn("false");

      final Optional<HttpHealthServer> server = HttpHealthServer.fromEnv(() -> true, "test-app");
      assertTrue(server.isEmpty());
    }
  }

  private static HttpHealthServer newServer(final BooleanSupplier supplier)
    throws Exception {
    return new HttpHealthServer("127.0.0.1", 0, "/health", supplier, "test-app");
  }

  private static HttpResponse<String> sendRequest(final HttpHealthServer server, final String method) throws Exception {
    final var address = server.getAddress();
    final var uri = URI.create("http://127.0.0.1:" + address.getPort() + "/health");

    final var request = HttpRequest.newBuilder(uri).method(method, HttpRequest.BodyPublishers.noBody()).build();

    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }
}
