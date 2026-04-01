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
import org.kpipe.config.HealthConfig;
import org.mockito.Mockito;

class HttpHealthServerTest {

  @Test
  void shouldNormalizePathAndDefaultAppName() throws Exception {
    final var server = new HttpHealthServer("127.0.0.1", 0, "health", () -> true, () -> 5L, () -> true, null);
    try (server) {
      server.start();
      final var response = sendRequest(server, "GET");
      assertEquals(200, response.statusCode());
      assertEquals("application/json; charset=utf-8", response.headers().firstValue("Content-Type").orElse(""));
      assertTrue(response.body().contains("\"status\": \"OK\""));
      assertTrue(response.body().contains("\"inFlight\": 5"));
      assertTrue(response.body().contains("\"paused\": true"));
    }
  }

  @Test
  void shouldReturnOkWhenHealthy() throws Exception {
    try (final var server = newServer(() -> true)) {
      server.start();

      final var response = sendRequest(server, "GET");
      assertEquals(200, response.statusCode());
      assertTrue(response.body().contains("\"status\": \"OK\""));
    }
  }

  @Test
  void shouldReturnUnhealthyWhenSupplierFalse() throws Exception {
    try (final var server = newServer(() -> false)) {
      server.start();

      final var response = sendRequest(server, "GET");
      assertEquals(503, response.statusCode());
      assertTrue(response.body().contains("\"status\": \"UNHEALTHY\""));
    }
  }

  @Test
  void shouldReturnUnhealthyWhenSupplierThrows() throws Exception {
    final BooleanSupplier supplier = () -> {
      throw new RuntimeException("boom");
    };

    try (final var server = newServer(supplier)) {
      server.start();

      final var response = sendRequest(server, "GET");
      assertEquals(503, response.statusCode());
      assertTrue(response.body().contains("\"status\": \"UNHEALTHY\""));
    }
  }

  @Test
  void shouldReturnMethodNotAllowedForNonGet() throws Exception {
    try (final var server = newServer(() -> true)) {
      server.start();

      final var response = sendRequest(server, "POST");
      assertEquals(405, response.statusCode());
      assertEquals("Method Not Allowed", response.body());
    }
  }

  @Test
  void shouldReturnEmptyWhenDisabledFromEnv() {
    try (final var mocked = Mockito.mockStatic(AppConfig.class)) {
      mocked.when(() -> AppConfig.getEnvOrDefault(HealthConfig.ENV_ENABLED, "true")).thenReturn("false");

      final Optional<HttpHealthServer> server = HttpHealthServer.fromEnv(() -> true, () -> 0L, () -> false, "test-app");
      assertTrue(server.isEmpty());
    }
  }

  private static HttpHealthServer newServer(final BooleanSupplier supplier) throws Exception {
    return new HttpHealthServer("127.0.0.1", 0, "/health", supplier, () -> 0L, () -> false, "test-app");
  }

  private static HttpResponse<String> sendRequest(final HttpHealthServer server, final String method) throws Exception {
    final var address = server.getAddress();
    final var uri = URI.create("http://127.0.0.1:" + address.getPort() + "/health");

    final var request = HttpRequest.newBuilder(uri).method(method, HttpRequest.BodyPublishers.noBody()).build();

    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }
}
