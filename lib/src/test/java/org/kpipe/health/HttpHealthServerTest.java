package org.kpipe.health;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.kpipe.config.AppConfig;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.sun.net.httpserver.HttpServer;

class HttpHealthServerTest {

  @Test
  void shouldNormalizePathAndDefaultAppName() throws Exception {
    final var server = newServer("health", () -> true, null);

    assertEquals("/health", getField(server, "path"));
    assertEquals("kpipe-app", getField(server, "appName"));

    server.close();
  }

  @Test
  void shouldReturnOkWhenHealthy() throws Exception {
    try (HttpHealthServer server = newServer("/health", () -> true, "test-app")) {
      server.start();

      final var response = sendRequest(server, "GET");
      assertEquals(200, response.statusCode());
      assertEquals("OK", response.body());
    }
  }

  @Test
  void shouldReturnUnhealthyWhenSupplierFalse() throws Exception {
    try (HttpHealthServer server = newServer("/health", () -> false, "test-app")) {
      server.start();

      final var response = sendRequest(server, "GET");
      assertEquals(503, response.statusCode());
      assertEquals("UNHEALTHY", response.body());
    }
  }

  @Test
  void shouldReturnUnhealthyWhenSupplierThrows() throws Exception {
    final Supplier<Boolean> supplier = () -> {
      throw new RuntimeException("boom");
    };

    try (HttpHealthServer server = newServer("/health", supplier, "test-app")) {
      server.start();

      final var response = sendRequest(server, "GET");
      assertEquals(503, response.statusCode());
      assertEquals("UNHEALTHY", response.body());
    }
  }

  @Test
  void shouldReturnMethodNotAllowedForNonGet() throws Exception {
    try (HttpHealthServer server = newServer("/health", () -> true, "test-app")) {
      server.start();

      final var response = sendRequest(server, "POST");
      assertEquals(405, response.statusCode());
      assertEquals("Method Not Allowed", response.body());
    }
  }

  @Test
  void shouldReturnEmptyWhenDisabledFromEnv() {
    try (MockedStatic<AppConfig> mocked = Mockito.mockStatic(AppConfig.class)) {
      mocked.when(() -> AppConfig.getEnvOrDefault(HttpHealthServer.ENV_ENABLED, "true"))
        .thenReturn("false");

      final Optional<HttpHealthServer> server = HttpHealthServer.fromEnv(() -> true, "test-app");
      assertTrue(server.isEmpty());
    }
  }

  private static HttpHealthServer newServer(
    final String path,
    final Supplier<Boolean> supplier,
    final String appName
  ) throws Exception {
    final Constructor<HttpHealthServer> ctor =
      HttpHealthServer.class.getDeclaredConstructor(
        String.class,
        int.class,
        String.class,
        Supplier.class,
        String.class
      );
    ctor.setAccessible(true);
    return ctor.newInstance("127.0.0.1", 0, path, supplier, appName);
  }

  private static HttpResponse<String> sendRequest(
    final HttpHealthServer server,
    final String method
  ) throws Exception {
    final HttpServer httpServer = getField(server, "server");
    final String path = getField(server, "path");
    final InetSocketAddress address = httpServer.getAddress();
    final URI uri = URI.create("http://127.0.0.1:" + address.getPort() + path);

    final HttpRequest request = HttpRequest.newBuilder(uri)
      .method(method, HttpRequest.BodyPublishers.noBody())
      .build();

    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }

  @SuppressWarnings("unchecked")
  private static <T> T getField(final HttpHealthServer server, final String name) throws Exception {
    final Field field = HttpHealthServer.class.getDeclaredField(name);
    field.setAccessible(true);
    return (T) field.get(server);
  }
}
