package io.github.eschizoid.kpipe.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.consumer.CircuitBreakerState;
import io.github.eschizoid.kpipe.consumer.HealthSnapshot;
import io.github.eschizoid.kpipe.consumer.PauseSource;
import io.github.eschizoid.kpipe.consumer.config.AppConfig;
import io.github.eschizoid.kpipe.consumer.config.HealthConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class HttpHealthServerTest {

  private static final HealthSnapshot HEALTHY = new HealthSnapshot(true, Set.of(), CircuitBreakerState.CLOSED);

  @Test
  void aHealthyConsumerAnswers200WithItsSnapshot() throws Exception {
    final var snapshot = new HealthSnapshot(
      true,
      Set.of(PauseSource.CIRCUIT_BREAKER, PauseSource.MANUAL),
      CircuitBreakerState.HALF_OPEN
    );
    try (final var server = started(() -> snapshot)) {
      final var response = send(server, "GET");
      assertEquals(200, response.statusCode());
      assertEquals("application/json; charset=utf-8", response.headers().firstValue("Content-Type").orElse(""));
      assertEquals(
        "{\"healthy\": true, \"running\": true, \"pauseSources\": [\"manual\", \"circuit_breaker\"], \"circuitBreaker\": \"half_open\"}",
        response.body()
      );
    }
  }

  @Test
  void anOpenBreakerAnswers503() throws Exception {
    final var snapshot = new HealthSnapshot(true, Set.of(PauseSource.CIRCUIT_BREAKER), CircuitBreakerState.OPEN);
    try (final var server = started(() -> snapshot)) {
      final var response = send(server, "GET");
      assertEquals(503, response.statusCode());
      assertTrue(response.body().startsWith("{\"healthy\": false, \"running\": true"), response.body());
    }
  }

  @Test
  void eachRequestReadsOneSnapshot() throws Exception {
    final var reads = new AtomicInteger();
    try (
      final var server = started(() -> {
        reads.incrementAndGet();
        return HEALTHY;
      })
    ) {
      send(server, "GET");
      send(server, "GET");
      assertEquals(2, reads.get());
    }
  }

  @Test
  void aSupplierThatThrowsAnswers503() throws Exception {
    try (
      final var server = started(() -> {
        throw new IllegalStateException("boom");
      })
    ) {
      final var response = send(server, "GET");
      assertEquals(503, response.statusCode());
      assertEquals("{\"healthy\": false}", response.body());
    }
  }

  @Test
  void aSupplierThatReturnsNullAnswers503() throws Exception {
    try (final var server = started(() -> null)) {
      final var response = send(server, "GET");
      assertEquals(503, response.statusCode());
      assertEquals("{\"healthy\": false}", response.body());
    }
  }

  @Test
  void anyOtherMethodAnswers405AndNamesGet() throws Exception {
    try (final var server = started(() -> HEALTHY)) {
      final var response = send(server, "POST");
      assertEquals(405, response.statusCode());
      assertEquals("GET", response.headers().firstValue("Allow").orElse(""));
      assertEquals("text/plain; charset=utf-8", response.headers().firstValue("Content-Type").orElse(""));
    }
  }

  @Test
  void aPathWithoutALeadingSlashIsServedWithOne() throws Exception {
    try (final var server = new HttpHealthServer("127.0.0.1", 0, "health", () -> HEALTHY, null)) {
      server.start();
      assertEquals(200, send(server, "GET").statusCode());
    }
  }

  @Test
  void fromEnvReturnsEmptyWhenDisabled() {
    try (final var mocked = Mockito.mockStatic(AppConfig.class)) {
      mocked.when(() -> AppConfig.getEnvOrDefault(HealthConfig.ENV_ENABLED, "true")).thenReturn("false");
      assertTrue(HttpHealthServer.fromEnv(() -> HEALTHY, "test-app").isEmpty());
    }
  }

  @Test
  void fromEnvReturnsAStartedServer() throws Exception {
    try (final var mocked = Mockito.mockStatic(AppConfig.class)) {
      mocked.when(() -> AppConfig.getEnvOrDefault(HealthConfig.ENV_ENABLED, "true")).thenReturn("true");
      mocked
        .when(() -> AppConfig.getEnvOrDefault(HealthConfig.ENV_HOST, HealthConfig.DEFAULT_HOST))
        .thenReturn("127.0.0.1");
      mocked.when(() -> AppConfig.getEnvOrDefault(HealthConfig.ENV_PORT, "8080")).thenReturn("0");
      mocked
        .when(() -> AppConfig.getEnvOrDefault(HealthConfig.ENV_PATH, HealthConfig.DEFAULT_PATH))
        .thenReturn("/health");

      final var maybeServer = HttpHealthServer.fromEnv(() -> HEALTHY, "test-app");
      assertTrue(maybeServer.isPresent());
      // fromEnv documents that the server is started: no start() call here.
      try (final var server = maybeServer.get()) {
        assertEquals(200, send(server, "GET").statusCode());
      }
    }
  }

  private static HttpHealthServer started(final Supplier<HealthSnapshot> health) throws Exception {
    final var server = new HttpHealthServer("127.0.0.1", 0, "/health", health, "test-app");
    server.start();
    return server;
  }

  private static HttpResponse<String> send(final HttpHealthServer server, final String method) throws Exception {
    final var uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/health");
    final var request = HttpRequest.newBuilder(uri).method(method, HttpRequest.BodyPublishers.noBody()).build();
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }
}
