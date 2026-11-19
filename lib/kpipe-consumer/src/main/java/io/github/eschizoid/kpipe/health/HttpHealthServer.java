package io.github.eschizoid.kpipe.health;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.eschizoid.kpipe.consumer.HealthSnapshot;
import io.github.eschizoid.kpipe.consumer.config.HealthConfig;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/// A small HTTP health endpoint on the JDK's built-in server. Each `GET` reads one
/// [HealthSnapshot] and answers `200` when it is healthy and `503` otherwise, with the snapshot
/// as JSON:
///
/// ```json
/// {"healthy": true, "running": true, "pauseSources": ["backpressure"], "circuitBreaker": "closed"}
/// ```
///
/// Enum values are lowercase, as in the OpenTelemetry attributes and the metrics log line. A
/// snapshot supplier that throws or returns null answers `503` with `{"healthy": false}`. `HEAD`
/// answers like `GET` without a body, for load balancers that probe with it; any other method
/// answers `405` with an `Allow: GET, HEAD` header.
public final class HttpHealthServer implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(HttpHealthServer.class.getName());

  private final HttpServer server;
  private final Supplier<HealthSnapshot> health;
  private final String path;
  private final String appName;
  private final AtomicBoolean started = new AtomicBoolean(false);

  /// Creates a server bound to `host:port` that serves `health` on `path`; call [#start()] to
  /// begin answering.
  ///
  /// @param host    the host to bind to, such as "0.0.0.0"
  /// @param port    the port to bind to, or 0 for any free port
  /// @param path    the path to serve, such as "/health"; a missing leading slash is added, and
  ///                null or blank serves [HealthConfig#DEFAULT_PATH]
  /// @param health  reads the health to serve, once per request, such as `handle::health`
  /// @param appName the name used in log lines, or null for "kpipe-app"
  /// @throws IOException if the server cannot be created or bound
  /// @throws NullPointerException if `health` is null
  public HttpHealthServer(
    final String host,
    final int port,
    final String path,
    final Supplier<HealthSnapshot> health,
    final String appName
  ) throws IOException {
    this.health = Objects.requireNonNull(health, "health cannot be null");
    this.path = normalizePath(path);
    this.appName = appName != null ? appName : "kpipe-app";
    this.server = HttpServer.create(new InetSocketAddress(host, port), 0);
    // HttpServer.create already bound the socket. If createContext throws, the socket would leak,
    // so close the server before propagating.
    try {
      this.server.createContext(this.path, this::handleHealth);
    } catch (final RuntimeException e) {
      this.server.stop(0);
      throw e;
    }
  }

  /// Creates and starts a server configured by the `HEALTH_*` environment variables (see
  /// [HealthConfig]), unless they disable it, logging as "kpipe-app".
  ///
  /// @param health reads the health to serve, once per request, such as `handle::health`
  /// @return the started server, or empty when disabled or when binding fails
  /// @throws NullPointerException if `health` is null
  public static Optional<HttpHealthServer> fromEnv(final Supplier<HealthSnapshot> health) {
    return fromEnv(health, null);
  }

  /// Creates and starts a server configured by the `HEALTH_*` environment variables (see
  /// [HealthConfig]), unless they disable it.
  ///
  /// @param health  reads the health to serve, once per request, such as `handle::health`
  /// @param appName the name used in log lines, or null for "kpipe-app"
  /// @return the started server, or empty when disabled or when binding fails
  /// @throws NullPointerException if `health` is null
  public static Optional<HttpHealthServer> fromEnv(final Supplier<HealthSnapshot> health, final String appName) {
    Objects.requireNonNull(health, "health cannot be null");
    if (!HealthConfig.isEnabled()) return Optional.empty();
    try {
      final var server = new HttpHealthServer(
        HealthConfig.getHost(),
        HealthConfig.getPort(),
        HealthConfig.getPath(),
        health,
        appName
      );
      server.start();
      return Optional.of(server);
    } catch (final IOException e) {
      LOGGER.log(Level.ERROR, "Failed to start health HTTP server", e);
      return Optional.empty();
    }
  }

  /// Start the health HTTP server. This method is idempotent and will only start the server once.
  public void start() {
    if (started.compareAndSet(false, true)) {
      server.start();
      LOGGER.log(Level.INFO, "Health HTTP server started on %s%s".formatted(server.getAddress(), path));
    }
  }

  @Override
  public void close() {
    if (started.compareAndSet(true, false)) {
      server.stop(0);
      LOGGER.log(Level.INFO, "Health HTTP server stopped");
    }
  }

  private void handleHealth(final HttpExchange exchange) throws IOException {
    final var method = exchange.getRequestMethod();
    final var head = "HEAD".equalsIgnoreCase(method);
    if (!head && !"GET".equalsIgnoreCase(method)) {
      exchange.getResponseHeaders().add("Allow", "GET, HEAD");
      send(exchange, 405, "text/plain; charset=utf-8", "Method Not Allowed", false);
      return;
    }
    final var snapshot = read();
    final var healthy = snapshot != null && snapshot.healthy();
    final var body = snapshot == null ? UNREADABLE : json(snapshot);
    send(exchange, healthy ? 200 : 503, "application/json; charset=utf-8", body, head);
  }

  private static final String UNREADABLE = "{\"healthy\": false}";

  private HealthSnapshot read() {
    try {
      final var snapshot = health.get();
      if (snapshot == null) LOGGER.log(Level.WARNING, "Health supplier returned null for %s".formatted(appName));
      return snapshot;
    } catch (final RuntimeException e) {
      LOGGER.log(Level.WARNING, "Health check failed for %s".formatted(appName), e);
      return null;
    }
  }

  /// Renders a snapshot as the endpoint's JSON body.
  ///
  /// @param snapshot the snapshot to render
  /// @return the JSON body
  static String json(final HealthSnapshot snapshot) {
    final var sources = snapshot
      .pauseSources()
      .stream()
      .map(source -> "\"" + lower(source) + "\"")
      .collect(Collectors.joining(", ", "[", "]"));
    return "{\"healthy\": %b, \"running\": %b, \"pauseSources\": %s, \"circuitBreaker\": \"%s\"}".formatted(
      snapshot.healthy(),
      snapshot.running(),
      sources,
      lower(snapshot.circuitBreaker())
    );
  }

  private static String lower(final Enum<?> value) {
    return value.name().toLowerCase(Locale.ROOT);
  }

  private static void send(
    final HttpExchange exchange,
    final int status,
    final String contentType,
    final String body,
    final boolean headersOnly
  ) throws IOException {
    exchange.getResponseHeaders().add("Content-Type", contentType);
    if (headersOnly) {
      // -1 tells the JDK server there is no body to send.
      exchange.sendResponseHeaders(status, -1);
      exchange.close();
      return;
    }
    final var payload = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, payload.length);
    try (final var out = exchange.getResponseBody()) {
      out.write(payload);
    }
  }

  private static String normalizePath(final String path) {
    if (path == null || path.isBlank()) return HealthConfig.DEFAULT_PATH;
    return path.startsWith("/") ? path : "/" + path;
  }

  /// Returns the actual bind address of the underlying [HttpServer].
  ///
  /// @return the [InetSocketAddress] the server is bound to
  public InetSocketAddress getAddress() {
    return server.getAddress();
  }
}
