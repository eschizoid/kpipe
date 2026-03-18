package org.kpipe.health;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.kpipe.config.AppConfig;

/// Lightweight HTTP health check server using the JDK built-in HttpServer.
public final class HttpHealthServer implements AutoCloseable {

  /// Environment variable name that toggles the health HTTP server. Set to "false" to disable.
  public static final String ENV_ENABLED = "HEALTH_HTTP_ENABLED";

  /// Environment variable name used to configure the HTTP bind port.
  public static final String ENV_PORT = "HEALTH_HTTP_PORT";

  /// Environment variable name used to configure the HTTP bind host.
  public static final String ENV_HOST = "HEALTH_HTTP_HOST";

  /// Environment variable name used to configure the health check path.
  public static final String ENV_PATH = "HEALTH_HTTP_PATH";

  /// Default HTTP port used when no environment configuration is provided.
  public static final int DEFAULT_PORT = 8080;

  /// Default host to bind the HTTP server to (all interfaces).
  public static final String DEFAULT_HOST = "0.0.0.0";

  /// Default HTTP path for the health endpoint.
  public static final String DEFAULT_PATH = "/health";

  private static final Logger LOGGER = System.getLogger(HttpHealthServer.class.getName());

  private final HttpServer server;
  private final BooleanSupplier healthSupplier;
  private final String path;
  private final String appName;
  private final AtomicBoolean started = new AtomicBoolean(false);

  /// Create a new {@link HttpHealthServer} bound to the provided host/port and exposing the
  /// configured health endpoint.
  ///
  /// @param host the host to bind the HTTP server to (e.g. "0.0.0.0")
  /// @param port the port to bind the HTTP server to (1-65535)
  /// @param path the HTTP path to expose the health check on (e.g. "/health")
  /// @param healthSupplier a supplier that returns true when the application is healthy
  /// @param appName optional application name used for logging (may be null)
  /// @throws IOException if the underlying HTTP server cannot be created or bound
  public HttpHealthServer(
    final String host,
    final int port,
    final String path,
    final BooleanSupplier healthSupplier,
    final String appName
  ) throws IOException {
    this.healthSupplier = Objects.requireNonNull(healthSupplier, "Health supplier cannot be null");
    this.path = normalizePath(path);
    this.appName = appName != null ? appName : "kpipe-app";
    this.server = HttpServer.create(new InetSocketAddress(host, port), 0);
    this.server.createContext(this.path, this::handleHealth);
  }

  /// Create an {@link HttpHealthServer} using environment variables when enabled.
  /// The method reads the following environment variables via {@link AppConfig}:
  /// <ul>
  ///   <li>{@value #ENV_ENABLED} - if set to "false" the server is disabled and empty is returned</li>
  ///   <li>{@value #ENV_HOST} - host to bind (defaults to {@value #DEFAULT_HOST})</li>
  ///   <li>{@value #ENV_PORT} - port to bind (defaults to {@value #DEFAULT_PORT})</li>
  ///   <li>{@value #ENV_PATH} - health endpoint path (defaults to {@value #DEFAULT_PATH})</li>
  /// </ul>
  ///
  /// @param healthSupplier supplier that returns true when the application is healthy
  /// @param appName optional application name used for logging
  /// @return Optional containing a started {@link HttpHealthServer} instance when enabled and
  ///     successfully constructed, or {@link Optional#empty()} when disabled or on failure
  public static Optional<HttpHealthServer> fromEnv(final BooleanSupplier healthSupplier, final String appName) {
    if ("false".equalsIgnoreCase(AppConfig.getEnvOrDefault(ENV_ENABLED, "true"))) return Optional.empty();

    final var host = AppConfig.getEnvOrDefault(ENV_HOST, DEFAULT_HOST);
    final var path = AppConfig.getEnvOrDefault(ENV_PATH, DEFAULT_PATH);
    final var port = parsePort(AppConfig.getEnvOrDefault(ENV_PORT, Integer.toString(DEFAULT_PORT)));

    try {
      return Optional.of(new HttpHealthServer(host, port, path, healthSupplier, appName));
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
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      sendResponse(exchange, 405, "Method Not Allowed");
      return;
    }

    final boolean healthy;
    try {
      healthy = healthSupplier.getAsBoolean();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Health check failed for %s".formatted(appName), e);
      sendResponse(exchange, 503, "UNHEALTHY");
      return;
    }

    if (healthy) sendResponse(exchange, 200, "OK"); else sendResponse(exchange, 503, "UNHEALTHY");
  }

  private static void sendResponse(final HttpExchange exchange, final int status, final String body)
    throws IOException {
    final var payload = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
    exchange.sendResponseHeaders(status, payload.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(payload);
    }
  }

  private static String normalizePath(final String path) {
    if (path == null || path.isBlank()) return DEFAULT_PATH;
    return path.startsWith("/") ? path : "/" + path;
  }

  private static int parsePort(final String value) {
    try {
      final var port = Integer.parseInt(value);
      if (port < 1 || port > 65535) return DEFAULT_PORT;
      return port;
    } catch (final NumberFormatException e) {
      return DEFAULT_PORT;
    }
  }

  /// Returns the actual bind address of the underlying {@link HttpServer}.
  ///
  /// @return the {@link InetSocketAddress} the server is bound to
  public InetSocketAddress getAddress() {
    return server.getAddress();
  }
}
