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
import java.util.function.Supplier;
import org.kpipe.config.AppConfig;

/// Lightweight HTTP health check server using the JDK built-in HttpServer.
public final class HttpHealthServer implements AutoCloseable {

  public static final String ENV_ENABLED = "HEALTH_HTTP_ENABLED";
  public static final String ENV_PORT = "HEALTH_HTTP_PORT";
  public static final String ENV_HOST = "HEALTH_HTTP_HOST";
  public static final String ENV_PATH = "HEALTH_HTTP_PATH";

  public static final int DEFAULT_PORT = 8080;
  public static final String DEFAULT_HOST = "0.0.0.0";
  public static final String DEFAULT_PATH = "/health";

  private static final Logger LOGGER = System.getLogger(HttpHealthServer.class.getName());

  private final HttpServer server;
  private final Supplier<Boolean> healthSupplier;
  private final String path;
  private final String appName;
  private final AtomicBoolean started = new AtomicBoolean(false);

  private HttpHealthServer(
    final String host,
    final int port,
    final String path,
    final Supplier<Boolean> healthSupplier,
    final String appName
  ) throws IOException {
    this.healthSupplier = Objects.requireNonNull(healthSupplier, "Health supplier cannot be null");
    this.path = normalizePath(path);
    this.appName = appName != null ? appName : "kpipe-app";
    this.server = HttpServer.create(new InetSocketAddress(host, port), 0);
    this.server.createContext(this.path, this::handleHealth);
  }

  public static Optional<HttpHealthServer> fromEnv(
    final Supplier<Boolean> healthSupplier,
    final String appName
  ) {
    final var enabled =
      !"false".equalsIgnoreCase(AppConfig.getEnvOrDefault(ENV_ENABLED, "true"));
    if (!enabled) {
      return Optional.empty();
    }

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
      healthy = Boolean.TRUE.equals(healthSupplier.get());
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Health check failed for %s".formatted(appName), e);
      sendResponse(exchange, 503, "UNHEALTHY");
      return;
    }

    if (healthy) {
      sendResponse(exchange, 200, "OK");
    } else {
      sendResponse(exchange, 503, "UNHEALTHY");
    }
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
}
