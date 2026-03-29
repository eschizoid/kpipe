package org.kpipe.health;

import org.kpipe.config.AppConfig;

/// Configuration constants for the health HTTP server.
public final class HealthConfig {

  private HealthConfig() {}

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

  /// Get the enabled status from environment.
  public static boolean isEnabled() {
    return !"false".equalsIgnoreCase(AppConfig.getEnvOrDefault(ENV_ENABLED, "true"));
  }

  /// Get the host from environment.
  public static String getHost() {
    return AppConfig.getEnvOrDefault(ENV_HOST, DEFAULT_HOST);
  }

  /// Get the port from environment.
  public static int getPort() {
    try {
      final var value = AppConfig.getEnvOrDefault(ENV_PORT, Integer.toString(DEFAULT_PORT));
      final var port = Integer.parseInt(value);
      if (port < 1 || port > 65535) return DEFAULT_PORT;
      return port;
    } catch (final NumberFormatException e) {
      return DEFAULT_PORT;
    }
  }

  /// Get the path from environment.
  public static String getPath() {
    return AppConfig.getEnvOrDefault(ENV_PATH, DEFAULT_PATH);
  }
}
