package org.kpipe;

import java.time.Duration;
import java.util.Map;

/// Runtime handle for a started KPipe pipeline. Wraps the underlying `KPipeRunner` and exposes
/// only the operations needed by facade users.
///
/// Implements [AutoCloseable] for try-with-resources usage; the default `close()` performs a
/// graceful shutdown with a 5-second timeout.
///
/// @since 1.11.0
public interface Handle extends AutoCloseable {
  /// Returns whether the underlying consumer is healthy.
  ///
  /// @return true when the consumer is running and the configured health check passes
  boolean isHealthy();

  /// Returns an unmodifiable snapshot of the consumer's metrics. All values are counters or
  /// gauges represented as `Long`. Returns an empty map when metrics are disabled on the
  /// underlying consumer.
  ///
  /// @return metric name -> value snapshot
  Map<String, Long> metrics();

  /// Waits up to `timeout` for the consumer to shut down.
  ///
  /// @param timeout maximum wait
  /// @return true if shutdown completed within the timeout
  boolean awaitShutdown(final Duration timeout);

  /// Initiates a graceful shutdown, waiting up to `timeout` for in-flight messages.
  ///
  /// @param timeout maximum wait for in-flight drain
  /// @return true if shutdown completed cleanly
  boolean shutdownGracefully(final Duration timeout);

  /// Default close — graceful shutdown with a 5-second timeout.
  @Override
  default void close() {
    shutdownGracefully(Duration.ofSeconds(5));
  }
}
