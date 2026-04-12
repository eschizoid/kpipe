package org.kpipe.metrics;

/// Defines a metrics reporting component that collects and publishes system metrics.
///
/// This interface provides a common contract for various metrics reporting implementations,
/// allowing for consistent monitoring across different system components.
///
/// Implementations can report metrics to different destinations such as logs, monitoring systems,
/// or dashboards. The core functionality is defined by {@link #reportMetrics()}, while lifecycle
/// methods {@link #start()} and {@link #stop()} are provided with default empty implementations.
///
/// Example usage:
///
/// ```java
/// // Create and use a metrics reporter
/// final var reporter = ConsumerMetricsReporter.forConsumer(consumer::getMetrics);
///
/// // Start the reporter (optional, if implemented)
/// reporter.start();
///
/// // Report metrics on demand
/// reporter.reportMetrics();
///
/// // Stop the reporter when done (optional, if implemented)
/// reporter.stop();
/// ```
public interface KPipeMetricsReporter {
  /// Reports collected metrics to the configured destination.
  void reportMetrics();

  /// Starts the metrics reporter. No-op by default.
  default void start() {}

  /// Stops the metrics reporter. No-op by default.
  default void stop() {}
}
