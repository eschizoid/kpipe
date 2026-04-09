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
/// final var reporter = ProcessorMetricsReporter.forRegistry(registry);
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
  /// Reports collected metrics to the configured destination. This is the core method that
  /// implementations must provide.
  void reportMetrics();

  /// Starts the metrics reporter.
  ///
  /// Implementations may use this method to initialize resources, schedule periodic reporting, or
  /// connect to external systems. The default implementation does nothing.
  default void start() {} // Optional operations with default implementations

  /// Stops the metrics reporter.
  ///
  /// Implementations may use this method to release resources, cancel scheduled tasks, or
  /// disconnect from external systems. The default implementation does nothing.
  default void stop() {}
}
