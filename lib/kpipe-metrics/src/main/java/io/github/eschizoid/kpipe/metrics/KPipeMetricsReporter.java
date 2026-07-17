package io.github.eschizoid.kpipe.metrics;

/// Defines a metrics reporting component that collects and publishes system metrics.
///
/// This interface provides a common contract for various metrics reporting implementations,
/// allowing for consistent monitoring across different system components. Implementations report
/// metrics to different destinations such as logs, monitoring systems, or dashboards.
///
/// The reporter is a pure sink — it exposes only [#reportMetrics()], invoked on whatever cadence the
/// host drives (the consumer runs it on a periodic daemon thread it owns; see the metrics-reporter
/// wiring in `KPipeConsumerBuilder`). The reporter does NOT own a lifecycle: there is deliberately no
/// `start()` / `stop()` on this SPI, because nothing in KPipe would call them — a reporter that needs
/// to manage its own resources should do so in its constructor and (if closeable) via `AutoCloseable`.
///
/// Example usage:
///
/// ```java
/// final var reporter = ConsumerMetricsReporter.forConsumer(consumer::getMetrics);
/// reporter.reportMetrics();
/// ```
public interface KPipeMetricsReporter {
  /// Reports collected metrics to the configured destination.
  void reportMetrics();
}
