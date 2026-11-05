package io.github.eschizoid.kpipe.metrics;

/// Reports metrics to a destination such as a log, on a cadence the consumer drives: it calls
/// [#reportMetrics()] from a daemon thread it owns, at the interval set by `withMetricsInterval`.
///
/// A reporter has no `start()` or `stop()`, because nothing in KPipe would call them. One that
/// holds resources should acquire them in its constructor and release them through
/// `AutoCloseable`.
///
/// ```java
/// final KPipeMetricsReporter reporter = () -> LOGGER.log(Level.INFO, consumer.metrics());
/// ```
public interface KPipeMetricsReporter {
  /// Reports collected metrics to the configured destination.
  void reportMetrics();
}
