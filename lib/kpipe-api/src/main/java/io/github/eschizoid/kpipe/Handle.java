package io.github.eschizoid.kpipe;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/// Runtime handle for a started KPipe pipeline. Wraps the underlying `KPipeConsumer` and exposes
/// only the operations needed by facade users.
///
/// Implements [AutoCloseable] for try-with-resources usage; the default `close()` performs a
/// graceful shutdown with a 5-second timeout.
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

  /// Blocks indefinitely until the consumer shuts down (typically via SIGTERM / SIGINT through
  /// the runner's shutdown hook). Returns when shutdown completes.
  ///
  /// @throws InterruptedException if the calling thread is interrupted while waiting
  void awaitShutdown() throws InterruptedException;

  /// Initiates a graceful shutdown, waiting up to `timeout` for in-flight messages.
  ///
  /// @param timeout maximum wait for in-flight drain
  /// @return true if shutdown completed cleanly
  boolean shutdownGracefully(final Duration timeout);

  /// Snapshot of the top `n` keys by current queue depth, deepest-first. Only meaningful for
  /// `ProcessingMode.KEY_ORDERED` consumers — returns an empty list for SEQUENTIAL and
  /// PARALLEL modes. Intended for ad-hoc diagnostics, not continuous metric emission. The
  /// null-keyed queue (records with a `null` Kafka key) appears with a `null` entry key in
  /// the result; `byte[]` keys are returned as a defensive copy.
  ///
  /// Default implementation returns an empty list so downstream `Handle` implementations
  /// outside this repository don't break when this method is added.
  ///
  /// @param n maximum number of entries to return (must be positive)
  /// @return ordered list of `(key, queueDepth)` entries; never null, may be empty
  default List<Map.Entry<byte[], Integer>> topKeyQueueDepths(final int n) {
    if (n <= 0) throw new IllegalArgumentException("n must be positive, got " + n);
    return List.of();
  }

  /// Default close — graceful shutdown with a 5-second timeout.
  @Override
  default void close() {
    shutdownGracefully(Duration.ofSeconds(5));
  }
}
