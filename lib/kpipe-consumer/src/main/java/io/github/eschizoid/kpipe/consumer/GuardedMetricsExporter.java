package io.github.eschizoid.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/// A consumer's side of its [MetricsExporter]: starts the export, forwards processing times to it,
/// and closes it. Anything the exporter throws is logged and never reaches the consumer's
/// processing, start or shutdown paths.
final class GuardedMetricsExporter {

  private static final Logger LOGGER = System.getLogger(GuardedMetricsExporter.class.getName());

  private final MetricsExporter exporter;
  private final AtomicBoolean recordFailureLogged = new AtomicBoolean();
  private volatile MetricsExporter.Export export = MetricsExporter.Export.NONE;

  /// @param exporter the exporter to guard, or null for none
  GuardedMetricsExporter(final MetricsExporter exporter) {
    this.exporter = exporter;
  }

  /// Starts the export. On failure the consumer runs without exporting.
  ///
  /// @param source what the exporter reads from the consumer
  void start(final MetricsSource source) {
    if (exporter == null) return;
    try {
      final var started = exporter.start(source);
      if (started != null) export = started;
    } catch (final RuntimeException | LinkageError e) {
      LOGGER.log(Level.WARNING, "Metrics exporter failed to start; this consumer's metrics will not be exported", e);
    }
  }

  /// @param topic the record's topic
  /// @param time the record's processing time
  void recordProcessingTime(final String topic, final Duration time) {
    try {
      export.recordProcessingTime(topic, time);
    } catch (final RuntimeException | LinkageError e) {
      final var level = recordFailureLogged.compareAndSet(false, true) ? Level.WARNING : Level.DEBUG;
      LOGGER.log(level, "Metrics exporter failed to record processing time", e);
    }
  }

  /// Closes the export; later processing times are dropped.
  void close() {
    final var closing = export;
    export = MetricsExporter.Export.NONE;
    try {
      closing.close();
    } catch (final RuntimeException | LinkageError e) {
      LOGGER.log(Level.WARNING, "Metrics exporter failed to close", e);
    }
  }
}
