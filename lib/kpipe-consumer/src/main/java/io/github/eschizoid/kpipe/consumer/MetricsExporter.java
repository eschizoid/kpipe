package io.github.eschizoid.kpipe.consumer;

import java.time.Duration;

/// Publishes consumers' metrics to a monitoring backend. `kpipe-metrics-otel` provides one for
/// OpenTelemetry:
///
/// ```java
/// final var metrics = OtelMetrics.of(openTelemetry);
///
/// KPipe.json("orders", props)
///     .withMetrics(metrics)
///     .toCustom(sink)
///     .start();
/// ```
///
/// Each consumer calls [#start] once when it starts, with a source the exporter reads whenever its
/// backend collects, and closes the returned [Export] when it stops. Counts and gauges come from
/// that source, so they match `KPipeConsumer.metrics()`. Only processing time, which a histogram
/// needs record by record, is pushed, through [Export#recordProcessingTime].
///
/// One exporter may serve any number of consumers; each [#start] returns that consumer's own
/// [Export]. An exporter that throws never affects processing: the consumer logs the exception
/// and carries on, and if [#start] throws, that consumer runs without exporting.
@FunctionalInterface
public interface MetricsExporter {
  /// Starts exporting one consumer.
  ///
  /// @param source what the exporter reads from the consumer
  /// @return the consumer's export, which the consumer closes when it stops
  Export start(MetricsSource source);

  /// One consumer's export.
  interface Export extends AutoCloseable {
    /// An export that does nothing.
    Export NONE = new Export() {};

    /// Records one processed record's processing time, from receiving it to its sink with retries
    /// included. Called on per-record routes only; batch routes do not measure it.
    ///
    /// @param topic the record's topic
    /// @param time the record's processing time
    default void recordProcessingTime(final String topic, final Duration time) {}

    /// Tells the exporter this consumer has stopped; the exporter may keep reporting its final
    /// counts. The consumer calls it after its dispatcher drains and its batch buffers flush, so a
    /// final read of the source holds every count the shutdown completed.
    @Override
    default void close() {}
  }
}
