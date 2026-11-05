/// KPipe metrics module — interfaces only, no telemetry backend.
///
/// The producer metrics SPI (`ProducerMetrics`, with a no-op default) and `KPipeMetricsReporter`.
/// Consumer metrics are exported through `MetricsExporter` in `kpipe-consumer`;
/// `kpipe-metrics-otel` implements both for OpenTelemetry.
module io.github.eschizoid.kpipe.metrics {
  exports io.github.eschizoid.kpipe.metrics;
}
