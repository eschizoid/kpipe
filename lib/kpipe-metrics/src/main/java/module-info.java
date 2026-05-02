/// KPipe metrics module — interfaces only, no telemetry backend.
///
/// Backend implementations (e.g. OpenTelemetry) live in separate modules. Add
/// `kpipe-metrics-otel` if you want OpenTelemetry-backed metrics; otherwise the
/// no-op default has zero overhead.
module org.kpipe.metrics {
  exports org.kpipe.metrics;
}
