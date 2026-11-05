/// KPipe Metrics OTel module — OpenTelemetry export of KPipe consumer and producer metrics.
///
/// Add this module only if you want to export KPipe metrics through the OpenTelemetry pipeline
/// (Prometheus, OTLP, etc.). The library code in `kpipe-metrics` ships interfaces only and
/// has no transitive dependency on the OpenTelemetry API. Wire-up is opt-in:
///
/// ```java
/// final var metrics = OtelMetrics.of(GlobalOpenTelemetry.get()); // one, shared by every consumer
/// builder.withMetrics(metrics);
/// ```
///
/// Bring your own SDK (`io.opentelemetry:opentelemetry-sdk` + an exporter); this module only
/// requires `opentelemetry-api`.
module io.github.eschizoid.kpipe.metrics.otel {
  requires transitive io.github.eschizoid.kpipe.metrics;
  requires transitive io.github.eschizoid.kpipe.core;
  requires transitive io.github.eschizoid.kpipe.consumer;
  // transitive: OtelMetrics and OtelProducerMetrics take an OpenTelemetry (an io.opentelemetry.api
  // type) in their public API, so callers must read it too.
  requires transitive io.opentelemetry.api;

  exports io.github.eschizoid.kpipe.metrics.otel;
}
