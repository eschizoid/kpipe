/// KPipe Metrics OTel module — OpenTelemetry-backed implementation of `kpipe-metrics` interfaces.
///
/// Add this module only if you want to export KPipe metrics through the OpenTelemetry pipeline
/// (Prometheus, OTLP, Jaeger, etc.). The library code in `kpipe-metrics` ships interfaces only and
/// has no transitive dependency on the OpenTelemetry API. Wire-up is opt-in:
///
/// ```java
/// final var otel = GlobalOpenTelemetry.get();
/// final var consumerMetrics = new OtelConsumerMetrics(otel);
/// builder.withMetrics(consumerMetrics);
/// ```
///
/// Bring your own SDK (`io.opentelemetry:opentelemetry-sdk` + an exporter); this module only
/// requires `opentelemetry-api`.
module io.github.eschizoid.kpipe.metrics.otel {
  requires io.github.eschizoid.kpipe.metrics;
  requires transitive io.github.eschizoid.kpipe.core;
  requires io.opentelemetry.api;

  exports io.github.eschizoid.kpipe.metrics.otel;
}
