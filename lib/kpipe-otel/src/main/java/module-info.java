/// KPipe on OpenTelemetry: [io.github.eschizoid.kpipe.otel.KPipeOtel] builds a tracer and a
/// metrics exporter on the OpenTelemetry API. The rest of KPipe does not depend on OpenTelemetry,
/// so this module is needed only to trace or export metrics with it:
///
/// ```java
/// builder
///   .withTracer(KPipeOtel.tracer(openTelemetry))
///   .withMetrics(KPipeOtel.metrics(openTelemetry));
/// ```
///
/// Bring your own SDK and exporter; this module requires only `opentelemetry-api`.
module io.github.eschizoid.kpipe.otel {
// transitive: KPipeOtel returns the consumer's MetricsExporter and the tracing SPI's Tracer.
  requires transitive io.github.eschizoid.kpipe.consumer;
  // transitive: KPipeOtel takes an OpenTelemetry and returns a BatchCallback, both
  // io.opentelemetry.api types.
  requires transitive io.opentelemetry.api;
  requires io.opentelemetry.context;

  exports io.github.eschizoid.kpipe.otel;
}
