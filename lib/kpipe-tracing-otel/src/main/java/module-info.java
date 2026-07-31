/// KPipe Tracing OTel module — OpenTelemetry-backed implementation of the
/// [io.github.eschizoid.kpipe.tracing.Tracer] SPI.
///
/// Add this module only when you want W3C `traceparent` propagation across the Kafka boundary.
/// The library code in `kpipe-consumer` / `kpipe-producer` depends only on the `kpipe-tracing`
/// SPI and has no transitive dependency on the OpenTelemetry API. Wire-up is opt-in:
///
/// ```java
/// final var otel = GlobalOpenTelemetry.get();
/// builder.withTracer(new OtelTracer(otel, "my-pipeline"));
/// ```
///
/// Bring your own SDK (`io.opentelemetry:opentelemetry-sdk` + an exporter); this module only
/// requires `opentelemetry-api`. Propagation is W3C-only in v1 (no B3, Datadog, or custom
/// propagators).
module io.github.eschizoid.kpipe.tracing.otel {
// Only the SPI — this module deliberately does not require kpipe-producer or kpipe-consumer.
  requires transitive io.github.eschizoid.kpipe.tracing;
  // transitive: OtelTracer takes an OpenTelemetry (an io.opentelemetry.api type) in its public API,
  // so callers must read it too.
  requires transitive io.opentelemetry.api;
  requires kafka.clients;
  requires io.opentelemetry.context;

  exports io.github.eschizoid.kpipe.tracing.otel;
}
