/// KPipe Tracing OTel module — OpenTelemetry-backed implementation of the
// [org.kpipe.producer.tracing.Tracer] SPI.
///
/// Add this module only when you want W3C `traceparent` propagation across the Kafka boundary.
/// The library code in `kpipe-consumer` / `kpipe-producer` ships the SPI only and has no
/// transitive dependency on the OpenTelemetry API. Wire-up is opt-in:
///
/// ```java
/// final var otel = GlobalOpenTelemetry.get();
/// builder.withTracer(new OtelTracer(otel, "my-pipeline"));
/// ```
///
/// Bring your own SDK (`io.opentelemetry:opentelemetry-sdk` + an exporter); this module only
/// requires `opentelemetry-api`. Propagation is W3C-only in v1 (no B3, Datadog, or custom
/// propagators).
module org.kpipe.tracing.otel {
  requires org.kpipe.producer;
  requires io.opentelemetry.api;
  requires kafka.clients;

  exports org.kpipe.tracing.otel;
}
