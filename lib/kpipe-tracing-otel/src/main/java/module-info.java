/// KPipe's OpenTelemetry tracer: `OtelTracer` implements the
/// [io.github.eschizoid.kpipe.tracing.Tracer] SPI on the OpenTelemetry API. `kpipe-consumer` and
/// `kpipe-producer` depend only on that SPI, so this module, and the OpenTelemetry API with it, is
/// needed only when tracing is wanted:
///
/// ```java
/// builder.withTracer(new OtelTracer(openTelemetry));
/// ```
///
/// Bring your own SDK and exporter; this module requires only `opentelemetry-api`. Context travels
/// through whichever text-map propagator the `OpenTelemetry` instance has, such as W3C trace
/// context or B3.
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
