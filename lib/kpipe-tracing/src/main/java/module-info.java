/// KPipe Tracing module — the vendor-neutral [io.github.eschizoid.kpipe.tracing.Tracer] SPI for
/// cross-Kafka-boundary trace propagation.
///
/// Ships no concrete implementation: `Tracer.noop()` is the zero-cost default used when tracing
/// is not configured. The OpenTelemetry-backed implementation lives in the opt-in
/// `kpipe-tracing-otel` module, which requires only this SPI — not `kpipe-producer` or
/// `kpipe-consumer`.
module io.github.eschizoid.kpipe.tracing {
// transitive: the Tracer signatures expose ConsumerRecord and Headers, so readers of this
// module must read kafka.clients too.
  requires transitive kafka.clients;

  exports io.github.eschizoid.kpipe.tracing;
}
