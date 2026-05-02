/// KPipe metrics OTel module — OpenTelemetry-backed implementation of [kpipe-metrics]
/// interfaces.
module org.kpipe.metrics.otel {
  requires org.kpipe.metrics;
  requires io.opentelemetry.api;

  exports org.kpipe.metrics.otel;
}
