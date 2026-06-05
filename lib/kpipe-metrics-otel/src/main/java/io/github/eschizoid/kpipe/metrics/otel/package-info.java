/// OpenTelemetry-backed implementation of the [io.github.eschizoid.kpipe.metrics] SPI.
///
/// - [OtelConsumerMetrics] — [io.github.eschizoid.kpipe.metrics.ConsumerMetrics] adapter that emits
// via the
///   OpenTelemetry `Meter` API.
/// - [OtelProducerMetrics] — [io.github.eschizoid.kpipe.metrics.ProducerMetrics] adapter for the
// producer side.
/// - [PipelineMetricsObserver] — pipeline-level observer that records per-stage timings and
///   throughput.
///
/// Wire-up is opt-in:
/// ```java
/// final var otel = GlobalOpenTelemetry.get();
/// builder.withMetrics(new OtelConsumerMetrics(otel));
/// ```
///
/// Bring your own SDK (`io.opentelemetry:opentelemetry-sdk` + an exporter); this module only
/// depends on `opentelemetry-api`.
package io.github.eschizoid.kpipe.metrics.otel;
