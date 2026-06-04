/// OpenTelemetry-backed implementation of the [org.kpipe.metrics] SPI.
///
/// - [OtelConsumerMetrics] — [org.kpipe.metrics.ConsumerMetrics] adapter that emits via the
///   OpenTelemetry `Meter` API.
/// - [OtelProducerMetrics] — [org.kpipe.metrics.ProducerMetrics] adapter for the producer side.
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
package org.kpipe.metrics.otel;
