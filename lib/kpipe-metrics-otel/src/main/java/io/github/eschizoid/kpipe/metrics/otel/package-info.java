/// OpenTelemetry export of KPipe metrics.
///
/// - [OtelMetrics] exports consumers' metrics; create one per `OpenTelemetry` and share it.
/// - [OtelProducerMetrics] is the [io.github.eschizoid.kpipe.metrics.ProducerMetrics] adapter for
///   the producer side.
///
/// ```java
/// final var metrics = OtelMetrics.of(GlobalOpenTelemetry.get()); // one, shared by every consumer
/// builder.withMetrics(metrics);
/// ```
///
/// Bring your own SDK (`io.opentelemetry:opentelemetry-sdk` + an exporter); this module only
/// depends on `opentelemetry-api`.
package io.github.eschizoid.kpipe.metrics.otel;
