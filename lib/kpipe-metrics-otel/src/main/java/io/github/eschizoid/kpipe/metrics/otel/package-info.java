/// OpenTelemetry export of KPipe consumer metrics: [OtelMetrics]. Create one per `OpenTelemetry`
/// and share it across every consumer.
///
/// ```java
/// final var metrics = OtelMetrics.of(GlobalOpenTelemetry.get());
/// builder.withMetrics(metrics);
/// ```
///
/// Bring your own SDK (`io.opentelemetry:opentelemetry-sdk` + an exporter); this module only
/// depends on `opentelemetry-api`.
package io.github.eschizoid.kpipe.metrics.otel;
