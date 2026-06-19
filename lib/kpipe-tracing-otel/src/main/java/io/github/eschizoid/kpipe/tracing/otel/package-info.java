/// OpenTelemetry-backed implementation of the [io.github.eschizoid.kpipe.producer.tracing.Tracer]
/// SPI.
///
/// - [OtelTracer] — wraps an OpenTelemetry `Tracer` and produces span scopes around producer
///   sends and consumer record processing. Injects / extracts W3C `traceparent` (and
///   `tracestate`) on Kafka record headers via private nested adapters.
///
/// Wire-up is opt-in:
/// ```java
/// final var otel = GlobalOpenTelemetry.get();
/// builder.withTracer(new OtelTracer(otel, "my-pipeline"));
/// ```
///
/// Propagation is W3C-only in v1 (no B3, Datadog, or custom propagators). Bring your own SDK
/// (`io.opentelemetry:opentelemetry-sdk` + an exporter); this module only depends on
/// `opentelemetry-api`.
package io.github.eschizoid.kpipe.tracing.otel;
