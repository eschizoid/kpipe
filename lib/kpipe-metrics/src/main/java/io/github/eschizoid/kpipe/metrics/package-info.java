/// Metrics SPI — interfaces only, no telemetry backend.
///
/// Defines the contracts that the consumer and producer runtimes call into to publish
/// counters, gauges, and timers:
///
/// - [ConsumerMetrics] / [ConsumerMetricsReporter] — consumer-side counters (records, lag, errors,
/// ...).
/// - [ProducerMetrics] — producer-side counters (sends, bytes, errors, ...).
/// - [KPipeMetricsReporter] — root reporter type used by the pipeline.
/// - [NoopConsumerMetrics] / [NoopProducerMetrics] — zero-overhead defaults used when no
///   backend is wired in.
///
/// Concrete backends live in separate modules. Add `kpipe-metrics-otel` to publish through
/// the OpenTelemetry pipeline (Prometheus, OTLP, Jaeger, ...); otherwise the no-op default
/// has zero overhead.
package io.github.eschizoid.kpipe.metrics;
