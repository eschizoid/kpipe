/// Metrics SPI — interfaces only, no telemetry backend.
///
/// - [ProducerMetrics] — producer-side counters (sends and failures, DLQ writes), with a
///   zero-overhead [ProducerMetrics#noop()] default.
/// - [KPipeMetricsReporter] — a reporter the consumer calls on a fixed interval, to write metrics
///   somewhere such as a log.
///
/// Consumer metrics are exported through `MetricsExporter` in `kpipe-consumer`;
/// `kpipe-metrics-otel` implements both for OpenTelemetry.
package io.github.eschizoid.kpipe.metrics;
