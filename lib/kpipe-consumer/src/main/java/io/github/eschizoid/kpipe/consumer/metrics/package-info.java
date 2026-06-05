/// Consumer-side metrics reporters.
///
/// Bridges runtime counters / timers from the consumer loop into the
/// [io.github.eschizoid.kpipe.metrics] SPI. [EntryMetricsReporter] is the per-record reporter
/// invoked at the entry of the processing pipeline — it records throughput,
/// lag, and per-stage timings without coupling the consumer to any specific
/// telemetry backend.
///
/// Concrete backends (OpenTelemetry, ...) are supplied by separate modules
/// such as `kpipe-metrics-otel`.
package io.github.eschizoid.kpipe.consumer.metrics;
