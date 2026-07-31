/// Kafka producer runtime.
///
/// Wraps the Kafka producer client behind a KPipe-friendly API and provides the lifecycle,
/// flush, and error-handling plumbing that the Kafka-backed sink relies on. Sub-packages
/// split concerns:
///
/// - [io.github.eschizoid.kpipe.producer.config] — immutable producer-side configuration.
/// - [io.github.eschizoid.kpipe.producer.sink]   — the Kafka-backed
/// [io.github.eschizoid.kpipe.sink.MessageSink] implementation.
///
/// Cross-Kafka-boundary tracing is pluggable via the
/// [io.github.eschizoid.kpipe.tracing.Tracer] SPI (the `kpipe-tracing` module, required
/// transitively); the OpenTelemetry-backed implementation is the opt-in `kpipe-tracing-otel`.
package io.github.eschizoid.kpipe.producer;
