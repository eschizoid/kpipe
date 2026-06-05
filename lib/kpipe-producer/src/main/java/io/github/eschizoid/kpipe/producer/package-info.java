/// Kafka producer runtime.
///
/// Wraps the Kafka producer client behind a KPipe-friendly API and provides the lifecycle,
/// flush, and error-handling plumbing that the Kafka-backed sink relies on. Sub-packages
/// split concerns:
///
/// - [io.github.eschizoid.kpipe.producer.config] — immutable producer-side configuration.
/// - [io.github.eschizoid.kpipe.producer.sink]   — the Kafka-backed
// [io.github.eschizoid.kpipe.sink.MessageSink] implementation.
/// - [io.github.eschizoid.kpipe.producer.tracing] — pluggable [Tracer] SPI for cross-Kafka-boundary
// tracing.
///
/// OpenTelemetry-backed tracing is provided by `kpipe-tracing-otel`; this module ships only
/// the SPI and a no-op default.
package io.github.eschizoid.kpipe.producer;
