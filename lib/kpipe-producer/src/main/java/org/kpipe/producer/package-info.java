/// Kafka producer runtime.
///
/// Wraps the Kafka producer client behind a KPipe-friendly API and provides the lifecycle,
/// flush, and error-handling plumbing that the Kafka-backed sink relies on. Sub-packages
/// split concerns:
///
/// - [org.kpipe.producer.config] — immutable producer-side configuration.
/// - [org.kpipe.producer.sink]   — the Kafka-backed [org.kpipe.sink.MessageSink] implementation.
/// - [org.kpipe.producer.tracing] — pluggable [Tracer] SPI for cross-Kafka-boundary tracing.
///
/// OpenTelemetry-backed tracing is provided by `kpipe-tracing-otel`; this module ships only
/// the SPI and a no-op default.
package org.kpipe.producer;
