/// Tracing SPI for cross-Kafka-boundary context propagation.
///
/// - [Tracer] — the SPI implemented by tracing backends; produces span scopes around
///   producer sends and injects propagation headers (e.g. W3C `traceparent`) into Kafka records.
/// - [NoopTracer] / [NoopSpanScope] — zero-overhead defaults used when no backend is wired in.
///
/// An OpenTelemetry-backed implementation is provided by `kpipe-tracing-otel`. The library
/// code in `kpipe-consumer` / `kpipe-producer` depends only on this SPI and has no transitive
/// dependency on any tracing vendor.
package org.kpipe.producer.tracing;
