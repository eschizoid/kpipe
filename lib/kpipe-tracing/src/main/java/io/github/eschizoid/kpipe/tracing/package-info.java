/// Tracing SPI: one span per consumed record, and that span's context in the headers of every
/// record written on the record's behalf.
///
/// - [Tracer]: the SPI a tracing backend implements. The consumer calls
///   [Tracer#startConsumerSpan] once per record, continuing any trace the record's headers carry.
/// - [Tracer.SpanScope]: one record's span. The consumer makes it current on the record's thread
///   with [Tracer.SpanScope#activate()], records failed attempts and the final failure on it, and
///   closes it when the record is done. [Tracer.SpanScope#inject] writes its context into the
///   headers of an outgoing record on any thread.
/// - [TraceContext]: the span of the record being processed, bound in a scoped value around each
///   record's processing. A Kafka sink reads it to write the record's trace, so it takes no
///   tracer.
/// - [Tracer#noop()] and [Tracer.SpanScope#noop()]: the defaults when no backend is configured,
///   which record nothing.
///
/// `kpipe-otel` implements the SPI with OpenTelemetry. This module depends only on
/// `kafka-clients`, so `kpipe-consumer` and `kpipe-producer` carry no tracing vendor.
package io.github.eschizoid.kpipe.tracing;
