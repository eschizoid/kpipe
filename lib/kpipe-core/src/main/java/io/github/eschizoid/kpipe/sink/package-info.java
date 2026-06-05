/// Sink SPI and reusable sink building blocks.
///
/// [MessageSink] is the terminal stage of every KPipe pipeline — it consumes processed records
/// and is responsible for delivery (Kafka, console, file, database, custom). This package ships
/// the SPI plus a handful of format-agnostic helpers:
///
/// - [BatchSink] / [BatchPolicy] / [BatchResult] — batching support with size / time / byte
/// triggers.
/// - [CompositeMessageSink] — fan-out to multiple sinks while preserving back-pressure semantics.
/// - [ConsoleSinkSupport] — shared scaffolding for the per-format `*ConsoleSink` implementations.
///
/// Format-specific sinks (`JsonConsoleSink`, `AvroConsoleSink`, ...) live in their respective
/// `kpipe-format-*` modules; the Kafka-backed sink lives in `kpipe-producer`.
package io.github.eschizoid.kpipe.sink;
