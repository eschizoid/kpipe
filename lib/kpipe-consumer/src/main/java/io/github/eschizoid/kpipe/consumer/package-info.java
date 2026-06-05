/// Kafka consumer pipeline runtime.
///
/// Houses the consumer loop, offset-management, processing-engine, and lifecycle plumbing that
/// drives a KPipe stream end-to-end: poll → decode → process → sink → commit. Format-specific
/// codecs (fastjson2, Avro, Protobuf) live in the `kpipe-format-*` modules and are wired in via
/// the [io.github.eschizoid.kpipe.registry.MessageFormat] SPI.
///
/// Most users do not touch these types directly — they are exposed through the fluent
/// [io.github.eschizoid.kpipe.KPipe] / [io.github.eschizoid.kpipe.Stream] API in `kpipe-api`.
package io.github.eschizoid.kpipe.consumer;
