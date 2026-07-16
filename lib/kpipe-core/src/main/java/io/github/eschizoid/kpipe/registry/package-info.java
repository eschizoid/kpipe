/// Format-agnostic pipeline machinery.
///
/// Defines the core SPI contracts that the rest of KPipe builds on:
///
/// - [MessageFormat] — codecs that turn raw Kafka bytes into typed records and back.
/// - [MessagePipeline] / [TypedPipelineBuilder] — the processing-chain abstraction.
/// - [MessageProcessorRegistry], [RegistryFunctions], [RegistryKey] —
///   registry of named, composable processors.
/// - [Operators] — common transformation building blocks.
/// - [SchemaResolver] — pluggable schema lookup (inline, classpath, file, Confluent SR, ...).
/// - [Result] — sealed success/failure container returned by processors.
///
/// This package depends on no other KPipe module; consumer, producer, and format modules
/// all build on top of these types.
package io.github.eschizoid.kpipe.registry;
