/// Kafka-backed [io.github.eschizoid.kpipe.sink.MessageSink] implementation.
///
/// [KafkaMessageSink] writes processed records to a Kafka topic, honoring the surrounding
/// pipeline's tracing, metrics, and batching contracts. Use it as the terminal stage of a
/// stream when you want to fan out to another topic (relay, enrichment, dead-letter, ...).
package io.github.eschizoid.kpipe.producer.sink;
