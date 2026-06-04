/// Producer-side configuration value types.
///
/// [KafkaProducerConfig] is an immutable carrier for the Kafka client properties (bootstrap
/// servers, serializers, acks, compression, batching, ...) used to construct the underlying
/// `KafkaProducer`. The type is pure data — no IO and no Kafka client coupling — so it stays
/// trivially testable and easy to populate from any configuration source.
package org.kpipe.producer.config;
