/// Consumer-side configuration value types.
///
/// Immutable carriers for the settings that drive a KPipe consumer:
///
/// - [AppConfig] — top-level application config (bootstrap, group, topics, ...).
/// - [KafkaConsumerConfig] — Kafka client properties (deserializers, offset reset, polling, ...).
/// - [HealthConfig] — opt-in HTTP health-server settings consumed by
// `io.github.eschizoid.kpipe.health`.
///
/// These types are pure data — no IO, no Kafka client coupling — so they remain trivially
/// testable and easy to construct from any configuration source (env, file, YAML, ...).
package io.github.eschizoid.kpipe.consumer.config;
