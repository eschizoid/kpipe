package org.kpipe.demo;

import java.time.Duration;

/// Configuration for the demo application with all three pipeline topics.
///
/// @param bootstrapServers Kafka bootstrap servers
/// @param consumerGroup Consumer group ID prefix
/// @param schemaRegistryUrl Schema Registry URL
/// @param jsonTopic Topic for JSON messages
/// @param avroTopic Topic for Avro messages
/// @param protoTopic Topic for Protobuf messages
/// @param pollTimeout Poll timeout duration
/// @param shutdownTimeout Shutdown timeout duration
/// @param metricsInterval Metrics reporting interval
public record DemoConfig(
  String bootstrapServers,
  String consumerGroup,
  String schemaRegistryUrl,
  String jsonTopic,
  String avroTopic,
  String protoTopic,
  Duration pollTimeout,
  Duration shutdownTimeout,
  Duration metricsInterval
) {
  private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://schema-registry:8081";

  /// Creates a DemoConfig from environment variables.
  public static DemoConfig fromEnv() {
    return new DemoConfig(
      env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
      env("KAFKA_CONSUMER_GROUP", "kpipe-demo"),
      env("SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY_URL),
      env("JSON_TOPIC", "json-topic"),
      env("AVRO_TOPIC", "avro-topic"),
      env("PROTO_TOPIC", "proto-topic"),
      Duration.ofMillis(Long.parseLong(env("KAFKA_POLL_TIMEOUT_MS", "100"))),
      Duration.ofSeconds(Long.parseLong(env("SHUTDOWN_TIMEOUT_SEC", "30"))),
      Duration.ofSeconds(Long.parseLong(env("METRICS_INTERVAL_SEC", "60")))
    );
  }

  private static String env(final String name, final String defaultValue) {
    final var value = System.getenv(name);
    return value != null && !value.isEmpty() ? value : defaultValue;
  }
}
