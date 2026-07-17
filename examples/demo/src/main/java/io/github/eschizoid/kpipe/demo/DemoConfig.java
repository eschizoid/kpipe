package io.github.eschizoid.kpipe.demo;

/// Configuration for the demo application with all three pipeline topics.
///
/// @param bootstrapServers Kafka bootstrap servers
/// @param consumerGroup Consumer group ID prefix
/// @param schemaRegistryUrl Schema Registry URL
/// @param jsonTopic Topic for JSON messages
/// @param avroTopic Topic for Avro messages
/// @param protoTopic Topic for Protobuf messages
public record DemoConfig(
  String bootstrapServers,
  String consumerGroup,
  String schemaRegistryUrl,
  String jsonTopic,
  String avroTopic,
  String protoTopic
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
      env("PROTO_TOPIC", "proto-topic")
    );
  }

  private static String env(final String name, final String defaultValue) {
    final var value = System.getenv(name);
    return value != null && !value.isEmpty() ? value : defaultValue;
  }
}
