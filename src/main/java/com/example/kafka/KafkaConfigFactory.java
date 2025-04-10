package com.example.kafka;

import java.util.Properties;

/**
 * A factory class that creates configuration properties for Kafka clients.
 *
 * <p>This utility class provides methods to create standardized Kafka configuration properties with
 * appropriate defaults for different client types (consumers, producers). It centralizes Kafka
 * configuration settings to ensure consistent configuration across the application.
 *
 * <p>The factory currently supports:
 *
 * <ul>
 *   <li>Consumer configuration with ByteArray serialization
 * </ul>
 */
public class KafkaConfigFactory {

  /**
   * Creates configuration properties for a Kafka consumer using ByteArray serialization.
   *
   * <p>This method configures the following properties:
   *
   * <ul>
   *   <li>Bootstrap servers (Kafka broker addresses)
   *   <li>Consumer group ID for consumer group management
   *   <li>ByteArray deserializers for both keys and values
   *   <li>Auto-commit enabled for offset management
   * </ul>
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create basic consumer configuration
   * Properties consumerProps = KafkaConfigFactory.createConsumerConfig(
   *     "localhost:9092",
   *     "my-consumer-group"
   * );
   *
   * // Create consumer with the configuration
   * KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);
   *
   * // Or use with the FunctionalKafkaConsumer
   * FunctionalKafkaConsumer<byte[], byte[]> functionalConsumer =
   *     new FunctionalKafkaConsumer<>(consumerProps, "topic-name", messageProcessor, pollTimeout);
   * }</pre>
   *
   * @param bootstrapServers Comma-separated list of host:port pairs for Kafka brokers
   * @param groupId Consumer group identifier for this consumer
   * @return Properties object configured for a Kafka consumer
   */
  public static Properties createConsumerConfig(String bootstrapServers, String groupId) {
    final var props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("group.id", groupId);
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("enable.auto.commit", "true");
    return props;
  }
}
