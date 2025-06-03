package org.kpipe.config;

import java.util.Properties;
import java.util.function.UnaryOperator;

/**
 * A utility class for creating and customizing Kafka consumer configuration properties.
 *
 * <p>Usage examples:
 *
 * <pre>{@code
 * // Basic consumer configuration
 * Properties consumerProps = KafkaConsumerConfig.createConsumerConfig(
 *     "localhost:9092",
 *     "my-consumer-group");
 *
 * // Consumer with customization
 * Properties customConsumerProps = KafkaConsumerConfig.createConsumerConfig(
 *     "localhost:9092",
 *     "my-consumer-group",
 *     props -> {
 *         Properties modified = new Properties();
 *         modified.putAll(props);
 *         modified.put("max.poll.records", "100");
 *         return modified;
 *     });
 *
 * // Using transformers
 * Properties noAutoCommitConsumer = KafkaConsumerConfig.createConsumerConfig(
 *     "localhost:9092",
 *     "my-consumer-group",
 *     KafkaConsumerConfig.withAutoCommitDisabled());
 *
 * // Using builder pattern
 * Properties builderProps = KafkaConsumerConfig.consumerBuilder()
 *     .withBootstrapServers("localhost:9092")
 *     .withGroupId("my-consumer-group")
 *     .withByteArrayDeserializers()
 *     .withAutoCommit(false)
 *     .withProperty("max.poll.records", "100")
 *     .build();
 * }</pre>
 */
public final class KafkaConsumerConfig {

  private KafkaConsumerConfig() {}

  /**
   * Creates configuration properties for a Kafka consumer with customization.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * // Basic usage
   * Properties props = KafkaConsumerConfig.createConsumerConfig(
   *     "localhost:9092",
   *     "my-consumer-group",
   *     p -> {
   *         Properties modified = new Properties();
   *         modified.putAll(p);
   *         modified.put("max.poll.records", "100");
   *         return modified;
   *     });
   *
   * // With transformer functions
   * Properties props = KafkaConsumerConfig.createConsumerConfig(
   *     "localhost:9092",
   *     "my-consumer-group",
   *     KafkaConsumerConfig.withAutoCommitDisabled());
   * }</pre>
   *
   * @param bootstrapServers Comma-separated list of host:port pairs for establishing the initial
   *     connection to the Kafka cluster
   * @param groupId The consumer group this consumer belongs to
   * @param customizer A function to apply additional configuration modifications
   * @return Properties configured for a Kafka consumer
   */
  public static Properties createConsumerConfig(
    final String bootstrapServers,
    final String groupId,
    final UnaryOperator<Properties> customizer
  ) {
    final var props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("group.id", groupId);
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("enable.auto.commit", "true");

    return customizer != null ? customizer.apply(props) : props;
  }

  /**
   * Creates default configuration properties for a Kafka consumer.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * Properties props = KafkaConsumerConfig.createConsumerConfig("localhost:9092", "my-consumer-group");
   * }</pre>
   *
   * @param bootstrapServers Comma-separated list of host:port pairs for establishing the initial
   *     connection to the Kafka cluster
   * @param groupId The consumer group this consumer belongs to
   * @return Properties configured for a Kafka consumer
   */
  public static Properties createConsumerConfig(final String bootstrapServers, final String groupId) {
    return createConsumerConfig(bootstrapServers, groupId, null);
  }

  /**
   * Creates a transformer that sets custom deserializers for keys and values.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * Properties props = KafkaConsumerConfig.createConsumerConfig(
   *     "localhost:9092",
   *     "my-consumer-group",
   *     KafkaConsumerConfig.withCustomDeserializers(
   *         "org.apache.kafka.common.serialization.StringDeserializer",
   *         "org.apache.kafka.common.serialization.StringDeserializer"
   *     ));
   * }</pre>
   *
   * @param keyDeserializer The fully qualified class name of the key deserializer
   * @param valueDeserializer The fully qualified class name of the value deserializer
   * @return A function that adds the custom deserializers to the properties
   */
  public static UnaryOperator<Properties> withCustomDeserializers(
    final String keyDeserializer,
    final String valueDeserializer
  ) {
    return props -> {
      final var newProps = new Properties();
      newProps.putAll(props);
      newProps.put("key.deserializer", keyDeserializer);
      newProps.put("value.deserializer", valueDeserializer);
      return newProps;
    };
  }

  /**
   * Creates a transformer that disables auto-commit for a consumer.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * // Use directly with createConsumerConfig
   * Properties props = KafkaConsumerConfig.createConsumerConfig(
   *     "localhost:9092",
   *     "my-consumer-group",
   *     KafkaConsumerConfig.withAutoCommitDisabled());
   * }</pre>
   *
   * @return A function that disables auto-commit in the properties
   */
  public static UnaryOperator<Properties> withAutoCommitDisabled() {
    return props -> {
      final var newProps = new Properties();
      newProps.putAll(props);
      newProps.put("enable.auto.commit", "false");
      return newProps;
    };
  }

  /**
   * Creates a transformer that sets a single property.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * Properties props = KafkaConsumerConfig.createConsumerConfig(
   *     "localhost:9092",
   *     "my-consumer-group",
   *     KafkaConsumerConfig.withProperty("max.poll.records", "100"));
   * }</pre>
   *
   * @param key The property key
   * @param value The property value
   * @return A function that adds the property to the configuration
   */
  public static UnaryOperator<Properties> withProperty(final String key, final String value) {
    return props -> {
      final var newProps = new Properties();
      newProps.putAll(props);
      newProps.put(key, value);
      return newProps;
    };
  }

  /**
   * Creates a new consumer configuration builder for fluent configuration.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * Properties props = KafkaConsumerConfig.consumerBuilder()
   *     .withBootstrapServers("localhost:9092,localhost:9093")
   *     .withGroupId("my-consumer-group")
   *     .withByteArrayDeserializers()
   *     .withAutoCommit(false)
   *     .withProperty("max.poll.records", "100")
   *     .withProperty("fetch.max.wait.ms", "500")
   *     .build();
   * }</pre>
   *
   * @return A new consumer configuration builder
   */
  public static ConsumerConfigBuilder consumerBuilder() {
    return new ConsumerConfigBuilder();
  }

  /**
   * A builder for creating consumer configurations in a fluent manner.
   *
   * <p>This class provides a fluent API for constructing Kafka consumer configurations step by
   * step.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * Properties props = KafkaConsumerConfig.consumerBuilder()
   *     .withBootstrapServers("localhost:9092")
   *     .withGroupId("my-consumer-group")
   *     .withByteArrayDeserializers()
   *     .withAutoCommit(false)
   *     .build();
   * }</pre>
   */
  public static class ConsumerConfigBuilder {

    private ConsumerConfigBuilder() {}

    private final Properties props = new Properties();

    /**
     * Sets the bootstrap servers.
     *
     * @param bootstrapServers Comma-separated list of host:port pairs
     * @return This builder for chaining
     */
    public ConsumerConfigBuilder withBootstrapServers(final String bootstrapServers) {
      props.put("bootstrap.servers", bootstrapServers);
      return this;
    }

    /**
     * Sets the consumer group ID.
     *
     * @param groupId The consumer group ID
     * @return This builder for chaining
     */
    public ConsumerConfigBuilder withGroupId(final String groupId) {
      props.put("group.id", groupId);
      return this;
    }

    /**
     * Configures byte array deserializers for both keys and values.
     *
     * @return This builder for chaining
     */
    public ConsumerConfigBuilder withByteArrayDeserializers() {
      props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      return this;
    }

    /**
     * Sets whether auto-commit should be enabled.
     *
     * @param enable True to enable auto-commit, false to disable
     * @return This builder for chaining
     */
    public ConsumerConfigBuilder withAutoCommit(final boolean enable) {
      props.put("enable.auto.commit", Boolean.toString(enable));
      return this;
    }

    /**
     * Adds a custom property to the configuration.
     *
     * @param key The property key
     * @param value The property value
     * @return This builder for chaining
     */
    public ConsumerConfigBuilder withProperty(final String key, final String value) {
      props.put(key, value);
      return this;
    }

    /**
     * Applies a custom transformer function to the current properties.
     *
     * @param customizer The transformer function to be applied
     * @return This builder for chaining
     */
    public ConsumerConfigBuilder with(final UnaryOperator<Properties> customizer) {
      final var updated = customizer.apply(props);
      props.clear();
      props.putAll(updated);
      return this;
    }

    /**
     * Builds the final properties object.
     *
     * @return The configured properties
     */
    public Properties build() {
      final var result = new Properties();
      result.putAll(props);
      return result;
    }
  }
}
