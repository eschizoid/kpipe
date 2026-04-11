package org.kpipe.producer.config;

import java.util.Properties;
import java.util.function.UnaryOperator;

/// A utility class for creating and customizing Kafka producer configuration properties.
///
/// Usage examples:
///
/// ```java
/// // Basic producer configuration
/// final var producerProps = KafkaProducerConfig.createProducerConfig(
///     "localhost:9092");
///
/// // Producer with customization
/// final var customProducerProps = KafkaProducerConfig.createProducerConfig(
///     "localhost:9092",
///     props -> {
///         Properties modified = new Properties();
///         modified.putAll(props);
///         modified.put("acks", "all");
///         return modified;
///     });
///
/// // Using transformers
/// final var acksAllProducer = KafkaProducerConfig.createProducerConfig(
///     "localhost:9092",
///     KafkaProducerConfig.withAcks("all"));
///
/// // Using builder pattern
/// final var builderProps = KafkaProducerConfig.producerBuilder()
///     .withBootstrapServers("localhost:9092")
///     .withByteArraySerializers()
///     .withAcks("all")
///     .withProperty("retries", "10")
///     .build();
/// ```
public final class KafkaProducerConfig {

  private KafkaProducerConfig() {}

  /// Creates configuration properties for a Kafka producer with customization.
  ///
  /// Example usage:
  ///
  /// ```java
  /// // Basic usage
  /// final var props = KafkaProducerConfig.createProducerConfig(
  ///     "localhost:9092",
  ///     p -> {
  ///         Properties modified = new Properties();
  ///         modified.putAll(p);
  ///         modified.put("acks", "all");
  ///         return modified;
  ///     });
  ///
  /// // With transformer functions
  /// final var props = KafkaProducerConfig.createProducerConfig(
  ///     "localhost:9092",
  ///     KafkaProducerConfig.withAcks("all"));
  /// ```
  ///
  /// @param bootstrapServers Comma-separated list of host:port pairs for establishing the initial
  ///     connection to the Kafka cluster
  /// @param customizer A function to apply additional configuration modifications
  /// @return Properties configured for a Kafka producer
  public static Properties createProducerConfig(
    final String bootstrapServers,
    final UnaryOperator<Properties> customizer
  ) {
    final var props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("acks", "1");

    return customizer != null ? customizer.apply(props) : props;
  }

  /// Creates default configuration properties for a Kafka producer.
  ///
  /// Example usage:
  ///
  /// ```java
  /// final var props = KafkaProducerConfig.createProducerConfig("localhost:9092");
  /// ```
  ///
  /// @param bootstrapServers Comma-separated list of host:port pairs for establishing the initial
  ///     connection to the Kafka cluster
  /// @return Properties configured for a Kafka producer
  public static Properties createProducerConfig(final String bootstrapServers) {
    return createProducerConfig(bootstrapServers, null);
  }

  /// Creates a transformer that sets custom serializers for keys and values.
  ///
  /// Example usage:
  ///
  /// ```java
  /// final var props = KafkaProducerConfig.createProducerConfig(
  ///     "localhost:9092",
  ///     KafkaProducerConfig.withCustomSerializers(
  ///         "org.apache.kafka.common.serialization.StringSerializer",
  ///         "org.apache.kafka.common.serialization.StringSerializer"
  ///     ));
  /// ```
  ///
  /// @param keySerializer The fully qualified class name of the key serializer
  /// @param valueSerializer The fully qualified class name of the value serializer
  /// @return A function that adds the custom serializers to the properties
  public static UnaryOperator<Properties> withCustomSerializers(
    final String keySerializer,
    final String valueSerializer
  ) {
    return props -> {
      final var newProps = new Properties();
      newProps.putAll(props);
      newProps.put("key.serializer", keySerializer);
      newProps.put("value.serializer", valueSerializer);
      return newProps;
    };
  }

  /// Creates a transformer that sets the acknowledgment level.
  ///
  /// Example usage:
  ///
  /// ```java
  /// final var props = KafkaProducerConfig.createProducerConfig(
  ///     "localhost:9092",
  ///     KafkaProducerConfig.withAcks("all"));
  /// ```
  ///
  /// @param acks The acknowledgment level (e.g., "0", "1", "all")
  /// @return A function that sets the acknowledgment level in the properties
  public static UnaryOperator<Properties> withAcks(final String acks) {
    return props -> {
      final var newProps = new Properties();
      newProps.putAll(props);
      newProps.put("acks", acks);
      return newProps;
    };
  }

  /// Creates a transformer that sets a single property.
  ///
  /// Example usage:
  ///
  /// ```java
  /// final var props = KafkaProducerConfig.createProducerConfig(
  ///     "localhost:9092",
  ///     KafkaProducerConfig.withProperty("retries", "10"));
  /// ```
  ///
  /// @param key The property key
  /// @param value The property value
  /// @return A function that adds the property to the configuration
  public static UnaryOperator<Properties> withProperty(final String key, final String value) {
    return props -> {
      final var newProps = new Properties();
      newProps.putAll(props);
      newProps.put(key, value);
      return newProps;
    };
  }

  /// Creates a new producer configuration builder for fluent configuration.
  ///
  /// Example usage:
  ///
  /// ```java
  /// final var props = KafkaProducerConfig.producerBuilder()
  ///     .withBootstrapServers("localhost:9092,localhost:9093")
  ///     .withByteArraySerializers()
  ///     .withAcks("all")
  ///     .withProperty("retries", "10")
  ///     .build();
  /// ```
  ///
  /// @return A new ProducerConfigBuilder instance
  public static ProducerConfigBuilder producerBuilder() {
    return new ProducerConfigBuilder();
  }

  /// A builder for creating producer configurations in a fluent manner.
  ///
  /// This class provides a fluent API for constructing Kafka producer configurations step by
  /// step.
  ///
  /// Example usage:
  ///
  /// ```java
  /// final var props = KafkaProducerConfig.producerBuilder()
  ///     .withBootstrapServers("localhost:9092")
  ///     .withByteArraySerializers()
  ///     .withAcks("all")
  ///     .build();
  /// ```
  public static class ProducerConfigBuilder {

    private ProducerConfigBuilder() {}

    private final Properties props = new Properties();

    /// Sets the bootstrap servers.
    ///
    /// @param bootstrapServers Comma-separated list of host:port pairs
    /// @return This builder for chaining
    public ProducerConfigBuilder withBootstrapServers(final String bootstrapServers) {
      props.put("bootstrap.servers", bootstrapServers);
      return this;
    }

    /// Configures byte array serializers for both keys and values.
    ///
    /// @return This builder for chaining
    public ProducerConfigBuilder withByteArraySerializers() {
      props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      return this;
    }

    /// Sets the acknowledgment level.
    ///
    /// @param acks The acknowledgment level
    /// @return This builder for chaining
    public ProducerConfigBuilder withAcks(final String acks) {
      props.put("acks", acks);
      return this;
    }

    /// Adds a custom property to the configuration.
    ///
    /// @param key The property key
    /// @param value The property value
    /// @return This builder for chaining
    public ProducerConfigBuilder withProperty(final String key, final String value) {
      props.put(key, value);
      return this;
    }

    /// Applies a custom transformer function to the current properties.
    ///
    /// @param customizer The transformer function to be applied
    /// @return This builder for chaining
    public ProducerConfigBuilder with(final UnaryOperator<Properties> customizer) {
      final var updated = customizer.apply(this.props);
      if (updated != this.props) {
        this.props.clear();
        this.props.putAll(updated);
      }
      return this;
    }

    /// Builds the final properties object.
    ///
    /// @return The configured properties
    public Properties build() {
      final var result = new Properties();
      result.putAll(props);
      return result;
    }
  }
}
