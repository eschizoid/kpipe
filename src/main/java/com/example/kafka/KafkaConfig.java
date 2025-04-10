package com.example.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Blocks the current thread until the application shuts down or the configured shutdown timeout is
 * reached. This method is typically called after {@link #start()} to keep the main thread alive
 * while the consumer runs.
 *
 * <p>Example usage in a standard application flow:
 *
 * <pre>
 * KafkaConfig config = KafkaConfig.fromEnv();
 * try (KafkaConsumerApp app = new KafkaConsumerApp(config)) {
 *     app.start();
 *     app.awaitShutdown(); // Block until shutdown signal or timeout
 * }
 * </pre>
 *
 * <p>Custom timeout example:
 *
 * <pre>
 * KafkaConfig config = new KafkaConfig(
 *     "localhost:9092",
 *     "my-group",
 *     "my-topic",
 *     "my-app",
 *     Duration.ofMillis(100),
 *     Duration.ofSeconds(60), // longer shutdown timeout
 *     Duration.ofMinutes(1),
 *     List.of("parseJson", "addTimestamp")
 * );
 * try (KafkaConsumerApp app = new KafkaConsumerApp(config)) {
 *     app.start();
 *     app.awaitShutdown();
 * }
 * </pre>
 */
public class KafkaConfig {

  public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);
  public static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);
  public static final Duration DEFAULT_METRICS_INTERVAL = Duration.ofMinutes(1);

  public final String bootstrapServers;
  public final String consumerGroup;
  public final String topic;
  public final String appName;
  public final Duration pollTimeout;
  public final Duration shutdownTimeout;
  public final Duration metricsInterval;
  public final List<String> processors;

  /**
   * Creates a new Kafka configuration with the specified parameters.
   *
   * @param bootstrapServers Kafka bootstrap servers (comma-separated list)
   * @param consumerGroup Kafka consumer group ID
   * @param topic Kafka topic to consume from
   * @param appName Application name for metrics and logging
   * @param pollTimeout Duration to wait in poll operations
   * @param shutdownTimeout Maximum duration to wait during graceful shutdown
   * @param metricsInterval Interval between metrics reporting
   * @param processors List of processor names to use in the processing pipeline
   */
  public KafkaConfig(
    final String bootstrapServers,
    final String consumerGroup,
    final String topic,
    final String appName,
    final Duration pollTimeout,
    final Duration shutdownTimeout,
    final Duration metricsInterval,
    final List<String> processors
  ) {
    this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "Bootstrap servers cannot be null");
    this.consumerGroup = Objects.requireNonNull(consumerGroup, "Consumer group cannot be null");
    this.topic = Objects.requireNonNull(topic, "Topic cannot be null");
    this.appName = Objects.requireNonNull(appName, "App name cannot be null");

    this.pollTimeout = validateDuration(pollTimeout, DEFAULT_POLL_TIMEOUT, "Poll timeout");
    this.shutdownTimeout = validateDuration(shutdownTimeout, DEFAULT_SHUTDOWN_TIMEOUT, "Shutdown timeout");
    this.metricsInterval = validateDuration(metricsInterval, DEFAULT_METRICS_INTERVAL, "Metrics interval");

    this.processors = processors != null ? List.copyOf(processors) : List.of();
  }

  /**
   * Validates that a duration is not null and not negative.
   *
   * @param duration The duration to validate
   * @param defaultValue Default value to use if duration is null
   * @param name Name of the duration parameter for error messages
   * @return The validated duration or default value
   * @throws IllegalArgumentException if duration is negative
   */
  private Duration validateDuration(final Duration duration, final Duration defaultValue, final String name) {
    final var result = duration != null ? duration : defaultValue;
    if (result.isNegative()) {
      throw new IllegalArgumentException(name + " cannot be negative");
    }
    return result;
  }

  /**
   * Creates a configuration from environment variables with sensible defaults.
   *
   * @return A new KafkaConfig instance configured from environment
   */
  public static KafkaConfig fromEnv() {
    return new KafkaConfig(
      getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
      getEnvOrDefault("KAFKA_CONSUMER_GROUP", "kpipe-group"),
      getEnvOrDefault("KAFKA_TOPIC", "json-topic"),
      getEnvOrDefault("APP_NAME", "kafka-consumer-app"),
      parseDurationWithFallback(getEnvOrDefault("KAFKA_POLL_TIMEOUT_MS", "100"), "100", Duration::ofMillis),
      parseDurationWithFallback(getEnvOrDefault("SHUTDOWN_TIMEOUT_SEC", "30"), "30", Duration::ofSeconds),
      parseDurationWithFallback(getEnvOrDefault("METRICS_INTERVAL_SEC", "60"), "60", Duration::ofSeconds),
      List.of(getEnvOrDefault("PROCESSOR_PIPELINE", "parseJson,addSource,markProcessed,addTimestamp").split(","))
    );
  }

  /**
   * Parses a string to duration with error handling.
   *
   * @param value String value to parse
   * @param defaultValue Default value to use if parsing fails
   * @param converter Function to convert parsed long to Duration
   * @return The resulting Duration
   */
  private static Duration parseDurationWithFallback(
    final String value,
    final String defaultValue,
    final Function<Long, Duration> converter
  ) {
    try {
      return converter.apply(Long.parseLong(value));
    } catch (final NumberFormatException e) {
      return converter.apply(Long.parseLong(defaultValue));
    }
  }

  /**
   * Gets environment variable or returns default if not set.
   *
   * @param name Environment variable name
   * @param defaultValue Default value if environment variable is not set
   * @return Value from environment or default
   */
  public static String getEnvOrDefault(final String name, final String defaultValue) {
    final String value = System.getenv(name);
    return value != null && !value.isEmpty() ? value : defaultValue;
  }
}
