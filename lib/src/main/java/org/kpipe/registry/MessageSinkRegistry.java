package org.kpipe.registry;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.kpipe.sink.AvroConsoleSink;
import org.kpipe.sink.MessageSink;

/**
 * A registry for managing message sinks used in Kafka message processing pipelines.
 *
 * <p>The {@code MessageSinkRegistry} provides a centralized repository for registering, retrieving,
 * and managing various message sinks. It supports metrics collection, pipeline creation, and error
 * handling for Kafka message consumers.
 *
 * <p>Key features include:
 *
 * <ul>
 *   <li>Registration and management of named message sinks
 *   <li>Creation of sink pipelines to send messages to multiple destinations
 *   <li>Built-in metrics tracking for each registered sink
 *   <li>Error handling utilities to prevent pipeline failures
 *   <li>Pre-registered logging sink for convenient debugging
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create a registry with default sinks
 * MessageSinkRegistry registry = new MessageSinkRegistry();
 *
 * // Register custom sinks
 * registry.register("database", new DatabaseSink<String, JsonNode>());
 * registry.register("metrics", new MetricsSink<String, JsonNode>());
 *
 * // Create a pipeline of multiple sinks
 * MessageSink<String, JsonNode> pipeline = registry.pipeline("logging", "database", "metrics");
 *
 * // Use the pipeline with a Kafka consumer
 * ConsumerRecord<String, JsonNode> record = consumer.poll();
 * JsonNode processedValue = processor.apply(record.value());
 * pipeline.send(record, processedValue);
 *
 * // Get metrics for a specific sink
 * Map<String, Object> metrics = registry.getMetrics("database");
 * }</pre>
 */
public class MessageSinkRegistry {

  private static final Logger LOGGER = System.getLogger(MessageSinkRegistry.class.getName());
  private final ConcurrentHashMap<String, SinkEntry<?, ?>> registry = new ConcurrentHashMap<>();

  private static class SinkEntry<K, V> {

    final MessageSink<K, V> sink;
    long messageCount = 0;
    long errorCount = 0;
    long totalProcessingTimeMs = 0;

    SinkEntry(final MessageSink<K, V> sink) {
      this.sink = sink;
    }

    public void send(final ConsumerRecord<K, V> record, final V processedValue) {
      final Supplier<Long> counterIncrement = () -> messageCount++;
      final Supplier<Long> errorIncrement = () -> errorCount++;
      final Consumer<Duration> timeAccumulator = duration -> totalProcessingTimeMs += duration.toMillis();

      final var timedExecution = RegistryFunctions.<V, Void>timedExecution(
        counterIncrement,
        errorIncrement,
        timeAccumulator
      );

      timedExecution.apply(
        processedValue,
        (V value) -> {
          sink.send(record, value);
          return null;
        }
      );
    }
  }

  /**
   * Constructs a new {@code MessageSinkRegistry} object with a default logging sink.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a new registry with the default logging sink
   * MessageSinkRegistry registry = new MessageSinkRegistry();
   * }</pre>
   */
  public MessageSinkRegistry() {
    // register("jsonLogging", new
    // JsonConsoleSink<>(System.getLogger(JsonConsoleSink.class.getName()), Level.INFO));
    register("avroLogging", new AvroConsoleSink<>(System.getLogger(AvroConsoleSink.class.getName()), Level.INFO));
  }

  /**
   * Registers a new message sink with the specified name.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Register a custom database sink
   * registry.register("database", new DatabaseSink<String, JsonNode>());
   * }</pre>
   *
   * @param name The unique name for the sink
   * @param sink The sink implementation to register
   * @param <K> The type of message key
   * @param <V> The type of message value
   * @throws NullPointerException if name or sink is null
   * @throws IllegalArgumentException if name is empty
   */
  public <K, V> void register(final String name, final MessageSink<K, V> sink) {
    Objects.requireNonNull(name, "Sink name cannot be null");
    Objects.requireNonNull(sink, "Sink function cannot be null");
    if (name.trim().isEmpty()) throw new IllegalArgumentException("Processor name cannot be empty");
    final var entry = new SinkEntry<>(sink);
    registry.put(name, entry);
  }

  /**
   * Unregisters a sink by name.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Remove a sink that's no longer needed
   * boolean wasRemoved = registry.unregister("temporarySink");
   * if (wasRemoved) {
   *     System.out.println("Sink was successfully removed");
   * }
   * }</pre>
   *
   * @param name The name of the sink to remove
   * @return true if the sink was removed, false if it wasn't found
   */
  public boolean unregister(final String name) {
    return registry.remove(name) != null;
  }

  /**
   * Removes all registered sinks.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Clear the registry when shutting down
   * registry.clear();
   * }</pre>
   */
  public void clear() {
    registry.clear();
  }

  /**
   * Retrieves a sink by name from the registry.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Get the logging sink
   * MessageSink<String, JsonNode> consoleSink = registry.get("logging");
   *
   * // Send a processed message to the sink
   * consoleSink.send(record, processedValue);
   * }</pre>
   *
   * @param name The name of the sink to retrieve
   * @param <K> The type of message key
   * @param <V> The type of message value
   * @return The sink, or null if not found
   */
  @SuppressWarnings("unchecked")
  public <K, V> MessageSink<K, V> get(final String name) {
    final var entry = (SinkEntry<K, V>) registry.get(name);
    if (entry == null) return null;
    return entry::send;
  }

  /**
   * Creates a composite sink that sends messages to multiple named sinks.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a composite sink that sends to multiple destinations
   * MessageSink<String, JsonNode> multiSink = registry.composite(
   *     "logging", "database", "monitoring"
   * );
   *
   * // Process a message through all sinks at once
   * multiSink.send(record, processedValue);
   * }</pre>
   *
   * @param sinkNames Names of sinks to include in the composite
   * @param <K> The type of message key
   * @param <V> The type of message value
   * @return A composite sink that delegates to all specified sinks
   */
  public <K, V> MessageSink<K, V> pipeline(final String... sinkNames) {
    return (record, processedValue) -> {
      for (final var name : sinkNames) {
        final var sink = this.<K, V>get(name);
        if (sink != null) {
          try {
            sink.send(record, processedValue);
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error sending to sink: %s".formatted(name), e);
          }
        }
      }
    };
  }

  /**
   * Returns an unmodifiable map of all registered sinks with their class names.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Get all registered sinks
   * Map<String, String> allSinks = registry.getAll();
   *
   * // List all available sink names and their types
   * allSinks.forEach((name, className) ->
   *     System.out.println(name + ": " + className)
   * );
   * }</pre>
   *
   * @return Unmodifiable map of all sink names and their class names
   */
  public Map<String, String> getAll() {
    return RegistryFunctions.createUnmodifiableView(
      registry,
      entry -> ((SinkEntry<?, ?>) entry).sink.getClass().getSimpleName()
    );
  }

  /**
   * Gets performance metrics for a specific sink.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Get metrics for the database sink
   * Map<String, Object> metrics = registry.getMetrics("database");
   * System.out.println("Messages processed: " + metrics.get("messageCount"));
   * System.out.println("Errors: " + metrics.get("errorCount"));
   * System.out.println("Avg processing time (ms): " + metrics.get("averageProcessingTimeMs"));
   * }</pre>
   *
   * @param name The sink name
   * @return Map containing metrics or empty map if sink not found
   */
  public Map<String, Object> getMetrics(final String name) {
    final var entry = registry.get(name);
    if (entry == null) return Map.of();
    return RegistryFunctions.createMetrics(entry.messageCount, entry.errorCount, entry.totalProcessingTimeMs);
  }

  /**
   * Wraps a sink with error handling logic that suppresses exceptions.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Get a sink that might throw exceptions
   * MessageSink<String, JsonNode> riskySink = registry.get("unreliableSink");
   *
   * // Wrap it with error handling
   * MessageSink<String, JsonNode> safeSink = MessageSinkRegistry.withErrorHandling(riskySink);
   *
   * // Safely send messages
   * safeSink.send(record, processedValue); // Won't throw exceptions
   * }</pre>
   *
   * @param sink The sink to wrap with error handling
   * @param <K> The type of message key
   * @param <V> The type of message value
   * @return A sink that handles errors during processing
   */
  public static <K, V> MessageSink<K, V> withErrorHandling(final MessageSink<K, V> sink) {
    return RegistryFunctions.withConsumerErrorHandling(sink::send, LOGGER)::accept;
  }
}
