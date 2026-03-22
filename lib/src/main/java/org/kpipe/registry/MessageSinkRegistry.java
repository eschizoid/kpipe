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
import org.kpipe.sink.JsonConsoleSink;
import org.kpipe.sink.MessageSink;

/// Registry for managing message sinks in KPipe pipelines.
///
/// This class provides registration, retrieval, and composition of message sinks for Kafka
/// consumers.
/// It supports metrics, error handling, and sink pipelines for flexible message delivery and
/// monitoring.
///
/// Example usage:
/// ```java
/// final var registry = new MessageSinkRegistry();
/// final var key = RegistryKey.json("mySink");
/// registry.register(key, String.class, (record, value) -> System.out.println(value));
/// final var sink = registry.get(key, String.class);
/// ```
public class MessageSinkRegistry {

  private static final Logger LOGGER = System.getLogger(MessageSinkRegistry.class.getName());
  private final ConcurrentHashMap<RegistryKey<?>, SinkEntry<?, ?>> registry = new ConcurrentHashMap<>();

  /// Pre-defined key for the JSON logging sink.
  public static final RegistryKey<byte[]> JSON_LOGGING = RegistryKey.of("jsonLogging", byte[].class);
  /// Pre-defined key for the Avro logging sink.
  public static final RegistryKey<byte[]> AVRO_LOGGING = RegistryKey.of("avroLogging", byte[].class);

  private static class SinkEntry<K, V> {

    final MessageSink<K, V> sink;
    final Class<K> keyType;
    final Class<V> valueType;
    long messageCount = 0;
    long errorCount = 0;
    long totalProcessingTimeMs = 0;

    SinkEntry(final MessageSink<K, V> sink, final Class<K> keyType, final Class<V> valueType) {
      this.sink = sink;
      this.keyType = keyType;
      this.valueType = valueType;
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

  /// Constructs a new `MessageSinkRegistry` object with a default logging sink.
  ///
  /// Example:
  ///
  /// ```java
  /// // Create a new registry with the default logging sink
  /// final var registry = new MessageSinkRegistry();
  /// ```
  public MessageSinkRegistry() {
    register(JSON_LOGGING, byte[].class, new JsonConsoleSink<>());
    register(AVRO_LOGGING, byte[].class, new AvroConsoleSink<>());
  }

  /// Registers a new message sink with the specified key and types.
  ///
  /// Example:
  ///
  /// ```java
  /// // Register a custom database sink with explicit types
  /// final var dbKey = RegistryKey.json("database");
  /// registry.register(dbKey, String.class, new DatabaseSink<>());
  /// ```
  ///
  /// @param key The type-safe key for the sink
  /// @param keyType The class representing the message key type
  /// @param sink The sink implementation to register
  /// @param <K> The type of message key
  /// @param <V> The type of message value
  public <K, V> void register(final RegistryKey<V> key, final Class<K> keyType, final MessageSink<K, V> sink) {
    Objects.requireNonNull(key, "Sink key cannot be null");
    Objects.requireNonNull(sink, "Sink implementation cannot be null");
    Objects.requireNonNull(keyType, "Key type cannot be null");

    final var entry = new SinkEntry<>(sink, keyType, key.type());
    registry.put(key, entry);
  }

  /// Unregisters a sink by key.
  ///
  /// @param key The key of the sink to remove
  /// @return true if the sink was removed, false if it wasn't found
  public boolean unregister(final RegistryKey<?> key) {
    return registry.remove(key) != null;
  }

  /// Removes all registered sinks.
  ///
  /// Example:
  ///
  /// ```java
  /// // Clear the registry when shutting down
  /// registry.clear();
  /// ```
  public void clear() {
    registry.clear();
  }

  /// Retrieves a sink by key from the registry, verifying the expected types.
  ///
  /// Example:
  ///
  /// ```java
  /// // Get the logging sink with type verification
  /// MessageSink<String, Map<String, Object>> consoleSink = registry.get(
  ///     RegistryKey.json("logging"), String.class);
  /// ```
  ///
  /// @param <K> the type of the message key
  /// @param <V> the type of the message value
  /// @param key the type-safe key of the sink to retrieve
  /// @param keyType the expected class of the message key
  /// @return the sink, or null if not found
  /// @throws IllegalArgumentException if the registered types do not match the requested types
  @SuppressWarnings("unchecked")
  public <K, V> MessageSink<K, V> get(final RegistryKey<V> key, final Class<K> keyType) {
    final var entry = (SinkEntry<K, V>) registry.get(key);
    if (entry == null) return null;

    if (!entry.keyType.isAssignableFrom(keyType)) {
      throw new IllegalArgumentException(
        "Key type mismatch for sink '" +
        key.name() +
        "'. Registered: " +
        entry.keyType.getSimpleName() +
        ", Requested: " +
        keyType.getSimpleName()
      );
    }
    return entry::send;
  }

  /// Creates a composite sink that sends messages to multiple sinks identified by keys.
  ///
  /// @param keyType The class representing the message key type
  /// @param sinkKeys Keys of sinks to include in the composite
  /// @param <K> The type of message key
  /// @param <V> The type of message value
  /// @return A composite sink that delegates to all specified sinks
  @SafeVarargs
  public final <K, V> MessageSink<K, V> pipeline(final Class<K> keyType, final RegistryKey<V>... sinkKeys) {
    return (record, processedValue) -> {
      for (final var key : sinkKeys) {
        final var sink = this.get(key, keyType);
        if (sink != null) {
          try {
            sink.send(record, processedValue);
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error sending to sink: %s".formatted(key.name()), e);
          }
        }
      }
    };
  }

  /// Returns an unmodifiable map of all registered sinks.
  ///
  /// @return Unmodifiable map of all sink keys and their class names
  public Map<RegistryKey<?>, String> getAll() {
    return RegistryFunctions.createUnmodifiableView(
      registry,
      entry -> {
        final var sinkEntry = (SinkEntry<?, ?>) entry;
        return "%s(%s, %s)".formatted(
            sinkEntry.sink.getClass().getSimpleName(),
            sinkEntry.keyType.getSimpleName(),
            sinkEntry.valueType.getSimpleName()
          );
      }
    );
  }

  /// Gets performance metrics for a specific sink.
  ///
  /// @param key The sink key
  /// @return Map containing metrics or empty map if sink not found
  public Map<String, Object> getMetrics(final RegistryKey<?> key) {
    final var entry = registry.get(key);
    if (entry == null) return Map.of();
    return RegistryFunctions.createMetrics(entry.messageCount, entry.errorCount, entry.totalProcessingTimeMs);
  }

  /// Wraps a sink with error handling logic that suppresses exceptions.
  ///
  /// Example:
  ///
  /// ```java
  /// // Get a sink that might throw exceptions
  /// final var unreliableKey = RegistryKey.json("unreliableSink");
  /// MessageSink<String, Map<String, Object>> riskySink = registry.get(unreliableKey,
  /// String.class);
  ///
  /// // Wrap it with error handling
  /// MessageSink<String, Map<String, Object>> safeSink =
  /// MessageSinkRegistry.withErrorHandling(riskySink);
  ///
  /// // Safely send messages
  /// safeSink.send(record, processedValue); // Won't throw exceptions
  /// ```
  ///
  /// @param sink The sink to wrap with error handling
  /// @param <K> The type of message key
  /// @param <V> The type of message value
  /// @return A sink that handles errors during processing
  public static <K, V> MessageSink<K, V> withErrorHandling(final MessageSink<K, V> sink) {
    return RegistryFunctions.withConsumerErrorHandling(sink::send, LOGGER)::accept;
  }
} // end MessageSinkRegistry
