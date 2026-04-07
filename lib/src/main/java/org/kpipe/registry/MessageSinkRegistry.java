package org.kpipe.registry;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.generic.GenericRecord;
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
  private final ConcurrentHashMap<RegistryKey<?>, RegistryEntry<?>> registry = new ConcurrentHashMap<>();

  /// Pre-defined key for the JSON logging sink (Map based).
  public static final RegistryKey<Map<String, Object>> JSON_LOGGING = RegistryKey.json("jsonLogging");
  /// Pre-defined key for the Avro logging sink (GenericRecord based).
  public static final RegistryKey<GenericRecord> AVRO_LOGGING = RegistryKey.avro("avroLogging");

  /// Constructs a new `MessageSinkRegistry` object with default logging sinks.
  public MessageSinkRegistry() {
    register(JSON_LOGGING, new JsonConsoleSink<>());
    register(AVRO_LOGGING, new AvroConsoleSink<>());
  }

  /// Registers all constants of an Enum that implements MessageSink<T>.
  ///
  /// Each constant's name is used as the key.
  ///
  /// @param <T>       The type of data the sink processes
  /// @param <E>       The Enum type that implements MessageSink<T>
  /// @param type      The class representing the data type
  /// @param enumClass The Enum class to register
  public <T, E extends Enum<E> & MessageSink<T>> void registerEnum(final Class<T> type, final Class<E> enumClass) {
    Objects.requireNonNull(type, "Type cannot be null");
    Objects.requireNonNull(enumClass, "Enum class cannot be null");
    for (final var constant : enumClass.getEnumConstants()) {
      register(RegistryKey.of(constant.name(), type), constant);
    }
  }

  /// Registers a new message sink with the specified key.
  ///
  /// @param key  The type-safe key for the sink
  /// @param sink The sink implementation to register
  /// @param <T>  The type of message value
  public <T> void register(final RegistryKey<T> key, final MessageSink<T> sink) {
    Objects.requireNonNull(key, "Sink key cannot be null");
    Objects.requireNonNull(sink, "Sink implementation cannot be null");

    registry.put(key, new RegistryEntry<>(sink));
  }

  /// Unregisters a sink by key.
  ///
  /// @param key The key of the sink to remove
  /// @return true if the sink was removed, false if it wasn't found
  public boolean unregister(final RegistryKey<?> key) {
    return registry.remove(key) != null;
  }

  /// Removes all registered sinks.
  public void clear() {
    registry.clear();
  }

  /// Retrieves a sink by key from the registry.
  ///
  /// @param <T> the type of the message value
  /// @param key the type-safe key of the sink to retrieve
  /// @return the sink, or null if not found
  @SuppressWarnings("unchecked")
  public <T> MessageSink<T> get(final RegistryKey<T> key) {
    return value -> {
      final var entry = (RegistryEntry<MessageSink<T>>) registry.get(key);
      if (entry != null) {
        entry.accept(value);
      } else {
        LOGGER.log(Level.WARNING, "No sink found in registry for key: {0}", key);
      }
    };
  }

  /// Creates a composite sink that sends objects to multiple sinks identified by keys.
  ///
  /// @param sinkKeys Keys of sinks to include in the composite
  /// @param <T> The type of the processed object
  /// @return A composite sink that delegates to all specified sinks
  @SafeVarargs
  public final <T> MessageSink<T> pipeline(final RegistryKey<T>... sinkKeys) {
    return TypedPipelineBuilder.compositeSink(this, sinkKeys);
  }

  /// Returns an unmodifiable map of all registered sinks.
  ///
  /// @return Unmodifiable map of all sink keys and their class names
  public Map<RegistryKey<?>, String> getAll() {
    return RegistryFunctions.createUnmodifiableView(registry, value -> {
      final var entry = (RegistryEntry<?>) value;
      final var sink = entry.value();
      return "%s".formatted(sink.getClass().getSimpleName());
    });
  }

  /// Gets performance metrics for a specific sink.
  ///
  /// @param key The sink key
  /// @return Map containing metrics or empty map if sink not found
  public Map<String, Object> getMetrics(final RegistryKey<?> key) {
    final var entry = registry.get(key);
    if (entry == null) return Map.of();
    return entry.getMetrics();
  }

  /// Wraps a sink with error handling logic that suppresses exceptions.
  ///
  /// @param sink The sink to wrap with error handling
  /// @param <T>  The type of message value
  /// @return A sink that handles errors during processing
  public static <T> MessageSink<T> withErrorHandling(final MessageSink<T> sink) {
    return RegistryFunctions.withConsumerErrorHandling(sink, LOGGER)::accept;
  }
}
