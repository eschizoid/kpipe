package org.kpipe.registry;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

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
  private final ConcurrentHashMap<RegistryKey<?>, SinkEntry<?>> registry = new ConcurrentHashMap<>();

  /// Pre-defined key for the JSON logging sink.
  public static final RegistryKey<byte[]> JSON_LOGGING = RegistryKey.of("jsonLogging", byte[].class);
  /// Pre-defined key for the Avro logging sink.
  public static final RegistryKey<byte[]> AVRO_LOGGING = RegistryKey.of("avroLogging", byte[].class);

  /// Pre-defined key for the JSON map logging sink.
  public static final RegistryKey<Map<String, Object>> JSON_MAP_LOGGING = RegistryKey.json("jsonLogging");
  /// Pre-defined key for the Avro generic record logging sink.
  public static final RegistryKey<GenericRecord> AVRO_GENERIC_LOGGING = RegistryKey.avro("avroLogging");

  private static class SinkEntry<T> {

    final MessageSink<T> sink;
    final Class<T> valueType;
    final LongAdder invocationCount = new LongAdder();
    final LongAdder errorCount = new LongAdder();
    final LongAdder totalProcessingTimeNs = new LongAdder();

    SinkEntry(final MessageSink<T> sink, final Class<T> valueType) {
      this.sink = sink;
      this.valueType = valueType;
    }

    public Map<String, Object> getMetrics() {
      final long count = invocationCount.sum();
      final long errors = errorCount.sum();
      final long timeNs = totalProcessingTimeNs.sum();
      final var metrics = new ConcurrentHashMap<String, Object>();
      metrics.put("invocationCount", count);
      metrics.put("errorCount", errors);
      metrics.put("averageProcessingTimeMs", count > 0 ? (timeNs / count) / 1_000_000.0 : 0);
      return metrics;
    }

    public void accept(final T processedValue) {
      final var start = System.nanoTime();
      try {
        sink.accept(processedValue);
        invocationCount.increment();
        totalProcessingTimeNs.add(System.nanoTime() - start);
      } catch (final Exception e) {
        errorCount.increment();
        throw e;
      }
    }
  }

  /// Constructs a new `MessageSinkRegistry` object with default logging sinks.
  public MessageSinkRegistry() {
    register(JSON_LOGGING, new JsonConsoleSink<>());
    register(AVRO_LOGGING, new AvroConsoleSink<>());
    register(JSON_MAP_LOGGING, new JsonConsoleSink<>());
    register(AVRO_GENERIC_LOGGING, new AvroConsoleSink<>());
  }

  /// Registers a new message sink with the specified key.
  ///
  /// @param key The type-safe key for the sink
  /// @param sink The sink implementation to register
  /// @param <T> The type of message value
  public <T> void register(final RegistryKey<T> key, final MessageSink<T> sink) {
    Objects.requireNonNull(key, "Sink key cannot be null");
    Objects.requireNonNull(sink, "Sink implementation cannot be null");

    final var entry = new SinkEntry<>(sink, key.type());
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
      final var entry = (SinkEntry<T>) registry.get(key);
      if (entry != null) {
        entry.accept(value);
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
    return processedValue -> {
      for (final var key : sinkKeys) {
        final var sink = this.get(key);
        if (sink != null) {
          try {
            sink.accept(processedValue);
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
    return RegistryFunctions.createUnmodifiableView(registry, entry -> {
      final var sinkEntry = (SinkEntry<?>) entry;
      return "%s(%s)".formatted(sinkEntry.sink.getClass().getSimpleName(), sinkEntry.valueType.getSimpleName());
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
  /// @param <T> The type of message value
  /// @return A sink that handles errors during processing
  public static <T> MessageSink<T> withErrorHandling(final MessageSink<T> sink) {
    return RegistryFunctions.withConsumerErrorHandling(sink::accept, LOGGER)::accept;
  }
}
