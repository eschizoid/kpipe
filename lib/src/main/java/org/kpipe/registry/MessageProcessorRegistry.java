package org.kpipe.registry;

import java.lang.System.Logger;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;
import org.kpipe.processor.JsonMessageProcessor;
import org.kpipe.sink.MessageSink;

/// Registry for managing and composing message processors in KPipe.
///
/// This class allows registration, retrieval, and composition of message processors for different
/// formats (JSON, Avro, Protobuf, POJO). It supports type-safe pipelines for Kafka message
/// processing and provides utilities for building and composing processing chains via
// [TypedPipelineBuilder].
///
/// Example usage:
/// ```java
/// final var registry = new MessageProcessorRegistry("my-app");
/// var pipeline =
// registry.pipeline(MessageFormat.JSON).add(RegistryKey.json("addTimestamp")).build();
/// ```
public class MessageProcessorRegistry {

  private static final Logger LOGGER = System.getLogger(MessageProcessorRegistry.class.getName());

  /// Pre-defined key for adding a source field to JSON messages.
  public static final RegistryKey<Map<String, Object>> JSON_ADD_SOURCE = RegistryKey.json("addSource");
  /// Pre-defined key for adding a timestamp field to JSON messages.
  public static final RegistryKey<Map<String, Object>> JSON_ADD_TIMESTAMP = RegistryKey.json("addTimestamp");
  /// Pre-defined key for marking JSON messages as processed.
  public static final RegistryKey<Map<String, Object>> JSON_MARK_PROCESSED = RegistryKey.json("markProcessed");

  private final ConcurrentHashMap<RegistryKey<?>, RegistryEntry<?>> registryMap = new ConcurrentHashMap<>();
  private final String sourceAppName;
  private final MessageFormat<?> messageFormat;

  /// A registry entry that maintains execution statistics.
  private static class RegistryEntry<T> {

    final T value;
    final java.util.concurrent.atomic.LongAdder invocationCount = new java.util.concurrent.atomic.LongAdder();
    final java.util.concurrent.atomic.LongAdder errorCount = new java.util.concurrent.atomic.LongAdder();
    final java.util.concurrent.atomic.LongAdder totalProcessingTimeNs = new java.util.concurrent.atomic.LongAdder();

    RegistryEntry(final T value) {
      this.value = value;
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

    @SuppressWarnings("unchecked")
    public <V> V apply(final V input) {
      final var start = System.nanoTime();
      try {
        final var result = ((UnaryOperator<V>) value).apply(input);
        invocationCount.increment();
        totalProcessingTimeNs.add(System.nanoTime() - start);
        return result;
      } catch (final Exception e) {
        errorCount.increment();
        throw e;
      }
    }

    @SuppressWarnings("unchecked")
    public <V> void accept(final V input) {
      final var start = System.nanoTime();
      try {
        ((MessageSink<V>) value).accept(input);
        invocationCount.increment();
        totalProcessingTimeNs.add(System.nanoTime() - start);
      } catch (final Exception e) {
        errorCount.increment();
        throw e;
      }
    }
  }

  private final MessageSinkRegistry sinkRegistry = new MessageSinkRegistry();

  /// Returns the sink registry associated with this processor registry.
  ///
  /// @return The MessageSinkRegistry instance
  public MessageSinkRegistry sinkRegistry() {
    return sinkRegistry;
  }

  /// Creates a fluent builder for typed pipelines.
  ///
  /// @param format The message format for serialization/deserialization.
  /// @param <T>    The type of the object in the pipeline.
  /// @return A new TypedPipelineBuilder.
  ///
  /// Example usage:
  /// ```java
  /// final var registry = new MessageProcessorRegistry("my-app");
  /// final var pipeline = registry.pipeline(MessageFormat.JSON)
  ///     .add(RegistryKey.json("addTimestamp"))
  ///     .build();
  /// ```
  public <T> TypedPipelineBuilder<T> pipeline(final MessageFormat<T> format) {
    return new TypedPipelineBuilder<>(format, this);
  }

  /// Registers a typed operator using a type-safe RegistryKey.
  ///
  /// @param <T> The type of data the operator processes
  /// @param key The type-safe key to register under
  /// @param operator The operator to register
  public <T> void registerOperator(final RegistryKey<T> key, final UnaryOperator<T> operator) {
    registryMap.put(key, new RegistryEntry<>(operator));
  }

  /// Registers all constants of an Enum that implements UnaryOperator<T>.
  ///
  /// Each constant's name is used as the key.
  ///
  /// @param <T> The type of data the operator processes
  /// @param <E> The Enum type that implements UnaryOperator<T>
  /// @param type The class representing the data type
  /// @param enumClass The Enum class to register
  public <T, E extends Enum<E> & UnaryOperator<T>> void registerEnum(final Class<T> type, final Class<E> enumClass) {
    Objects.requireNonNull(type, "Type cannot be null");
    Objects.requireNonNull(enumClass, "Enum class cannot be null");
    for (final var constant : enumClass.getEnumConstants()) {
      registerOperator(RegistryKey.of(constant.name(), type), constant);
    }
  }

  /// Retrieves a typed operator using a type-safe RegistryKey.
  ///
  /// @param <T> The type of data the operator processes
  /// @param key The type-safe key to retrieve
  /// @param operator The operator to wrap
  /// @return The wrapped operator, or the original operator if no wrapping is needed
  @SuppressWarnings("unchecked")
  public <T> UnaryOperator<T> wrapOperator(final RegistryKey<T> key, final UnaryOperator<T> operator) {
    return input -> {
      final var entry = (RegistryEntry<UnaryOperator<T>>) registryMap.get(key);
      if (entry == null) return operator.apply(input);
      return entry.apply(input);
    };
  }

  /// Wraps a sink with additional functionality, such as metrics collection.
  ///
  /// @param <T> The type of data the sink processes
  /// @param key The type-safe key to retrieve
  /// @param sink The sink to wrap
  /// @return The wrapped sink, or the original sink if no wrapping is needed
  @SuppressWarnings("unchecked")
  public <T> MessageSink<T> wrapSink(final RegistryKey<T> key, final MessageSink<T> sink) {
    return input -> {
      final var entry = (RegistryEntry<MessageSink<T>>) registryMap.get(key);
      if (entry != null) {
        entry.accept(input);
      } else {
        final var registeredSink = sinkRegistry.get(key);
        if (registeredSink != null) registeredSink.accept(input);
        else sink.accept(input);
      }
    };
  }

  /// Retrieves a typed operator using a type-safe RegistryKey.
  ///
  /// @param <T> The type of data the operator processes
  /// @param key The type-safe key to retrieve
  /// @return The registered operator, or null if not found
  @SuppressWarnings("unchecked")
  public <T> UnaryOperator<T> getOperator(final RegistryKey<T> key) {
    return input -> {
      final var entry = (RegistryEntry<UnaryOperator<T>>) registryMap.get(key);
      if (entry != null) {
        return entry.apply(input);
      }
      return input;
    };
  }

  /// Creates a new registry with JSON as the default message format.
  ///
  /// @param sourceAppName Application name to use as source identifier
  public MessageProcessorRegistry(final String sourceAppName) {
    this(sourceAppName, MessageFormat.JSON);
  }

  /// Creates a new registry with the specified message format.
  ///
  /// @param sourceAppName Application name to use as source identifier
  /// @param messageFormat Message format to use (JSON, AVRO, PROTOBUF)
  public MessageProcessorRegistry(final String sourceAppName, final MessageFormat<?> messageFormat) {
    this.sourceAppName = Objects.requireNonNull(sourceAppName, "Source app name cannot be null");
    this.messageFormat = Objects.requireNonNull(messageFormat, "Message format cannot be null");

    registerDefaultProcessors();
  }

  /// Registers default processors based on the configured message format.
  private void registerDefaultProcessors() {
    // Register default operators for optimized pipelines
    if (messageFormat == MessageFormat.JSON) {
      registerOperator(JSON_ADD_SOURCE, JsonMessageProcessor.addFieldOperator("source", sourceAppName));
      registerOperator(JSON_ADD_TIMESTAMP, JsonMessageProcessor.addTimestampOperator("timestamp"));
      registerOperator(JSON_MARK_PROCESSED, JsonMessageProcessor.addFieldOperator("processed", "true"));
    }
  }

  /// Adds a schema and registers its processors.
  ///
  /// @param key                The schema key
  /// @param fullyQualifiedName The fully qualified name of the schema
  /// @param location           The schema location or content
  public void addSchema(final String key, final String fullyQualifiedName, final String location) {
    // Register the schema with MessageFormat
    messageFormat.addSchema(key, fullyQualifiedName, location);
  }

  /// Unregisters a processor.
  ///
  /// @param key The key of the processor to remove
  /// @return true if the processor was removed, false if it wasn't found
  public boolean unregister(final RegistryKey<?> key) {
    return registryMap.remove(key) != null;
  }

  /// Clears all processors from the registry.
  public void clear() {
    registryMap.clear();
  }

  /// Returns all registered processor keys.
  ///
  /// @return Unmodifiable set of all registered processor keys.
  public Set<RegistryKey<?>> getKeys() {
    return Collections.unmodifiableSet(registryMap.keySet());
  }

  /// Gets metrics for a processor.
  ///
  /// @param key The processor key
  /// @return Map containing metrics or empty map if processor not found
  public Map<String, Object> getMetrics(final RegistryKey<?> key) {
    final var entry = registryMap.get(key);
    if (entry == null) return Map.of();
    return entry.getMetrics();
  }
}
