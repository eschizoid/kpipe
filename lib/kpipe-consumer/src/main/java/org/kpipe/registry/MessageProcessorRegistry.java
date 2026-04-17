package org.kpipe.registry;

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
/// [TypedPipelineBuilder].
///
/// Example usage:
/// ```java
/// final var registry = new MessageProcessorRegistry("my-app");
/// var pipeline =
/// registry.pipeline(MessageFormat.JSON).add(RegistryKey.json("addTimestamp")).build();
///
/// // Optional: Wrap with error handling
/// final var safeSink = MessageSinkRegistry.withErrorHandling(new MySink());
/// registry.sinkRegistry().register(RegistryKey.json("safeSink"), safeSink);
/// ```
public class MessageProcessorRegistry {

  private static final System.Logger LOGGER = System.getLogger(MessageProcessorRegistry.class.getName());

  /// Pre-defined key for adding a source field to JSON messages.
  public static final RegistryKey<Map<String, Object>> JSON_ADD_SOURCE = RegistryKey.json("addSource");
  /// Pre-defined key for adding a timestamp field to JSON messages.
  public static final RegistryKey<Map<String, Object>> JSON_ADD_TIMESTAMP = RegistryKey.json("addTimestamp");
  /// Pre-defined key for marking JSON messages as processed.
  public static final RegistryKey<Map<String, Object>> JSON_MARK_PROCESSED = RegistryKey.json("markProcessed");

  private final ConcurrentHashMap<RegistryKey<?>, RegistryEntry<?>> registryMap = new ConcurrentHashMap<>();
  private final String sourceAppName;
  private final MessageFormat<?> messageFormat;

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
  public <T> void register(final RegistryKey<T> key, final UnaryOperator<T> operator) {
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
      register(RegistryKey.of(constant.name(), type), constant);
    }
  }

  /// Retrieves a typed operator from the registry.
  ///
  /// @param <T> The type of data the operator processes.
  /// @param key The type-safe key to retrieve.
  /// @return The registered operator, or a no-op operator if not found.
  @SuppressWarnings("unchecked")
  public <T> UnaryOperator<T> getOperator(final RegistryKey<T> key) {
    return input -> {
      final var entry = (RegistryEntry<UnaryOperator<T>>) registryMap.get(key);
      if (entry != null) return entry.apply(input);
      return input;
    };
  }

  /// Retrieves a message sink from the sink registry.
  ///
  /// @param <T> The type of data the sink processes.
  /// @param key The type-safe key to retrieve.
  /// @return The registered sink, or a no-op sink if not found.
  public <T> MessageSink<T> getSink(final RegistryKey<T> key) {
    return sinkRegistry.get(key);
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
      register(JSON_ADD_SOURCE, JsonMessageProcessor.addFieldOperator("source", sourceAppName));
      register(JSON_ADD_TIMESTAMP, JsonMessageProcessor.addTimestampOperator("timestamp"));
      register(JSON_MARK_PROCESSED, JsonMessageProcessor.addFieldOperator("processed", "true"));
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

  /// Unregisters a processor or sink.
  ///
  /// @param key The key to remove
  /// @return true if it was removed, false if it wasn't found
  public boolean unregister(final RegistryKey<?> key) {
    return registryMap.remove(key) != null || sinkRegistry.unregister(key);
  }

  /// Clears all processors and sinks from the registry.
  public void clear() {
    registryMap.clear();
    sinkRegistry.clear();
  }

  /// Returns an unmodifiable set of all registered processor keys.
  ///
  /// @return Unmodifiable set of all registered processor keys.
  public Set<RegistryKey<?>> getKeys() {
    return Collections.unmodifiableSet(registryMap.keySet());
  }

  /// Returns an unmodifiable map of all registered processors and sinks.
  ///
  /// @return Unmodifiable map of all keys and their simple class names
  public Map<RegistryKey<?>, String> getAll() {
    return RegistryFunctions.createUnmodifiableView(registryMap, value -> {
      final var entry = (RegistryEntry<?>) value;
      return "%s".formatted(entry.value().getClass().getSimpleName());
    });
  }

  /// Gets performance metrics for a specific processor or sink.
  ///
  /// @param key The key to look up
  /// @return Map containing metrics or empty map if not found
  public Map<String, Object> getMetrics(final RegistryKey<?> key) {
    final var entry = registryMap.get(key);
    if (entry != null) return entry.getMetrics();
    return sinkRegistry.getMetrics(key);
  }

  /// Wraps an operator with error handling logic that suppresses exceptions and returns the
  /// original input.
  ///
  /// @param operator The operator to wrap with error handling
  /// @param <T>      The type of message value
  /// @return An operator that handles errors during processing, returning original input on error
  public static <T> UnaryOperator<T> withErrorHandling(final UnaryOperator<T> operator) {
    return RegistryFunctions.withOperatorErrorHandling(operator, LOGGER);
  }
}
