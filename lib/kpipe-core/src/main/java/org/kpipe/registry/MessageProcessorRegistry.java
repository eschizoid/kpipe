package org.kpipe.registry;

import java.lang.System.Logger.Level;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;

/// Registry for managing and composing message processors in KPipe.
///
/// This class allows registration, retrieval, and composition of message processors for any
/// supported format (JSON via `kpipe-format-json`, Avro via `kpipe-format-avro`, Protobuf via
/// `kpipe-format-protobuf`, or a custom [MessageFormat] implementation). It supports type-safe
/// pipelines for Kafka message processing and provides utilities for building and composing
/// processing chains via [TypedPipelineBuilder].
///
/// Example usage:
/// ```java
/// final var registry = new MessageProcessorRegistry();
/// var pipeline =
///   registry.pipeline(JsonFormat.INSTANCE).add(RegistryKey.json("addTimestamp")).build();
///
/// // Optional: Wrap with error handling
/// final var safeSink = MessageSinkRegistry.withErrorHandling(new MySink());
/// registry.sinkRegistry().register(RegistryKey.json("safeSink"), safeSink);
/// ```
public class MessageProcessorRegistry {

  private static final System.Logger LOGGER = System.getLogger(MessageProcessorRegistry.class.getName());

  private final ConcurrentHashMap<RegistryKey<?>, RegistryEntry<?>> registryMap = new ConcurrentHashMap<>();
  private volatile String sourceAppName;
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
  /// final var registry = new MessageProcessorRegistry();
  /// final var pipeline = registry.pipeline(JsonFormat.INSTANCE)
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
  /// If no operator is registered under `key`, the returned operator passes the input through
  /// unchanged and logs a WARNING. A missing operator is almost always a configuration error;
  /// silent pass-through would let pipelines drop their transforms unnoticed.
  ///
  /// @param <T> The type of data the operator processes.
  /// @param key The type-safe key to retrieve.
  /// @return The registered operator, or a logging pass-through operator if not found.
  @SuppressWarnings("unchecked")
  public <T> UnaryOperator<T> getOperator(final RegistryKey<T> key) {
    return input -> {
      final var entry = (RegistryEntry<UnaryOperator<T>>) registryMap.get(key);
      if (entry != null) return entry.apply(input);
      LOGGER.log(Level.WARNING, "No operator registered under key %s — passing input through unchanged".formatted(key));
      return input;
    };
  }

  /// Creates a new registry with the byte-passthrough format and an empty source app name.
  ///
  /// This is the preferred form when `sourceAppName` is not needed. Use
  // [#withSourceAppName(String)]
  /// to set the app name later if required.
  public MessageProcessorRegistry() {
    this("", MessageFormat.bytes());
  }

  /// Creates a new registry with the specified message format and an empty source app name.
  ///
  /// This is the preferred form when `sourceAppName` is not needed. Use
  // [#withSourceAppName(String)]
  /// to set the app name later if required.
  ///
  /// @param messageFormat Message format to use (e.g. `JsonFormat.INSTANCE`, `AvroFormat.INSTANCE`,
  ///                      `ProtobufFormat.INSTANCE`, `MessageFormat.bytes()`, or a custom impl)
  public MessageProcessorRegistry(final MessageFormat<?> messageFormat) {
    this("", messageFormat);
  }

  /// Creates a new registry with the byte-passthrough format as the default.
  ///
  /// Kept for backwards compatibility. Prefer the no-arg [#MessageProcessorRegistry()] constructor
  /// (combined with [#withSourceAppName(String)] when needed) when `sourceAppName` is not required.
  ///
  /// @param sourceAppName Application name to use as source identifier
  public MessageProcessorRegistry(final String sourceAppName) {
    this(sourceAppName, MessageFormat.bytes());
  }

  /// Creates a new registry with the specified message format.
  ///
  /// Kept for backwards compatibility. Prefer the format-only
  // [#MessageProcessorRegistry(MessageFormat)]
  /// constructor (combined with [#withSourceAppName(String)] when needed) when `sourceAppName` is
  /// not required.
  ///
  /// @param sourceAppName Application name to use as source identifier
  /// @param messageFormat Message format to use (e.g. `JsonFormat.INSTANCE`, `AvroFormat.INSTANCE`,
  ///                      `ProtobufFormat.INSTANCE`, `MessageFormat.bytes()`, or a custom impl)
  public MessageProcessorRegistry(final String sourceAppName, final MessageFormat<?> messageFormat) {
    this.sourceAppName = Objects.requireNonNull(sourceAppName, "Source app name cannot be null");
    this.messageFormat = Objects.requireNonNull(messageFormat, "Message format cannot be null");
  }

  /// Returns the source application name this registry was created with — useful for
  // format-specific
  /// helper modules that register default operators (e.g. an "addSource" processor that stamps the
  /// app name onto each message).
  ///
  /// @return the source app name
  public String sourceAppName() {
    return sourceAppName;
  }

  /// Sets the source application name fluently. Useful when constructing the registry via the
  /// no-arg or format-only constructors and the app name needs to be supplied later.
  ///
  /// @param name the source application name (must not be null)
  /// @return this registry instance for chaining
  public MessageProcessorRegistry withSourceAppName(final String name) {
    this.sourceAppName = Objects.requireNonNull(name, "Source app name cannot be null");
    return this;
  }

  /// Returns the [MessageFormat] this registry was constructed with. Useful for callers that
  /// want to inspect or reuse the format (e.g. to build a sibling pipeline). Defaults to
  /// [MessageFormat#bytes()] when the registry was constructed with a no-arg or
  /// `sourceAppName`-only constructor.
  ///
  /// @return the configured message format (never null)
  public MessageFormat<?> messageFormat() {
    return messageFormat;
  }

  /// Unregisters a processor by key. Sinks are managed via [#sinkRegistry()].
  ///
  /// @param key The key to remove
  /// @return true if a processor was removed, false otherwise
  public boolean unregister(final RegistryKey<?> key) {
    return registryMap.remove(key) != null;
  }

  /// Clears all processors. Sinks are managed via [#sinkRegistry()].
  public void clear() {
    registryMap.clear();
  }

  /// Returns an unmodifiable set of all registered processor keys.
  ///
  /// @return Unmodifiable set of all registered processor keys.
  public Set<RegistryKey<?>> getKeys() {
    return Collections.unmodifiableSet(registryMap.keySet());
  }

  /// Returns an unmodifiable map of all registered processors.
  ///
  /// @return Unmodifiable map of all processor keys and their simple class names
  public Map<RegistryKey<?>, String> getAll() {
    return RegistryFunctions.createUnmodifiableView(registryMap, value -> {
      final var entry = (RegistryEntry<?>) value;
      return "%s".formatted(entry.value().getClass().getSimpleName());
    });
  }

  /// Gets performance metrics for a specific processor. For sink metrics use
  /// [#sinkRegistry()] then [MessageSinkRegistry#getMetrics(RegistryKey)].
  ///
  /// @param key The processor key to look up
  /// @return Map containing metrics or empty map if not found
  public Map<String, Object> getMetrics(final RegistryKey<?> key) {
    final var entry = registryMap.get(key);
    if (entry != null) return entry.getMetrics();
    return Map.of();
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
