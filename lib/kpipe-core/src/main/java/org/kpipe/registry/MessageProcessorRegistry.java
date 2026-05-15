package org.kpipe.registry;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import org.kpipe.sink.MessageSink;

/// Registry for managing message processors AND message sinks in KPipe pipelines.
///
/// Holds two type-safe maps keyed by [RegistryKey]: one for [UnaryOperator]s (the transforms
/// you compose with `.pipe(...)`), one for [MessageSink]s (the terminal consumers). The two
/// namespaces share the same key shape but are stored independently — a key may exist as an
/// operator, as a sink, as both, or as neither.
///
/// Example:
///
/// ```java
/// final var registry = new MessageProcessorRegistry(JsonFormat.INSTANCE);
///
/// // Operator side
/// registry.register(RegistryKey.json("addTimestamp"),
///                   m -> { m.put("ts", System.currentTimeMillis()); return m; });
///
/// // Sink side — same `register`, just a different functional interface
/// registry.register(RegistryKey.json("dbSink"), record -> database.insert(record));
///
/// // Build a pipeline that uses both
/// final var pipeline = registry.pipeline(JsonFormat.INSTANCE)
///   .add(RegistryKey.json("addTimestamp"))
///   .toSink(RegistryKey.json("dbSink"))
///   .build();
/// ```
///
/// **Concurrency**: both maps are [ConcurrentHashMap]s; `register`/`get*`/`unregister*` are
/// thread-safe. Lookups return wrapping operators/sinks that read the current map entry at
/// invocation time, so live re-registration is observable.
public class MessageProcessorRegistry {

  private static final Logger LOGGER = System.getLogger(MessageProcessorRegistry.class.getName());

  private final ConcurrentHashMap<RegistryKey<?>, RegistryEntry<?>> operatorMap = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<RegistryKey<?>, RegistryEntry<?>> sinkMap = new ConcurrentHashMap<>();
  private final MessageFormat<?> messageFormat;

  /// Creates a new registry with the byte-passthrough format.
  public MessageProcessorRegistry() {
    this(MessageFormat.bytes());
  }

  /// Creates a new registry with the specified message format.
  ///
  /// @param messageFormat Message format to use (e.g. `JsonFormat.INSTANCE`, `AvroFormat.INSTANCE`,
  ///                      `ProtobufFormat.INSTANCE`, `MessageFormat.bytes()`, or a custom impl)
  public MessageProcessorRegistry(final MessageFormat<?> messageFormat) {
    this.messageFormat = Objects.requireNonNull(messageFormat, "Message format cannot be null");
  }

  /// Returns the [MessageFormat] this registry was constructed with.
  ///
  /// @return the configured message format (never null)
  public MessageFormat<?> messageFormat() {
    return messageFormat;
  }

  /// Creates a fluent builder for typed pipelines.
  ///
  /// @param format The message format for serialization/deserialization.
  /// @param <T>    The type of the object in the pipeline.
  /// @return A new TypedPipelineBuilder.
  public <T> TypedPipelineBuilder<T> pipeline(final MessageFormat<T> format) {
    return new TypedPipelineBuilder<>(format, this);
  }

  // ─────────────────────────── Operators ───────────────────────────

  /// Registers a typed operator under `key`.
  public <T> void register(final RegistryKey<T> key, final UnaryOperator<T> operator) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(operator, "operator cannot be null");
    operatorMap.put(key, new RegistryEntry<>(operator));
  }

  /// Registers every constant of an `Enum` that implements `UnaryOperator<T>`. Each constant's
  /// `name()` becomes the key.
  public <T, E extends Enum<E> & UnaryOperator<T>> void registerEnum(final Class<T> type, final Class<E> enumClass) {
    Objects.requireNonNull(type, "type cannot be null");
    Objects.requireNonNull(enumClass, "enumClass cannot be null");
    for (final var constant : enumClass.getEnumConstants()) {
      register(RegistryKey.of(constant.name(), type), constant);
    }
  }

  /// Retrieves a typed operator. If `key` is not registered, returns a logging pass-through —
  /// a missing operator is almost always a config error and silent pass-through would mask the
  /// bug.
  @SuppressWarnings("unchecked")
  public <T> UnaryOperator<T> getOperator(final RegistryKey<T> key) {
    return input -> {
      final var entry = (RegistryEntry<UnaryOperator<T>>) operatorMap.get(key);
      if (entry != null) return entry.apply(input);
      LOGGER.log(Level.WARNING, "No operator registered under key %s — passing input through unchanged".formatted(key));
      return input;
    };
  }

  /// Removes an operator entry. Returns true iff a value was present.
  public boolean unregister(final RegistryKey<?> key) {
    return operatorMap.remove(key) != null;
  }

  /// Removes every operator entry. Sinks are untouched — use [#clearSinks()] for those.
  public void clear() {
    operatorMap.clear();
  }

  /// Returns an unmodifiable view of registered operator keys.
  public Set<RegistryKey<?>> getKeys() {
    return Collections.unmodifiableSet(operatorMap.keySet());
  }

  /// Returns an unmodifiable view of registered operators (key → implementation simple name).
  public Map<RegistryKey<?>, String> getAll() {
    return RegistryFunctions.createUnmodifiableView(operatorMap, value -> {
      final var entry = (RegistryEntry<?>) value;
      return entry.value().getClass().getSimpleName();
    });
  }

  /// Gets metrics for a specific operator entry, or an empty map if `key` is not registered.
  public Map<String, Object> getMetrics(final RegistryKey<?> key) {
    final var entry = operatorMap.get(key);
    return entry == null ? Map.of() : entry.getMetrics();
  }

  /// Wraps an operator with error-handling logic that suppresses exceptions and returns the
  /// original input.
  public static <T> UnaryOperator<T> withErrorHandling(final UnaryOperator<T> operator) {
    return RegistryFunctions.withOperatorErrorHandling(operator, LOGGER);
  }

  // ───────────────────────────── Sinks ─────────────────────────────

  /// Registers a typed sink under `key`. Overload of the operator [#register] — Java resolves
  /// by argument type ([UnaryOperator] vs [MessageSink]).
  public <T> void register(final RegistryKey<T> key, final MessageSink<T> sink) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(sink, "sink cannot be null");
    sinkMap.put(key, new RegistryEntry<>(sink));
  }

  /// Registers every constant of an `Enum` that implements `MessageSink<T>`.
  public <T, E extends Enum<E> & MessageSink<T>> void registerSinkEnum(
    final Class<T> type,
    final Class<E> enumClass
  ) {
    Objects.requireNonNull(type, "type cannot be null");
    Objects.requireNonNull(enumClass, "enumClass cannot be null");
    for (final var constant : enumClass.getEnumConstants()) {
      register(RegistryKey.of(constant.name(), type), constant);
    }
  }

  /// Retrieves a typed sink. Unlike operators, a missing sink does NOT pass through — it logs at
  /// WARNING and drops the value (no usable fallback for a void terminal).
  @SuppressWarnings("unchecked")
  public <T> MessageSink<T> getSink(final RegistryKey<T> key) {
    return value -> {
      final var entry = (RegistryEntry<MessageSink<T>>) sinkMap.get(key);
      if (entry != null) entry.accept(value);
      else LOGGER.log(Level.WARNING, "No sink found in registry for key: {0}", key);
    };
  }

  /// Removes a sink entry. Returns true iff a value was present.
  public boolean unregisterSink(final RegistryKey<?> key) {
    return sinkMap.remove(key) != null;
  }

  /// Removes every sink entry. Operators are untouched.
  public void clearSinks() {
    sinkMap.clear();
  }

  /// Returns an unmodifiable view of registered sink keys.
  public Set<RegistryKey<?>> getSinkKeys() {
    return Collections.unmodifiableSet(sinkMap.keySet());
  }

  /// Returns an unmodifiable view of registered sinks (key → implementation simple name).
  public Map<RegistryKey<?>, String> getAllSinks() {
    return RegistryFunctions.createUnmodifiableView(sinkMap, value -> {
      final var entry = (RegistryEntry<?>) value;
      return entry.value().getClass().getSimpleName();
    });
  }

  /// Gets metrics for a specific sink entry, or an empty map if `key` is not registered.
  public Map<String, Object> getSinkMetrics(final RegistryKey<?> key) {
    final var entry = sinkMap.get(key);
    return entry == null ? Map.of() : entry.getMetrics();
  }

  /// Wraps a sink with error-handling logic that suppresses exceptions. Overload of the operator
  /// [#withErrorHandling(UnaryOperator)] — Java resolves by argument type.
  public static <T> MessageSink<T> withErrorHandling(final MessageSink<T> sink) {
    return RegistryFunctions.withConsumerErrorHandling(sink, LOGGER)::accept;
  }

  /// Creates a composite sink that delivers each value to every registered sink under `sinkKeys`,
  /// in order. A failure in one sink is logged at WARNING but does not stop the others.
  @SafeVarargs
  public final <T> MessageSink<T> compositeSink(final RegistryKey<T>... sinkKeys) {
    return value -> {
      for (final var key : sinkKeys) {
        try {
          getSink(key).accept(value);
        } catch (final Exception e) {
          LOGGER.log(Level.WARNING, "Sink %s threw during compositeSink delivery; continuing".formatted(key), e);
        }
      }
    };
  }
}
