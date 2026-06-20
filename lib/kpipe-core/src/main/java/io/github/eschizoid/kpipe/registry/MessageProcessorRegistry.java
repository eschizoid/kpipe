package io.github.eschizoid.kpipe.registry;

import io.github.eschizoid.kpipe.sink.MessageSink;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/// Registry for managing message processors AND message sinks in KPipe pipelines.
///
/// Holds two type-safe maps keyed by [RegistryKey]: one for [UnaryOperator]s (the transforms
/// you compose with `.pipe(...)`), one for [MessageSink]s (the terminal consumers). The two
/// namespaces share the same key shape but are stored independently — a key may exist as an
/// operator, as a sink, as both, or as neither. Internally both namespaces share a single
/// `Namespace` helper so the structural-mirror methods all funnel into one body each.
///
/// Example:
///
/// ```java
/// final var registry = new MessageProcessorRegistry();
///
/// // Operator side
/// registry.registerOperator(RegistryKey.json("addTimestamp"),
///                           m -> { m.put("ts", System.currentTimeMillis()); return m; });
///
/// // Sink side
/// registry.registerSink(RegistryKey.json("dbSink"), record -> database.insert(record));
///
/// // Build a pipeline that uses both
/// final var pipeline = registry.pipeline(JsonFormat.INSTANCE)
///   .add(RegistryKey.json("addTimestamp"))
///   .toSink(RegistryKey.json("dbSink"))
///   .build();
/// ```
///
/// **Concurrency**: both maps are [ConcurrentHashMap]s; `registerOperator`/`registerSink`/`get*`/
/// `unregister*` are thread-safe. Lookups return wrapping operators/sinks that read the current
/// map entry at invocation time, so live re-registration is observable.
public class MessageProcessorRegistry {

  private static final Logger LOGGER = System.getLogger(MessageProcessorRegistry.class.getName());

  /// Internal helper: each namespace owns one map and exposes the structural CRUD methods. Keeps
  /// the public registry from carrying two hand-rolled mirrors. The user-facing methods
  /// (`register`, `getOperator` / `getSink`, etc.) still split by overload / suffix to preserve
  /// the type-safe API; only the *plumbing* is shared.
  private static final class Namespace {

    private final ConcurrentHashMap<RegistryKey<?>, RegistryEntry<?>> map = new ConcurrentHashMap<>();

    <T> void put(final RegistryKey<T> key, final Object value) {
      map.put(key, new RegistryEntry<>(value));
    }

    /// Returns the entry under `key` cast to the caller-supplied wrapper type, or null if
    /// nothing is registered. The wrapper type cannot be inferred from the key (which carries
    /// the *value* type, e.g. `Map<String, Object>`, not the wrapper `UnaryOperator<Map<...>>`),
    /// so callers supply it as a type witness. Type-safety of the registration is enforced at
    /// the public `register(RegistryKey<T>, ...)` overloads.
    @SuppressWarnings("unchecked")
    <W> RegistryEntry<W> getAs(final RegistryKey<?> key) {
      return (RegistryEntry<W>) map.get(key);
    }

    boolean remove(final RegistryKey<?> key) {
      return map.remove(key) != null;
    }

    void clear() {
      map.clear();
    }

    Set<RegistryKey<?>> keys() {
      return Collections.unmodifiableSet(map.keySet());
    }

    Map<RegistryKey<?>, String> all() {
      return RegistryFunctions.createUnmodifiableView(map, value -> {
        final var entry = (RegistryEntry<?>) value;
        return entry.value().getClass().getSimpleName();
      });
    }

    Map<String, Object> metrics(final RegistryKey<?> key) {
      final var entry = map.get(key);
      return entry == null ? Map.of() : entry.getMetrics();
    }
  }

  private final Namespace operators = new Namespace();
  private final Namespace sinks = new Namespace();

  /// Tracks which unrouted keys have already been warned about, so the per-record WARNING for a
  /// missing operator/sink fires once per distinct key instead of once per record. The WARNING
  /// signal is load-bearing — operators need to know a key is unrouted — but repeating it per
  /// record floods logs during a sustained misconfiguration. Bounded in practice: keyed by
  /// [RegistryKey], whose cardinality is the (finite) set of keys a pipeline ever looks up. Kept
  /// separate per namespace so an unrouted key warned on the operator side still warns once on the
  /// sink side (the two lookups are independent).
  ///
  /// The one unbounded case is a caller that generates fresh `RegistryKey` instances dynamically
  /// (e.g. a key derived from per-record data) and routes none of them — each distinct miss adds
  /// a set entry. That's a pathological misuse (the registry is meant for a fixed, declared key
  /// set), but if it ever becomes a real concern, cap the set or switch to a bounded LRU.
  private final Set<RegistryKey<?>> warnedMissingOperatorKeys = ConcurrentHashMap.newKeySet();

  private final Set<RegistryKey<?>> warnedMissingSinkKeys = ConcurrentHashMap.newKeySet();

  /// Creates a new registry. The registry is format-agnostic — supply the format per pipeline via
  /// [#pipeline(MessageFormat)].
  public MessageProcessorRegistry() {}

  /// Creates a fluent builder for typed pipelines.
  ///
  /// @param format the message format that drives serialisation / deserialisation
  /// @param <T>    the pipeline value type
  /// @return a new [TypedPipelineBuilder] bound to this registry
  public <T> TypedPipelineBuilder<T> pipeline(final MessageFormat<T> format) {
    return new TypedPipelineBuilder<>(format, this);
  }

  // ─────────────────────────── Operators ───────────────────────────

  /// Registers a typed operator under `key`.
  ///
  /// @param key      the registry key (must be non-null)
  /// @param operator the operator to register (must be non-null)
  /// @param <T>      the pipeline value type the operator acts on
  public <T> void registerOperator(final RegistryKey<T> key, final UnaryOperator<T> operator) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(operator, "operator cannot be null");
    operators.put(key, operator);
  }

  /// Registers every constant of an `Enum` that implements `UnaryOperator<T>`. Each constant's
  /// `name()` becomes the key.
  ///
  /// @param type      the runtime class for the pipeline value type
  /// @param enumClass the enum class whose constants are operators
  /// @param <T>       the pipeline value type
  /// @param <E>       the enum type implementing `UnaryOperator<T>`
  public <T, E extends Enum<E> & UnaryOperator<T>> void registerEnum(final Class<T> type, final Class<E> enumClass) {
    Objects.requireNonNull(type, "type cannot be null");
    Objects.requireNonNull(enumClass, "enumClass cannot be null");
    for (final var constant : enumClass.getEnumConstants()) {
      registerOperator(RegistryKey.of(constant.name(), type), constant);
    }
  }

  /// Retrieves a typed operator. If `key` is not registered, returns a logging pass-through —
  /// a missing operator is almost always a config error and silent pass-through would mask the
  /// bug.
  ///
  /// @param key the registry key to look up
  /// @param <T> the pipeline value type
  /// @return the registered operator, or a logging pass-through if `key` is not registered
  public <T> UnaryOperator<T> getOperator(final RegistryKey<T> key) {
    return input -> {
      final RegistryEntry<UnaryOperator<T>> entry = operators.getAs(key);
      if (entry == null) {
        // Warn once per distinct key — repeats are suppressed so a sustained misconfig
        // doesn't flood logs while still surfacing the unrouted key the first time.
        if (warnedMissingOperatorKeys.add(key)) {
          LOGGER.log(
            Level.WARNING,
            "No operator registered under key {0} — passing input through unchanged",
            key
          );
        }
        return input;
      }
      return entry.apply(input);
    };
  }

  /// Removes an operator entry. Sinks are untouched — use [#unregisterSink(RegistryKey)] for
  /// those.
  ///
  /// @param key the registry key to remove
  /// @return `true` iff an entry was removed
  public boolean unregister(final RegistryKey<?> key) {
    return operators.remove(key);
  }

  /// Removes every operator entry. Sinks are untouched — use [#clearSinks()] for those.
  public void clear() {
    operators.clear();
  }

  /// Returns the registered operator keys.
  ///
  /// @return an unmodifiable view of registered operator keys
  public Set<RegistryKey<?>> getKeys() {
    return operators.keys();
  }

  /// Gets metrics for a specific operator entry.
  ///
  /// @param key the registry key to look up
  /// @return the entry's metrics, or an empty map if `key` is not registered
  public Map<String, Object> getMetrics(final RegistryKey<?> key) {
    return operators.metrics(key);
  }

  /// Wraps an operator with error-handling logic that suppresses exceptions and returns the
  /// original input.
  ///
  /// @param operator the operator to wrap
  /// @param <T>      the pipeline value type
  /// @return a new operator that logs and swallows exceptions raised by the original
  public static <T> UnaryOperator<T> withOperatorErrorHandling(final UnaryOperator<T> operator) {
    return RegistryFunctions.withOperatorErrorHandling(operator, LOGGER);
  }

  // ───────────────────────────── Sinks ─────────────────────────────

  /// Registers a typed sink under `key`.
  ///
  /// @param key  the registry key (must be non-null)
  /// @param sink the sink to register (must be non-null)
  /// @param <T>  the pipeline value type the sink consumes
  public <T> void registerSink(final RegistryKey<T> key, final MessageSink<T> sink) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(sink, "sink cannot be null");
    sinks.put(key, sink);
  }

  /// Retrieves a typed sink. Unlike operators, a missing sink does NOT pass through — it logs at
  /// WARNING and drops the value (no usable fallback for a void terminal).
  ///
  /// @param key the registry key to look up
  /// @param <T> the pipeline value type the sink consumes
  /// @return the registered sink, or a logging drop-sink if `key` is not registered
  public <T> MessageSink<T> getSink(final RegistryKey<T> key) {
    return value -> {
      final RegistryEntry<MessageSink<T>> entry = sinks.getAs(key);
      if (entry == null) {
        // Warn once per distinct key — repeats are suppressed so a sustained misconfig
        // doesn't flood logs while still surfacing the unrouted key the first time.
        if (warnedMissingSinkKeys.add(key)) {
          LOGGER.log(Level.WARNING, "No sink found in registry for key: {0}", key);
        }
        return;
      }
      entry.accept(value);
    };
  }

  /// Removes a sink entry.
  ///
  /// @param key the registry key to remove
  /// @return `true` iff an entry was removed
  public boolean unregisterSink(final RegistryKey<?> key) {
    return sinks.remove(key);
  }

  /// Removes every sink entry. Operators are untouched.
  public void clearSinks() {
    sinks.clear();
  }

  /// Returns the registered sink keys.
  ///
  /// @return an unmodifiable view of registered sink keys
  public Set<RegistryKey<?>> getSinkKeys() {
    return sinks.keys();
  }

  /// Returns a summary of registered sinks.
  ///
  /// @return an unmodifiable view of registered sinks (key → implementation simple name)
  public Map<RegistryKey<?>, String> getAllSinks() {
    return sinks.all();
  }

  /// Gets metrics for a specific sink entry.
  ///
  /// @param key the registry key to look up
  /// @return the entry's metrics, or an empty map if `key` is not registered
  public Map<String, Object> getSinkMetrics(final RegistryKey<?> key) {
    return sinks.metrics(key);
  }

  /// Wraps a sink with error-handling logic that suppresses exceptions.
  ///
  /// @param sink the sink to wrap
  /// @param <T>  the pipeline value type
  /// @return a new sink that logs and swallows exceptions raised by the original
  public static <T> MessageSink<T> withSinkErrorHandling(final MessageSink<T> sink) {
    return RegistryFunctions.withConsumerErrorHandling(sink, LOGGER)::accept;
  }

  /// Creates a composite sink that delivers each value to every registered sink under `sinkKeys`,
  /// in order. A failure in one sink is logged at WARNING but does not stop the others.
  ///
  /// @param sinkKeys the registry keys whose sinks to fan out to
  /// @param <T>      the pipeline value type
  /// @return a fan-out sink that delivers to each registered sink in order
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
