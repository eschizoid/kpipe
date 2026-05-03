package org.kpipe.registry;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// Reusable [UnaryOperator] factories for use with [TypedPipelineBuilder#add(UnaryOperator)].
///
/// These helpers cover common patterns that would otherwise require an inline lambda
/// or custom operator implementation.
///
/// Example:
/// ```java
/// import static org.kpipe.registry.Operators.filter;
///
/// registry.pipeline(JsonFormat.INSTANCE)
///     .add(filter(msg -> "active".equals(msg.get("status"))))
///     .toSink(activeMessageSink)
///     .build();
/// ```
public final class Operators {

  private Operators() {}

  /// Returns an operator that keeps messages matching the predicate and drops others.
  ///
  /// Per the [MessagePipeline] error contract, returning `null` from an operator signals
  /// intentional filtering — the consumer marks the offset as processed but does not
  /// invoke any sink. Filtered messages are not counted as errors.
  ///
  /// Example:
  /// ```java
  /// // Keep only messages where the "status" field equals "active"
  /// .add(filter(msg -> "active".equals(msg.get("status"))))
  /// ```
  ///
  /// @param keep predicate that returns true for messages to keep, false to drop
  /// @param <T> the operator's value type
  /// @return an operator that returns its input when `keep.test(input)` is true, else null
  public static <T> UnaryOperator<T> filter(final Predicate<T> keep) {
    return value -> keep.test(value) ? value : null;
  }

  /// Returns an operator that drops messages matching the predicate and keeps others.
  ///
  /// Inverse of [#filter] — useful when the predicate naturally expresses "should drop"
  /// rather than "should keep."
  ///
  /// Example:
  /// ```java
  /// .add(drop(msg -> Boolean.TRUE.equals(msg.get("deleted"))))
  /// ```
  ///
  /// @param drop predicate that returns true for messages to drop
  /// @param <T> the operator's value type
  /// @return an operator that returns null when `drop.test(input)` is true, else its input
  public static <T> UnaryOperator<T> drop(final Predicate<T> drop) {
    return value -> drop.test(value) ? null : value;
  }

  /// Returns an operator that runs a side effect and passes the value through unchanged.
  ///
  /// Useful for logging, metrics, or invoking a downstream system without modifying
  /// the message.
  ///
  /// Example:
  /// ```java
  /// .add(tap(msg -> log.info("processing {}", msg.get("id"))))
  /// ```
  ///
  /// @param sideEffect the action to run on each value
  /// @param <T> the operator's value type
  /// @return an operator that runs `sideEffect.accept(input)` and returns input unchanged
  public static <T> UnaryOperator<T> tap(final Consumer<T> sideEffect) {
    return value -> {
      sideEffect.accept(value);
      return value;
    };
  }

  /// Maps each element via the given function. Equivalent to passing the function directly to
  /// `.add(...)`, but reads more naturally for inline transformations.
  ///
  /// Example:
  /// ```java
  /// registry.pipeline(JsonFormat.INSTANCE)
  ///     .add(Operators.map(msg -> { msg.put("processed", true); return msg; }))
  ///     .build();
  /// ```
  ///
  /// @param mapper the function to apply to each value
  /// @param <T> the operator's value type
  /// @return an operator that returns `mapper.apply(input)`
  public static <T> UnaryOperator<T> map(final Function<T, T> mapper) {
    return value -> mapper.apply(value);
  }

  /// Alias for [#tap] using the Stream API vocabulary.
  ///
  /// @param sideEffect the action to run on each value
  /// @param <T> the operator's value type
  /// @return an operator that runs `sideEffect.accept(input)` and returns input unchanged
  public static <T> UnaryOperator<T> peek(final Consumer<T> sideEffect) {
    return tap(sideEffect);
  }

  /// Wraps an operator with error handling that suppresses exceptions and returns the original
  /// input on failure.
  ///
  /// Convenience facade over [MessageProcessorRegistry#withErrorHandling] so users can stay on
  /// the [Operators] namespace without importing the registry just to wrap an operator.
  ///
  /// Example:
  /// ```java
  /// .add(Operators.safe(Operators.map(msg -> riskyTransform(msg))))
  /// ```
  ///
  /// @param op the operator to wrap with error handling
  /// @param <T> the operator's value type
  /// @return an operator that returns its input on exception (logged via the registry's logger)
  public static <T> UnaryOperator<T> safe(final UnaryOperator<T> op) {
    return MessageProcessorRegistry.withErrorHandling(op);
  }

  /// Returns an operator that filters out messages missing the given field.
  ///
  /// The operator returns `null` (the [MessagePipeline] filter signal) when the input map is
  /// `null` or does not contain `fieldName`; otherwise the input map is passed through unchanged.
  ///
  /// Example:
  /// ```java
  /// .add(requireField("user_id"))
  /// ```
  ///
  /// @param fieldName the key that must be present in the message map
  /// @return an operator that returns the input map when the field is present, else `null`
  public static UnaryOperator<Map<String, Object>> requireField(final String fieldName) {
    return msg -> (msg != null && msg.containsKey(fieldName)) ? msg : null;
  }

  /// Returns an operator that renames a key in a [Map] message.
  ///
  /// If the source key is absent (or the map is `null`), the operator returns the input
  /// unchanged. When present, the value is moved from `from` to `to` in-place.
  ///
  /// Example:
  /// ```java
  /// .add(rename("userId", "user_id"))
  /// ```
  ///
  /// @param from the source key
  /// @param to the destination key
  /// @return an operator that renames the key in-place, or returns the input unchanged when absent
  public static UnaryOperator<Map<String, Object>> rename(final String from, final String to) {
    return msg -> {
      if (msg == null || !msg.containsKey(from)) return msg;
      msg.put(to, msg.remove(from));
      return msg;
    };
  }

  /// Returns an operator that chains multiple operators left-to-right.
  ///
  /// If any operator in the chain returns `null` (the [MessagePipeline] filter signal),
  /// downstream operators are short-circuited and the composed operator returns `null`.
  /// Calling `compose()` with no arguments yields an identity operator.
  ///
  /// Example:
  /// ```java
  /// final var op = Operators.compose(
  ///     Operators.requireField("id"),
  ///     Operators.rename("ts", "timestamp"));
  /// ```
  ///
  /// @param ops operators to chain in order
  /// @param <T> the operator's value type
  /// @return a single operator equivalent to applying each step in order, short-circuiting on null
  @SafeVarargs
  public static <T> UnaryOperator<T> compose(final UnaryOperator<T>... ops) {
    return value -> {
      var v = value;
      for (final var op : ops) {
        if (v == null) return null;
        v = op.apply(v);
      }
      return v;
    };
  }

  /// Returns an operator that removes one or more fields from a [Map] message.
  ///
  /// Mutates the input map in-place. Returns `null` when the input is `null`. Missing keys are
  /// silently ignored. This mirrors `JsonMessageProcessor.removeFieldsOperator` but lives on the
  /// generic [Operators] namespace so users do not need to import a format-specific class.
  ///
  /// Example:
  /// ```java
  /// .add(removeFields("password", "secret_token"))
  /// ```
  ///
  /// @param fields the keys to remove from the message
  /// @return an operator that removes the fields in-place, or returns `null` when the input is null
  public static UnaryOperator<Map<String, Object>> removeFields(final String... fields) {
    return msg -> {
      if (msg == null) return null;
      for (final var f : fields) msg.remove(f);
      return msg;
    };
  }
}
