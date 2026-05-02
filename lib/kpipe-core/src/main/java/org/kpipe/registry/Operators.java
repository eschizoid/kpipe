package org.kpipe.registry;

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
  public static <T> UnaryOperator<T> tap(final java.util.function.Consumer<T> sideEffect) {
    return value -> {
      sideEffect.accept(value);
      return value;
    };
  }
}
