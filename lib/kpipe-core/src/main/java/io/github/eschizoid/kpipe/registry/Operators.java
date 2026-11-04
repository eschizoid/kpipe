package io.github.eschizoid.kpipe.registry;

import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// Reusable [UnaryOperator] factories for use with [TypedPipelineBuilder#add(UnaryOperator)].
///
/// These helpers work on any payload type. Operators that change the fields of a JSON record,
/// such as `set`, `remove` and `rename`, are `JsonFields` in `kpipe-format-json`.
///
/// Example:
/// ```java
/// import static io.github.eschizoid.kpipe.registry.Operators.filter;
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
  /// the message. Mirrors `java.util.stream.Stream.peek`.
  ///
  /// Example:
  /// ```java
  /// .add(peek(msg -> log.info("processing {}", msg.get("id"))))
  /// ```
  ///
  /// @param sideEffect the action to run on each value
  /// @param <T> the operator's value type
  /// @return an operator that runs `sideEffect.accept(input)` and returns input unchanged
  public static <T> UnaryOperator<T> peek(final Consumer<T> sideEffect) {
    return value -> {
      sideEffect.accept(value);
      return value;
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
  ///     Operators.filter(order -> order.amount() > 0),
  ///     Operators.peek(order -> log.info("order {}", order.id())));
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
}
