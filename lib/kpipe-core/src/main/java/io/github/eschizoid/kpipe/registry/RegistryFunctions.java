package io.github.eschizoid.kpipe.registry;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;

/// Utility functions for registry implementations in KPipe.
///
/// This class provides reusable helpers for error handling and registry data access. It is used by
/// registry classes to ensure consistent and efficient behavior across message processing
/// pipelines.
///
/// Example usage:
/// ```java
/// final var safeOp = RegistryFunctions.withOperatorErrorHandling(operator, logger);
/// ```
public final class RegistryFunctions {

  private RegistryFunctions() {}

  /// Creates a consumer wrapper that suppresses exceptions thrown by the wrapped consumer,
  /// logging them instead of propagating.
  ///
  /// @param <T>       the type of the input to the consumer
  /// @param operation the consumer to wrap with error handling
  /// @param logger    the logger instance to use for logging exceptions
  /// @return a consumer that executes the operation but suppresses and logs any exceptions
  public static <T> Consumer<T> withConsumerErrorHandling(final Consumer<T> operation, final Logger logger) {
    return input -> {
      try {
        operation.accept(input);
      } catch (final Exception e) {
        logger.log(Level.WARNING, "Error in operation", e);
      }
    };
  }

  /// Creates a UnaryOperator wrapper that suppresses exceptions thrown by the wrapped operator,
  /// logging them and returning the original input instead of propagating.
  ///
  /// @param <T>       the type of the input to the operator
  /// @param operation the operator to wrap with error handling
  /// @param logger    the logger instance to use for logging exceptions
  /// @return an operator that executes the operation but suppresses and logs any exceptions,
  ///     returning original input on error
  public static <T> UnaryOperator<T> withOperatorErrorHandling(final UnaryOperator<T> operation, final Logger logger) {
    return input -> {
      try {
        return operation.apply(input);
      } catch (final Exception e) {
        logger.log(Level.WARNING, "Error in operator", e);
        return input;
      }
    };
  }

  /// Creates an unmodifiable view of a registry where values are transformed using the provided
  /// mapper function.
  ///
  /// @param <K> the key type
  /// @param <V> the mapped value type
  /// @param registry the source registry to create a view from
  /// @param valueMapper the function to transform registry values to the target type
  /// @return an unmodifiable map containing the transformed values
  public static <K, V> Map<K, V> createUnmodifiableView(
    final Map<K, ?> registry,
    final Function<Object, V> valueMapper
  ) {
    final var result = new ConcurrentHashMap<K, V>();
    registry.forEach((key, value) -> result.put(key, valueMapper.apply(value)));
    return Collections.unmodifiableMap(result);
  }
}
