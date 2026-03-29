package org.kpipe.registry;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;

/// Utility functions for registry implementations in KPipe.
///
/// This class provides reusable helpers for error handling, metrics, timing, and registry data
/// access.
/// It is used by registry classes to ensure consistent and efficient behavior across message
/// processing pipelines.
///
/// Example usage:
/// ```java
/// final var safeFn = RegistryFunctions.withErrorHandling(fn, defaultValue);
/// ```
public final class RegistryFunctions {

  private RegistryFunctions() {}

  /// Creates a metrics map containing operation statistics for monitoring and reporting.
  ///
  /// @param operationCount the total number of operations performed
  /// @param errorCount the total number of errors encountered
  /// @param totalTimeMs the total processing time in milliseconds
  /// @return an unmodifiable map containing metrics with keys "messageCount"/"invocationCount",
  ///     "errorCount", and "averageProcessingTimeMs"
  public static Map<String, Object> createMetrics(
    final long operationCount,
    final long errorCount,
    final long totalTimeMs
  ) {
    final var metrics = new ConcurrentHashMap<String, Object>();
    metrics.put("invocationCount", operationCount);
    metrics.put("errorCount", errorCount);
    metrics.put("averageProcessingTimeMs", operationCount > 0 ? totalTimeMs / operationCount : 0);
    return metrics;
  }

  /// Creates a function wrapper that handles exceptions gracefully, returning a default value
  /// when the wrapped function throws an exception.
  ///
  /// @param <T> the input type
  /// @param <R> the return type
  /// @param operation the function to wrap with error handling
  /// @param defaultValue the value to return in case of exception
  /// @param logger the logger instance to use for logging exceptions
  /// @return a function that executes the operation and returns its result or the default value on
  ///     error
  public static <T, R> Function<T, R> withFunctionErrorHandling(
    final Function<T, R> operation,
    final R defaultValue,
    final Logger logger
  ) {
    return input -> {
      try {
        return operation.apply(input);
      } catch (final Exception e) {
        logger.log(Level.WARNING, "Error in operation", e);
        return defaultValue;
      }
    };
  }

  /// Creates a consumer wrapper that suppresses exceptions thrown by the wrapped consumer,
  /// logging them instead of propagating.
  ///
  /// @param <T> the type of the first input to the consumer
  /// @param <U> the type of the second input to the consumer
  /// @param operation the consumer to wrap with error handling
  /// @param logger the logger instance to use for logging exceptions
  /// @return a consumer that executes the operation but suppresses and logs any exceptions
  public static <T, U> BiConsumer<T, U> withConsumerErrorHandling(
    final BiConsumer<T, U> operation,
    final Logger logger
  ) {
    return (input1, input2) -> {
      try {
        operation.accept(input1, input2);
      } catch (final Exception e) {
        logger.log(Level.WARNING, "Error in operation", e);
      }
    };
  }

  /// Creates a function wrapper that measures the execution time of operations and tracks
  /// metrics.
  ///
  /// @param <T> the input type
  /// @param <R> the return type
  /// @param counterIncrement supplier that increments and returns the operation counter
  /// @param errorCountIncrement supplier that increments and returns the error counter
  /// @param timeAccumulator consumer that records the execution duration
  /// @return a function that executes the operation with timing and metrics tracking
  public static <T, R> BiFunction<T, Function<T, R>, R> timedExecution(
    final Supplier<Long> counterIncrement,
    final Supplier<Long> errorCountIncrement,
    final Consumer<Duration> timeAccumulator
  ) {
    return (input, operation) -> {
      final var start = Instant.now();
      try {
        final var result = operation.apply(input);
        counterIncrement.get();
        timeAccumulator.accept(Duration.between(start, Instant.now())); // add time
        return result;
      } catch (final Exception e) {
        errorCountIncrement.get();
        throw e;
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
