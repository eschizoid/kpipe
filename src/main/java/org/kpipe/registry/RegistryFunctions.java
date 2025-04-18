package org.kpipe.registry;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;

/**
 * Provides shared utility functions for registry implementations in the KPipe framework.
 *
 * <p>This class contains reusable methods that support the common patterns used across different
 * registry types (message processors, sinks, etc.) including:
 *
 * <ul>
 *   <li>Error handling and resilience wrappers
 *   <li>Performance metrics collection and reporting
 *   <li>Time measurement for operations
 *   <li>Registry data view transformations
 * </ul>
 *
 * <p>These utilities help ensure consistent behavior across different registry implementations and
 * reduce code duplication. They provide standardized approaches to common concerns like error
 * handling, metrics, and data access patterns.
 *
 * <p>Example usage for error handling:
 *
 * <pre>{@code
 * // Create an error-tolerant function that returns a default value on exception
 * Function<byte[], byte[]> safeFunction = RegistryFunctions.withFunctionErrorHandling(
 *     riskyOperation,
 *     defaultValue,
 *     LOGGER
 * );
 *
 * // Create an error-tolerant consumer that swallows exceptions
 * BiConsumer<Record, Value> safeConsumer = RegistryFunctions.withConsumerErrorHandling(
 *     riskyConsumer,
 *     LOGGER
 * );
 * }</pre>
 *
 * <p>Example usage for metrics and timed execution:
 *
 * <pre>{@code
 * // Create a timed execution wrapper
 * BiFunction<Input, Function<Input, Output>, Output> timedExec =
 *     RegistryFunctions.timedExecution(
 *         counter::incrementAndGet,
 *         errorCounter::incrementAndGet,
 *         (count, duration) -> totalTimeNs.addAndGet(duration.toNanos())
 *     );
 *
 * // Execute an operation with timing
 * Output result = timedExec.apply(input, operation);
 *
 * // Get metrics
 * Map<String, Object> metrics = RegistryFunctions.createMetrics(
 *     counter.get(),
 *     errorCounter.get(),
 *     totalTimeNs.get()
 * );
 * }</pre>
 */
public class RegistryFunctions {

  /** Create metrics map from operational statistics. */
  public static Map<String, Object> createMetrics(
    final long operationCount,
    final long errorCount,
    final long totalTimeNs
  ) {
    final var metrics = new ConcurrentHashMap<String, Object>();
    metrics.put("invocationCount", operationCount);
    metrics.put("errorCount", errorCount);
    metrics.put("averageProcessingTimeNs", operationCount > 0 ? totalTimeNs / operationCount : 0);
    return metrics;
  }

  /** Creates a generic error handler wrapper for operations. */
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

  /** Creates a generic error handler wrapper for consumers. */
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

  /** Generic timed execution with metrics tracking */
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

  /** Creates an unmodifiable view of a registry */
  public static <K, V> Map<K, V> createUnmodifiableView(
    final Map<K, ?> registry,
    final Function<Object, V> valueMapper
  ) {
    final var result = new ConcurrentHashMap<K, V>();
    registry.forEach((key, value) -> result.put(key, valueMapper.apply(value)));
    return Collections.unmodifiableMap(result);
  }
}
