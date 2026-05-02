package org.kpipe.registry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.UnaryOperator;
import org.kpipe.sink.MessageSink;

/// Internal registry entry that maintains execution statistics and metrics for processors and
/// sinks.
///
/// This class provides a unified way to track invocations, errors, and processing time for both
/// [UnaryOperator] and [MessageSink] implementations.
///
/// @param <T> The type of the value being registered (e.g., UnaryOperator or MessageSink)
class RegistryEntry<T> {

  private final T value;
  private final LongAdder invocationCount = new LongAdder();
  private final LongAdder errorCount = new LongAdder();
  private final LongAdder totalProcessingTimeNs = new LongAdder();

  RegistryEntry(final T value) {
    this.value = value;
  }

  /// Returns the underlying registered value.
  public T value() {
    return value;
  }

  /// Returns execution metrics for this entry.
  public Map<String, Object> getMetrics() {
    final long count = invocationCount.sum();
    final long errors = errorCount.sum();
    final long timeNs = totalProcessingTimeNs.sum();
    final var metrics = new ConcurrentHashMap<String, Object>();
    metrics.put("invocationCount", count);
    metrics.put("errorCount", errors);
    metrics.put("averageProcessingTimeMs", count > 0 ? (timeNs / count) / 1_000_000.0 : 0.0);
    return metrics;
  }

  /// Executes the entry as a UnaryOperator.
  ///
  /// @param input The input value
  /// @param <V>   The type of the input/output value
  /// @return The result of the operator application
  @SuppressWarnings("unchecked")
  public <V> V apply(final V input) {
    final var start = System.nanoTime();
    try {
      final var result = ((UnaryOperator<V>) value).apply(input);
      invocationCount.increment();
      totalProcessingTimeNs.add(System.nanoTime() - start);
      return result;
    } catch (final Exception e) {
      errorCount.increment();
      throw e;
    }
  }

  /// Executes the entry as a MessageSink.
  ///
  /// @param input The input value to be consumed
  /// @param <V>   The type of the input value
  @SuppressWarnings("unchecked")
  public <V> void accept(final V input) {
    final var start = System.nanoTime();
    try {
      ((MessageSink<V>) value).accept(input);
      invocationCount.increment();
      totalProcessingTimeNs.add(System.nanoTime() - start);
    } catch (final Exception e) {
      errorCount.increment();
      throw e;
    }
  }
}
