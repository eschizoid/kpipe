package io.github.eschizoid.kpipe.registry;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;

/// Helpers for [MessageProcessorRegistry]: sink error handling and read-only views of its maps.
final class RegistryFunctions {

  private RegistryFunctions() {}

  /// Creates a consumer wrapper that suppresses exceptions thrown by the wrapped consumer,
  /// logging them instead of propagating.
  ///
  /// @param <T>       the type of the input to the consumer
  /// @param operation the consumer to wrap with error handling
  /// @param logger    the logger instance to use for logging exceptions
  /// @return a consumer that executes the operation but suppresses and logs any exceptions
  static <T> Consumer<T> withConsumerErrorHandling(final Consumer<T> operation, final Logger logger) {
    return input -> {
      try {
        operation.accept(input);
      } catch (final Exception e) {
        logger.log(Level.WARNING, "Error in operation", e);
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
  static <K, V> Map<K, V> createUnmodifiableView(final Map<K, ?> registry, final Function<Object, V> valueMapper) {
    final var result = new ConcurrentHashMap<K, V>();
    registry.forEach((key, value) -> result.put(key, valueMapper.apply(value)));
    return Collections.unmodifiableMap(result);
  }
}
