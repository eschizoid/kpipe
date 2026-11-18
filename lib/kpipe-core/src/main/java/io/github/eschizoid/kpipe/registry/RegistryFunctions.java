package io.github.eschizoid.kpipe.registry;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;

/// Helpers for [MessageProcessorRegistry]: read-only views of its maps.
final class RegistryFunctions {

  private RegistryFunctions() {}

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
