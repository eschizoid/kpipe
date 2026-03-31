package org.kpipe.registry;

import java.util.Map;
import org.apache.avro.generic.GenericRecord;

/// Type-safe identifier for registry entries in KPipe.
///
/// This record combines a name and a type to uniquely identify entries in registries (processors,
/// sinks, etc.). It ensures type safety and clarity when retrieving or registering components in
/// the pipeline system.
///
/// Example:
/// ```java
/// RegistryKey<Map<String, Object>> key = RegistryKey.json("addTimestamp");
/// ```
///
/// @param <T> The type of data the key refers to
/// @param name The unique name of the registry entry
/// @param type The class representing the type
public record RegistryKey<T>(String name, Class<T> type) {
  /// Creates a type-safe registry key for a given type.
  ///
  /// @param <T> The type of data the key refers to
  /// @param name The unique name of the registry entry
  /// @param type The class representing the type
  /// @return A new type-safe RegistryKey
  @SuppressWarnings("unchecked")
  public static <T> RegistryKey<T> of(final String name, final Class<?> type) {
    return new RegistryKey<>(name, (Class<T>) type);
  }

  /// Convenience factory for JSON-like map keys (Map<String, Object>).
  ///
  /// @param name The unique name of the registry entry
  /// @return A new RegistryKey for JSON map data
  public static RegistryKey<Map<String, Object>> json(final String name) {
    return of(name, Map.class);
  }

  /// Convenience factory for Avro GenericRecord keys.
  ///
  /// @param name The unique name of the registry entry
  /// @return A new RegistryKey for Avro GenericRecord data
  public static RegistryKey<GenericRecord> avro(final String name) {
    return of(name, GenericRecord.class);
  }

  /// Convenience factory for sink keys.
  ///
  /// @param <T> The type of the processed object the sink accepts
  /// @param name The unique name of the sink
  /// @param type The class representing the type
  /// @return A new type-safe RegistryKey for a sink
  public static <T> RegistryKey<T> sink(final String name, final Class<T> type) {
    return new RegistryKey<>(name, type);
  }
}
