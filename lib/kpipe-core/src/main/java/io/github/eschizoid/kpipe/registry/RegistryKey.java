package io.github.eschizoid.kpipe.registry;

import java.util.Map;

/// Type-safe identifier for registry entries in KPipe.
///
/// This record combines a name and a type to uniquely identify entries in registries (processors,
/// sinks, etc.). It ensures type safety and clarity when retrieving or registering components in
/// the pipeline system.
///
/// Format-specific convenience factories live in their respective modules:
///
/// - `io.github.eschizoid.kpipe.format.avro.AvroRegistryKey.of(name)` —
// `RegistryKey<GenericRecord>`
/// - `io.github.eschizoid.kpipe.format.protobuf.ProtobufRegistryKey.of(name)` —
// `RegistryKey<Message>`
///
/// JSON keys are provided here directly because `Map<String, Object>` is a JDK type with no
/// format runtime dependency.
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
  /// @param <T>  The type of data the key refers to.
  /// @param name The unique name of the registry entry.
  /// @param type The class representing the type.
  /// @return A new type-safe RegistryKey.
  public static <T> RegistryKey<T> of(final String name, final Class<T> type) {
    return new RegistryKey<>(name, type);
  }

  /// Convenience factory for JSON-like map keys (`Map<String, Object>`).
  ///
  /// @param name The unique name of the registry entry.
  /// @return A new RegistryKey for JSON map data.
  @SuppressWarnings("unchecked")
  public static RegistryKey<Map<String, Object>> json(final String name) {
    return of(name, (Class<Map<String, Object>>) (Class<?>) Map.class);
  }
}
