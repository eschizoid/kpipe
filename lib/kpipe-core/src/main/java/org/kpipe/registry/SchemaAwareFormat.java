package org.kpipe.registry;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

/// A [MessageFormat] whose payloads carry a registered schema or descriptor.
///
/// Avro records and Protobuf messages are bound to a schema/descriptor — the format must hold
/// it to deserialize correctly. Schema-less formats (`bytes`, JSON-as-`Map`) implement
/// [MessageFormat] directly and do not see these methods on their public surface.
///
/// @param <T> the deserialized payload type
public interface SchemaAwareFormat<T> extends MessageFormat<T> {
  /// Returns an unmodifiable view of all schemas registered with this format.
  ///
  /// @return Map of schema keys to their schema information
  Map<String, SchemaInfo> getSchemas();

  /// Finds a schema by its key.
  ///
  /// @param key The schema key to search for
  /// @return An Optional containing the SchemaInfo if found, or empty if not found
  Optional<SchemaInfo> findSchema(final String key);

  /// Removes all schemas registered with this message format.
  void clearSchemas();

  /// Adds a schema to this format and registers it with the appropriate processor.
  ///
  /// @param key Schema identification key
  /// @param fullyQualifiedName Fully qualified schema name
  /// @param location Location of the schema definition
  void addSchema(final String key, final String fullyQualifiedName, final String location);

  /// Finds schemas matching the given predicate.
  ///
  /// @param predicate Condition to test schemas against
  /// @return List of matching schema information
  List<SchemaInfo> findSchemas(final Predicate<SchemaInfo> predicate);

  /// Represents schema information with a fully qualified name and location.
  ///
  /// @param fullyQualifiedName Fully qualified name of the schema
  /// @param location Location or content of the schema (file path, URL, or inline content)
  record SchemaInfo(String fullyQualifiedName, String location) {
    /// Canonical constructor for SchemaInfo.
    ///
    /// @param fullyQualifiedName Fully qualified name of the schema
    /// @param location Location or content of the schema (file path, URL, or inline content)
    public SchemaInfo {
      Objects.requireNonNull(fullyQualifiedName, "Fully qualified name cannot be null");
      Objects.requireNonNull(location, "Location cannot be null");
    }
  }
}
