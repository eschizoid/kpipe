package org.kpipe.registry;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

/// Represents a message format abstraction for KPipe pipelines.
///
/// Implementations live in their own modules so users only pull in the format runtimes they
/// actually use:
///
/// - `org.kpipe.format.json.JsonFormat.INSTANCE` — JSON via DSL-JSON (`kpipe-format-json`)
/// - `org.kpipe.format.avro.AvroFormat.INSTANCE` — Avro (`kpipe-format-avro`)
/// - `org.kpipe.format.protobuf.ProtobufFormat.INSTANCE` — Protobuf (`kpipe-format-protobuf`)
/// - [#bytes] — identity passthrough for byte[] payloads (`kpipe-consumer`)
///
/// Custom formats can implement this interface directly. Schema operations are optional —
/// formats without schemas should return empty collections from [#getSchemas]/[#findSchema]
/// and treat [#addSchema] as a no-op.
///
/// @param <T> the type of data handled by this message format
public interface MessageFormat<T> {
  /// Returns the identity-passthrough [MessageFormat] for `byte[]` payloads.
  ///
  /// Use with the [TypedPipelineBuilder] when you want fluent operator chaining
  /// without real deserialization — tests, benchmarks, byte-level routing pipelines.
  ///
  /// @return a singleton MessageFormat<byte[]> whose deserialize/serialize are identity
  static MessageFormat<byte[]> bytes() {
    return BytesFormat.INSTANCE;
  }

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

  /// Serializes the given data to a byte array.
  ///
  /// @param data The data to serialize
  /// @return The serialized byte array
  byte[] serialize(final T data);

  /// Deserializes the given byte array to data of type T.
  ///
  /// @param data The serialized byte array
  /// @return The deserialized data
  T deserialize(final byte[] data);

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
