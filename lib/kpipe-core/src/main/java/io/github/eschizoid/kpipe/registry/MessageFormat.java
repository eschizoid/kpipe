package io.github.eschizoid.kpipe.registry;

/// Represents a message format abstraction for KPipe pipelines.
///
/// Implementations live in their own modules so users only pull in the format runtimes they
/// actually use:
///
/// - `io.github.eschizoid.kpipe.format.json.JsonFormat.INSTANCE` — JSON via fastjson2
// (`kpipe-format-json`)
/// - `io.github.eschizoid.kpipe.format.avro.AvroFormat` — Avro (`kpipe-format-avro`); construct
// with
///   `new AvroFormat(schema)` or `AvroFormat.of(schemaJson)`.
/// - `io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat` — Protobuf
// (`kpipe-format-protobuf`); construct
///   with `new ProtobufFormat(descriptor)`.
/// - [#bytes] — identity passthrough for byte[] payloads (`kpipe-core`)
///
/// Schema-bound formats (Avro, Protobuf) take their schema or descriptor at construction time so
/// each format instance owns exactly one wire shape — no JVM-global mutable schema registry.
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
}
