package org.kpipe.registry;

/// Represents a message format abstraction for KPipe pipelines.
///
/// Implementations live in their own modules so users only pull in the format runtimes they
/// actually use:
///
/// - `org.kpipe.format.json.JsonFormat.INSTANCE` — JSON via DSL-JSON (`kpipe-format-json`)
/// - `org.kpipe.format.avro.AvroFormat.INSTANCE` — Avro (`kpipe-format-avro`)
/// - `org.kpipe.format.protobuf.ProtobufFormat.INSTANCE` — Protobuf (`kpipe-format-protobuf`)
/// - [#bytes] — identity passthrough for byte[] payloads (`kpipe-core`)
///
/// Formats whose payloads carry a schema or descriptor (Avro, Protobuf) implement
/// [SchemaAwareFormat] instead, which adds schema-management methods on top of this contract.
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
