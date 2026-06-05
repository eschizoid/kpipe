package io.github.eschizoid.kpipe.format.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import java.util.Objects;

/// Protobuf codec for KPipe — stateless `MessageFormat<Message>` bound to a single
/// [Descriptor]. One descriptor per instance, no mutable state. For multiple descriptors keyed
/// by name, use [ProtobufDescriptorCatalog] and pass the resolved descriptor here.
///
/// ```java
/// // Direct: you have a Descriptor in hand.
/// final var format = new ProtobufFormat(UserProto.getDescriptor());
///
/// // Catalog-mediated: multiple descriptors keyed by name.
/// final var catalog = new ProtobufDescriptorCatalog();
/// catalog.add("user", UserProto.getDescriptor());
/// final var format = new ProtobufFormat(catalog.get("user"));
/// ```
public final class ProtobufFormat implements MessageFormat<Message> {

  private final Descriptor descriptor;

  /// Constructs a codec bound to `descriptor`.
  ///
  /// @param descriptor the Protobuf message descriptor used for deserialization (must be non-null)
  public ProtobufFormat(final Descriptor descriptor) {
    this.descriptor = Objects.requireNonNull(descriptor, "descriptor cannot be null");
  }

  /// Returns the descriptor this codec is bound to.
  ///
  /// @return the bound descriptor (never null)
  public Descriptor descriptor() {
    return descriptor;
  }

  /// Creates a new [ProtobufConsoleSink] for [Message] payloads.
  ///
  /// @return a new console sink
  public ProtobufConsoleSink<Message> consoleSink() {
    return new ProtobufConsoleSink<>();
  }

  /// Serializes a Protobuf [Message] to bytes.
  ///
  /// @param data the message to serialize
  /// @return the binary-encoded bytes, or null if `data` is null
  @Override
  public byte[] serialize(final Message data) {
    if (data == null) return null;
    return data.toByteArray();
  }

  /// Deserializes bytes to a Protobuf [Message] using the bound descriptor.
  ///
  /// @param data the binary-encoded bytes
  /// @return the decoded message (a [DynamicMessage]), or null if `data` is null/empty
  @Override
  public Message deserialize(final byte[] data) {
    if (data == null || data.length == 0) return null;
    try {
      return DynamicMessage.parseFrom(descriptor, data);
    } catch (final InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to deserialize Protobuf message", e);
    }
  }
}
