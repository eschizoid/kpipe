package org.kpipe.registry;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.kpipe.processor.ProtobufMessageProcessor;

/// Protobuf implementation of MessageFormat for KPipe.
///
/// This class manages Protobuf descriptors and provides serialization/deserialization for Protobuf
/// messages. It supports registering descriptors and integrates with the ProtobufMessageProcessor
/// for descriptor registration and optimized Protobuf handling.
///
/// Example:
/// ```java
/// final var protoFormat = MessageFormat.PROTOBUF;
/// protoFormat.addDescriptor("user", UserProto.getDescriptor());
/// protoFormat.withDefaultDescriptor("user");
/// byte[] bytes = protoFormat.serialize(message);
/// Message result = protoFormat.deserialize(bytes);
/// ```
public final class ProtobufFormat implements MessageFormat<Message> {

  private final Map<String, SchemaInfo> schemas = new ConcurrentHashMap<>();
  private final Map<String, Descriptors.Descriptor> descriptors = new ConcurrentHashMap<>();
  private String defaultDescriptorKey;

  /// Constructs a new ProtobufFormat instance.
  public ProtobufFormat() {
    // Default constructor
  }

  /// Registers a Protobuf descriptor with this format.
  ///
  /// @param key The key to register the descriptor under
  /// @param descriptor The Protobuf descriptor
  /// @return This ProtobufFormat instance
  public ProtobufFormat addDescriptor(final String key, final Descriptors.Descriptor descriptor) {
    descriptors.put(key, descriptor);
    ProtobufMessageProcessor.registerDescriptor(key, descriptor);
    schemas.put(key, new SchemaInfo(descriptor.getFullName(), descriptor.toProto().toString()));
    return this;
  }

  /// Sets the default descriptor key to use for deserialization.
  ///
  /// @param descriptorKey The descriptor key
  /// @return This ProtobufFormat instance
  public ProtobufFormat withDefaultDescriptor(final String descriptorKey) {
    this.defaultDescriptorKey = descriptorKey;
    return this;
  }

  /// Gets a registered descriptor by key.
  ///
  /// @param key The descriptor key
  /// @return The descriptor, or null if not found
  public Descriptors.Descriptor getDescriptor(final String key) {
    return descriptors.get(key);
  }

  /// Returns an unmodifiable view of all schemas registered with this format.
  ///
  /// @return Map of schema keys to their schema information
  @Override
  public Map<String, SchemaInfo> getSchemas() {
    return Collections.unmodifiableMap(schemas);
  }

  /// Finds a schema by its key.
  ///
  /// @param key the schema key to search for
  /// @return an Optional containing the SchemaInfo if found, or empty if not found
  @Override
  public Optional<SchemaInfo> findSchema(final String key) {
    return Optional.ofNullable(schemas.get(key));
  }

  /// Removes all schemas and descriptors registered with this message format.
  @Override
  public void clearSchemas() {
    schemas.clear();
    descriptors.clear();
    defaultDescriptorKey = null;
  }

  /// Adds a schema to this format. For Protobuf, prefer [addDescriptor] which registers the
  /// actual descriptor. This method stores schema metadata only.
  ///
  /// @param key schema identification key
  /// @param fullyQualifiedName fully qualified schema name
  /// @param location location of the schema definition (e.g., .proto file path)
  @Override
  public void addSchema(final String key, final String fullyQualifiedName, final String location) {
    schemas.put(key, new SchemaInfo(fullyQualifiedName, location));
  }

  /// Finds schemas matching the given predicate.
  ///
  /// @param predicate condition to test schemas against
  /// @return list of matching schema information
  @Override
  public List<SchemaInfo> findSchemas(final Predicate<SchemaInfo> predicate) {
    return schemas.values().stream().filter(predicate).toList();
  }

  /// Serializes the given Protobuf Message to a byte array.
  ///
  /// @param data the Protobuf message to serialize
  /// @return the serialized byte array
  @Override
  public byte[] serialize(final Message data) {
    if (data == null) return null;
    return data.toByteArray();
  }

  /// Deserializes the given byte array to a Protobuf Message using the default descriptor.
  ///
  /// @param data the serialized byte array
  /// @return the deserialized message as a DynamicMessage
  @Override
  public Message deserialize(final byte[] data) {
    if (data == null || data.length == 0) return null;
    if (defaultDescriptorKey == null) {
      throw new UnsupportedOperationException(
        "Protobuf deserialization requires a default descriptor key. Use withDefaultDescriptor()."
      );
    }
    final var descriptor = descriptors.get(defaultDescriptorKey);
    if (descriptor == null) {
      throw new IllegalArgumentException("No descriptor found for key: %s".formatted(defaultDescriptorKey));
    }
    try {
      return DynamicMessage.parseFrom(descriptor, data);
    } catch (final InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to deserialize Protobuf message", e);
    }
  }
}
