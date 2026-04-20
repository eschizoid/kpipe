package org.kpipe.processor;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/// Provides utility functions for processing Protobuf messages. This class contains common message
/// processors for use with the MessageProcessorRegistry to transform and enhance Protobuf data in
/// byte array format.
///
/// Example usage:
///
/// ```java
/// // Register a descriptor
/// ProtobufMessageProcessor.registerDescriptor("user", UserProto.getDescriptor());
///
/// // Create operators
/// final var addField = ProtobufMessageProcessor.addFieldOperator("source", "my-app");
/// final var addTimestamp = ProtobufMessageProcessor.addTimestampOperator("processed_at");
/// ```
public class ProtobufMessageProcessor {

  private ProtobufMessageProcessor() {}

  private static final ConcurrentHashMap<String, Descriptors.Descriptor> DESCRIPTOR_REGISTRY =
    new ConcurrentHashMap<>();

  /// Registers a Protobuf descriptor with the given name.
  ///
  /// @param name The name to register the descriptor under
  /// @param descriptor The Protobuf descriptor
  public static void registerDescriptor(final String name, final Descriptors.Descriptor descriptor) {
    DESCRIPTOR_REGISTRY.put(name, descriptor);
  }

  /// Gets a registered descriptor by name.
  ///
  /// @param name The name of the descriptor to retrieve
  /// @return The descriptor, or null if not found
  public static Descriptors.Descriptor getDescriptor(final String name) {
    return DESCRIPTOR_REGISTRY.get(name);
  }

  /// Creates an operator that sets a field value on a Protobuf message.
  ///
  /// ```java
  /// final var operator = ProtobufMessageProcessor.addFieldOperator("source", "my-app");
  /// ```
  ///
  /// @param fieldName The field name to set
  /// @param value The value to set
  /// @return UnaryOperator that sets a field on a Protobuf message
  public static UnaryOperator<Message> addFieldOperator(final String fieldName, final Object value) {
    return message -> {
      final var descriptor = message.getDescriptorForType();
      final var field = descriptor.findFieldByName(fieldName);
      if (field == null) return message;
      return message.toBuilder().setField(field, value).build();
    };
  }

  /// Creates an operator that sets multiple fields on a Protobuf message.
  ///
  /// ```java
  /// final var fields = Map.of("source", "my-app", "version", "1.0");
  /// final var operator = ProtobufMessageProcessor.addFieldsOperator(fields);
  /// ```
  ///
  /// @param fields Map of field names and values to set
  /// @return UnaryOperator that sets multiple fields on a Protobuf message
  public static UnaryOperator<Message> addFieldsOperator(final Map<String, Object> fields) {
    return message -> {
      final var descriptor = message.getDescriptorForType();
      var builder = message.toBuilder();
      for (final var entry : fields.entrySet()) {
        final var field = descriptor.findFieldByName(entry.getKey());
        if (field != null) builder = builder.setField(field, entry.getValue());
      }
      return builder.build();
    };
  }

  /// Creates an operator that sets a timestamp field (as epoch millis long) on a Protobuf message.
  ///
  /// ```java
  /// final var operator = ProtobufMessageProcessor.addTimestampOperator("processed_at");
  /// ```
  ///
  /// @param fieldName The name of the timestamp field
  /// @return UnaryOperator that sets the current timestamp on a Protobuf message
  public static UnaryOperator<Message> addTimestampOperator(final String fieldName) {
    return message -> {
      final var descriptor = message.getDescriptorForType();
      final var field = descriptor.findFieldByName(fieldName);
      if (field == null) return message;
      return message.toBuilder().setField(field, System.currentTimeMillis()).build();
    };
  }

  /// Creates an operator that clears specified fields from a Protobuf message.
  ///
  /// ```java
  /// final var operator = ProtobufMessageProcessor.removeFieldsOperator("password", "ssn");
  /// ```
  ///
  /// @param fields Field names to clear
  /// @return UnaryOperator that clears fields from a Protobuf message
  public static UnaryOperator<Message> removeFieldsOperator(final String... fields) {
    final var fieldsToRemove = Set.of(fields);
    return message -> {
      final var descriptor = message.getDescriptorForType();
      var builder = message.toBuilder();
      for (final var name : fieldsToRemove) {
        final var field = descriptor.findFieldByName(name);
        if (field != null) builder = builder.clearField(field);
      }
      return builder.build();
    };
  }

  /// Creates an operator that transforms a field using the provided function.
  ///
  /// ```java
  /// final var operator = ProtobufMessageProcessor.transformFieldOperator(
  ///     "email",
  ///     val -> val.toString().toLowerCase()
  /// );
  /// ```
  ///
  /// @param fieldName Field to transform
  /// @param transformer Function to apply to the field value
  /// @return UnaryOperator that transforms the specified field in a Protobuf message
  public static UnaryOperator<Message> transformFieldOperator(
    final String fieldName,
    final Function<Object, Object> transformer
  ) {
    return message -> {
      final var descriptor = message.getDescriptorForType();
      final var field = descriptor.findFieldByName(fieldName);
      if (field == null) return message;
      final var currentValue = message.getField(field);
      return message.toBuilder().setField(field, transformer.apply(currentValue)).build();
    };
  }

  /// Clears the descriptor registry.
  public static void clearDescriptorRegistry() {
    DESCRIPTOR_REGISTRY.clear();
  }
}

