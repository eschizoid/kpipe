package org.kpipe.processor;

import java.io.ByteArrayOutputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.util.Utf8;

/// Provides utility functions for processing Avro messages. This class contains common message
/// processors for use with the MessageProcessorRegistry to transform and enhance Avro data in byte
/// array format.
///
/// Example usage:
///
/// ```java
/// // Register a schema
/// final var userSchemaJson = """
///   {
///     "type": "record",
///     "name": "User",
///     "fields": [
///       {"name": "name", "type": "string"},
///       {"name": "age", "type": "int"}
///     ]
///   }
///   """;
/// AvroMessageProcessor.registerSchema("userSchema", userSchemaJson);
///
/// // Create an optimized pipeline using these processors
/// final var pipeline = registry.pipeline(MessageFormat.AVRO)
///     .add(RegistryKey.avro("addTimestamp_userSchema"))
///     .build();
///
/// // Process a message
/// byte[] result = pipeline.apply(avroBytes);
/// ```
public class AvroMessageProcessor {

  private AvroMessageProcessor() {}

  private static final Logger LOGGER = System.getLogger(AvroMessageProcessor.class.getName());
  private static final ConcurrentHashMap<String, Schema> SCHEMA_REGISTRY = new ConcurrentHashMap<>();

  private static final ScopedValue<Schema.Parser> SCHEMA_PARSER = ScopedValue.newInstance();
  private static final ScopedValue<ByteArrayOutputStream> OUTPUT_STREAM_CACHE = ScopedValue.newInstance();
  private static final ScopedValue<BinaryEncoder> ENCODER_CACHE = ScopedValue.newInstance();
  private static final ScopedValue<BinaryDecoder> DECODER_CACHE = ScopedValue.newInstance();

  /// Registers an Avro schema with the given name.
  ///
  /// ```java
  /// final var userSchemaJson = """
  ///   {
  ///     "type": "record",
  ///     "name": "User",
  ///     "fields": [
  ///       {"name": "name", "type": "string"},
  ///       {"name": "age", "type": "int"}
  ///     ]
  ///   }
  ///   """;
  /// AvroMessageProcessor.registerSchema("userSchema", userSchemaJson);
  /// ```
  ///
  /// @param name The name to register the schema under
  /// @param schemaJson The Avro schema in JSON format
  /// @throws org.apache.avro.SchemaParseException if the schema is invalid
  public static void registerSchema(final String name, final String schemaJson) {
    final var parser = SCHEMA_PARSER.isBound() ? SCHEMA_PARSER.get() : new Schema.Parser();
    final var schema = parser.parse(schemaJson);
    SCHEMA_REGISTRY.put(name, schema);
  }

  /// Gets a registered schema by name.
  ///
  /// ```java
  /// final var schema = AvroMessageProcessor.getSchema("userSchema");
  /// ```
  ///
  /// @param name The name of the schema to retrieve
  /// @return The schema, or null if not found
  public static Schema getSchema(final String name) {
    return SCHEMA_REGISTRY.get(name);
  }

  /// Creates an operator that adds a field with specified key and value to an Avro record.
  ///
  /// ```java
  /// final var operator = AvroMessageProcessor.addFieldOperator("source", "my-app");
  /// ```
  ///
  /// @param key The field name to add
  /// @param value The value to associate with the field
  /// @return UnaryOperator that adds a field to an Avro record
  public static UnaryOperator<GenericRecord> addFieldOperator(final String key, final Object value) {
    return record -> {
      record.put(key, value);
      return record;
    };
  }

  /// Creates an operator that adds multiple fields to an Avro record.
  ///
  /// ```java
  /// final var fields = Map.of("source", "my-app", "version", "1.0");
  /// final var operator = AvroMessageProcessor.addFieldsOperator(fields);
  /// ```
  ///
  /// @param fields Map of field names and values to add
  /// @return UnaryOperator that adds multiple fields to an Avro record
  public static UnaryOperator<GenericRecord> addFieldsOperator(final Map<String, Object> fields) {
    return record -> {
      fields.forEach(record::put);
      return record;
    };
  }

  /// Creates an operator that adds a timestamp field to an Avro record.
  ///
  /// ```java
  /// final var operator = AvroMessageProcessor.addTimestampOperator("processed_at");
  /// ```
  ///
  /// @param fieldName The name of the timestamp field
  /// @return UnaryOperator that adds current timestamp to an Avro record
  public static UnaryOperator<GenericRecord> addTimestampOperator(final String fieldName) {
    return record -> {
      record.put(fieldName, System.currentTimeMillis());
      return record;
    };
  }

  /// Creates an operator that removes specified fields from an Avro record.
  ///
  /// ```java
  /// final var operator = AvroMessageProcessor.removeFieldsOperator(schema, "password", "ssn");
  /// ```
  ///
  /// @param schema The schema of the record
  /// @param fields Field names to remove
  /// @return UnaryOperator that removes fields from an Avro record
  public static UnaryOperator<GenericRecord> removeFieldsOperator(final Schema schema, final String... fields) {
    final var fieldsToRemove = Set.of(fields);
    return record -> {
      final var newRecord = new GenericData.Record(schema);
      schema
        .getFields()
        .forEach(field -> {
          final var fieldName = field.name();
          if (fieldsToRemove.contains(fieldName)) {
            final Schema fieldSchema = field.schema();
            switch (fieldSchema.getType()) {
              case STRING:
                newRecord.put(fieldName, "");
                break;
              case UNION:
                final boolean allowsNull = fieldSchema
                  .getTypes()
                  .stream()
                  .anyMatch(s -> s.getType() == Schema.Type.NULL);
                newRecord.put(fieldName, allowsNull ? null : field.defaultVal());
                break;
              default:
                newRecord.put(fieldName, field.defaultVal());
            }
          } else {
            // Copy existing value
            newRecord.put(fieldName, record.get(fieldName));
          }
        });
      return newRecord;
    };
  }

  /// Creates an operator that transforms a field using the provided function.
  ///
  /// ```java
  /// final var operator = AvroMessageProcessor.transformFieldOperator(
  ///     schema,
  ///     "email",
  ///     val -> val.toString().toLowerCase()
  /// );
  /// ```
  ///
  /// @param schema The Avro schema to use
  /// @param fieldName Field to transform
  /// @param transformer Function to apply to the field value
  /// @return UnaryOperator that transforms the specified field in an Avro record
  public static UnaryOperator<GenericRecord> transformFieldOperator(
    final Schema schema,
    final String fieldName,
    final Function<Object, Object> transformer
  ) {
    return record -> {
      final var newRecord = new GenericData.Record(schema);

      schema
        .getFields()
        .forEach(field -> {
          final var currentFieldName = field.name();
          final var value = record.get(currentFieldName);
          if (currentFieldName.equals(fieldName)) {
            final Schema fieldSchema = field.schema();
            switch (fieldSchema.getType()) {
              case UNION: {
                // Handle union types: try transform, then validate against union member
                // types
                var transformedValue = value;
                if (value != null) {
                  transformedValue = transformer.apply(value instanceof Utf8 ? value.toString() : value);

                  // Validate transformed value against the union schema
                  var isValid = false;
                  for (final var typeSchema : fieldSchema.getTypes()) {
                    if (typeSchema.getType() == Schema.Type.NULL) {
                      if (transformedValue == null) {
                        isValid = true;
                        break;
                      }
                      continue;
                    }
                    if (isCompatibleWithSchema(transformedValue, typeSchema)) {
                      isValid = true;
                      break;
                    }
                  }

                  if (!isValid) {
                    LOGGER.log(
                      Level.WARNING,
                      "Transformed value %s is not compatible with union schema for field %s, keeping original".formatted(
                        transformedValue,
                        fieldName
                      )
                    );
                    transformedValue = value; // Revert to original if invalid
                  }
                }
                newRecord.put(currentFieldName, transformedValue);
                break;
              }
              case STRING: {
                // Handle Avro's Utf8 and Strings consistently
                final var inputAsString =
                  value instanceof Utf8 ? value.toString() : (value == null ? null : value.toString());
                final var transformedValue = transformer.apply(inputAsString);
                newRecord.put(currentFieldName, transformedValue);
                break;
              }
              default: {
                // Apply transformer to other value types
                newRecord.put(currentFieldName, transformer.apply(value));
                break;
              }
            }
          } else {
            // Copy value unchanged
            newRecord.put(currentFieldName, value);
          }
        });

      return newRecord;
    };
  }

  /// Checks if a value is compatible with an Avro schema type.
  ///
  /// @param value The value to check
  /// @param schema The schema to validate against
  /// @return true if compatible, false otherwise
  private static boolean isCompatibleWithSchema(Object value, Schema schema) {
    return switch (schema.getType()) {
      case NULL -> value == null;
      case BOOLEAN -> value instanceof Boolean;
      case INT -> value instanceof Integer;
      case LONG -> value instanceof Long;
      case FLOAT -> value instanceof Float;
      case DOUBLE -> value instanceof Double;
      case STRING -> value instanceof String || value instanceof Utf8;
      case ENUM -> value instanceof GenericEnumSymbol ||
      (value instanceof String && schema.hasEnumSymbol((String) value));
      default -> GenericData.get().validate(schema, value);
    };
  }

  /// Executes an operation within scoped caches for Avro processing.
  ///
  /// @param <T> The result type of the operation
  /// @param operation The operation to execute within the scoped caches
  /// @return The result of the operation
  public static <T> T inScopedCaches(final ScopedValue.CallableOp<T, Exception> operation) {
    try {
      return ScopedValue.where(OUTPUT_STREAM_CACHE, new ByteArrayOutputStream(8192))
        .where(ENCODER_CACHE, null)
        .where(DECODER_CACHE, null)
        .where(SCHEMA_PARSER, new Schema.Parser())
        .call(operation);
    } catch (final Exception e) {
      throw new RuntimeException("Error executing in Avro scoped caches", e);
    }
  }

  /// Clears the schema registry.
  public static void clearSchemaRegistry() {
    SCHEMA_REGISTRY.clear();
  }
}
