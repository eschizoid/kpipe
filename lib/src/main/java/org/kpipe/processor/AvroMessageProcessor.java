package org.kpipe.processor;

import java.io.ByteArrayInputStream;
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
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
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
/// final var pipeline = registry.avroPipeline(
///     "userSchema",
///     "addTimestamp_userSchema"
/// );
///
/// // Process a message
/// byte[] result = pipeline.apply(avroBytes);
/// ```
public class AvroMessageProcessor {

  private AvroMessageProcessor() {}

  private static final Logger LOGGER = System.getLogger(AvroMessageProcessor.class.getName());
  private static final byte[] EMPTY_AVRO = new byte[0];
  private static final ConcurrentHashMap<String, Schema> SCHEMA_REGISTRY = new ConcurrentHashMap<>();

  private static final ThreadLocal<Schema.Parser> SCHEMA_PARSER = ThreadLocal.withInitial(Schema.Parser::new);
  private static final ThreadLocal<ByteArrayOutputStream> OUTPUT_STREAM_CACHE = ThreadLocal.withInitial(() ->
    new ByteArrayOutputStream(8192)
  );
  private static final ThreadLocal<BinaryEncoder> ENCODER_CACHE = ThreadLocal.withInitial(() -> null);
  private static final ThreadLocal<BinaryDecoder> DECODER_CACHE = ThreadLocal.withInitial(() -> null);

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
    final var schema = SCHEMA_PARSER.get().parse(schemaJson);
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
            // Handle removal based on a field type
            if (field.schema().getType() == Schema.Type.STRING) {
              newRecord.put(fieldName, "");
            } else if (field.schema().getType() == Schema.Type.UNION) {
              // For union types, check if null is allowed
              if (field.schema().getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL)) {
                newRecord.put(fieldName, null);
              } else {
                // Use default value if provided
                newRecord.put(fieldName, field.defaultVal());
              }
            } else {
              // Use default value for other types
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
            // Field to transform
            if (field.schema().getType() == Schema.Type.UNION) {
              // Handle union types
              var transformedValue = value;
              if (value != null) {
                // Apply transformation
                transformedValue = transformer.apply(value instanceof Utf8 ? value.toString() : value);

                // Validate transformed value against the union schema
                var isValid = false;
                for (final var typeSchema : field.schema().getTypes()) {
                  // Skip null schema
                  if (typeSchema.getType() == Schema.Type.NULL) {
                    if (transformedValue == null) {
                      isValid = true;
                      break;
                    }
                    continue;
                  }

                  // Check if a transformation result matches a valid type
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
            } else if (value instanceof Utf8) {
              // Handle Avro's Utf8 type
              final var stringValue = value.toString();
              final var transformedValue = transformer.apply(stringValue);
              newRecord.put(currentFieldName, transformedValue);
            } else {
              // Apply transformer to other value types
              newRecord.put(currentFieldName, transformer.apply(value));
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

  /// Applies a processing function to parsed Avro data with internal caching for
  /// performance.
  ///
  /// ```java
  /// byte[] result = AvroMessageProcessor.processAvro(
  ///     avroBytes,
  ///     schema,
  ///     record -> {
  ///         record.put("status", "processed");
  ///         return record;
  ///     }
  /// );
  /// ```
  ///
  /// @param avroBytes The raw Avro data as a byte array
  /// @param schema The Avro schema to use for parsing and serializing
  /// @param processor Function to transform the parsed Avro record
  /// @return Serialized Avro bytes after processing
  public static byte[] processAvro(
    final byte[] avroBytes,
    final Schema schema,
    final Function<GenericRecord, GenericRecord> processor
  ) {
    return processAvro(avroBytes, 0, schema, processor);
  }

  /// Applies a processing function to parsed Avro data, optionally skipping a prefix.
  ///
  /// This method is particularly useful for handling Avro data with custom headers or
  /// magic bytes without needing to copy the byte array first.
  ///
  /// ```java
  /// // Skip 5 magic bytes and process
  /// byte[] result = AvroMessageProcessor.processAvro(avroBytes, 5, schema, record -> record);
  /// ```
  ///
  /// @param avroBytes The raw Avro data as a byte array
  /// @param offset The number of bytes to skip at the start
  /// @param schema The Avro schema to use for parsing and serializing
  /// @param processor Function to transform the parsed Avro record
  /// @return Serialized Avro bytes after processing
  public static byte[] processAvro(
    final byte[] avroBytes,
    final int offset,
    final Schema schema,
    final Function<GenericRecord, GenericRecord> processor
  ) {
    if (avroBytes == null || avroBytes.length <= offset) return EMPTY_AVRO;

    try {
      // Create a reader and writer
      final var reader = new GenericDatumReader<GenericRecord>(schema);
      final var writer = new GenericDatumWriter<GenericRecord>(schema);

      // Deserialize using cached decoder
      final var inputStream = new ByteArrayInputStream(avroBytes, offset, avroBytes.length - offset);
      final var decoder = DecoderFactory.get().binaryDecoder(inputStream, DECODER_CACHE.get());
      DECODER_CACHE.set(decoder);
      final var record = reader.read(null, decoder);

      // Apply the processor function to make a copy with transformations
      if (record == null) return EMPTY_AVRO;
      final var processed = processor.apply(record);

      if (processed == null) return EMPTY_AVRO;
      LOGGER.log(Level.DEBUG, "Processed record: %s".formatted(processed));

      // Reuse output stream for better performance
      final var outputStream = OUTPUT_STREAM_CACHE.get();
      outputStream.reset();

      // Reuse encoder for better performance
      final var encoder = EncoderFactory.get().binaryEncoder(outputStream, ENCODER_CACHE.get());
      ENCODER_CACHE.set(encoder);

      writer.write(processed, encoder);
      encoder.flush();

      return outputStream.toByteArray();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error processing Avro", e);
      return EMPTY_AVRO;
    }
  }

  /// Clears the schema registry.
  public static void clearSchemaRegistry() {
    SCHEMA_REGISTRY.clear();
    SCHEMA_PARSER.set(new Schema.Parser());
  }
}
