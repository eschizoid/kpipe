package org.kpipe.processor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

/**
 * Provides utility functions for processing Avro messages. This class contains common message
 * processors for use with the MessageProcessorRegistry to transform and enhance Avro data in byte
 * array format.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Register a schema
 * final var userSchemaJson = """
 *   {
 *     "type": "record",
 *     "name": "User",
 *     "fields": [
 *       {"name": "name", "type": "string"},
 *       {"name": "age", "type": "int"}
 *     ]
 *   }
 *   """;
 * AvroMessageProcessor.registerSchema("userSchema", userSchemaJson);
 *
 * // Create a pipeline using these processors
 * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
 *     "parseAvro",
 *     MessageProcessorRegistry.get("addTimestampAvro")
 * );
 *
 * // Process a message
 * byte[] result = pipeline.apply(avroBytes);
 * }</pre>
 */
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

  /**
   * Registers an Avro schema with the given name.
   *
   * <pre>{@code
   * final var userSchemaJson = """
   *   {
   *     "type": "record",
   *     "name": "User",
   *     "fields": [
   *       {"name": "name", "type": "string"},
   *       {"name": "age", "type": "int"}
   *     ]
   *   }
   *   """;
   * AvroMessageProcessor.registerSchema("userSchema", userSchemaJson);
   * }</pre>
   *
   * @param name The name to register the schema under
   * @param schemaJson The Avro schema in JSON format
   * @throws org.apache.avro.SchemaParseException if the schema is invalid
   */
  public static void registerSchema(final String name, final String schemaJson) {
    final var schema = SCHEMA_PARSER.get().parse(schemaJson);
    SCHEMA_REGISTRY.put(name, schema);
  }

  /**
   * Gets a registered schema by name.
   *
   * <pre>{@code
   * final var schema = AvroMessageProcessor.getSchema("userSchema");
   * }</pre>
   *
   * @param name The name of the schema to retrieve
   * @return The schema, or null if not found
   */
  public static Schema getSchema(final String name) {
    return SCHEMA_REGISTRY.get(name);
  }

  /**
   * Creates a processor function that parses raw Avro bytes using a registered schema.
   *
   * <pre>{@code
   * final var userSchemaJson = """
   *   {
   *     "type": "record",
   *     "name": "User",
   *     "fields": [
   *       {"name": "name", "type": "string"},
   *       {"name": "age", "type": "int"}
   *     ]
   *   }
   *   """;
   * AvroMessageProcessor.registerSchema("userSchema", userSchemaJson);
   *
   * // Register the processor
   * MessageProcessorRegistry.register("parseAvro", AvroMessageProcessor.parseAvro("userSchema"));
   *
   * // Use it to validate Avro
   * byte[] rawMessage = ...; // Avro serialized bytes
   * byte[] validAvro = MessageProcessorRegistry.get("parseAvro").apply(rawMessage);
   * }</pre>
   *
   * @param schemaName The name of the registered schema to use for parsing
   * @return Function that parses and reserializes Avro data without modifying content
   * @throws IllegalArgumentException if the schema is not registered
   */
  public static Function<byte[], byte[]> parseAvro(final String schemaName) {
    final var schema = getSchema(schemaName);
    if (schema == null) throw new IllegalArgumentException("Schema not registered: %s".formatted(schemaName));
    return parseAvro(schema);
  }

  /**
   * Creates a processor function that parses Avro bytes with a magic byte prefix (e.g., Confluent
   * wire format). This is useful when consuming Avro data from sources like Kafka that use a schema
   * registry and prepend a magic byte and schema ID.
   *
   * <p>The function skips the first `magicByte` bytes (typically 5 for Confluent: 1 magic byte +
   * 4-byte schema ID).
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Register schema with the registry
   * MessageFormat.AVRO.addSchema(
   *   "customerSchema",
   *   "com.example.Customer",
   *   "http://schema-registry:8081/subjects/com.example.Customer/versions/latest"
   * );
   *
   * // Register an Avro processor that handles Confluent Schema Registry wire format
   * registry.register(
   *   "parseAvroCustomer",
   *   AvroMessageProcessor.parseAvroWithMagicBytes("customerSchema", 5)
   * );
   *
   * // Use it in a processing pipeline
   * final var pipeline = registry.pipeline("parseAvroCustomer", "addMetadata");
   * final var processedValue = pipeline.apply(confluentAvroBytes);
   *
   * // Now you can work with the parsed Avro record as a byte array with the
   * // original Avro content but without the magic byte header
   * }</pre>
   *
   * @param schemaName The name of the registered schema to use for parsing
   * @param magicByte The number of bytes to skip at the start (e.g., 5 for Confluent-encoded Avro)
   * @return Function that parses and reserializes Avro data after removing the magic byte prefix
   */
  public static Function<byte[], byte[]> parseAvroWithMagicBytes(final String schemaName, final int magicByte) {
    return avroBytes -> {
      if (avroBytes.length > magicByte && avroBytes[0] == 0) {
        final var content = Arrays.copyOfRange(avroBytes, magicByte, avroBytes.length);
        return processAvro(content, getSchema(schemaName), record -> record);
      } else {
        return processAvro(avroBytes, getSchema(schemaName), record -> record);
      }
    };
  }

  /**
   * Creates a processor function that parses raw Avro bytes. This can be used to validate Avro or
   * as the first step in a processing pipeline.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Register the processor
   * MessageProcessorRegistry.register("parseAvro", AvroMessageProcessor.parseAvro(schema));
   *
   * // Use it to validate Avro
   * byte[] rawMessage = ...; // Avro serialized bytes
   * byte[] validAvro = MessageProcessorRegistry.get("parseAvro").apply(rawMessage);
   * }</pre>
   *
   * @param schema The Avro schema to use for parsing
   * @return Function that parses and reserializes Avro data without modifying content
   */
  public static Function<byte[], byte[]> parseAvro(final Schema schema) {
    return avroBytes -> processAvro(avroBytes, schema, record -> record);
  }

  /**
   * Creates a processor function that adds a field with a specified key and value to Avro data,
   * using a registered schema.
   *
   * <p>Example:
   *
   * <pre>{@code
   * final var userSchemaJson = """
   *   {
   *     "type": "record",
   *     "name": "User",
   *     "fields": [
   *       {"name": "name", "type": "string"},
   *       {"name": "age", "type": "int"},
   *       {"name": "source", "type": "string", "default": ""}
   *     ]
   *   }
   *   """;
   * AvroMessageProcessor.registerSchema("userSchema", userSchemaJson);
   *
   * // Create a processor that adds a source field
   * Function<byte[], byte[]> addSourceProcessor =
   *     AvroMessageProcessor.addField("userSchema", "source", "payment-service");
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("addSourceAvro", addSourceProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseAvro", "addSourceAvro", "addTimestampAvro"
   * );
   *
   * // Apply to a message
   * byte[] inputAvro = ...; // Avro serialized bytes
   * byte[] outputAvro = pipeline.apply(inputAvro);
   * }</pre>
   *
   * @param schemaName The name of the registered schema to use
   * @param key The field name to add
   * @param value The value to associate with the field
   * @return Function that adds a field to Avro data
   * @throws IllegalArgumentException if the schema is not registered
   */
  public static Function<byte[], byte[]> addField(final String schemaName, final String key, final String value) {
    final var schema = getSchema(schemaName);
    if (schema == null) throw new IllegalArgumentException("Schema not registered: %s".formatted(schemaName));
    return addField(schema, key, value);
  }

  /**
   * Creates a processor function that adds a field with a specified key and value to Avro data.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a processor that adds a source field
   * Function<byte[], byte[]> addSourceProcessor =
   *     AvroMessageProcessor.addField(schema, "source", "payment-service");
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("addSourceAvro", addSourceProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseAvro", "addSourceAvro", "addTimestampAvro"
   * );
   *
   * // Apply to a message
   * byte[] inputAvro = ...; // Avro serialized bytes
   * byte[] outputAvro = pipeline.apply(inputAvro);
   * }</pre>
   *
   * @param schema The Avro schema to use
   * @param key The field name to add
   * @param value The value to associate with the field
   * @return Function that adds a field to Avro data
   */
  public static Function<byte[], byte[]> addField(final Schema schema, final String key, final String value) {
    return avroBytes ->
      processAvro(
        avroBytes,
        schema,
        record -> {
          record.put(key, value);
          return record;
        }
      );
  }

  /**
   * Creates a processor that adds a timestamp field to Avro data, using a registered schema.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Register a schema
   * final var userSchemaJson = """
   *   {
   *     "type": "record",
   *     "name": "User",
   *     "fields": [
   *       {"name": "name", "type": "string"},
   *       {"name": "age", "type": "int"},
   *       {"name": "processedAt", "type": "long", "default": 0}
   *     ]
   *   }
   *   """;
   * AvroMessageProcessor.registerSchema("userSchema", userSchemaJson);
   *
   * // Create a processor that adds a timestamp
   * Function<byte[], byte[]> addTimestampProcessor =
   *     AvroMessageProcessor.addTimestamp("userSchema", "processedAt");
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("addTimestampAvro", addTimestampProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseAvro", "addTimestampAvro"
   * );
   *
   * // Apply to a message
   * byte[] inputAvro = ...; // Avro serialized bytes
   * byte[] outputAvro = pipeline.apply(inputAvro);
   * }</pre>
   *
   * @param schemaName The name of the registered schema to use
   * @param fieldName The name of the timestamp field
   * @return Function that adds the current timestamp to Avro
   * @throws IllegalArgumentException if the schema is not registered
   */
  public static Function<byte[], byte[]> addTimestamp(final String schemaName, final String fieldName) {
    final var schema = getSchema(schemaName);
    if (schema == null) throw new IllegalArgumentException("Schema not registered: %s".formatted(schemaName));
    return addTimestamp(schema, fieldName);
  }

  /**
   * Creates a processor that adds a timestamp field to Avro data.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a processor that adds a timestamp
   * Function<byte[], byte[]> addTimestampProcessor =
   *     AvroMessageProcessor.addTimestamp(schema, "processedAt");
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("addTimestampAvro", addTimestampProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseAvro", "addTimestampAvro"
   * );
   *
   * // Apply to a message
   * byte[] inputAvro = ...; // Avro serialized bytes
   * byte[] outputAvro = pipeline.apply(inputAvro);
   * }</pre>
   *
   * @param schema The Avro schema to use
   * @param fieldName The name of the timestamp field
   * @return Function that adds the current timestamp to Avro
   */
  public static Function<byte[], byte[]> addTimestamp(final Schema schema, final String fieldName) {
    return avroBytes ->
      processAvro(
        avroBytes,
        schema,
        record -> {
          record.put(fieldName, System.currentTimeMillis());
          return record;
        }
      );
  }

  /**
   * Creates a processor that removes specified fields from Avro data, using a registered schema.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Register a schema
   * final var userSchemaJson = """
   *   {
   *     "type": "record",
   *     "name": "User",
   *     "fields": [
   *       {"name": "name", "type": "string"},
   *       {"name": "password", "type": ["null", "string"], "default": null},
   *       {"name": "creditCard", "type": ["null", "string"], "default": null}
   *     ]
   *   }
   *   """;
   * AvroMessageProcessor.registerSchema("userSchema", userSchemaJson);
   *
   * // Create a processor that removes sensitive fields
   * Function<byte[], byte[]> sanitizeProcessor =
   *     AvroMessageProcessor.removeFields("userSchema", "password", "creditCard");
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("sanitizeDataAvro", sanitizeProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseAvro", "sanitizeDataAvro", "addSourceAvro"
   * );
   *
   * // Apply to a message
   * byte[] inputAvro = ...; // Avro serialized bytes
   * byte[] outputAvro = pipeline.apply(inputAvro);
   * }</pre>
   *
   * @param schemaName The name of the registered schema to use
   * @param fields Field names to remove
   * @return Function that removes fields from Avro
   * @throws IllegalArgumentException if the schema is not registered
   */
  public static Function<byte[], byte[]> removeFields(final String schemaName, final String... fields) {
    final var schema = getSchema(schemaName);
    if (schema == null) throw new IllegalArgumentException("Schema not registered: %s".formatted(schemaName));
    return removeFields(schema, fields);
  }

  /**
   * Creates a processor that removes specified fields from Avro data.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a processor that removes sensitive fields
   * Function<byte[], byte[]> sanitizeProcessor =
   *     AvroMessageProcessor.removeFields(schema, "password", "creditCard", "ssn");
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("sanitizeDataAvro", sanitizeProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseAvro", "sanitizeDataAvro", "addSourceAvro"
   * );
   *
   * // Apply to a message
   * byte[] inputAvro = ...; // Avro serialized bytes
   * byte[] outputAvro = pipeline.apply(inputAvro);
   * }</pre>
   *
   * @param schema The Avro schema to use
   * @param fields Field names to remove
   * @return Function that removes fields from Avro
   */
  public static Function<byte[], byte[]> removeFields(final Schema schema, final String... fields) {
    final var fieldsToRemove = Set.of(fields);
    return avroBytes -> {
      if (avroBytes == null || avroBytes.length == 0) {
        return EMPTY_AVRO;
      }
      try {
        final var reader = new GenericDatumReader<GenericRecord>(schema);
        final var inputStream = new ByteArrayInputStream(avroBytes);
        final var decoder = DecoderFactory.get().binaryDecoder(inputStream, DECODER_CACHE.get());
        DECODER_CACHE.set(decoder);
        final var record = reader.read(null, decoder);
        if (record == null) {
          return EMPTY_AVRO;
        }

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

        final var outputStream = OUTPUT_STREAM_CACHE.get();
        outputStream.reset();
        final var writer = new GenericDatumWriter<GenericRecord>(schema);
        final var encoder = EncoderFactory.get().binaryEncoder(outputStream, ENCODER_CACHE.get());
        ENCODER_CACHE.set(encoder);
        writer.write(newRecord, encoder);
        encoder.flush();
        return outputStream.toByteArray();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error processing Avro", e);
        return EMPTY_AVRO;
      }
    };
  }

  /**
   * Creates a processor that transforms a field using the provided function, using a registered
   * schema.
   *
   * <p>Example:
   *
   * <pre>{@code
   * final var userSchemaJson = """
   *   {
   *     "type": "record",
   *     "name": "User",
   *     "fields": [
   *       {"name": "name", "type": "string"},
   *       {"name": "message", "type": "string"}
   *     ]
   *   }
   *   """;
   *
   * // Create a processor that transforms the message text to uppercase
   * Function<byte[], byte[]> uppercaseProcessor =
   *     AvroMessageProcessor.transformField("userSchema", "message", value -> {
   *         if (value instanceof String) {
   *             return ((String) value).toUpperCase();
   *         }
   *         return value;
   *     });
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("uppercaseMessageAvro", uppercaseProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseAvro", "uppercaseMessageAvro"
   * );
   *
   * // Apply to a message
   * byte[] inputAvro = ...; // Avro serialized bytes
   * byte[] outputAvro = pipeline.apply(inputAvro);
   * }</pre>
   *
   * @param schemaName The name of the registered schema to use
   * @param fieldName Field to transform
   * @param transformer Function to apply to the field value
   * @return Function that transforms the specified field
   * @throws IllegalArgumentException if the schema is not registered
   */
  public static Function<byte[], byte[]> transformField(
    final String schemaName,
    final String fieldName,
    final Function<Object, Object> transformer
  ) {
    final var schema = getSchema(schemaName);
    if (schema == null) throw new IllegalArgumentException("Schema not registered: %s".formatted(schemaName));
    return transformField(schema, fieldName, transformer);
  }

  /**
   * Creates a processor that transforms a field using the provided function.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a processor that transforms the message text to uppercase
   * Function<byte[], byte[]> uppercaseProcessor =
   *     AvroMessageProcessor.transformField(schema, "message", value -> {
   *         if (value instanceof String) {
   *             return ((String) value).toUpperCase();
   *         }
   *         return value;
   *     });
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("uppercaseMessageAvro", uppercaseProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseAvro", "uppercaseMessageAvro"
   * );
   *
   * // Apply to a message
   * byte[] inputAvro = ...; // Avro serialized bytes
   * byte[] outputAvro = pipeline.apply(inputAvro);
   * }</pre>
   *
   * @param schema The Avro schema to use
   * @param fieldName Field to transform
   * @param transformer Function to apply to the field value
   * @return Function that transforms the specified field
   */
  public static Function<byte[], byte[]> transformField(
    final Schema schema,
    final String fieldName,
    final Function<Object, Object> transformer
  ) {
    if (schema == null) return bytes -> bytes;
    return avroBytes ->
      processAvro(
        avroBytes,
        schema,
        record -> {
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
        }
      );
  }

  /**
   * Checks if a value is compatible with an Avro schema type.
   *
   * @param value The value to check
   * @param schema The schema to validate against
   * @return true if compatible, false otherwise
   */
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

  /**
   * Helper method that applies a processing function to parsed Avro data. Uses cached resources for
   * better performance.
   *
   * @param avroBytes The raw Avro data as a byte array
   * @param schema The Avro schema to use for parsing and serializing
   * @param processor Function to transform the parsed Avro record
   * @return Serialized Avro bytes after processing
   */
  private static byte[] processAvro(
    final byte[] avroBytes,
    final Schema schema,
    final Function<GenericRecord, GenericRecord> processor
  ) {
    if (avroBytes == null || avroBytes.length == 0) return EMPTY_AVRO;

    try {
      // Create a reader and writer
      final var reader = new GenericDatumReader<GenericRecord>(schema);
      final var writer = new GenericDatumWriter<GenericRecord>(schema);

      // Deserialize using cached decoder
      final var inputStream = new ByteArrayInputStream(avroBytes);
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

  /**
   * Composes multiple Avro processors into a single function.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create individual processors
   * Function<byte[], byte[]> parseProcessor = AvroMessageProcessor.parseAvro("userSchema");
   * Function<byte[], byte[]> addTimestampProcessor = AvroMessageProcessor.addTimestamp("userSchema", "timestamp");
   * Function<byte[], byte[]> uppercaseProcessor = AvroMessageProcessor.transformField(
   *     "userSchema", "name", value -> value instanceof String ? ((String) value).toUpperCase() : value);
   *
   * // Compose them into a single pipeline
   * Function<byte[], byte[]> pipeline = AvroMessageProcessor.compose(
   *     parseProcessor, addTimestampProcessor, uppercaseProcessor);
   *
   * // Apply to data
   * byte[] result = pipeline.apply(avroBytes);
   * }</pre>
   *
   * @param processors The processors to compose in order
   * @return A single function that applies all processors in sequence
   */
  @SafeVarargs
  public static Function<byte[], byte[]> compose(final Function<byte[], byte[]>... processors) {
    return Arrays.stream(processors).reduce(Function.identity(), Function::andThen);
  }

  /**
   * Represents the result of an Avro processing operation with additional status information.
   *
   * @param data The processed Avro data
   * @param success True if the processing was successful, false otherwise
   * @param error The error message if the processing failed, null otherwise
   * @see AvroMessageProcessor#processWithResult(String, byte[], Function)
   */
  public record AvroResult(byte[] data, boolean success, String error) {}

  /**
   * Processes Avro data with better error handling and detailed result.
   *
   * <p>Example:
   *
   * <pre>{@code
   * AvroMessageProcessor.registerSchema("userSchema", userSchemaJson);
   *
   * // Process with better error handling
   * AvroResult result = AvroMessageProcessor.processWithResult(
   *     "userSchema",
   *     avroBytes,
   *     record -> {
   *         record.put("processedAt", System.currentTimeMillis());
   *         return record;
   *     }
   * );
   *
   * if (result.isSuccess()) {
   *     byte[] processedData = result.getData();
   *     // Use the processed data
   * } else {
   *     String error = result.getError();
   *     // Handle the error
   * }
   * }</pre>
   *
   * @param schemaName The name of the registered schema
   * @param avroBytes The Avro data to process
   * @param processor The function to apply to the record
   * @return An AvroResult containing the result data and status information
   */
  public static AvroResult processWithResult(
    final String schemaName,
    final byte[] avroBytes,
    final Function<GenericRecord, GenericRecord> processor
  ) {
    try {
      final var schema = getSchema(schemaName);
      if (schema == null) return new AvroResult(EMPTY_AVRO, false, "Schema not found: %s".formatted(schemaName));

      final var result = processAvro(avroBytes, schema, processor);
      return new AvroResult(result, true, null);
    } catch (final Exception e) {
      return new AvroResult(EMPTY_AVRO, false, e.getMessage());
    }
  }

  /**
   * Processes multiple Avro records in a batch using the same processor.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Batch process multiple records
   * List<byte[]> records = List.of(record1Bytes, record2Bytes, record3Bytes);
   * Function<byte[], byte[]> addTimestamp = AvroMessageProcessor.addTimestamp("userSchema", "timestamp");
   *
   * List<byte[]> results = AvroMessageProcessor.processBatch(records, addTimestamp);
   * }</pre>
   *
   * @param records List of Avro records to process
   * @param processor The processor functions to apply to each record
   * @return List of processed records
   */
  public static List<byte[]> processBatch(final List<byte[]> records, final Function<byte[], byte[]> processor) {
    return records
      .stream()
      .parallel() // Process in parallel for better performance
      .map(processor)
      .collect(Collectors.toList());
  }

  /**
   * Creates a processor that adds multiple fields to Avro data at once.
   *
   * <p>Example:
   *
   * <pre>{@code
   * Map<String, Object> fields = Map.of(
   *     "source", "payment-service",
   *     "version", "1.0",
   *     "environment", "production"
   * );
   *
   * Function<byte[], byte[]> addMetadata = AvroMessageProcessor.addFields("metadataSchema", fields);
   * byte[] result = addMetadata.apply(avroBytes);
   * }</pre>
   *
   * @param schemaName The name of the registered schema to use
   * @param fields Map of field names and values to add
   * @return Function that adds multiple fields to Avro data
   */
  public static Function<byte[], byte[]> addFields(final String schemaName, final Map<String, Object> fields) {
    final var schema = getSchema(schemaName);
    if (schema == null) throw new IllegalArgumentException("Schema not registered: %s".formatted(schemaName));

    return avroBytes ->
      processAvro(
        avroBytes,
        schema,
        record -> {
          fields.forEach(record::put);
          return record;
        }
      );
  }

  /** Clears the schema registry. */
  public static void clearSchemaRegistry() {
    SCHEMA_REGISTRY.clear();
    SCHEMA_PARSER.set(new Schema.Parser());
  }
}
