package org.kpipe.registry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.kpipe.processor.AvroMessageProcessor;

/// Avro implementation of MessageFormat for KPipe.
///
/// This class manages Avro schemas and provides serialization for Avro GenericRecord messages.
/// It supports registering schemas from various sources and integrates with the
/// AvroMessageProcessor for schema registration and optimized Avro handling.
///
/// Typical usage involves registering schemas and then serializing Avro records for Kafka
/// pipelines.
///
/// Example:
/// ```java
/// final var avroFormat = new AvroFormat(location -> Files.readString(Paths.get(location)));
/// avroFormat.addSchema("user", "com.example.User", "schemas/user.avsc");
/// byte[] bytes = avroFormat.serialize(record);
/// ```
public final class AvroFormat implements MessageFormat<GenericRecord> {

  private final Map<String, SchemaInfo> schemas = new ConcurrentHashMap<>();
  private final Function<String, String> schemaReader;
  private String defaultSchemaKey;

  /// Constructs a new AvroFormat with the specified schema reader function.
  ///
  /// @param schemaReader Function to read schema content from a location
  public AvroFormat(final Function<String, String> schemaReader) {
    this.schemaReader = schemaReader;
  }

  /// Sets the default schema key to use for deserialization.
  ///
  /// @param schemaKey The schema key
  /// @return This AvroFormat instance
  public AvroFormat withDefaultSchema(String schemaKey) {
    this.defaultSchemaKey = schemaKey;
    return this;
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

  /// Removes all schemas registered with this message format.
  @Override
  public void clearSchemas() {
    schemas.clear();
  }

  /// Adds a schema to this format and registers it with the AvroMessageProcessor.
  ///
  /// @param key schema identification key
  /// @param fullyQualifiedName fully qualified schema name
  /// @param location location of the schema definition
  @Override
  public void addSchema(final String key, final String fullyQualifiedName, final String location) {
    schemas.put(key, new SchemaInfo(fullyQualifiedName, location));
    try {
      final var schemaJson = schemaReader.apply(location);
      AvroMessageProcessor.registerSchema(key, schemaJson);
    } catch (final Exception e) {
      throw new IllegalArgumentException(
        "Failed to register Avro schema for key '%s': %s".formatted(key, e.getMessage()),
        e
      );
    }
  }

  /// Finds schemas matching the given predicate.
  ///
  /// @param predicate condition to test schemas against
  /// @return list of matching schema information
  @Override
  public List<SchemaInfo> findSchemas(final Predicate<SchemaInfo> predicate) {
    return schemas.values().stream().filter(predicate).toList();
  }

  /// Serializes the given Avro GenericRecord to a byte array.
  ///
  /// @param data the Avro record to serialize
  /// @return the serialized byte array
  @Override
  public byte[] serialize(final GenericRecord data) {
    try (final var output = new ByteArrayOutputStream()) {
      final var writer = new GenericDatumWriter<GenericRecord>(data.getSchema());
      final var encoder = EncoderFactory.get().binaryEncoder(output, null);
      writer.write(data, encoder);
      encoder.flush();
      return output.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to serialize Avro record", e);
    }
  }

  /// Deserializes the given byte array to an Avro GenericRecord.
  ///
  /// @param data the serialized byte array
  /// @return the deserialized record
  @Override
  public GenericRecord deserialize(final byte[] data) {
    if (data == null || data.length == 0) return null;
    if (defaultSchemaKey == null) throw new UnsupportedOperationException("Avro deserialization requires a default schema key. Use withDefaultSchema().");
    final var schema = AvroMessageProcessor.getSchema(defaultSchemaKey);
    if (schema == null) throw new IllegalArgumentException("No schema found for key: %s".formatted(defaultSchemaKey));
    return AvroMessageProcessor.inScopedCaches(() -> {
      final var datumReader = new org.apache.avro.generic.GenericDatumReader<GenericRecord>(schema);
      final var decoder = org.apache.avro.io.DecoderFactory.get().binaryDecoder(data, null);
      try {
        return datumReader.read(null, decoder);
      } catch (final IOException e) {
        throw new RuntimeException("Failed to deserialize Avro record", e);
      }
    });
  }
}
