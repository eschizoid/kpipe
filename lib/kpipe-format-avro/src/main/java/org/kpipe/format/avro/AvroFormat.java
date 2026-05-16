package org.kpipe.format.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.kpipe.registry.MessageFormat;

/// Avro codec for KPipe — stateless `MessageFormat<GenericRecord>` bound to a single [Schema].
/// One schema per instance, no mutable state. For multiple schemas keyed by name, use
/// [AvroSchemaCatalog] and pass the resolved schema here.
///
/// ```java
/// // Direct: you have a Schema in hand.
/// final var format = new AvroFormat(userSchema);
///
/// // Catalog-mediated: multiple schemas keyed by name.
/// final var catalog = new AvroSchemaCatalog();
/// catalog.add("user", userSchemaJson);
/// final var format = new AvroFormat(catalog.get("user"));
///
/// // From inline JSON (the most common test/example shape):
/// final var format = AvroFormat.of(userSchemaJson);
/// ```
public final class AvroFormat implements MessageFormat<GenericRecord> {

  private final Schema schema;

  /// Constructs a codec bound to `schema`.
  ///
  /// @param schema the Avro schema used for both serialize and deserialize (must be non-null)
  public AvroFormat(final Schema schema) {
    this.schema = Objects.requireNonNull(schema, "schema cannot be null");
  }

  /// Convenience constructor that parses the schema from inline JSON. Equivalent to
  /// `new AvroFormat(new Schema.Parser().parse(schemaJson))`.
  ///
  /// @param schemaJson the Avro schema as JSON
  /// @return a codec bound to the parsed schema
  public static AvroFormat of(final String schemaJson) {
    Objects.requireNonNull(schemaJson, "schemaJson cannot be null");
    return new AvroFormat(new Schema.Parser().parse(schemaJson));
  }

  /// Returns the schema this codec is bound to.
  ///
  /// @return the bound schema (never null)
  public Schema schema() {
    return schema;
  }

  /// Creates a new [AvroConsoleSink] using this format's schema. Useful as the facade
  /// `toConsole()` factory.
  ///
  /// @return a console sink bound to this format's schema
  public AvroConsoleSink<GenericRecord> consoleSink() {
    return new AvroConsoleSink<>(schema);
  }

  /// Serializes a `GenericRecord` to bytes using the bound schema.
  ///
  /// @param data the record to serialize
  /// @return the binary-encoded bytes
  @Override
  public byte[] serialize(final GenericRecord data) {
    if (data == null) return null;
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

  /// Deserializes bytes to a `GenericRecord` using the bound schema.
  ///
  /// @param data the binary-encoded bytes
  /// @return the decoded record, or null if `data` is null/empty
  @Override
  public GenericRecord deserialize(final byte[] data) {
    if (data == null || data.length == 0) return null;
    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var decoder = DecoderFactory.get().binaryDecoder(data, null);
    try {
      return reader.read(null, decoder);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to deserialize Avro record", e);
    }
  }
}
