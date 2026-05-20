package org.kpipe.format.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.SchemaResolver;

/// Avro codec for KPipe. Operates in one of two modes:
///
/// **Static mode** — `new AvroFormat(schema)` or `AvroFormat.of(schemaJson)`. The codec is
/// bound to a single [Schema] for both serialize and deserialize. Suitable for inline /
/// classpath schemas or shops with strict append-only schema evolution (a v1-bound consumer
/// can decode v2 producer bytes as long as evolution only appends optional fields).
///
/// **Schema-Registry mode** — `AvroFormat.withRegistry(resolver)`. The codec reads the
/// Confluent wire envelope (1-byte magic + 4-byte big-endian schema ID) off each record,
/// looks up the writer's schema via the provided [SchemaResolver], and decodes against it.
/// Schemas resolved through the registry are cached in-process keyed by ID; cardinality is
/// naturally bounded by how often producers register new schemas. Serialize is not supported
/// in this mode (KPipe is a consumer-first library; if you need to write back to a topic
/// through SR, construct a static-mode format with the writer schema you want).
///
/// ```java
/// // Static — schema in hand.
/// final var fmt = new AvroFormat(userSchema);
///
/// // Static — inline JSON.
/// final var fmt = AvroFormat.of(userSchemaJson);
///
/// // Static — catalog-mediated.
/// final var catalog = new AvroSchemaCatalog();
/// catalog.add("user", userSchemaJson);
/// final var fmt = new AvroFormat(catalog.get("user"));
///
/// // Schema-Registry — per-record auto-lookup.
/// try (var resolver = new CachedSchemaResolver(
///     new ConfluentSchemaResolver("http://schema-registry:8081"))) {
///   final var fmt = AvroFormat.withRegistry(resolver);
///   // fmt.deserialize(record.value()) reads the envelope, fetches the schema, decodes.
/// }
/// ```
///
/// Schema-Registry mode replaces the manual `KPipe.avro(...).skipBytes(5).pipe(...)` pattern.
/// Do **not** combine `withRegistry(...)` with `skipBytes(5)` — the format already consumes
/// the envelope.
public final class AvroFormat implements MessageFormat<GenericRecord> {

  /// Confluent wire envelope: 1-byte magic (0x00) + 4-byte big-endian schema ID.
  private static final int ENVELOPE_LENGTH = 5;
  private static final byte CONFLUENT_MAGIC = 0x00;

  private final Schema staticSchema;
  private final SchemaResolver resolver;
  private final ConcurrentHashMap<Integer, Schema> resolvedSchemas;

  /// Constructs a static-mode codec bound to `schema`.
  ///
  /// @param schema the Avro schema used for both serialize and deserialize (must be non-null)
  public AvroFormat(final Schema schema) {
    this.staticSchema = Objects.requireNonNull(schema, "schema cannot be null");
    this.resolver = null;
    this.resolvedSchemas = null;
  }

  private AvroFormat(final SchemaResolver resolver) {
    this.staticSchema = null;
    this.resolver = Objects.requireNonNull(resolver, "resolver cannot be null");
    this.resolvedSchemas = new ConcurrentHashMap<>();
  }

  /// Convenience factory that parses the schema from inline JSON. Equivalent to
  /// `new AvroFormat(new Schema.Parser().parse(schemaJson))`.
  ///
  /// @param schemaJson the Avro schema as JSON
  /// @return a static-mode codec bound to the parsed schema
  public static AvroFormat of(final String schemaJson) {
    Objects.requireNonNull(schemaJson, "schemaJson cannot be null");
    return new AvroFormat(new Schema.Parser().parse(schemaJson));
  }

  /// Constructs a Schema-Registry-backed codec. Each call to [#deserialize] reads the wire
  /// envelope off the record, extracts the schema ID, looks up the writer's schema via
  /// `resolver`, and decodes the remaining bytes against it. Resolved schemas are cached
  /// internally by ID.
  ///
  /// @param resolver schema resolver (typically wrap with `CachedSchemaResolver` for the
  ///                 HTTP-call cache; this format also caches the parsed `Schema` per ID)
  /// @return a registry-backed codec
  public static AvroFormat withRegistry(final SchemaResolver resolver) {
    return new AvroFormat(resolver);
  }

  /// Returns true if this format reads the Confluent wire envelope per record.
  ///
  /// @return true in Schema-Registry mode, false in static mode
  public boolean isRegistryBacked() {
    return resolver != null;
  }

  /// Returns the schema this codec is bound to in static mode.
  ///
  /// @return the bound schema in static mode; null in Schema-Registry mode
  public Schema schema() {
    return staticSchema;
  }

  /// Creates an [AvroConsoleSink] using this format's schema. Only available in static mode.
  ///
  /// @return a console sink bound to this format's schema
  /// @throws IllegalStateException if this format is in Schema-Registry mode
  public AvroConsoleSink<GenericRecord> consoleSink() {
    if (staticSchema == null) throw new IllegalStateException(
      "consoleSink() requires a static schema; AvroFormat.withRegistry(...) has no fixed schema. " +
        "Either construct AvroFormat with a Schema, or instantiate AvroConsoleSink directly with the schema you want."
    );
    return new AvroConsoleSink<>(staticSchema);
  }

  /// Serializes a `GenericRecord` to bytes. Only available in static mode.
  ///
  /// @param data the record to serialize
  /// @return the binary-encoded bytes
  /// @throws UnsupportedOperationException if this format is in Schema-Registry mode
  @Override
  public byte[] serialize(final GenericRecord data) {
    if (resolver != null) throw new UnsupportedOperationException(
      "AvroFormat.withRegistry(...) is consumer-only — serialize would require a writer schema. " +
        "Use new AvroFormat(writerSchema) for the producer side."
    );
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

  /// Deserializes bytes to a `GenericRecord`. In static mode, the bound schema is used for
  /// both writer and reader. In Schema-Registry mode, the writer schema is resolved per record
  /// from the wire envelope and used to decode the remaining bytes.
  ///
  /// @param data the binary-encoded bytes (with Confluent envelope in Schema-Registry mode)
  /// @return the decoded record, or null if `data` is null/empty
  @Override
  public GenericRecord deserialize(final byte[] data) {
    if (data == null || data.length == 0) return null;
    return resolver != null ? deserializeFromEnvelope(data) : deserializeStatic(data);
  }

  private GenericRecord deserializeStatic(final byte[] data) {
    final var reader = new GenericDatumReader<GenericRecord>(staticSchema);
    final var decoder = DecoderFactory.get().binaryDecoder(data, null);
    try {
      return reader.read(null, decoder);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to deserialize Avro record", e);
    }
  }

  private GenericRecord deserializeFromEnvelope(final byte[] data) {
    if (data.length < ENVELOPE_LENGTH) throw new RuntimeException(
      "Record too short for Confluent wire envelope: " + data.length + " bytes; expected at least " + ENVELOPE_LENGTH
    );
    if (data[0] != CONFLUENT_MAGIC) throw new RuntimeException(
      "Unexpected magic byte 0x%02x; expected 0x00 (Confluent Schema Registry envelope)".formatted(data[0] & 0xff)
    );
    final int schemaId =
      ((data[1] & 0xff) << 24) | ((data[2] & 0xff) << 16) | ((data[3] & 0xff) << 8) | (data[4] & 0xff);

    final var schema = resolvedSchemas.computeIfAbsent(schemaId, id -> {
      final var json = resolver.lookupById(id);
      if (json == null || json.isBlank()) throw new RuntimeException(
        "Schema resolver returned empty schema for id " + id
      );
      return new Schema.Parser().parse(json);
    });

    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var decoder = DecoderFactory.get().binaryDecoder(data, ENVELOPE_LENGTH, data.length - ENVELOPE_LENGTH, null);
    try {
      return reader.read(null, decoder);
    } catch (final IOException e) {
      throw new RuntimeException(
        "Failed to decode Avro record under Confluent wire envelope (schema id " + schemaId + ")",
        e
      );
    }
  }
}
