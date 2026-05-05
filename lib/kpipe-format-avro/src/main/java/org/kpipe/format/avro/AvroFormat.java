package org.kpipe.format.avro;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.RegistryKey;
import org.kpipe.registry.SchemaAwareFormat;

/// Avro implementation of MessageFormat for KPipe.
///
/// Manages Avro schemas (parsed and metadata) and provides serialization / deserialization for
/// `GenericRecord` messages. Schemas registered via [#addSchema] are parsed once and cached on
/// this instance.
///
/// Example:
/// ```java
/// final var avroFormat = new AvroFormat(location -> Files.readString(Paths.get(location)));
/// avroFormat.addSchema("user", "com.example.User", "schemas/user.avsc");
/// byte[] bytes = avroFormat.serialize(record);
/// ```
public final class AvroFormat implements SchemaAwareFormat<GenericRecord> {

  /// Shared singleton instance using the default HTTP/file/classpath schema reader.
  /// Use this rather than constructing a new AvroFormat unless you need a custom schema source
  /// or an isolated schema-registration scope.
  ///
  /// **Footgun warning:** this `INSTANCE` is a JVM-global mutable singleton — schemas registered
  /// here are visible to every caller using `AvroFormat.INSTANCE`. For libraries, tests, or any
  /// code that needs an isolated schema scope, construct a dedicated instance with
  /// `new AvroFormat()` (or `new AvroFormat(reader)`) and use distinct schema keys.
  public static final AvroFormat INSTANCE = new AvroFormat();

  /// Registry key for the pre-built Avro logging sink registered by [#newRegistry()].
  public static final RegistryKey<GenericRecord> AVRO_LOGGING = AvroRegistryKey.of("avroLogging");

  private final Map<String, SchemaInfo> schemas = new ConcurrentHashMap<>();
  private final Map<String, Schema> parsedSchemas = new ConcurrentHashMap<>();
  private final Function<String, String> schemaReader;
  private String defaultSchemaKey;

  /// Constructs a new AvroFormat using the default schema reader (HTTP / file path / classpath /
  /// inline content).
  public AvroFormat() {
    this(AvroFormat::readSchemaFromLocation);
  }

  /// Constructs a new AvroFormat with the specified schema reader function.
  ///
  /// @param schemaReader Function to read schema content from a location
  public AvroFormat(final Function<String, String> schemaReader) {
    this.schemaReader = schemaReader;
  }

  /// Creates a new [MessageProcessorRegistry] pre-wired with [AvroFormat#INSTANCE] and an
  /// [AvroConsoleSink] registered under [#AVRO_LOGGING].
  ///
  /// The default `AvroConsoleSink` uses the schema registered under key `"1"` on
  /// [AvroFormat#INSTANCE]; callers who use a different schema key should build the registry
  /// directly and register a sink with the appropriate schema instead.
  ///
  /// @return a new pre-configured registry
  public static MessageProcessorRegistry newRegistry() {
    final var registry = new MessageProcessorRegistry("kpipe-format-avro", INSTANCE);
    registry.sinkRegistry().register(AVRO_LOGGING, new AvroConsoleSink<>());
    return registry;
  }

  /// Creates a new [AvroConsoleSink] for the given Avro schema.
  ///
  /// @param schema the Avro schema used to decode incoming `byte[]` payloads
  /// @return a new console sink
  public static AvroConsoleSink<GenericRecord> consoleSink(final Schema schema) {
    return new AvroConsoleSink<>(schema);
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
    parsedSchemas.clear();
  }

  /// Adds a schema to this format. Reads the schema text from `location` (HTTP URL, classpath
  /// resource, file path, or inline content) via the configured schema reader, parses it, and
  /// caches both the parsed [Schema] and the [SchemaInfo] metadata on this instance.
  ///
  /// @param key schema identification key
  /// @param fullyQualifiedName fully qualified schema name
  /// @param location location of the schema definition
  @Override
  public void addSchema(final String key, final String fullyQualifiedName, final String location) {
    schemas.put(key, new SchemaInfo(fullyQualifiedName, location));
    try {
      final var schemaJson = schemaReader.apply(location);
      parsedSchemas.put(key, new Schema.Parser().parse(schemaJson));
    } catch (final Exception e) {
      throw new IllegalArgumentException(
        "Failed to register Avro schema for key '%s': %s".formatted(key, e.getMessage()),
        e
      );
    }
  }

  /// Adds a schema from inline JSON content. Parses the schema once, caches the parsed [Schema]
  /// on this instance, and stores `SchemaInfo` derived from the parsed schema's full name.
  ///
  /// Convenience for tests and callers who already have schema JSON in hand and don't need the
  /// `schemaReader` indirection.
  ///
  /// @param key schema identification key
  /// @param schemaJson the Avro schema definition as JSON
  /// @return this AvroFormat instance
  public AvroFormat addSchema(final String key, final String schemaJson) {
    final var schema = new Schema.Parser().parse(schemaJson);
    parsedSchemas.put(key, schema);
    schemas.put(key, new SchemaInfo(schema.getFullName(), schemaJson));
    return this;
  }

  /// Returns the parsed [Schema] registered under `key`, or `null` if no schema is registered.
  ///
  /// @param key the schema key
  /// @return the parsed schema, or `null` if not registered
  public Schema getSchema(final String key) {
    return parsedSchemas.get(key);
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
    if (defaultSchemaKey == null) throw new UnsupportedOperationException(
      "Avro deserialization requires a default schema key. Use withDefaultSchema()."
    );
    final var schema = parsedSchemas.get(defaultSchemaKey);
    if (schema == null) throw new IllegalArgumentException("No schema found for key: %s".formatted(defaultSchemaKey));
    final var datumReader = new org.apache.avro.generic.GenericDatumReader<GenericRecord>(schema);
    final var decoder = org.apache.avro.io.DecoderFactory.get().binaryDecoder(data, null);
    try {
      return datumReader.read(null, decoder);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to deserialize Avro record", e);
    }
  }

  /// Default schema reader supporting HTTP URLs, classpath resources, file paths, and inline
  // content.
  ///
  /// - `http://` / `https://` — fetched as a Confluent Schema Registry response and unwrapped from
  ///   the JSON `{"schema":"..."}` envelope.
  /// - `classpath:/path/to/schema.avsc` — read from the module classpath.
  /// - `file://`, `/absolute/path`, `*.json`, `*.avsc`, or any existing file path — read as UTF-8
  // text.
  /// - Anything else — treated as inline schema content.
  private static String readSchemaFromLocation(final String location) {
    try {
      if (location.startsWith("http://") || location.startsWith("https://")) {
        try (final var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
          final var request = HttpRequest.newBuilder()
            .uri(URI.create(location))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();

          final var response = client.send(request, HttpResponse.BodyHandlers.ofString());

          if (response.statusCode() < 200 || response.statusCode() >= 300) throw new IOException(
            "Failed to fetch schema: HTTP %d".formatted(response.statusCode())
          );

          final var responseBody = response.body();
          if (responseBody == null || responseBody.isEmpty()) throw new IOException(
            "Empty response from schema registry"
          );

          try {
            final var dslJson = new DslJson<>();
            final var bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            final var result = dslJson.deserialize(Map.class, new ByteArrayInputStream(bytes));

            if (result == null) throw new IOException("Failed to deserialize schema registry response");
            if (!result.containsKey("schema")) throw new IOException("Schema field not found in response");
            return (String) result.get("schema");
          } catch (final Exception e) {
            throw new IOException(
              "Error parsing schema registry response: " +
                responseBody.substring(0, Math.min(100, responseBody.length())),
              e
            );
          }
        }
      } else if (location.startsWith("classpath:")) {
        final var resourcePath = location.substring("classpath:".length());
        try (final var inputStream = AvroFormat.class.getResourceAsStream(resourcePath)) {
          if (inputStream == null) throw new IOException("Classpath resource not found: %s".formatted(resourcePath));
          return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
      } else if (
        location.startsWith("file://") ||
        location.startsWith("/") ||
        location.endsWith(".json") ||
        location.endsWith(".avsc") ||
        Files.exists(Paths.get(location))
      ) {
        final var filePath = location.startsWith("file://") ? location.substring(7) : location;
        return Files.readString(Paths.get(filePath));
      }
      return location;
    } catch (final Exception e) {
      throw new RuntimeException("Failed to read schema from location: %s".formatted(location), e);
    }
  }
}
