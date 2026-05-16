package org.kpipe.format.avro;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.avro.Schema;

/// A keyed catalog of parsed Avro schemas. Each catalog instance owns its own schemas — build
/// one explicitly, look up the schema you need, then pass it to a fresh `AvroFormat(schema)` for
/// the codec side:
///
/// ```java
/// final var catalog = new AvroSchemaCatalog();
/// catalog.add("user", userSchemaJson);
/// catalog.add("order", "com.example.Order", "schemas/order.avsc");
///
/// final var userFormat = new AvroFormat(catalog.get("user"));
/// final var orderFormat = new AvroFormat(catalog.get("order"));
/// ```
///
/// A catalog is useful when you have N schemas keyed by name (e.g. fetched from Confluent Schema
/// Registry via `ConfluentSchemaResolver`). If you already have a single `Schema` in hand, skip
/// the catalog and construct `new AvroFormat(schema)` directly.
public final class AvroSchemaCatalog {

  private final Map<String, Schema> parsed = new ConcurrentHashMap<>();
  private final Function<String, String> schemaReader;

  /// Creates an empty catalog backed by the default schema reader (classpath / file / inline).
  public AvroSchemaCatalog() {
    this(AvroSchemaCatalog::defaultReader);
  }

  /// Creates an empty catalog backed by `schemaReader`. Use this to wire a custom location-to-JSON
  /// resolver (e.g. fetching from a Schema Registry).
  ///
  /// @param schemaReader function that takes a location string and returns the schema JSON
  public AvroSchemaCatalog(final Function<String, String> schemaReader) {
    this.schemaReader = Objects.requireNonNull(schemaReader, "schemaReader cannot be null");
  }

  /// Adds a schema by inline JSON. Parses once and caches the parsed [Schema] under `key`.
  ///
  /// @param key        the lookup key
  /// @param schemaJson the Avro schema as JSON
  /// @return this catalog for fluent chaining
  public AvroSchemaCatalog add(final String key, final String schemaJson) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(schemaJson, "schemaJson cannot be null");
    parsed.put(key, new Schema.Parser().parse(schemaJson));
    return this;
  }

  /// Adds a schema by reading from a location (classpath / file path / inline). The
  /// `fullyQualifiedName` parameter is accepted for compatibility but not used — the parsed
  /// schema's `getFullName()` is authoritative.
  ///
  /// @param key                the lookup key
  /// @param fullyQualifiedName the Avro full name (informational only)
  /// @param location           the location string passed to the schema reader
  /// @return this catalog for fluent chaining
  public AvroSchemaCatalog add(final String key, final String fullyQualifiedName, final String location) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(location, "location cannot be null");
    final var schemaJson = schemaReader.apply(location);
    return add(key, schemaJson);
  }

  /// Returns the parsed [Schema] registered under `key`, or null if missing. Use [#find(String)]
  /// for an `Optional`-returning variant.
  ///
  /// @param key the lookup key
  /// @return the parsed schema, or null if not registered
  public Schema get(final String key) {
    return parsed.get(key);
  }

  /// Returns the parsed [Schema] registered under `key`, wrapped in `Optional`.
  ///
  /// @param key the lookup key
  /// @return `Optional.of(schema)` if registered, `Optional.empty()` otherwise
  public Optional<Schema> find(final String key) {
    return Optional.ofNullable(parsed.get(key));
  }

  /// Returns an unmodifiable view of every schema in the catalog.
  ///
  /// @return key → parsed-schema view
  public Map<String, Schema> all() {
    return Collections.unmodifiableMap(parsed);
  }

  /// Removes every schema from the catalog.
  public void clear() {
    parsed.clear();
  }

  /// Default schema reader: classpath, file paths, or inline JSON. HTTP fetching is intentionally
  /// not supported here — use `ConfluentSchemaResolver` from `kpipe-schema-registry-confluent` for
  /// that, then pass the returned JSON to [#add(String, String)].
  private static String defaultReader(final String location) {
    try {
      if (location.startsWith("http://") || location.startsWith("https://")) {
        throw new IllegalArgumentException(
          "HTTP schema URLs are not supported by AvroSchemaCatalog. Use ConfluentSchemaResolver " +
            "from kpipe-schema-registry-confluent and pass the resolved JSON to add(key, schemaJson)."
        );
      }
      if (location.startsWith("classpath:")) {
        final var resourcePath = location.substring("classpath:".length());
        try (final var in = AvroSchemaCatalog.class.getResourceAsStream(resourcePath)) {
          if (in == null) {
            throw new IllegalArgumentException("Classpath resource not found: %s".formatted(resourcePath));
          }
          return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
      }
      if (
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
    } catch (final RuntimeException re) {
      throw re;
    } catch (final Exception e) {
      throw new RuntimeException("Failed to read schema from location: %s".formatted(location), e);
    }
  }
}
