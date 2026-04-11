package org.kpipe.registry;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.kpipe.processor.JsonMessageProcessor;

/// JSON implementation of MessageFormat for KPipe.
///
/// This class manages JSON schemas and provides serialization/deserialization for JSON messages as
/// Map<String, Object>.
/// It is used for schema-less or schema-light pipelines and integrates with DslJson for efficient
/// processing.
///
/// Example:
/// ```java
/// final var jsonFormat = new JsonFormat();
/// jsonFormat.addSchema("user", "User", "schemas/user.json");
/// final var map = Map.of("id", "123", "name", "John");
/// byte[] bytes = jsonFormat.serialize(map);
/// Map<String, Object> result = jsonFormat.deserialize(bytes);
/// ```
public final class JsonFormat implements MessageFormat<Map<String, Object>> {

  /// Constructs a new JsonFormat instance.
  public JsonFormat() {
    // Default constructor
  }

  private final Map<String, SchemaInfo> schemas = new ConcurrentHashMap<>();
  private static final DslJson<Map<String, Object>> DSL_JSON = new DslJson<>();

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

  /// Adds a schema to this format and registers it.
  ///
  /// @param key schema identification key
  /// @param fullyQualifiedName fully qualified schema name
  /// @param location location of the schema definition
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

  /// Serializes the given data to a byte array.
  ///
  /// @param data the data to serialize
  /// @return the serialized byte array
  @Override
  public byte[] serialize(final Map<String, Object> data) {
    if (data == null) return null;
    try {
      return JsonMessageProcessor.inScopedCaches(() -> {
        try (final var output = new ByteArrayOutputStream()) {
          DSL_JSON.serialize(data, output);
          return output.toByteArray();
        }
      });
    } catch (final Exception e) {
      throw new RuntimeException("Failed to serialize JSON", e);
    }
  }

  /// Deserializes the given byte array to a Map<String, Object>.
  ///
  /// @param data the serialized byte array
  /// @return the deserialized map
  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Object> deserialize(final byte[] data) {
    if (data == null || data.length == 0) return null;
    try (final var input = new ByteArrayInputStream(data)) {
      return (Map<String, Object>) DSL_JSON.deserialize(Map.class, input);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to deserialize JSON", e);
    }
  }
}
