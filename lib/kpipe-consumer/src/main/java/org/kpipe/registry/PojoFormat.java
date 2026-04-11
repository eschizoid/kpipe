package org.kpipe.registry;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/// POJO implementation of MessageFormat for KPipe.
///
/// This class manages POJO schemas and provides serialization/deserialization for POJO messages
/// using JSON. It is used for pipelines that work with strongly-typed Java objects and integrates
/// with DslJson for efficient processing.
///
/// Example:
/// ```java
/// final var pojoFormat = new PojoFormat<>(User.class);
/// pojoFormat.addSchema("user", "User", "schemas/user.json");
/// final var user = new User("123", "John");
/// byte[] bytes = pojoFormat.serialize(user);
/// User result = pojoFormat.deserialize(bytes);
/// ```
///
/// @param <T> the POJO type handled by this format
public final class PojoFormat<T> implements MessageFormat<T> {

  private final Class<T> clazz;
  private final Map<String, SchemaInfo> schemas = new ConcurrentHashMap<>();
  private static final DslJson<Object> DSL_JSON = new DslJson<>(new DslJson.Settings<>().includeServiceLoader());

  /// Constructs a new PojoFormat for the specified POJO class.
  ///
  /// @param clazz the POJO class to use for serialization/deserialization
  public PojoFormat(final Class<T> clazz) {
    this.clazz = Objects.requireNonNull(clazz, "Class cannot be null");
  }

  /// Returns an unmodifiable view of all schemas registered with this format.
  ///
  /// @return map of schema keys to their schema information
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

  /// Serializes the given POJO to a byte array using JSON.
  ///
  /// @param data the POJO to serialize
  /// @return the serialized byte array
  @Override
  public byte[] serialize(final T data) {
    try (final var output = new ByteArrayOutputStream()) {
      DSL_JSON.serialize(data, output);
      return output.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to serialize POJO", e);
    }
  }

  /// Deserializes the given byte array to a POJO of type T.
  ///
  /// @param data the serialized byte array
  /// @return the deserialized POJO
  @Override
  public T deserialize(final byte[] data) {
    try (final var input = new ByteArrayInputStream(data)) {
      return DSL_JSON.deserialize(clazz, input);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to deserialize POJO", e);
    }
  }
}
