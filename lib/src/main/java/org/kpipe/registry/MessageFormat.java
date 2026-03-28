package org.kpipe.registry;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
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
import java.util.function.Predicate;
import org.apache.avro.generic.GenericRecord;

/// Represents a message format abstraction for KPipe pipelines.
///
/// This sealed interface defines the contract for supported message formats (JSON, Avro, Protobuf,
/// POJO), including schema management, serialization, and deserialization. Implementations provide
/// format-specific logic for working with Kafka message data and schema registries.
///
/// Example usage:
/// ```java
/// MessageFormat<Map<String, Object>> json = MessageFormat.JSON;
/// MessageFormat<GenericRecord> avro = MessageFormat.AVRO;
/// ```
///
/// @param <T> the type of data handled by this message format
public sealed interface MessageFormat<T> permits JsonFormat, AvroFormat, ProtobufFormat, PojoFormat {
  /// JSON message format.
  MessageFormat<Map<String, Object>> JSON = new JsonFormat();

  /// AVRO message format with support for schema retrieval from HTTP endpoints and file system.
  MessageFormat<GenericRecord> AVRO = new AvroFormat(MessageFormat::readAvroSchema);

  /// Protocol Buffers message format.
  MessageFormat<Object> PROTOBUF = new ProtobufFormat();

  /// Creates a new POJO message format based on JSON.
  ///
  /// @param <T> The POJO type
  /// @param clazz The POJO class
  /// @return A new MessageFormat for the specified POJO type
  static <T> MessageFormat<T> pojo(final Class<T> clazz) {
    return new PojoFormat<>(clazz);
  }

  /// Returns an unmodifiable view of all schemas registered with this format.
  ///
  /// @return Map of schema keys to their schema information
  Map<String, SchemaInfo> getSchemas();

  /// Finds a schema by its key.
  ///
  /// @param key The schema key to search for
  /// @return An Optional containing the SchemaInfo if found, or empty if not found
  Optional<SchemaInfo> findSchema(final String key);

  /// Removes all schemas registered with this message format.
  void clearSchemas();

  /// Adds a schema to this format and registers it with the appropriate processor.
  ///
  /// @param key Schema identification key
  /// @param fullyQualifiedName Fully qualified schema name
  /// @param location Location of the schema definition
  void addSchema(final String key, final String fullyQualifiedName, final String location);

  /// Finds schemas matching the given predicate.
  ///
  /// @param predicate Condition to test schemas against
  /// @return List of matching schema information
  List<SchemaInfo> findSchemas(final Predicate<SchemaInfo> predicate);

  /// Serializes the given data to a byte array.
  ///
  /// @param data The data to serialize
  /// @return The serialized byte array
  byte[] serialize(final T data);

  /// Deserializes the given byte array to data of type T.
  ///
  /// @param data The serialized byte array
  /// @return The deserialized data
  T deserialize(final byte[] data);

  /// Represents schema information with a fully qualified name and location.
  ///
  /// @param fullyQualifiedName Fully qualified name of the schema
  /// @param location Location or content of the schema (file path, URL, or inline content)
  record SchemaInfo(String fullyQualifiedName, String location) {
    /// Canonical constructor for SchemaInfo.
    ///
    /// @param fullyQualifiedName Fully qualified name of the schema
    /// @param location Location or content of the schema (file path, URL, or inline content)
    public SchemaInfo {
      Objects.requireNonNull(fullyQualifiedName, "Fully qualified name cannot be null");
      Objects.requireNonNull(location, "Location cannot be null");
    }
  }

  /// Internal helper to read Avro schema from various locations.
  private static String readAvroSchema(final String location) {
    try {
      // Check if HTTP URL
      if (location.startsWith("http://") || location.startsWith("https://")) {
        final HttpResponse<String> response;
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
          final var request = HttpRequest.newBuilder()
            .uri(URI.create(location))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();

          response = client.send(request, HttpResponse.BodyHandlers.ofString());
        }

        if (response.statusCode() >= 200 && response.statusCode() < 300) {
          final var responseBody = response.body();
          if (responseBody == null || responseBody.isEmpty()) throw new IOException(
            "Empty response from schema registry"
          );

          try {
            final var dslJson = new DslJson<>();
            final var bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            final var result = dslJson.deserialize(Map.class, new ByteArrayInputStream(bytes));

            if (result == null) throw new IOException("Failed to deserialize schema registry response");

            if (result.containsKey("schema")) return (String) result.get("schema");
            else throw new IOException("Schema field not found in response");
          } catch (final Exception e) {
            throw new IOException(
              "Error parsing schema registry response: " +
                responseBody.substring(0, Math.min(100, responseBody.length())),
              e
            );
          }
        } else {
          throw new IOException("Failed to fetch schema: HTTP %d".formatted(response.statusCode()));
        }
      }
      // Check if classpath resource
      else if (location.startsWith("classpath:")) {
        final var resourcePath = location.substring("classpath:".length());
        try (final var inputStream = MessageFormat.class.getResourceAsStream(resourcePath)) {
          if (inputStream == null) throw new IOException("Classpath resource not found: %s".formatted(resourcePath));
          return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
      }
      // Check if file path
      else if (
        location.startsWith("file://") ||
        location.startsWith("/") ||
        location.endsWith(".json") ||
        location.endsWith(".avsc") ||
        Files.exists(Paths.get(location))
      ) {
        final var filePath = location.startsWith("file://") ? location.substring(7) : location;
        return Files.readString(Paths.get(filePath));
      }
      // Treat as inline schema
      return location;
    } catch (final Exception e) {
      throw new RuntimeException("Failed to read schema from location: %s".formatted(location), e);
    }
  }
}
