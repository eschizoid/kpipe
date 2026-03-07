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
import java.util.function.Function;
import java.util.function.Predicate;
import org.kpipe.processor.AvroMessageProcessor;

/// Represents supported message formats in the message processing system. Each format defines
/// specific serialization, schema handling, and processing behaviors for messages.
///
/// Three standard formats are supported:
///
/// * {@link #JSON} - For processing JSON messages with schema-less operations
/// * {@link #AVRO} - For Apache Avro format with schema registry integration
/// * {@link #PROTOBUF} - For Protocol Buffer messages
///
/// Each format maintains its own registry of schemas and provides format-specific message
/// processors. The AVRO format has advanced schema handling capabilities including loading schemas
/// from HTTP endpoints, file system locations, or inline content.
///
/// # Basic Usage Examples
///
/// ## 1. Using with MessageProcessorRegistry
///
/// ```java
/// // Create a processor registry with JSON format (default)
/// final var jsonRegistry = new MessageProcessorRegistry("my-app");
///
/// // Or explicitly specify AVRO format
/// final var avroRegistry = new MessageProcessorRegistry("my-app", MessageFormat.AVRO);
///
/// // Get available processors for the format
/// final var processors = jsonRegistry.getAll();
/// ```
///
/// ## 2. Registering Schemas
///
/// ```java
/// // Register schema using inline schema definition
/// final var userSchema = """
///   {
///     "type": "record",
///     "name": "User",
///     "namespace": "com.example",
///     "fields": [
///       {"name": "id", "type": "string"},
///       {"name": "name", "type": "string"},
///       {"name": "email", "type": "string"}
///     ]
///   }
///   """;
/// MessageFormat.AVRO.addSchema("user", "com.example.User", userSchema);
///
/// // Register schema from file path
/// MessageFormat.AVRO.addSchema(
///     "product",
///     "com.example.Product",
///     "/schemas/product.avsc"
/// );
///
/// // Register schema from HTTP location
/// MessageFormat.AVRO.addSchema(
///     "event",
///     "com.example.Event",
///     "https://schema-registry.example.com/schemas/event.avsc"
/// );
/// ```
///
/// ## 3. Finding and Using Schemas
///
/// ```java
/// // Get all registered schemas for a format
/// final var schemas = MessageFormat.AVRO.getSchemas();
///
/// // Find a specific schema by key
/// final var userSchema = MessageFormat.AVRO.findSchema("user")
///     .orElseThrow(() -> new IllegalStateException("User schema not found"));
///
/// // Find schemas matching a condition
/// final var userSchemas = MessageFormat.AVRO.findSchemas(
///     schema -> schema.fullyQualifiedName().startsWith("com.example.user")
/// );
/// ```
///
/// ## 4. Working with Format-Specific Processors
///
/// ```java
/// // Create a processor pipeline in the registry
/// final var registry = new MessageProcessorRegistry("my-app");
/// registry.addSchema("user", "com.example.User", "/schemas/user.avsc");
///
/// final var pipeline = registry.avroPipeline(
///     "user",               // Use 'user' schema
///     "addTimestamp_user",  // Add timestamp operator
///     "addSource_user"      // Add source operator
/// );
///
/// // Process a message
/// final var result = pipeline.apply(inputBytes);
/// ```
///
/// ## 5. Schema Location Handling
///
/// The AVRO format supports multiple schema source locations:
///
/// * HTTP/HTTPS URLs: `https://example.com/schemas/user.avsc`
/// * File paths: `/path/to/schema.avsc` or `file:///path/to/schema.avsc`
/// * Classpath resources: `classpath:/schemas/user.avsc`
/// * Inline schema content: Pass the JSON schema definition directly
///
/// @see SchemaInfo For structure of schema information
/// @see MessageProcessorRegistry For using formats in message processing
/// @see org.kpipe.processor.AvroMessageProcessor For AVRO message processing
public enum MessageFormat {
  /// JSON message format.
  JSON(new HashMap<>(), location -> location),

  /// AVRO message format with support for schema retrieval from HTTP endpoints and file system.
  ///
  /// Handles schemas from inline content, file paths (local or file:// protocol), and HTTP/HTTPS
  /// URLs.
  AVRO(
    new HashMap<>(),
    location -> {
      try {
        // Check if HTTP URL
        if (location.startsWith("http://") || location.startsWith("https://")) {
          final HttpResponse<String> response;
          try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final var request = HttpRequest
              .newBuilder()
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

              if (result.containsKey("schema")) return (String) result.get("schema"); else throw new IOException(
                "Schema field not found in response"
              );
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
        // Check if file path
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
  ),

  /// Protocol Buffers message format.
  PROTOBUF(new HashMap<>(), location -> location);

  /// Map of schema keys to schema information, maintained by each format.
  protected final Map<String, SchemaInfo> schemas;

  /// Function to read schema content from various locations
  protected final Function<String, String> schemaReader;

  /// Constructs a message format with a schema information map and schema reader function.
  ///
  /// @param schemas Initial schema map
  /// @param schemaReader Function to read schema content from a location
  MessageFormat(final Map<String, SchemaInfo> schemas, final Function<String, String> schemaReader) {
    this.schemas = schemas;
    this.schemaReader = schemaReader;
  }

  /// Returns an unmodifiable view of all schemas registered with this format.
  ///
  /// @return Map of schema keys to their schema information
  public Map<String, SchemaInfo> getSchemas() {
    return Collections.unmodifiableMap(schemas);
  }

  /// Finds a schema by its key.
  ///
  /// @param key The schema key to search for
  /// @return An Optional containing the SchemaInfo if found, or empty if not found
  public Optional<SchemaInfo> findSchema(final String key) {
    return Optional.ofNullable(schemas.get(key));
  }

  /// Removes all schemas registered with this message format.
  ///
  /// Useful for resetting the schema registry, especially in tests or reinitialization
  /// scenarios.
  public void clearSchemas() {
    schemas.clear();
  }

  /// Adds a schema to this format and registers it with the appropriate processor.
  ///
  /// @param key Schema identification key
  /// @param fullyQualifiedName Fully qualified schema name
  /// @param location Location of the schema definition
  public void addSchema(final String key, final String fullyQualifiedName, final String location) {
    schemas.put(key, new SchemaInfo(fullyQualifiedName, location));

    // For AVRO format, also register the schema with AvroMessageProcessor
    if (this == AVRO) {
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
  }

  /// Finds schemas matching the given predicate.
  ///
  /// @param predicate Condition to test schemas against
  /// @return List of matching schema information
  public List<SchemaInfo> findSchemas(final Predicate<SchemaInfo> predicate) {
    return schemas.values().stream().filter(predicate).toList();
  }

  /// Represents schema information with a fully qualified name and location.
  ///
  /// @param fullyQualifiedName Fully qualified name of the schema
  /// @param location Location or content of the schema (file path, URL, or inline content)
  public record SchemaInfo(String fullyQualifiedName, String location) {
    /// Creates new schema information instance.
    ///
    /// @param fullyQualifiedName Fully qualified name of the schema
    /// @param location Location or content of the schema
    public SchemaInfo(final String fullyQualifiedName, final String location) {
      this.fullyQualifiedName = Objects.requireNonNull(fullyQualifiedName, "Fully qualified name cannot be null");
      this.location = Objects.requireNonNull(location, "Location cannot be null");
    }

    /// Returns the fully qualified name of this schema.
    ///
    /// @return The fully qualified name
    @Override
    public String fullyQualifiedName() {
      return fullyQualifiedName;
    }

    /// Returns the location or content of this schema.
    ///
    /// @return The schema location
    @Override
    public String location() {
      return location;
    }
  }
}
