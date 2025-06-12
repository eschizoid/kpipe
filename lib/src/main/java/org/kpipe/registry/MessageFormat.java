package org.kpipe.registry;

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
import org.kpipe.processor.JsonMessageProcessor;

/**
 * Represents supported message formats in the message processing system. Each format defines
 * specific serialization, schema handling, and processing behaviors for messages.
 *
 * <p>Three standard formats are supported:
 *
 * <ul>
 *   <li>{@link #JSON} - For processing JSON messages with schema-less operations
 *   <li>{@link #AVRO} - For Apache Avro format with schema registry integration
 *   <li>{@link #PROTOBUF} - For Protocol Buffer messages
 * </ul>
 *
 * <p>Each format maintains its own registry of schemas and provides format-specific message
 * processors. The AVRO format has advanced schema handling capabilities including loading schemas
 * from HTTP endpoints, file system locations, or inline content.
 *
 * <h2>Basic Usage Examples</h2>
 *
 * <h3>1. Using with MessageProcessorRegistry</h3>
 *
 * <pre>{@code
 * // Create a processor registry with JSON format (default)
 * final var jsonRegistry = new MessageProcessorRegistry("my-app");
 *
 * // Or explicitly specify AVRO format
 * final var avroRegistry = new MessageProcessorRegistry("my-app", MessageFormat.AVRO);
 *
 * // Get available processors for the format
 * final var processors = jsonRegistry.getAll();
 * }</pre>
 *
 * <h3>2. Registering Schemas</h3>
 *
 * <pre>{@code
 * // Register schema using inline schema definition
 * final var userSchema = """
 *   {
 *     "type": "record",
 *     "name": "User",
 *     "namespace": "com.example",
 *     "fields": [
 *       {"name": "id", "type": "string"},
 *       {"name": "name", "type": "string"},
 *       {"name": "email", "type": "string"}
 *     ]
 *   }
 *   """;
 * MessageFormat.AVRO.addSchema("user", "com.example.User", userSchema);
 *
 * // Register schema from file path
 * MessageFormat.AVRO.addSchema(
 *     "product",
 *     "com.example.Product",
 *     "/schemas/product.avsc"
 * );
 *
 * // Register schema from HTTP location
 * MessageFormat.AVRO.addSchema(
 *     "event",
 *     "com.example.Event",
 *     "https://schema-registry.example.com/schemas/event.avsc"
 * );
 * }</pre>
 *
 * <h3>3. Finding and Using Schemas</h3>
 *
 * <pre>{@code
 * // Get all registered schemas for a format
 * final var schemas = MessageFormat.AVRO.getSchemas();
 *
 * // Find a specific schema by key
 * final var userSchema = MessageFormat.AVRO.findSchema("user")
 *     .orElseThrow(() -> new IllegalStateException("User schema not found"));
 *
 * // Find schemas matching a condition
 * final var userSchemas = MessageFormat.AVRO.findSchemas(
 *     schema -> schema.fullyQualifiedName().startsWith("com.example.user")
 * );
 * }</pre>
 *
 * <h3>4. Working with Format-Specific Processors</h3>
 *
 * <pre>{@code
 * // Get default processors for a format
 * final var jsonProcessors = MessageFormat.JSON.getDefaultProcessors("my-app");
 *
 * // Get schema-specific processors for AVRO
 * final var userProcessors = MessageFormat.AVRO.getSchemaProcessors("user", "my-app");
 *
 * // Create a processor pipeline in the registry
 * final var registry = new MessageProcessorRegistry("my-app");
 * registry.registerAvroSchema("user", "com.example.User", userSchemaContent);
 *
 * final var pipeline = registry.pipeline(
 *     "parseAvro_user",     // Parse using user schema
 *     "addTimestamp_user",  // Add timestamp to parsed record
 *     "markProcessed"       // Mark as processed
 * );
 *
 * // Process a message
 * final var result = pipeline.apply(inputBytes);
 * }</pre>
 *
 * <h3>5. Schema Location Handling</h3>
 *
 * <p>The AVRO format supports multiple schema source locations:
 *
 * <ul>
 *   <li>HTTP/HTTPS URLs: {@code "https://example.com/schemas/user.avsc"}
 *   <li>File paths: {@code "/path/to/schema.avsc"} or {@code "file:///path/to/schema.avsc"}
 *   <li>Classpath resources: {@code "classpath:/schemas/user.avsc"}
 *   <li>Inline schema content: Pass the JSON schema definition directly
 * </ul>
 *
 * @see SchemaInfo For structure of schema information
 * @see MessageProcessorRegistry For using formats in message processing
 * @see org.kpipe.processor.AvroMessageProcessor For AVRO message processing
 * @see org.kpipe.processor.JsonMessageProcessor For JSON message processing
 */
public enum MessageFormat {
  /** JSON message format. */
  JSON(new HashMap<>(), location -> location) {
    @Override
    public Map<String, Function<byte[], byte[]>> getDefaultProcessors(final String sourceAppName) {
      // Implementation unchanged
      return Map.of(
        "parseJson",
        JsonMessageProcessor.parseJson(),
        "addSource",
        JsonMessageProcessor.addField("source", sourceAppName),
        "markProcessed",
        JsonMessageProcessor.addField("processed", "true"),
        "addTimestamp",
        JsonMessageProcessor.addTimestamp("timestamp")
      );
    }
  },

  /**
   * AVRO message format with support for schema retrieval from HTTP endpoints and file system.
   * Handles schemas from inline content, file paths (local or file:// protocol), and HTTP/HTTPS
   * URLs.
   */
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
            return response.body();
          } else {
            throw new IOException("Failed to fetch schema: HTTP %d".formatted(response.statusCode()));
          }
        }
        // Check if file path
        else if (location.startsWith("classpath:")) {
          final var resourcePath = location.substring("classpath:".length());
          try (final var inputStream = MessageFormat.class.getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
              throw new IOException("Classpath resource not found: " + resourcePath);
            }
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
      } catch (Exception e) {
        throw new RuntimeException("Failed to read schema from location: " + location, e);
      }
    }
  ) {
    @Override
    public Map<String, Function<byte[], byte[]>> getDefaultProcessors(final String sourceAppName) {
      return Collections.emptyMap();
    }

    @Override
    public Map<String, Function<byte[], byte[]>> getSchemaProcessors(
      final String schemaKey,
      final String sourceAppName
    ) {
      final var schemaInfo = schemas.get(schemaKey);
      if (schemaInfo == null) {
        return Collections.emptyMap();
      }

      final var processors = new HashMap<String, Function<byte[], byte[]>>();

      processors.put("parseAvro_%s".formatted(schemaKey), AvroMessageProcessor.parseAvro(schemaKey));
      processors.put(
        "addSource_%s".formatted(schemaKey),
        AvroMessageProcessor.addField(schemaKey, "source", sourceAppName)
      );
      processors.put(
        "markProcessed_%s".formatted(schemaKey),
        AvroMessageProcessor.addField(schemaKey, "processed", "true")
      );
      processors.put("addTimestamp_%s".formatted(schemaKey), AvroMessageProcessor.addTimestamp(schemaKey, "timestamp"));

      return processors;
    }
  },

  /** Protocol Buffers message format. */
  PROTOBUF(new HashMap<>(), location -> location) {
    @Override
    public Map<String, Function<byte[], byte[]>> getDefaultProcessors(final String sourceAppName) {
      return Collections.emptyMap();
    }
  };

  /** Map of schema keys to schema information, maintained by each format. */
  protected final Map<String, SchemaInfo> schemas;

  /** Function to read schema content from various locations */
  protected final Function<String, String> schemaReader;

  /**
   * Constructs a message format with a schema information map and schema reader function.
   *
   * @param schemas Initial schema map
   * @param schemaReader Function to read schema content from a location
   */
  MessageFormat(final Map<String, SchemaInfo> schemas, final Function<String, String> schemaReader) {
    this.schemas = schemas;
    this.schemaReader = schemaReader;
  }

  /**
   * Returns an unmodifiable view of all schemas registered with this format.
   *
   * @return Map of schema keys to their schema information
   */
  public Map<String, SchemaInfo> getSchemas() {
    return Collections.unmodifiableMap(schemas);
  }

  /**
   * Returns the default processors for this message format that don't depend on schemas.
   *
   * @param key The schema key to look up
   * @return Optional containing the schema info if found, or empty if not found
   */
  public Optional<SchemaInfo> findSchema(String key) {
    return Optional.ofNullable(schemas.get(key));
  }

  /**
   * Removes all schemas registered with this message format. Useful for resetting the schema
   * registry, especially in tests or reinitialization scenarios.
   */
  public void clearSchemas() {
    schemas.clear();
  }

  /**
   * Adds a schema to this format and registers it with the appropriate processor.
   *
   * @param key Schema identification key
   * @param fullyQualifiedName Fully qualified schema name
   * @param location Location of the schema definition
   */
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

  /**
   * Returns the default processors for this message format that don't depend on schemas.
   *
   * @param sourceAppName The source application name for metadata
   * @return Map of processor names to processor functions
   */
  public abstract Map<String, Function<byte[], byte[]>> getDefaultProcessors(final String sourceAppName);

  /**
   * Returns processors that are specific to a schema. The Default implementation returns an empty
   * map, but is overridden by formats that support schemas.
   *
   * @param schemaKey The schema key
   * @param sourceAppName The source application name for metadata
   * @return Map of processor names to processor functions
   */
  public Map<String, Function<byte[], byte[]>> getSchemaProcessors(final String schemaKey, final String sourceAppName) {
    return Collections.emptyMap();
  }

  /**
   * Finds schemas matching the given predicate.
   *
   * @param predicate Condition to test schemas against
   * @return List of matching schema information
   */
  public List<SchemaInfo> findSchemas(final Predicate<SchemaInfo> predicate) {
    return schemas.values().stream().filter(predicate).toList();
  }

  /**
   * Represents schema information with a fully qualified name and location.
   *
   * @param fullyQualifiedName Fully qualified name of the schema
   * @param location Location or content of the schema (file path, URL, or inline content)
   */
  public record SchemaInfo(String fullyQualifiedName, String location) {
    /**
     * Creates new schema information instance.
     *
     * @param fullyQualifiedName Fully qualified name of the schema
     * @param location Location or content of the schema
     */
    public SchemaInfo(final String fullyQualifiedName, final String location) {
      this.fullyQualifiedName = Objects.requireNonNull(fullyQualifiedName, "Fully qualified name cannot be null");
      this.location = Objects.requireNonNull(location, "Location cannot be null");
    }

    /**
     * Returns the fully qualified name of this schema.
     *
     * @return The fully qualified name
     */
    @Override
    public String fullyQualifiedName() {
      return fullyQualifiedName;
    }

    /**
     * Returns the location or content of this schema.
     *
     * @return The schema location
     */
    @Override
    public String location() {
      return location;
    }
  }
}
