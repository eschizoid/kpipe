package org.kpipe.registry;

import java.lang.System.Logger;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;

/**
 * A registry for managing and composing byte array message processors. This registry allows
 * registering, retrieving, and composing functions that transform byte[] arrays, particularly
 * useful for message processing pipelines.
 *
 * <p>The registry supports different message formats (JSON, AVRO, PROTOBUF) and provides
 * integration with schema-based processors. Each processor is registered with a unique name and can
 * be composed into processing pipelines.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create a registry with JSON format
 * final var registry = new MessageProcessorRegistry("my-app");
 *
 * // Register a custom processor
 * registry.register("lowercase", bytes -> new String(bytes).toLowerCase().getBytes());
 *
 * // Create and use a pipeline
 * final var pipeline = registry.pipeline("parseJson", "addTimestamp", "lowercase");
 * final var result = pipeline.apply(inputBytes);
 * }</pre>
 *
 * <p>For AVRO format with schemas:
 *
 * <pre>{@code
 * // Create a registry with AVRO format
 * final var registry = new MessageProcessorRegistry("my-app", MessageFormat.AVRO);
 *
 * // Register an Avro schema
 * final var userSchemaJson = """
 *   {
 *     "type": "record",
 *     "name": "User",
 *     "fields": [
 *       {"name": "name", "type": "string"},
 *       {"name": "age", "type": "int"}
 *     ]
 *   }
 *   """;
 * registry.registerAvroSchema("userSchema", "com.example.User", userSchemaJson);
 *
 * // Create and use a schema-specific pipeline
 * final var pipeline = registry.pipeline(
 *     "parseAvro_userSchema",
 *     "addTimestamp_userSchema"
 * );
 * final var result = pipeline.apply(inputBytes);
 * }</pre>
 */
public class MessageProcessorRegistry {

  private static final Logger LOGGER = System.getLogger(MessageProcessorRegistry.class.getName());
  private final ConcurrentHashMap<String, ProcessorEntry> registry = new ConcurrentHashMap<>();
  private final byte[] defaultErrorValue;
  private final String sourceAppName;
  private final MessageFormat messageFormat;

  /** A processor entry in the registry that maintains execution statistics. */
  private static class ProcessorEntry {

    final Function<byte[], byte[]> processor;
    long invocationCount = 0;
    long errorCount = 0;
    long totalProcessingTimeMs = 0;

    ProcessorEntry(final Function<byte[], byte[]> processor) {
      this.processor = processor;
    }

    public byte[] execute(final byte[] input) {
      final Supplier<Long> counterIncrement = () -> invocationCount++;
      final Supplier<Long> errorIncrement = () -> errorCount++;
      final Consumer<Duration> timeAccumulator = duration -> totalProcessingTimeMs += duration.toNanos();

      final var timedExecution = RegistryFunctions.<byte[], byte[]>timedExecution(
        counterIncrement,
        errorIncrement,
        timeAccumulator
      );

      return timedExecution.apply(input, processor);
    }
  }

  /**
   * Creates a new registry with JSON as the default message format.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a registry for JSON processing
   * final var registry = new MessageProcessorRegistry("order-service");
   *
   * // Default JSON processors are automatically registered
   * final var pipeline = registry.pipeline("parseJson", "addTimestamp");
   * }</pre>
   *
   * @param sourceAppName Application name to use as source identifier
   */
  public MessageProcessorRegistry(final String sourceAppName) {
    this(sourceAppName, MessageFormat.JSON);
  }

  /**
   * Creates a new registry with the specified message format.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a registry for AVRO processing
   * final var registry = new MessageProcessorRegistry("user-service", MessageFormat.AVRO);
   *
   * // Register an AVRO schema
   * final var userSchemaJson = """
   *   {
   *     "type": "record",
   *     "name": "User",
   *     "fields": [
   *       {"name": "name", "type": "string"},
   *       {"name": "email", "type": "string"}
   *     ]
   *   }
   *   """;
   * registry.registerAvroSchema("userSchema", "com.example.User", userSchemaJson);
   * }</pre>
   *
   * @param sourceAppName Application name to use as source identifier
   * @param messageFormat Message format to use (JSON, AVRO, PROTOBUF)
   */
  public MessageProcessorRegistry(final String sourceAppName, final MessageFormat messageFormat) {
    this(sourceAppName, messageFormat, "{}".getBytes());
  }

  /**
   * Creates a new registry with the specified message format and default error value.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a registry with custom error response
   * final var errorResponse = "{\"error\":true,\"message\":\"Processing failed\"}".getBytes();
   * final var registry = new MessageProcessorRegistry(
   *     "payment-service",
   *     MessageFormat.JSON,
   *     errorResponse
   * );
   *
   * // Register a processor that might fail
   * registry.register("riskyOperation", bytes -> {
   *     // If this throws an exception, the errorResponse will be returned
   *     if (bytes.length == 0) throw new IllegalArgumentException("Empty input");
   *     return processBytes(bytes);
   * });
   * }</pre>
   *
   * @param sourceAppName Application name to use as source identifier
   * @param messageFormat Message format to use (JSON, AVRO, PROTOBUF)
   * @param defaultErrorValue Value to return when processors throw exceptions
   */
  public MessageProcessorRegistry(
    final String sourceAppName,
    final MessageFormat messageFormat,
    final byte[] defaultErrorValue
  ) {
    this.sourceAppName = Objects.requireNonNull(sourceAppName, "Source app name cannot be null");
    this.messageFormat = Objects.requireNonNull(messageFormat, "Message format cannot be null");
    this.defaultErrorValue = Objects.requireNonNull(defaultErrorValue, "Default error value cannot be null");

    registerDefaultProcessors();
  }

  /** Registers default processors based on the configured message format. */
  private void registerDefaultProcessors() {
    // Register format-specific default processors
    final var defaultProcessors = messageFormat.getDefaultProcessors(sourceAppName);
    defaultProcessors.forEach(this::register);
  }

  /**
   * Registers schema-specific processors for a given schema key. This is primarily used for AVRO
   * and PROTOBUF formats.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // After registering a schema
   * registry.registerAvroSchema("userEvent", "com.example.UserEvent", userSchemaJson);
   *
   * // Explicitly register schema processors if needed
   * registry.registerSchemaProcessors("userEvent");
   *
   * // Now you can use schema-specific processors
   * final var pipeline = registry.pipeline(
   *     "parseAvro_userEvent",
   *     "addTimestamp_userEvent"
   * );
   * }</pre>
   *
   * @param schemaKey The schema key to register processors for
   */
  public void registerSchemaProcessors(String schemaKey) {
    final var schemaProcessors = messageFormat.getSchemaProcessors(schemaKey, sourceAppName);
    schemaProcessors.forEach(this::register);
  }

  /**
   * Registers a processor function with a name.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Register a custom processor
   * registry.register("addCorrelationId", bytes -> {
   *     try {
   *         final var inputStream = new ByteArrayInputStream(bytes);
   *         final var parsed = DSL_JSON.deserialize(Map.class, inputStream);
   *         if (parsed == null) {
   *             return bytes;
   *         }
   *
   *         parsed.put("correlationId", UUID.randomUUID().toString());
   *
   *         final var outputStream = new ByteArrayOutputStream();
   *         DSL_JSON.serialize(parsed, outputStream);
   *         return outputStream.toByteArray();
   *     } catch (Exception e) {
   *         throw new RuntimeException("Failed to add correlation ID", e);
   *     }
   * });
   *
   * // Use it in a pipeline
   * final var pipeline = registry.pipeline("parseJson", "addCorrelationId", "addTimestamp");
   * }</pre>
   *
   * @param name The unique name for the processor
   * @param processor The function that processes byte arrays
   * @throws NullPointerException if name or processor is null
   * @throws IllegalArgumentException if name is empty
   */
  public void register(final String name, final Function<byte[], byte[]> processor) {
    Objects.requireNonNull(name, "Processor name cannot be null");
    Objects.requireNonNull(processor, "Processor function cannot be null");

    if (name.trim().isEmpty()) {
      throw new IllegalArgumentException("Processor name cannot be empty");
    }

    final var entry = new ProcessorEntry(withErrorHandling(processor, defaultErrorValue));
    registry.put(name, entry);
  }

  /**
   * Adds a schema and registers its processors.
   *
   * <p>For AVRO format, this also registers the schema with the AvroMessageProcessor and registers
   * all related schema-specific processors.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Add a schema and register related processors
   * final var orderEventSchema = """
   *   {
   *     "type": "record",
   *     "name": "OrderEvent",
   *     "fields": [
   *       {"name": "orderId", "type": "string"},
   *       {"name": "amount", "type": "double"}
   *     ]
   *   }
   *   """;
   * registry.addSchema("orderEvent", "com.example.OrderEvent", orderEventSchema);
   *
   * // Use schema-specific processors
   * final var pipeline = registry.pipeline(
   *     "parseAvro_orderEvent",
   *     "addTimestamp_orderEvent"
   * );
   * }</pre>
   *
   * @param key The schema key
   * @param fullyQualifiedName The fully qualified name of the schema
   * @param location The schema location or content
   * @return This registry instance for chaining
   */
  public MessageProcessorRegistry addSchema(final String key, final String fullyQualifiedName, final String location) {
    if (messageFormat == MessageFormat.AVRO) {
      // Register the schema with MessageFormat
      messageFormat.addSchema(key, fullyQualifiedName, location);

      // Register schema-specific processors
      registerSchemaProcessors(key);
    }
    return this;
  }

  /**
   * Registers an Avro schema and its corresponding processors.
   *
   * <p>This is a convenience method specifically for AVRO format that directly takes the schema
   * JSON content. Internally, it calls {@link #addSchema} and registers all schema-specific
   * processors.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Register an Avro schema with inline schema definition
   * final var userSchema = """
   *   {
   *     "type": "record",
   *     "name": "UserProfile",
   *     "fields": [
   *       {"name": "userId", "type": "string"},
   *       {"name": "name", "type": "string"},
   *       {"name": "email", "type": "string"},
   *       {"name": "createdAt", "type": "long", "default": 0}
   *     ]
   *   }
   *   """;
   * registry.registerAvroSchema("userProfile", "com.example.UserProfile", userSchema);
   *
   * // Create a pipeline using schema-specific processors
   * final var pipeline = registry.pipeline(
   *     "parseAvro_userProfile",
   *     "addTimestamp_userProfile"
   * );
   * }</pre>
   *
   * @param key Schema identification key
   * @param fullyQualifiedName Fully qualified schema name
   * @param schemaJson The schema content in JSON format
   * @return This registry for method chaining
   */
  public MessageProcessorRegistry registerAvroSchema(
    final String key,
    final String fullyQualifiedName,
    final String schemaJson
  ) {
    if (messageFormat == MessageFormat.AVRO) {
      // Pass the schema content directly
      return addSchema(key, fullyQualifiedName, schemaJson);
    }
    return this;
  }

  /**
   * Unregisters a processor.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Remove a processor that's no longer needed
   * final var removed = registry.unregister("addCorrelationId");
   * if (removed) {
   *     System.out.println("Processor was removed");
   * } else {
   *     System.out.println("Processor didn't exist");
   * }
   * }</pre>
   *
   * @param name The name of the processor to remove
   * @return true if the processor was removed, false if it wasn't found
   */
  public boolean unregister(final String name) {
    return registry.remove(name) != null;
  }

  /**
   * Clears all processors from the registry.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Remove all processors
   * registry.clear();
   *
   * // Re-register just the ones you need
   * registry.register("customProcessor", myProcessor);
   * }</pre>
   *
   * <p>Note that this does not affect the schemas registered with MessageFormat.
   */
  public void clear() {
    registry.clear();
  }

  /**
   * Gets a processor by name. If no processor is found with the given name, returns the identity
   * function (which returns the input unchanged).
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Get a processor and use it
   * final var processor = registry.get("parseJson");
   * final var parsedJson = processor.apply(rawBytes);
   *
   * // Get a non-existent processor (returns identity function)
   * final var missing = registry.get("nonExistent");
   * final var unchanged = missing.apply(rawBytes); // Returns rawBytes unchanged
   * }</pre>
   *
   * @param name The name of the processor to retrieve
   * @return The processor function, or identity function if not found
   */
  public Function<byte[], byte[]> get(final String name) {
    final var entry = registry.get(name);
    return entry != null ? entry::execute : Function.identity();
  }

  /**
   * Creates a processor pipeline by chaining multiple named processors in sequence.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a pipeline from named processors
   * final var jsonPipeline = registry.pipeline(
   *     "parseJson",      // Parse raw JSON
   *     "addSource",      // Add source app name
   *     "addTimestamp",   // Add processing timestamp
   *     "markProcessed"   // Mark as processed
   * );
   *
   * // For Avro with schemas
   * final var avroPipeline = registry.pipeline(
   *     "parseAvro_userEvent",       // Parse Avro using userEvent schema
   *     "addTimestamp_userEvent",    // Add timestamp to Avro record
   *     "addSource_userEvent"        // Add source field
   * );
   *
   * // Use the pipeline
   * final var processedBytes = jsonPipeline.apply(rawBytes);
   * }</pre>
   *
   * @param processorNames Names of processors to chain in sequence
   * @return A function representing the processing pipeline
   */
  public Function<byte[], byte[]> pipeline(final String... processorNames) {
    return message -> {
      var result = message;
      for (final var name : processorNames) {
        result = get(name).apply(result);
      }
      return result;
    };
  }

  /**
   * Creates a pipeline from a list of processor functions.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a list of processor functions
   * final var processors = List.of(
   *     registry.get("parseJson"),
   *     bytes -> {
   *         // Custom inline processor using DslJson
   *         try (final var input = new ByteArrayInputStream(bytes);
   *              final var output = new ByteArrayOutputStream()) {
   *             final var parsed = DSL_JSON.deserialize(Map.class, input);
   *             if (parsed == null) {
   *                 return bytes;
   *             }
   *             parsed.put("customField", "value");
   *             DSL_JSON.serialize(parsed, output);
   *             return output.toByteArray();
   *         } catch (Exception e) {
   *             return bytes;
   *         }
   *     },
   *     registry.get("addTimestamp")
   * );
   *
   * // Create a pipeline from the list
   * final var pipeline = registry.pipeline(processors);
   * final var result = pipeline.apply(inputBytes);
   * }</pre>
   *
   * @param processors List of processor functions to chain
   * @return A function representing the processing pipeline
   * @throws NullPointerException if processors list is null
   */
  public Function<byte[], byte[]> pipeline(final List<Function<byte[], byte[]>> processors) {
    Objects.requireNonNull(processors, "Processor list cannot be null");
    return message -> {
      byte[] result = message;
      for (final var processor : processors) {
        result = processor.apply(result);
      }
      return result;
    };
  }

  /**
   * Adds error handling to a processor.
   *
   * <p>When the processor throws an exception, this wrapper catches it, logs the error, and returns
   * the provided default value instead.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a processor that might fail
   * final var riskyProcessor = bytes -> {
   *     // This might throw exceptions
   *     try (final var input = new ByteArrayInputStream(bytes);
   *          final var output = new ByteArrayOutputStream()) {
   *         final var parsed = DSL_JSON.deserialize(Map.class, input);
   *         // Do something with parsed JSON
   *         DSL_JSON.serialize(parsed, output);
   *         return output.toByteArray();
   *     } catch (Exception e) {
   *         throw new RuntimeException("Failed to process JSON", e);
   *     }
   * };
   *
   * // Wrap with error handling
   * final var defaultValue = "{\"error\":true}".getBytes();
   * final var safeProcessor = MessageProcessorRegistry.withErrorHandling(riskyProcessor, defaultValue);
   *
   * // Now it won't throw exceptions
   * final var result = safeProcessor.apply(inputBytes);
   * }</pre>
   *
   * @param processor The processor to wrap with error handling
   * @param defaultValue The default value to return on error
   * @return A function that handles errors during processing
   */
  public static Function<byte[], byte[]> withErrorHandling(
    final Function<byte[], byte[]> processor,
    final byte[] defaultValue
  ) {
    return RegistryFunctions.withFunctionErrorHandling(processor, defaultValue, LOGGER);
  }

  /**
   * Creates a conditional processor that applies one of two processors based on a condition.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a condition that checks if input is empty
   * final var isEmpty = (Predicate<byte[]>) bytes -> bytes == null || bytes.length == 0;
   *
   * // Processors for each case
   * final var emptyHandler = bytes -> "{\"empty\":true}".getBytes();
   * final var normalProcessor = registry.get("parseJson");
   *
   * // Create conditional processor
   * final var conditionalProcessor =
   *     MessageProcessorRegistry.when(isEmpty, emptyHandler, normalProcessor);
   *
   * // Use the conditional processor
   * final var result = conditionalProcessor.apply(inputBytes);
   * }</pre>
   *
   * @param condition Predicate to evaluate messages
   * @param ifTrue Processor to use when condition is true
   * @param ifFalse Processor to use when condition is false
   * @return A function that conditionally processes messages
   * @throws NullPointerException if any argument is null
   */
  public static Function<byte[], byte[]> when(
    final Predicate<byte[]> condition,
    final Function<byte[], byte[]> ifTrue,
    final Function<byte[], byte[]> ifFalse
  ) {
    Objects.requireNonNull(condition, "Condition cannot be null");
    Objects.requireNonNull(ifTrue, "True processor cannot be null");
    Objects.requireNonNull(ifFalse, "False processor cannot be null");

    return bytes -> condition.test(bytes) ? ifTrue.apply(bytes) : ifFalse.apply(bytes);
  }

  /**
   * Returns all registered processors.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Get all registered processors
   * final var allProcessors = registry.getAll();
   *
   * // Print all processor names
   * allProcessors.keySet().forEach(System.out::println);
   *
   * // Use a specific processor
   * final var processor = allProcessors.get("addTimestamp");
   * final var result = processor.apply(inputBytes);
   * }</pre>
   *
   * @return Unmodifiable map of all processor names and functions
   */
  public Map<String, Function<byte[], byte[]>> getAll() {
    return RegistryFunctions.createUnmodifiableView(registry, entry -> ((ProcessorEntry) entry)::execute);
  }

  /**
   * Gets metrics for a processor including invocation count, error count, and processing time.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Get metrics for a processor
   * final var metrics = registry.getMetrics("parseJson");
   *
   * // Print the metrics
   * System.out.println("Invocations: " + metrics.get("invocationCount"));
   * System.out.println("Errors: " + metrics.get("errorCount"));
   * System.out.println("Total processing time (ms): " + metrics.get("processingTimeMs"));
   * }</pre>
   *
   * @param name The processor name
   * @return Map containing metrics or empty map if processor not found
   */
  public Map<String, Object> getMetrics(final String name) {
    final var entry = registry.get(name);
    if (entry == null) {
      return Map.of();
    }

    return RegistryFunctions.createMetrics(entry.invocationCount, entry.errorCount, entry.totalProcessingTimeMs);
  }

  /**
   * Gets the source application name configured for this registry.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Get the configured source app name
   * final var appName = registry.getSourceAppName();
   * System.out.println("This registry is configured for: " + appName);
   * }</pre>
   *
   * @return The source application name
   */
  public String getSourceAppName() {
    return sourceAppName;
  }

  /**
   * Gets the message format used by this registry.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Check if registry uses AVRO format
   * if (registry.getMessageFormat() == MessageFormat.AVRO) {
   *     // Register an AVRO schema
   *     final var eventSchema = """
   *       {
   *         "type": "record",
   *         "name": "Event",
   *         "fields": [
   *           {"name": "id", "type": "string"},
   *           {"name": "timestamp", "type": "long"}
   *         ]
   *       }
   *       """;
   *     registry.addSchema("event", "com.example.Event", eventSchema);
   * }
   * }</pre>
   *
   * @return The message format
   */
  public MessageFormat getMessageFormat() {
    return messageFormat;
  }
}
