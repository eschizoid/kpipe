package org.kpipe.registry;

import java.lang.System.Logger;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;
import org.apache.avro.generic.GenericRecord;
import org.kpipe.processor.AvroMessageProcessor;
import org.kpipe.processor.JsonMessageProcessor;

/// A registry for managing and composing byte array message processors. This registry allows
/// registering, retrieving, and composing functions that transform byte[] arrays, particularly
/// useful for message processing pipelines.
///
/// The registry supports different message formats (JSON, AVRO, PROTOBUF) and provides
/// integration with schema-based processors. Each processor is registered with a unique name and
/// can be composed into processing pipelines.
///
/// Example usage:
///
/// ```java
/// // Create a registry
/// final var registry = new MessageProcessorRegistry("my-app");
///
/// // Create and use an optimized JSON pipeline
/// final var pipeline = registry.jsonPipeline("addTimestamp", "sanitizeData");
/// final var result = pipeline.apply(inputBytes);
/// ```
///
/// For AVRO format with schemas:
///
/// ```java
/// // Create a registry
/// final var registry = new MessageProcessorRegistry("my-app");
///
/// // Add an Avro schema (automatically registers common operators)
/// registry.addSchema("user", "com.example.User", "schemas/user.avsc");
///
/// // Create and use an optimized Avro pipeline
/// final var pipeline = registry.avroPipeline("user", "addSource_user", "addTimestamp_user");
/// final var result = pipeline.apply(inputBytes);
/// ```
public class MessageProcessorRegistry {

  private static final Logger LOGGER = System.getLogger(MessageProcessorRegistry.class.getName());
  private final ConcurrentHashMap<String, UnaryOperator<Map<String, Object>>> jsonOperators = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, UnaryOperator<GenericRecord>> avroOperators = new ConcurrentHashMap<>();

  /// Registers an operator that transforms a JSON map.
  ///
  /// ```java
  /// registry.registerJsonOperator("myTransform", map -> {
  ///     map.put("processed", true);
  ///     return map;
  /// });
  /// ```
  ///
  /// @param name The name to register the operator under
  /// @param operator The operator to register
  public void registerJsonOperator(final String name, final UnaryOperator<Map<String, Object>> operator) {
    jsonOperators.put(name, operator);
  }

  /// Registers an operator that transforms an Avro record.
  ///
  /// ```java
  /// registry.registerAvroOperator("myTransform", record -> {
  ///     record.put("processed", true);
  ///     return record;
  /// });
  /// ```
  ///
  /// @param name The name to register the operator under
  /// @param operator The operator to register
  public void registerAvroOperator(final String name, final UnaryOperator<GenericRecord> operator) {
    avroOperators.put(name, operator);
  }

  /// Creates an optimized JSON pipeline that deserializes once, applies all operators, and
  /// serializes once.
  ///
  /// ```java
  /// final var pipeline = registry.jsonPipeline("addTimestamp", "sanitizeData");
  /// ```
  ///
  /// @param operatorNames Names of JSON operators to chain
  /// @return A function representing the optimized JSON processing pipeline
  public Function<byte[], byte[]> jsonPipeline(final String... operatorNames) {
    UnaryOperator<Map<String, Object>> combined = obj -> obj;
    for (final var name : operatorNames) {
      final var operator = jsonOperators.get(name);
      if (operator != null) {
        combined = compose(combined, operator);
      }
    }
    final var finalCombined = combined;
    return bytes -> JsonMessageProcessor.processJson(bytes, finalCombined);
  }

  /// Creates an optimized Avro pipeline that deserializes once, applies all operators, and
  /// serializes once.
  ///
  /// ```java
  /// final var pipeline = registry.avroPipeline("user", "addSource_user", "addTimestamp_user");
  /// ```
  ///
  /// @param schemaKey The key of the schema to use for all operations
  /// @param operatorNames Names of Avro operators to chain
  /// @return A function representing the optimized Avro processing pipeline
  public Function<byte[], byte[]> avroPipeline(final String schemaKey, final String... operatorNames) {
    return avroPipeline(schemaKey, 0, operatorNames);
  }

  /// Creates an optimized Avro pipeline that deserializes once from a given offset, applies all
  /// operators, and serializes once.
  ///
  /// This is useful for handling Avro data with magic bytes (e.g., Confluent wire format).
  ///
  /// ```java
  /// // Skip 5 magic bytes
  /// final var pipeline = registry.avroPipeline("user", 5, "addSource_user");
  /// ```
  ///
  /// @param schemaKey The key of the schema to use for all operations
  /// @param offset The number of bytes to skip at the start of input
  /// @param operatorNames Names of Avro operators to chain
  /// @return A function representing the optimized Avro processing pipeline
  public Function<byte[], byte[]> avroPipeline(
    final String schemaKey,
    final int offset,
    final String... operatorNames
  ) {
    final var schema = AvroMessageProcessor.getSchema(schemaKey);
    if (schema == null) throw new IllegalArgumentException("Schema not found: %s".formatted(schemaKey));

    UnaryOperator<GenericRecord> combined = record -> record;
    for (final var name : operatorNames) {
      final var operator = avroOperators.get(name);
      if (operator != null) combined = compose(combined, operator);
    }
    final var finalCombined = combined;
    return bytes -> AvroMessageProcessor.processAvro(bytes, offset, schema, finalCombined);
  }

  private <T> UnaryOperator<T> compose(UnaryOperator<T> first, UnaryOperator<T> second) {
    return t -> second.apply(first.apply(t));
  }

  private final ConcurrentHashMap<String, ProcessorEntry> registry = new ConcurrentHashMap<>();
  private final byte[] defaultErrorValue;
  private final String sourceAppName;
  private final MessageFormat messageFormat;

  /// A processor entry in the registry that maintains execution statistics.
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

  /// Creates a new registry with JSON as the default message format.
  ///
  /// @param sourceAppName Application name to use as source identifier
  public MessageProcessorRegistry(final String sourceAppName) {
    this(sourceAppName, MessageFormat.JSON);
  }

  /// Creates a new registry with the specified message format.
  ///
  /// @param sourceAppName Application name to use as source identifier
  /// @param messageFormat Message format to use (JSON, AVRO, PROTOBUF)
  public MessageProcessorRegistry(final String sourceAppName, final MessageFormat messageFormat) {
    this(sourceAppName, messageFormat, "{}".getBytes());
  }

  /// Creates a new registry with the specified message format and default error value.
  ///
  /// @param sourceAppName Application name to use as source identifier
  /// @param messageFormat Message format to use (JSON, AVRO, PROTOBUF)
  /// @param defaultErrorValue Value to return when processors throw exceptions
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

  /// Registers default processors based on the configured message format.
  private void registerDefaultProcessors() {
    // Register default operators for optimized pipelines
    if (messageFormat == MessageFormat.JSON) {
      registerJsonOperator("addSource", JsonMessageProcessor.addFieldOperator("source", sourceAppName));
      registerJsonOperator("addTimestamp", JsonMessageProcessor.addTimestampOperator("timestamp"));
      registerJsonOperator("markProcessed", JsonMessageProcessor.addFieldOperator("processed", "true"));
    }
  }

  /// Registers schema-specific processors for a given schema key. This is primarily used for
  /// AVRO and PROTOBUF formats.
  ///
  /// @param schemaKey The schema key to register processors for
  public void registerSchemaProcessors(String schemaKey) {
    // Register schema-specific operators for optimized pipelines
    if (messageFormat == MessageFormat.AVRO) {
      final var schema = AvroMessageProcessor.getSchema(schemaKey);
      if (schema != null) {
        registerAvroOperator("addSource_" + schemaKey, AvroMessageProcessor.addFieldOperator("source", sourceAppName));
        registerAvroOperator("addTimestamp_" + schemaKey, AvroMessageProcessor.addTimestampOperator("timestamp"));
        registerAvroOperator("markProcessed_" + schemaKey, AvroMessageProcessor.addFieldOperator("processed", "true"));
      }
    }
  }

  /// Registers a processor function with a name.
  ///
  /// @param name The unique name for the processor
  /// @param processor The function that processes byte arrays
  /// @throws NullPointerException if name or processor is null
  /// @throws IllegalArgumentException if name is empty
  public void register(final String name, final Function<byte[], byte[]> processor) {
    Objects.requireNonNull(name, "Processor name cannot be null");
    Objects.requireNonNull(processor, "Processor function cannot be null");

    if (name.trim().isEmpty()) throw new IllegalArgumentException("Processor name cannot be empty");

    final var entry = new ProcessorEntry(withErrorHandling(processor, defaultErrorValue));
    registry.put(name, entry);
  }

  /// Adds a schema and registers its processors.
  ///
  /// @param key                The schema key
  /// @param fullyQualifiedName The fully qualified name of the schema
  /// @param location           The schema location or content
  public void addSchema(final String key, final String fullyQualifiedName, final String location) {
    if (messageFormat == MessageFormat.AVRO) {
      // Register the schema with MessageFormat
      messageFormat.addSchema(key, fullyQualifiedName, location);

      // Register schema-specific processors
      registerSchemaProcessors(key);
    }
  }

  /// Registers an Avro schema and its corresponding processors.
  ///
  /// @param key                Schema identification key
  /// @param fullyQualifiedName Fully qualified schema name
  /// @param schemaJson         The schema content in JSON format
  public void registerAvroSchema(final String key, final String fullyQualifiedName, final String schemaJson) {
    if (messageFormat == MessageFormat.AVRO) addSchema(key, fullyQualifiedName, schemaJson);
  }

  /// Unregisters a processor.
  ///
  /// @param name The name of the processor to remove
  /// @return true if the processor was removed, false if it wasn't found
  public boolean unregister(final String name) {
    return registry.remove(name) != null;
  }

  /// Clears all processors from the registry.
  public void clear() {
    registry.clear();
  }

  /// Gets a processor by name. If no processor is found with the given name, returns the
  /// identity function (which returns the input unchanged).
  ///
  /// @param name The name of the processor to retrieve
  /// @return The processor function, or identity function if not found
  public Function<byte[], byte[]> get(final String name) {
    final var entry = registry.get(name);
    return entry != null ? entry::execute : Function.identity();
  }

  /// Adds error handling to a processor.
  ///
  /// When the processor throws an exception, this wrapper catches it, logs the error, and returns
  /// the provided default value instead.
  ///
  /// Example:
  ///
  /// ```java
  /// // Create a processor that might fail
  /// final var riskyProcessor = bytes -> {
  ///     // This might throw exceptions
  ///     try (final var input = new ByteArrayInputStream(bytes);
  ///          final var output = new ByteArrayOutputStream()) {
  ///         final var parsed = DSL_JSON.deserialize(Map.class, input);
  ///         // Do something with parsed JSON
  ///         DSL_JSON.serialize(parsed, output);
  ///         return output.toByteArray();
  ///     } catch (Exception e) {
  ///         throw new RuntimeException("Failed to process JSON", e);
  ///     }
  /// };
  ///
  /// // Wrap with error handling
  /// final var defaultValue = "{\"error\":true}".getBytes();
  /// final var safeProcessor = MessageProcessorRegistry.withErrorHandling(riskyProcessor,
  ///     defaultValue);
  ///
  /// // Now it won't throw exceptions
  /// final var result = safeProcessor.apply(inputBytes);
  /// ```
  ///
  /// @param processor The processor to wrap with error handling
  /// @param defaultValue The default value to return on error
  /// @return A function that handles errors during processing
  public static Function<byte[], byte[]> withErrorHandling(
    final Function<byte[], byte[]> processor,
    final byte[] defaultValue
  ) {
    return RegistryFunctions.withFunctionErrorHandling(processor, defaultValue, LOGGER);
  }

  /// Creates a conditional processor that applies one of two processors based on a condition.
  ///
  /// Example:
  ///
  /// ```java
  /// // Create a condition that checks if input is empty
  /// final var isEmpty = (Predicate<byte[]>) bytes -> bytes == null || bytes.length == 0;
  ///
  /// // Processors for each case
  /// final var emptyHandler = bytes -> "{\"empty\":true}".getBytes();
  /// final var normalProcessor = registry.get("parseJson");
  ///
  /// // Create conditional processor
  /// final var conditionalProcessor =
  ///     MessageProcessorRegistry.when(isEmpty, emptyHandler, normalProcessor);
  ///
  /// // Use the conditional processor
  /// final var result = conditionalProcessor.apply(inputBytes);
  /// ```
  ///
  /// @param condition Predicate to evaluate messages
  /// @param ifTrue Processor to use when the condition is true
  /// @param ifFalse Processor to use when the condition is false
  /// @return A function that conditionally processes messages
  /// @throws NullPointerException if any argument is null
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

  /// Returns all registered processors.
  ///
  /// Example:
  ///
  /// ```java
  /// // Get all registered processors
  /// final var allProcessors = registry.getAll();
  ///
  /// // Print all processor names
  /// allProcessors.keySet().forEach(System.out::println);
  ///
  /// // Use a specific processor
  /// final var processor = allProcessors.get("addTimestamp");
  /// final var result = processor.apply(inputBytes);
  /// ```
  ///
  /// @return Unmodifiable map of all processor names and functions
  public Map<String, Function<byte[], byte[]>> getAll() {
    return RegistryFunctions.createUnmodifiableView(registry, entry -> ((ProcessorEntry) entry)::execute);
  }

  /// Gets metrics for a processor including invocation count, error count, and processing time.
  ///
  /// Example:
  ///
  /// ```java
  /// // Get metrics for a processor
  /// final var metrics = registry.getMetrics("parseJson");
  ///
  /// // Print the metrics
  /// System.out.println("Invocations: " + metrics.get("invocationCount"));
  /// System.out.println("Errors: " + metrics.get("errorCount"));
  /// System.out.println("Total processing time (ms): " + metrics.get("processingTimeMs"));
  /// ```
  ///
  /// @param name The processor name
  /// @return Map containing metrics or empty map if processor not found
  public Map<String, Object> getMetrics(final String name) {
    final var entry = registry.get(name);
    if (entry == null) return Map.of();
    return RegistryFunctions.createMetrics(entry.invocationCount, entry.errorCount, entry.totalProcessingTimeMs);
  }

  /// Gets the source application name configured for this registry.
  ///
  /// Example:
  ///
  /// ```java
  /// // Get the configured source app name
  /// final var appName = registry.getSourceAppName();
  /// System.out.println("This registry is configured for: " + appName);
  /// ```
  ///
  /// @return The source application name
  public String getSourceAppName() {
    return sourceAppName;
  }

  /// Gets the message format used by this registry.
  ///
  /// Example:
  ///
  /// ```java
  /// // Check if registry uses AVRO format
  /// if (registry.getMessageFormat() == MessageFormat.AVRO) {
  ///     // Register an AVRO schema
  ///     final var eventSchema = """
  ///       {
  ///         "type": "record",
  ///         "name": "Event",
  ///         "fields": [
  ///           {"name": "id", "type": "string"},
  ///           {"name": "timestamp", "type": "long"}
  ///         ]
  ///       }
  ///       """;
  ///     registry.addSchema("event", "com.example.Event", eventSchema);
  /// }
  /// ```
  ///
  /// @return The message format
  public MessageFormat getMessageFormat() {
    return messageFormat;
  }
}
