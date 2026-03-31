package org.kpipe.registry;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;
import org.apache.avro.generic.GenericRecord;
import org.kpipe.processor.AvroMessageProcessor;
import org.kpipe.processor.JsonMessageProcessor;

/// Registry for managing and composing message processors in KPipe.
///
/// This class allows registration, retrieval, and composition of byte array message processors for
/// different formats (JSON, Avro, Protobuf). It supports schema-based and type-safe pipelines for
/// Kafka message processing, and provides utilities for building and composing processing chains.
///
/// Example usage:
/// ```java
/// final var registry = new MessageProcessorRegistry("my-app");
/// registry.addSchema("user", "User", "schemas/user.avsc");
/// var pipeline = registry.avroPipelineBuilder("user").build();
/// ```
public class MessageProcessorRegistry {

  private static final Logger LOGGER = System.getLogger(MessageProcessorRegistry.class.getName());

  /// Pre-defined key for adding a source field to JSON messages.
  public static final RegistryKey<Map<String, Object>> JSON_ADD_SOURCE = RegistryKey.json("addSource");
  /// Pre-defined key for adding a timestamp field to JSON messages.
  public static final RegistryKey<Map<String, Object>> JSON_ADD_TIMESTAMP = RegistryKey.json("addTimestamp");
  /// Pre-defined key for marking JSON messages as processed.
  public static final RegistryKey<Map<String, Object>> JSON_MARK_PROCESSED = RegistryKey.json("markProcessed");

  private final ConcurrentHashMap<RegistryKey<?>, RegistryEntry<?>> registry = new ConcurrentHashMap<>();
  private final byte[] defaultErrorValue;
  private final String sourceAppName;
  private final MessageFormat<?> messageFormat;

  /// A registry entry that maintains execution statistics.
  private static class RegistryEntry<T> {

    final T value;
    long invocationCount = 0;
    long errorCount = 0;
    long totalProcessingTimeMs = 0;

    RegistryEntry(final T value) {
      this.value = value;
    }

    @SuppressWarnings("unchecked")
    public byte[] execute(final byte[] input) {
      if (!(value instanceof Function)) throw new UnsupportedOperationException(
        "Entry value is not a Function<byte[], byte[]>"
      );
      final var processor = (Function<byte[], byte[]>) value;

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

  /// Creates a fluent builder for JSON pipelines.
  ///
  /// @return A new JsonPipelineBuilder
  public JsonPipelineBuilder jsonPipelineBuilder() {
    return new JsonPipelineBuilder();
  }

  /// Creates a fluent builder for Avro pipelines.
  ///
  /// @param schemaKey The key of the schema to use
  /// @return A new AvroPipelineBuilder
  public AvroPipelineBuilder avroPipelineBuilder(final String schemaKey) {
    return new AvroPipelineBuilder(schemaKey);
  }

  /// Creates a fluent builder for Avro pipelines with a custom schema offset.
  ///
  /// @param schemaKey The key of the schema to use
  /// @param offset The schema offset in the byte array
  /// @return A new AvroPipelineBuilder
  public AvroPipelineBuilder avroPipelineBuilder(final String schemaKey, final int offset) {
    return new AvroPipelineBuilder(schemaKey, offset);
  }

  /// Creates a fluent builder for POJO pipelines.
  ///
  /// @param <T> The POJO type
  /// @param clazz The class of the POJO
  /// @return A new PojoPipelineBuilder
  public <T> PojoPipelineBuilder<T> pojoPipelineBuilder(final Class<T> clazz) {
    return new PojoPipelineBuilder<>(clazz);
  }

  /// A fluent builder for creating type-safe JSON processing pipelines.
  /// Fluent builder for creating type-safe JSON processing pipelines.
  public class JsonPipelineBuilder {

    private final List<UnaryOperator<Map<String, Object>>> operators = new ArrayList<>();

    /// Constructs a new JsonPipelineBuilder.
    public JsonPipelineBuilder() {}

    /// Adds a JSON operator to the pipeline.
    ///
    /// @param operator the operator to add
    /// @return this builder instance
    public JsonPipelineBuilder add(final UnaryOperator<Map<String, Object>> operator) {
      operators.add(operator);
      return this;
    }

    /// Adds a pre-registered JSON operator by key.
    ///
    /// @param key the registry key for the operator
    /// @return this builder instance
    public JsonPipelineBuilder add(final RegistryKey<Map<String, Object>> key) {
      final var operator = getOperator(key);
      if (operator != null) operators.add(operator);
      return this;
    }

    /// Builds the pipeline as a function from byte[] to byte[].
    ///
    /// @return the composed pipeline function
    public Function<byte[], byte[]> build() {
      final var finalCombined = operators.stream().reduce(obj -> obj, (acc, op) -> t -> op.apply(acc.apply(t)));
      return bytes -> JsonMessageProcessor.inScopedCaches(() -> JsonMessageProcessor.processJson(bytes, finalCombined));
    }
  }

  /// A fluent builder for creating type-safe Avro processing pipelines.
  /// Fluent builder for creating type-safe Avro processing pipelines.
  public class AvroPipelineBuilder {

    private final String schemaKey;
    private final int offset;
    private final List<UnaryOperator<GenericRecord>> operators = new ArrayList<>();

    /// Constructs an AvroPipelineBuilder with the given schema key and default offset.
    ///
    /// @param schemaKey the schema key to use
    private AvroPipelineBuilder(final String schemaKey) {
      this(schemaKey, 5); // Default Confluent offset
    }

    /// Constructs an AvroPipelineBuilder with the given schema key and offset.
    ///
    /// @param schemaKey the schema key to use
    /// @param offset the offset in the byte array
    private AvroPipelineBuilder(final String schemaKey, final int offset) {
      this.schemaKey = schemaKey;
      this.offset = offset;
    }

    /// Adds an Avro operator to the pipeline.
    ///
    /// @param operator the operator to add
    /// @return this builder instance
    public AvroPipelineBuilder add(final UnaryOperator<GenericRecord> operator) {
      operators.add(operator);
      return this;
    }

    /// Adds a pre-registered Avro operator by key.
    ///
    /// @param key the registry key for the operator
    /// @return this builder instance
    public AvroPipelineBuilder add(final RegistryKey<GenericRecord> key) {
      final var operator = getOperator(key);
      if (operator != null) operators.add(operator);
      return this;
    }

    /// Builds the pipeline as a function from byte[] to byte[].
    ///
    /// @return the composed pipeline function
    public Function<byte[], byte[]> build() {
      final var schema = AvroMessageProcessor.getSchema(schemaKey);
      if (schema == null) throw new IllegalArgumentException("Schema not found: " + schemaKey);

      final var finalCombined = operators.stream().reduce(record -> record, (acc, op) -> t -> op.apply(acc.apply(t)));
      return bytes ->
        AvroMessageProcessor.inScopedCaches(() ->
          AvroMessageProcessor.processAvro(bytes, offset, schema, finalCombined)
        );
    }
  }

  /// A fluent builder for creating type-safe POJO processing pipelines.
  ///
  /// @param <T> The POJO type
  public class PojoPipelineBuilder<T> {

    private final Class<T> clazz;
    private final List<UnaryOperator<T>> operators = new ArrayList<>();

    /// Constructs a new PojoPipelineBuilder for the specified class.
    ///
    /// @param clazz the POJO class
    public PojoPipelineBuilder(final Class<T> clazz) {
      this.clazz = Objects.requireNonNull(clazz, "Class cannot be null");
    }

    /// Adds a POJO operator to the pipeline.
    ///
    /// @param operator the operator to add
    /// @return this builder instance
    public PojoPipelineBuilder<T> add(final UnaryOperator<T> operator) {
      operators.add(operator);
      return this;
    }

    /// Adds a pre-registered POJO operator by key.
    ///
    /// @param key the registry key for the operator
    /// @return this builder instance
    public PojoPipelineBuilder<T> add(final RegistryKey<T> key) {
      final var operator = getOperator(key);
      if (operator != null) operators.add(operator);
      return this;
    }

    /// Builds the pipeline as a function from byte[] to byte[].
    ///
    /// @return the composed pipeline function
    public Function<byte[], byte[]> build() {
      final var format = MessageFormat.pojo(clazz);
      final var finalCombined = operators.stream().reduce(obj -> obj, (acc, op) -> t -> op.apply(acc.apply(t)));

      return bytes -> {
        try {
          if (bytes == null || bytes.length == 0) return bytes;
          final var deserialized = format.deserialize(bytes);
          if (deserialized == null) return bytes;
          final var processed = finalCombined.apply(deserialized);
          return (processed == null) ? bytes : format.serialize(processed);
        } catch (final Exception e) {
          LOGGER.log(Level.WARNING, "Error in POJO pipeline execution", e);
          return defaultErrorValue;
        }
      };
    }
  }

  /// Registers a typed operator using a type-safe RegistryKey.
  ///
  /// @param <T> The type of data the operator processes
  /// @param key The type-safe key to register under
  /// @param operator The operator to register
  public <T> void registerOperator(final RegistryKey<T> key, final UnaryOperator<T> operator) {
    registry.put(key, new RegistryEntry<>(operator));
  }

  /// Registers all constants of an Enum that implements UnaryOperator<T>.
  ///
  /// Each constant's name is used as the key.
  ///
  /// @param <T> The type of data the operator processes
  /// @param <E> The Enum type that implements UnaryOperator<T>
  /// @param type The class representing the data type
  /// @param enumClass The Enum class to register
  public <T, E extends Enum<E> & UnaryOperator<T>> void registerEnum(final Class<T> type, final Class<E> enumClass) {
    Objects.requireNonNull(type, "Type cannot be null");
    Objects.requireNonNull(enumClass, "Enum class cannot be null");
    for (final var constant : enumClass.getEnumConstants()) {
      registerOperator(RegistryKey.of(constant.name(), type), constant);
    }
  }

  /// Retrieves a typed operator using a type-safe RegistryKey.
  ///
  /// @param <T> The type of data the operator processes
  /// @param key The type-safe key to retrieve
  /// @return The registered operator, or null if not found
  @SuppressWarnings("unchecked")
  public <T> UnaryOperator<T> getOperator(final RegistryKey<T> key) {
    final var entry = (RegistryEntry<UnaryOperator<T>>) registry.get(key);
    return entry != null ? entry.value : null;
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
  public MessageProcessorRegistry(final String sourceAppName, final MessageFormat<?> messageFormat) {
    this(sourceAppName, messageFormat, "{}".getBytes());
  }

  /// Creates a new registry with the specified message format and default error value.
  ///
  /// @param sourceAppName Application name to use as source identifier
  /// @param messageFormat Message format to use (JSON, AVRO, PROTOBUF)
  /// @param defaultErrorValue Value to return when processors throw exceptions
  public MessageProcessorRegistry(
    final String sourceAppName,
    final MessageFormat<?> messageFormat,
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
      registerOperator(JSON_ADD_SOURCE, JsonMessageProcessor.addFieldOperator("source", sourceAppName));
      registerOperator(JSON_ADD_TIMESTAMP, JsonMessageProcessor.addTimestampOperator("timestamp"));
      registerOperator(JSON_MARK_PROCESSED, JsonMessageProcessor.addFieldOperator("processed", "true"));
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
        registerOperator(
          RegistryKey.avro("addSource_" + schemaKey),
          AvroMessageProcessor.addFieldOperator("source", sourceAppName)
        );
        registerOperator(
          RegistryKey.avro("addTimestamp_" + schemaKey),
          AvroMessageProcessor.addTimestampOperator("timestamp")
        );
        registerOperator(
          RegistryKey.avro("markProcessed_" + schemaKey),
          AvroMessageProcessor.addFieldOperator("processed", "true")
        );
      }
    }
  }

  /// Registers a processor function with a type-safe key.
  ///
  /// @param key The type-safe key for the processor
  /// @param processor The function that processes byte arrays
  /// @throws NullPointerException if key or processor is null
  public void register(final RegistryKey<Function<byte[], byte[]>> key, final Function<byte[], byte[]> processor) {
    Objects.requireNonNull(key, "Processor key cannot be null");
    Objects.requireNonNull(processor, "Processor function cannot be null");

    final var entry = new RegistryEntry<>(withErrorHandling(processor, defaultErrorValue));
    registry.put(key, entry);
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

  /// Unregisters a processor.
  ///
  /// @param key The key of the processor to remove
  /// @return true if the processor was removed, false if it wasn't found
  public boolean unregister(final RegistryKey<?> key) {
    return registry.remove(key) != null;
  }

  /// Clears all processors from the registry.
  public void clear() {
    registry.clear();
  }

  /// Gets a processor by key. If no processor is found, returns the identity function.
  ///
  /// @param key The key of the processor to retrieve
  /// @return The processor function, or identity function if not found
  @SuppressWarnings("unchecked")
  public Function<byte[], byte[]> get(final RegistryKey<Function<byte[], byte[]>> key) {
    final var entry = (RegistryEntry<Function<byte[], byte[]>>) registry.get(key);
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
  /// final var emptyHandler = (Function<byte[], byte[]>) bytes -> "{\"empty\":true}".getBytes();
  /// final var normalProcessor = registry.get(RegistryKey.of("parseJson", Function.class));
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
  /// @return Unmodifiable map of all processor keys and functions
  public Map<RegistryKey<?>, Function<byte[], byte[]>> getAll() {
    return RegistryFunctions.createUnmodifiableView(registry, entry -> {
      final var regEntry = (RegistryEntry<?>) entry;
      if (regEntry.value instanceof Function) return regEntry::execute;
      return null;
    });
  }

  /// Gets metrics for a processor.
  ///
  /// @param key The processor key
  /// @return Map containing metrics or empty map if processor not found
  public Map<String, Object> getMetrics(final RegistryKey<?> key) {
    final var entry = registry.get(key);
    if (entry == null) return Map.of();
    return RegistryFunctions.createMetrics(entry.invocationCount, entry.errorCount, entry.totalProcessingTimeMs);
  }
}
