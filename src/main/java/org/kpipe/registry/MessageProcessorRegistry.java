package org.kpipe.registry;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;
import org.kpipe.processor.JsonMessageProcessor;

/**
 * A registry for managing and composing byte array message processors used in Kafka message
 * processing. This registry stores named {@link Function} instances that transform byte[] messages,
 * typically containing JSON data.
 *
 * <p>The registry provides methods to:
 *
 * <ul>
 *   <li>Register processors with unique names
 *   <li>Retrieve processors by name
 *   <li>Create processing pipelines by chaining multiple processors
 *   <li>Add error handling to processors
 *   <li>Apply conditional processing logic
 * </ul>
 *
 * <p>The registry comes pre-configured with several common processors:
 *
 * <ul>
 *   <li>{@code parseJson}: Validates and normalizes JSON data
 *   <li>{@code addSource}: Adds a source identifier to messages
 *   <li>{@code markProcessed}: Marks messages as processed
 *   <li>{@code addTimestamp}: Adds processing timestamp to messages
 * </ul>
 */
public class MessageProcessorRegistry {

  private static final Logger LOGGER = System.getLogger(MessageProcessorRegistry.class.getName());
  private final ConcurrentHashMap<String, ProcessorEntry> registry = new ConcurrentHashMap<>();
  private final byte[] defaultErrorValue;
  private final String sourceAppName;

  /** Class to hold a processor and its metrics */
  private static class ProcessorEntry {

    final Function<byte[], byte[]> processor;
    long invocationCount = 0;
    long errorCount = 0;
    long totalProcessingTimeNs = 0;

    ProcessorEntry(final Function<byte[], byte[]> processor) {
      this.processor = processor;
    }

    public byte[] execute(final byte[] input) {
      final Supplier<Long> counterIncrement = () -> invocationCount++;
      final Supplier<Long> errorIncrement = () -> errorCount++;
      final Consumer<Duration> timeAccumulator = duration -> totalProcessingTimeNs += duration.toNanos();

      final var timedExecution = RegistryFunctions.<byte[], byte[]>timedExecution(
        counterIncrement,
        errorIncrement,
        timeAccumulator
      );

      return timedExecution.apply(input, processor);
    }
  }

  /**
   * Constructs a registry with configured application name.
   *
   * @param sourceAppName The application name to use in processors
   */
  public MessageProcessorRegistry(final String sourceAppName) {
    this(sourceAppName, "{}".getBytes());
  }

  /**
   * Constructs a fully configured registry.
   *
   * @param sourceAppName The application name to use in processors
   * @param defaultErrorValue The default value to return on errors
   */
  public MessageProcessorRegistry(final String sourceAppName, final byte[] defaultErrorValue) {
    this.sourceAppName = Objects.requireNonNull(sourceAppName, "Source app name cannot be null");
    this.defaultErrorValue = Objects.requireNonNull(defaultErrorValue, "Default error value cannot be null");

    // Register default processors
    register("parseJson", JsonMessageProcessor.parseJson());
    register("addSource", JsonMessageProcessor.addField("source", sourceAppName));
    register("markProcessed", JsonMessageProcessor.addField("processed", "true"));
    register("addTimestamp", JsonMessageProcessor.addTimestamp("timestamp"));
  }

  /**
   * Registers a new message processor function with the specified name.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Register a processor that encrypts message content
   * registry.register("encrypt", message -> {
   *     byte[] encrypted = encryptionService.encrypt(message);
   *     return encrypted;
   * });
   * }</pre>
   *
   * @param name The unique name for the processor
   * @param processor The function that processes byte arrays
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
   * Unregisters a processor by name.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Remove a processor that's no longer needed
   * boolean wasRemoved = registry.unregister("oldProcessor");
   * if (wasRemoved) {
   *     System.out.println("Processor was successfully removed");
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
   * Removes all registered processors.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Clear the registry when shutting down
   * registry.clear();
   * }</pre>
   */
  public void clear() {
    registry.clear();
  }

  /**
   * Retrieves a processor by name from the registry.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Get the JSON parsing processor
   * Function<byte[], byte[]> jsonParser = registry.get("parseJson");
   *
   * // Process a raw message
   * byte[] rawMessage = "{\"key\":\"value\"}".getBytes();
   * byte[] parsedMessage = jsonParser.apply(rawMessage);
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
   * Creates a pipeline by chaining multiple named processors.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a pipeline that parses JSON, adds timestamps and marks as processed
   * Function<byte[], byte[]> pipeline = registry.pipeline(
   *     "parseJson", "addTimestamp", "markProcessed"
   * );
   *
   * // Process a message through the pipeline
   * byte[] result = pipeline.apply(rawMessage);
   * }</pre>
   *
   * @param processorNames Names of processors to chain in sequence
   * @return A function representing the processing pipeline
   */
  public Function<byte[], byte[]> pipeline(final String... processorNames) {
    return message -> {
      var result = message;
      for (final String name : processorNames) {
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
   * // Create a custom list of processors
   * List<Function<byte[], byte[]>> processors = List.of(
   *     registry.get("parseJson"),
   *     message -> DslJsonMessageProcessors.addField("environment", "production").apply(message),
   *     registry.get("addTimestamp")
   * );
   *
   * // Build a pipeline from the list
   * Function<byte[], byte[]> customPipeline = registry.pipeline(processors);
   * byte[] processed = customPipeline.apply(rawMessage);
   * }</pre>
   *
   * @param processors List of processor functions to chain
   * @return A function representing the processing pipeline
   */
  public Function<byte[], byte[]> pipeline(final List<Function<byte[], byte[]>> processors) {
    Objects.requireNonNull(processors, "Processor list cannot be null");
    return message -> {
      byte[] result = message;
      for (final Function<byte[], byte[]> processor : processors) {
        result = processor.apply(result);
      }
      return result;
    };
  }

  /**
   * Wraps a processor with error handling logic.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Get a processor that might throw exceptions
   * Function<byte[], byte[]> riskyProcessor = registry.get("complexTransformation");
   *
   * // Wrap it with error handling, returning empty JSON on failure
   * Function<byte[], byte[]> safeProcessor = MessageProcessorRegistry.withErrorHandling(
   *     riskyProcessor,
   *     "{}".getBytes()
   * );
   *
   * // Safely process messages
   * byte[] result = safeProcessor.apply(message); // Won't throw exceptions
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
    return message -> {
      try {
        return processor.apply(message);
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error processing message", e);
        return defaultValue != null ? defaultValue : "{}".getBytes();
      }
    };
  }

  /**
   * Creates a conditional processor that routes messages based on a predicate.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a predicate that checks if the message is valid JSON
   * Predicate<byte[]> isValidJson = message -> {
   *     try {
   *         return new String(message).contains("\"valid\":true");
   *     } catch (Exception e) {
   *         return false;
   *     }
   * };
   *
   * // Create conditional processor
   * Function<byte[], byte[]> conditionalProcessor = MessageProcessorRegistry.when(
   *     isValidJson,
   *     registry.get("processValidMessage"),
   *     registry.get("handleInvalidMessage")
   * );
   *
   * // Process messages conditionally
   * byte[] result = conditionalProcessor.apply(message);
   * }</pre>
   *
   * @param condition Predicate to evaluate messages
   * @param ifTrue Processor to use when condition is true
   * @param ifFalse Processor to use when condition is false
   * @return A function that conditionally processes messages
   */
  public static Function<byte[], byte[]> when(
    final Predicate<byte[]> condition,
    final Function<byte[], byte[]> ifTrue,
    final Function<byte[], byte[]> ifFalse
  ) {
    Objects.requireNonNull(condition, "Condition predicate cannot be null");
    Objects.requireNonNull(ifTrue, "True-branch processor cannot be null");
    Objects.requireNonNull(ifFalse, "False-branch processor cannot be null");

    return message -> condition.test(message) ? ifTrue.apply(message) : ifFalse.apply(message);
  }

  /**
   * Returns an unmodifiable map of all registered processors.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Get all registered processors
   * Map<String, Function<byte[], byte[]>> allProcessors = registry.getAll();
   *
   * // List all available processor names
   * allProcessors.keySet().forEach(name ->
   *     System.out.println("Available processor: " + name)
   * );
   *
   * // Count registered processors
   * int processorCount = allProcessors.size();
   * }</pre>
   *
   * @return Unmodifiable map of all processor names and functions
   */
  public Map<String, Function<byte[], byte[]>> getAll() {
    return RegistryFunctions.createUnmodifiableView(registry, entry -> ((ProcessorEntry) entry)::execute);
  }

  /**
   * Gets metrics for a specific processor.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Get metrics for the JSON parser
   * Map<String, Object> metrics = registry.getMetrics("parseJson");
   * System.out.println("Invocation count: " + metrics.get("invocationCount"));
   * System.out.println("Error rate: " +
   *     (Long)metrics.get("errorCount") / (Long)metrics.get("invocationCount"));
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

    return RegistryFunctions.createMetrics(entry.invocationCount, entry.errorCount, entry.totalProcessingTimeNs);
  }

  /**
   * Gets the source application name configured for this registry.
   *
   * @return The source application name
   */
  public String getSourceAppName() {
    return sourceAppName;
  }
}
