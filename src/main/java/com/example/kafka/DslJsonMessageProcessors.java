package com.example.kafka;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;

/**
 * Provides utility functions for processing JSON messages using DSL-JSON library. This class
 * contains common message processors for use with the MessageProcessorRegistry to transform and
 * enhance JSON data in byte array format.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create a pipeline using these processors
 * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
 *     "parseJson",
 *     MessageProcessorRegistry.get("addTimestamp")
 * );
 *
 * // Process a message
 * byte[] result = pipeline.apply("{\"data\":\"value\"}".getBytes());
 * }</pre>
 */
public class DslJsonMessageProcessors {

  private static final Logger LOGGER = System.getLogger(DslJsonMessageProcessors.class.getName());
  private static final DslJson<Map<String, Object>> DSL_JSON = new DslJson<>();
  private static final byte[] EMPTY_JSON = "{}".getBytes(StandardCharsets.UTF_8);

  /**
   * Creates a processor function that parses raw JSON bytes. This can be used to validate JSON or
   * as the first step in a processing pipeline.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Register the processor
   * MessageProcessorRegistry.register("validateJson", DslJsonMessageProcessors.parseJson());
   *
   * // Use it to validate JSON
   * byte[] rawMessage = "{\"key\":\"value\"}".getBytes();
   * byte[] validJson = MessageProcessorRegistry.get("validateJson").apply(rawMessage);
   * }</pre>
   *
   * @return Function that parses and reserializes JSON data without modifying content
   */
  public static Function<byte[], byte[]> parseJson() {
    return jsonBytes -> processJson(jsonBytes, obj -> obj);
  }

  /**
   * Creates a processor function that adds a field with specified key and value to JSON data.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a processor that adds a source field
   * Function<byte[], byte[]> addSourceProcessor =
   *     DslJsonMessageProcessors.addField("source", "payment-service");
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("addSource", addSourceProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseJson", "addSource", "addTimestamp"
   * );
   *
   * // Apply to a message
   * byte[] inputJson = "{\"amount\":100}".getBytes();
   * byte[] outputJson = pipeline.apply(inputJson);
   * // Result: {"amount":100,"source":"payment-service","timestamp":"1634567890123"}
   * }</pre>
   *
   * @param key The field name to add
   * @param value The value to associate with the field
   * @return Function that adds a field to JSON data
   */
  public static Function<byte[], byte[]> addField(final String key, final String value) {
    return jsonBytes ->
      processJson(
        jsonBytes,
        obj -> {
          obj.put(key, value);
          return obj;
        }
      );
  }

  /**
   * Creates a processor that adds a timestamp field to JSON data.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a processor that adds a timestamp
   * Function<byte[], byte[]> addTimestampProcessor =
   *     DslJsonMessageProcessors.addTimestamp("processedAt");
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("addTimestamp", addTimestampProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseJson", "addTimestamp"
   * );
   *
   * // Apply to a message
   * byte[] inputJson = "{\"data\":\"value\"}".getBytes();
   * byte[] outputJson = pipeline.apply(inputJson);
   * // Result: {"data":"value","processedAt":1634567890123}
   * }</pre>
   *
   * @param fieldName The name of the timestamp field
   * @return Function that adds current timestamp to JSON
   */
  public static Function<byte[], byte[]> addTimestamp(final String fieldName) {
    return jsonBytes ->
      processJson(
        jsonBytes,
        obj -> {
          obj.put(fieldName, System.currentTimeMillis());
          return obj;
        }
      );
  }

  /**
   * Creates a processor that removes specified fields from JSON data.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a processor that removes sensitive fields
   * Function<byte[], byte[]> sanitizeProcessor =
   *     DslJsonMessageProcessors.removeFields("password", "creditCard", "ssn");
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("sanitizeData", sanitizeProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseJson", "sanitizeData", "addSource"
   * );
   *
   * // Apply to a message
   * byte[] inputJson = "{\"username\":\"user1\",\"password\":\"secret\",\"email\":\"user@example.com\"}".getBytes();
   * byte[] outputJson = pipeline.apply(inputJson);
   * // Result: {"username":"user1","email":"user@example.com"}
   * }</pre>
   *
   * @param fields Field names to remove
   * @return Function that removes fields from JSON
   */
  public static Function<byte[], byte[]> removeFields(final String... fields) {
    return jsonBytes ->
      processJson(
        jsonBytes,
        obj -> {
          for (final String field : fields) {
            obj.remove(field);
          }
          return obj;
        }
      );
  }

  /**
   * Creates a processor that transforms a field using the provided function.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a processor that transforms the message text to uppercase
   * Function<byte[], byte[]> uppercaseProcessor =
   *     DslJsonMessageProcessors.transformField("message", value -> {
   *         if (value instanceof String) {
   *             return ((String) value).toUpperCase();
   *         }
   *         return value;
   *     });
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("uppercaseMessage", uppercaseProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseJson", "uppercaseMessage"
   * );
   *
   * // Apply to a message
   * byte[] inputJson = "{\"message\":\"hello world\",\"id\":123}".getBytes();
   * byte[] outputJson = pipeline.apply(inputJson);
   * // Result: {"message":"HELLO WORLD","id":123}
   * }</pre>
   *
   * @param field Field to transform
   * @param transformer Function to apply to the field value
   * @return Function that transforms the specified field
   */
  public static Function<byte[], byte[]> transformField(
    final String field,
    final Function<Object, Object> transformer
  ) {
    return jsonBytes ->
      processJson(
        jsonBytes,
        obj -> {
          if (obj.containsKey(field)) {
            obj.put(field, transformer.apply(obj.get(field)));
          }
          return obj;
        }
      );
  }

  /**
   * Creates a processor that merges additional JSON data.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create a map of metadata to merge
   * Map<String, Object> metadata = new HashMap<>();
   * metadata.put("version", "1.0");
   * metadata.put("environment", "production");
   * metadata.put("service", "payment-processor");
   *
   * // Create a processor that adds standard metadata
   * Function<byte[], byte[]> addMetadataProcessor =
   *     DslJsonMessageProcessors.mergeWith(metadata);
   *
   * // Register it in the registry
   * MessageProcessorRegistry.register("addMetadata", addMetadataProcessor);
   *
   * // Use it in a pipeline
   * Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
   *     "parseJson", "addMetadata", "addTimestamp"
   * );
   *
   * // Apply to a message
   * byte[] inputJson = "{\"orderId\":\"ABC123\",\"amount\":99.95}".getBytes();
   * byte[] outputJson = pipeline.apply(inputJson);
   * // Result: {"orderId":"ABC123","amount":99.95,"version":"1.0","environment":"production","service":"payment-processor"}
   * }</pre>
   *
   * @param additionalJson JSON to merge with processed messages
   * @return Function that merges JSON data
   */
  public static Function<byte[], byte[]> mergeWith(final Map<String, Object> additionalJson) {
    return jsonBytes ->
      processJson(
        jsonBytes,
        obj -> {
          obj.putAll(additionalJson);
          return obj;
        }
      );
  }

  /**
   * Helper method that applies a processing function to parsed JSON data.
   *
   * @param jsonBytes The raw JSON data as byte array
   * @param processor Function to transform the parsed JSON object
   * @return Serialized JSON bytes after processing
   */
  private static byte[] processJson(
    final byte[] jsonBytes,
    final Function<Map<String, Object>, Map<String, Object>> processor
  ) {
    if (jsonBytes == null || jsonBytes.length == 0) {
      return EMPTY_JSON;
    }
    try (final var input = new ByteArrayInputStream(jsonBytes); final var output = new ByteArrayOutputStream()) {
      final var parsed = DSL_JSON.deserialize(Map.class, input);
      if (parsed == null) {
        return EMPTY_JSON;
      }
      final var processed = processor.apply(parsed);
      if (processed == null) {
        return EMPTY_JSON;
      }
      DSL_JSON.serialize(processed, output);
      return output.toByteArray();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error processing JSON", e);
      return EMPTY_JSON;
    }
  }
}
