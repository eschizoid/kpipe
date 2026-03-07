package org.kpipe.processor;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/// Provides utility functions for processing JSON messages using DSL-JSON library. This class
/// contains common message processors for use with the MessageProcessorRegistry to transform and
/// enhance JSON data in byte array format.
///
/// Example usage:
///
/// ```java
/// // Create a pipeline using these processors
/// Function<byte[], byte[]> pipeline = MessageProcessorRegistry.jsonPipeline(
///     "addTimestamp",
///     "sanitizeData"
/// );
///
/// // Process a message
/// byte[] result = pipeline.apply("{\"data\":\"value\"}".getBytes());
/// ```
public class JsonMessageProcessor {

  private JsonMessageProcessor() {}

  private static final Logger LOGGER = System.getLogger(JsonMessageProcessor.class.getName());
  private static final DslJson<Map<String, Object>> DSL_JSON = new DslJson<>();
  private static final byte[] EMPTY_JSON = "{}".getBytes(StandardCharsets.UTF_8);

  /// Creates an operator that adds a field with specified key and value to a JSON map.
  ///
  /// ```java
  /// final var operator = JsonMessageProcessor.addFieldOperator("source", "my-app");
  /// ```
  ///
  /// @param key The field name to add
  /// @param value The value to associate with the field
  /// @return UnaryOperator that adds a field to a JSON map
  public static UnaryOperator<Map<String, Object>> addFieldOperator(final String key, final String value) {
    return obj -> {
      obj.put(key, value);
      return obj;
    };
  }

  /// Creates an operator that adds a timestamp field to a JSON map.
  ///
  /// ```java
  /// final var operator = JsonMessageProcessor.addTimestampOperator("processed_at");
  /// ```
  ///
  /// @param fieldName The name of the timestamp field
  /// @return UnaryOperator that adds current timestamp to a JSON map
  public static UnaryOperator<Map<String, Object>> addTimestampOperator(final String fieldName) {
    return obj -> {
      obj.put(fieldName, System.currentTimeMillis());
      return obj;
    };
  }

  /// Creates an operator that removes specified fields from a JSON map.
  ///
  /// ```java
  /// final var operator = JsonMessageProcessor.removeFieldsOperator("password", "ssn");
  /// ```
  ///
  /// @param fields Field names to remove
  /// @return UnaryOperator that removes fields from a JSON map
  public static UnaryOperator<Map<String, Object>> removeFieldsOperator(final String... fields) {
    return obj -> {
      for (final String field : fields) {
        obj.remove(field);
      }
      return obj;
    };
  }

  /// Creates an operator that transforms a field using the provided function.
  ///
  /// ```java
  /// final var operator = JsonMessageProcessor.transformFieldOperator(
  ///     "email",
  ///     val -> val.toString().toLowerCase()
  /// );
  /// ```
  ///
  /// @param field Field to transform
  /// @param transformer Function to apply to the field value
  /// @return UnaryOperator that transforms the specified field in a JSON map
  public static UnaryOperator<Map<String, Object>> transformFieldOperator(
    final String field,
    final Function<Object, Object> transformer
  ) {
    return obj -> {
      if (obj.containsKey(field)) {
        obj.put(field, transformer.apply(obj.get(field)));
      }
      return obj;
    };
  }

  /// Creates an operator that merges additional JSON data into a JSON map.
  ///
  /// ```java
  /// final var metadata = Map.of("version", "1.0", "env", "prod");
  /// final var operator = JsonMessageProcessor.mergeWithOperator(metadata);
  /// ```
  ///
  /// @param additionalJson JSON to merge
  /// @return UnaryOperator that merges JSON data into a JSON map
  public static UnaryOperator<Map<String, Object>> mergeWithOperator(final Map<String, Object> additionalJson) {
    return obj -> {
      obj.putAll(additionalJson);
      return obj;
    };
  }

  /// Applies a processing function to parsed JSON data.
  ///
  /// ```java
  /// byte[] result = JsonMessageProcessor.processJson(
  ///     jsonBytes,
  ///     map -> {
  ///         map.put("status", "processed");
  ///         return map;
  ///     }
  /// );
  /// ```
  ///
  /// @param jsonBytes The raw JSON data as a byte array
  /// @param processor Function to transform the parsed JSON object
  /// @return Serialized JSON bytes after processing
  public static byte[] processJson(
    final byte[] jsonBytes,
    final Function<Map<String, Object>, Map<String, Object>> processor
  ) {
    if (jsonBytes == null || jsonBytes.length == 0) return EMPTY_JSON;
    try (final var input = new ByteArrayInputStream(jsonBytes); final var output = new ByteArrayOutputStream()) {
      final var parsed = DSL_JSON.deserialize(Map.class, input);
      if (parsed == null) return EMPTY_JSON;
      @SuppressWarnings("unchecked")
      final var processed = processor.apply(parsed);
      if (processed == null) return EMPTY_JSON;
      DSL_JSON.serialize(processed, output);
      return output.toByteArray();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error processing JSON", e);
      return EMPTY_JSON;
    }
  }
}
