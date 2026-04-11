package org.kpipe.processor;

import java.io.ByteArrayOutputStream;
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
/// // Create an optimized pipeline using these processors
/// final var pipeline = registry.pipeline(MessageFormat.JSON)
///     .add(RegistryKey.json("addTimestamp"))
///     .add(RegistryKey.json("sanitizeData"))
///     .build();
///
/// // Process a message
/// byte[] result = pipeline.apply("{\"data\":\"value\"}".getBytes());
/// ```
public class JsonMessageProcessor {

  private JsonMessageProcessor() {}

  private static final ScopedValue<ByteArrayOutputStream> OUTPUT_STREAM_CACHE = ScopedValue.newInstance();

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
      if (obj.containsKey(field)) obj.put(field, transformer.apply(obj.get(field)));
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

  /// Executes an operation within scoped caches for JSON processing.
  ///
  /// @param <T> The result type of the operation
  /// @param operation The operation to execute within the scoped caches
  /// @return The result of the operation
  public static <T> T inScopedCaches(final ScopedValue.CallableOp<T, Exception> operation) {
    try {
      return ScopedValue.where(OUTPUT_STREAM_CACHE, new ByteArrayOutputStream(8192)).call(operation);
    } catch (final Exception e) {
      throw new RuntimeException("Error executing in JSON scoped caches", e);
    }
  }
}
