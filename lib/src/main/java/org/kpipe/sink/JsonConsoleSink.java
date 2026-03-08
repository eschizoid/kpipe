package org.kpipe.sink;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// A sink that logs processed Kafka messages with JSON formatting.
///
/// <p>This implementation of {@link MessageSink} provides logging functionality for Kafka messages
/// and their processed values. It formats the message content as JSON for better readability and
/// debugging. The sink handles various message value types, with special treatment for byte arrays.
///
/// <p>Features:
///
/// <ul>
///   <li>JSON formatting of message metadata and content
///   <li>Special handling for byte arrays (attempts UTF-8 decoding)
///   <li>Automatic detection of JSON-looking content in byte arrays
///   <li>For JSON objects, parse + reserialize into normalized JSON output
///   <li>For JSON arrays, parse + reserialize into normalized JSON output
///   <li>Fallback to raw UTF-8 string when JSON parsing fails
///   <li>Performance optimization by checking log level before processing
///   <li>Robust error handling that logs exceptions without disrupting the main processing flow
/// </ul>
///
/// @param <K> The type of message key
/// @param <V> The type of message value
public record JsonConsoleSink<K, V>() implements MessageSink<K, V> {
  private static final DslJson<Object> DSL_JSON = new DslJson<>();
  private static final Logger LOGGER = System.getLogger(JsonConsoleSink.class.getName());
  private static final Level LOG_LEVEL = Level.INFO;

  /// Logs a message with its key and value.
  ///
  /// @param record The original Kafka consumer record
  /// @param processedValue The value after processing
  @Override
  public void send(final ConsumerRecord<K, V> record, final V processedValue) {
    try {
      // Skip if the logging level doesn't require it
      if (!LOGGER.isLoggable(LOG_LEVEL)) return;
      final var logData = LinkedHashMap.newLinkedHashMap(5);
      logData.put("topic", record.topic());
      logData.put("partition", record.partition());
      logData.put("offset", record.offset());
      logData.put("key", String.valueOf(record.key()));
      logData.put("processedMessage", formatValue(processedValue));

      try (final var out = new ByteArrayOutputStream()) {
        DSL_JSON.serialize(logData, out);
        LOGGER.log(LOG_LEVEL, out.toString(StandardCharsets.UTF_8));
      } catch (final IOException e) {
        LOGGER.log(
          Level.WARNING,
          "Failed to processed message (topic=%s, partition=%d, offset=%d)".formatted(
              record.topic(),
              record.partition(),
              record.offset()
            )
        );
      }
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Error in ConsoleSink while processing message", e);
    }
  }

  /// Formats a value for logging with special handling for different types.
  ///
  /// <p>Byte array behavior:
  ///
  /// <ul>
  ///   <li>empty arrays -> `"empty"`
  ///   <li>JSON objects -> parsed and reserialized for stable JSON formatting
  ///   <li>JSON arrays -> parsed and reserialized for stable JSON formatting
  ///   <li>invalid JSON / non-JSON text -> returned as decoded UTF-8 text
  /// </ul>
  ///
  /// @param value The value to format
  /// @return A string representation of the value suitable for logging
  private String formatValue(final V value) {
    if (value == null) return "null";
    if (value instanceof byte[] bytes) {
      if (bytes.length == 0) return "empty";
      return formatByteArray(bytes);
    }
    return String.valueOf(value);
  }

  /// Attempts to produce a log-friendly representation of UTF-8 bytes.
  ///
  /// <p>This method intentionally does not throw; it falls back to a best-effort string so sink
  /// logging never breaks the consumer processing path.
  private String formatByteArray(final byte[] bytes) {
    try {
      final var strValue = new String(bytes, StandardCharsets.UTF_8);
      if (isLikelyJson(strValue)) {
        final var trimmed = strValue.trim();
        final var json = trimmed.startsWith("[")
          ? DSL_JSON.deserialize(List.class, new ByteArrayInputStream(bytes))
          : DSL_JSON.deserialize(Map.class, new ByteArrayInputStream(bytes));
        try (final var out = new ByteArrayOutputStream()) {
          DSL_JSON.serialize(json, out);
          return out.toString(StandardCharsets.UTF_8);
        }
      }
      return strValue;
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Failed to parse/format as JSON", e);
      return "";
    }
  }

  private boolean isLikelyJson(final String value) {
    final var trimmed = value.trim();
    return (
      (trimmed.startsWith("{") || trimmed.startsWith("[")) &&
      (trimmed.endsWith("}") || trimmed.endsWith("]"))
    );
  }
}
