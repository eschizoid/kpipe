package org.kpipe.sink;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A sink that logs processed Kafka messages with JSON formatting.
 *
 * <p>This implementation of {@link MessageSink} provides logging functionality for Kafka messages
 * and their processed values. It formats the message content as JSON for better readability and
 * debugging. The sink handles various message value types, with special treatment for byte arrays.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>JSON formatting of message metadata and content
 *   <li>Special handling for byte arrays (attempts UTF-8 decoding)
 *   <li>Automatic detection of JSON content in byte arrays
 *   <li>Fallback to Base64 encoding for binary data
 *   <li>Performance optimization by checking log level before processing
 *   <li>Robust error handling that logs exceptions without disrupting the main processing flow
 * </ul>
 *
 * @param <K> The type of message key
 * @param <V> The type of message value
 */
public class LoggingSink<K, V> implements MessageSink<K, V> {

  private static final System.Logger LOGGER = System.getLogger(LoggingSink.class.getName());
  private static final DslJson<Object> DSL_JSON = new DslJson<>();
  private final Level logLevel;

  /** Creates a LoggingSink with default INFO log level. */
  public LoggingSink() {
    this(Level.INFO);
  }

  /**
   * Creates a LoggingSink with specified log level.
   *
   * @param logLevel the level at which to log messages
   */
  public LoggingSink(final Level logLevel) {
    this.logLevel = logLevel;
  }

  /**
   * Processes a Kafka message by logging it with its metadata and processed value. This
   * implementation ensures that any internal exceptions do not affect the main processing flow or
   * metric collection.
   *
   * @param record The original Kafka consumer record
   * @param processedValue The value after processing
   */
  @Override
  public void accept(final ConsumerRecord<K, V> record, V processedValue) {
    try {
      // Skip if logging level doesn't require it
      if (!LOGGER.isLoggable(logLevel)) {
        return;
      }

      // Create log data structure
      final var logData = Map.of(
        "topic",
        record.topic(),
        "partition",
        record.partition(),
        "offset",
        record.offset(),
        "key",
        String.valueOf(record.key()),
        "processedMessage",
        formatValue(processedValue)
      );

      // Serialize and log
      try (final var out = new ByteArrayOutputStream()) {
        DSL_JSON.serialize(logData, out);
        LOGGER.log(logLevel, out.toString(StandardCharsets.UTF_8));
      } catch (final IOException e) {
        // Fallback logging if serialization fails
        LOGGER.log(
          logLevel,
          "Processed message (topic=%s, partition=%d, offset=%d)".formatted(
              record.topic(),
              record.partition(),
              record.offset()
            )
        );
      }
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error in LoggingSink while processing message", e);
    }
  }

  /**
   * Formats a value for logging with special handling for different types.
   *
   * @param value The value to format
   * @return A string representation of the value suitable for logging
   */
  private String formatValue(V value) {
    if (value == null) {
      return "null";
    }

    if (value instanceof byte[] bytes) {
      if (bytes.length == 0) {
        return "empty";
      }

      return formatByteArray(bytes);
    }

    return String.valueOf(value);
  }

  /**
   * Formats a byte array for logging, handling UTF-8 text and JSON content.
   *
   * @param bytes The byte array to format
   * @return A formatted string representation
   */
  private String formatByteArray(byte[] bytes) {
    try {
      final var strValue = new String(bytes, StandardCharsets.UTF_8);

      // Handle JSON formatting if possible
      if (isLikelyJson(strValue)) {
        try {
          final var json = DSL_JSON.deserialize(Object.class, new ByteArrayInputStream(bytes));
          final var out = new ByteArrayOutputStream();
          DSL_JSON.serialize(json, out);
          return out.toString(StandardCharsets.UTF_8);
        } catch (final Exception e) {
          LOGGER.log(Level.DEBUG, "Failed to parse/format JSON content, falling back to raw string", e);
        }
      }
      return strValue;
    } catch (final Exception e) {
      LOGGER.log(Level.DEBUG, "Failed to parse/format as JSON", e);
      return "Base64: %s".formatted(Base64.getEncoder().encodeToString(bytes));
    }
  }

  /**
   * Checks if a string value is likely to be JSON.
   *
   * @param value The string to check
   * @return true if the string appears to be JSON
   */
  private boolean isLikelyJson(String value) {
    return (
      (value.startsWith("{") || value.startsWith("[")) && (value.trim().endsWith("}") || value.trim().endsWith("]"))
    );
  }
}
