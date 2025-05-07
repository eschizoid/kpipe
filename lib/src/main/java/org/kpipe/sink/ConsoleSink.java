package org.kpipe.sink;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
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
public class ConsoleSink<K, V> implements MessageSink<K, V> {

  private static final DslJson<Object> DSL_JSON = new DslJson<>();
  private final Level logLevel;
  private final Logger logger;

  /**
   * Creates a ConsoleSink with the specified log level.
   *
   * @param logger The logger to use for logging messages
   * @param logLevel The log level to use for logging messages
   */
  public ConsoleSink(final System.Logger logger, final Level logLevel) {
    this.logLevel = logLevel;
    this.logger = logger;
  }

  /**
   * Logs a message with its key and value.
   *
   * @param record The original Kafka consumer record
   * @param processedValue The value after processing
   */
  @Override
  public void send(final ConsumerRecord<K, V> record, final V processedValue) {
    try {
      // Skip if the logging level doesn't require it
      if (!logger.isLoggable(logLevel)) {
        return;
      }

      // Create log data structure
      final var logData = LinkedHashMap.newLinkedHashMap(5);
      logData.put("topic", record.topic());
      logData.put("partition", record.partition());
      logData.put("offset", record.offset());
      logData.put("key", String.valueOf(record.key()));
      logData.put("processedMessage", formatValue(processedValue));

      try (final var out = new ByteArrayOutputStream()) {
        DSL_JSON.serialize(logData, out);
        logger.log(logLevel, out.toString(StandardCharsets.UTF_8));
      } catch (final IOException e) {
        logger.log(
          Level.WARNING,
          "Failed to processed message (topic=%s, partition=%d, offset=%d)".formatted(
              record.topic(),
              record.partition(),
              record.offset()
            )
        );
      }
    } catch (final Exception e) {
      logger.log(Level.ERROR, "Error in ConsoleSink while processing message", e);
    }
  }

  /**
   * Formats a value for logging with special handling for different types.
   *
   * @param value The value to format
   * @return A string representation of the value suitable for logging
   */
  private String formatValue(final V value) {
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

  private String formatByteArray(final byte[] bytes) {
    try {
      final var strValue = new String(bytes, StandardCharsets.UTF_8);

      if (isLikelyJson(strValue)) {
        try {
          final var json = DSL_JSON.deserialize(Object.class, new ByteArrayInputStream(bytes));
          final var out = new ByteArrayOutputStream();
          DSL_JSON.serialize(json, out);
          return out.toString(StandardCharsets.UTF_8);
        } catch (final Exception e) {
          logger.log(Level.DEBUG, "Failed to parse/format JSON content, falling back to raw string", e);
        }
      }
      return strValue;
    } catch (final Exception e) {
      logger.log(Level.DEBUG, "Failed to parse/format as JSON", e);
      return "";
    }
  }

  private boolean isLikelyJson(String value) {
    return (
      (value.startsWith("{") || value.startsWith("[")) && (value.trim().endsWith("}") || value.trim().endsWith("]"))
    );
  }
}
