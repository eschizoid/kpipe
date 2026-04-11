package org.kpipe.consumer.sink;

import org.kpipe.sink.MessageSink;
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

/// A sink that logs processed messages with JSON formatting.
///
/// @param <T> The type of the processed object
public record JsonConsoleSink<T>() implements MessageSink<T> {
  private static final DslJson<Object> DSL_JSON = new DslJson<>();
  private static final Logger LOGGER = System.getLogger(JsonConsoleSink.class.getName());
  private static final Level LOG_LEVEL = Level.INFO;

  /// Logs a processed value.
  ///
  /// @param processedValue The value after processing
  @Override
  public void accept(final T processedValue) {
    try {
      // Skip if the logging level doesn't require it
      if (!LOGGER.isLoggable(LOG_LEVEL)) return;
      final var logData = LinkedHashMap.newLinkedHashMap(1);
      logData.put("processedMessage", formatValue(processedValue));

      try (final var out = new ByteArrayOutputStream()) {
        DSL_JSON.serialize(logData, out);
        LOGGER.log(LOG_LEVEL, out.toString(StandardCharsets.UTF_8));
      } catch (final IOException e) {
        LOGGER.log(Level.WARNING, "Failed to serialize log data");
      }
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Error in ConsoleSink while processing message", e);
    }
  }

  /// Formats a value for logging with special handling for different types.
  private String formatValue(final T value) {
    if (value == null) return "null";
    if (value instanceof byte[] bytes) {
      if (bytes.length == 0) return "empty";
      return formatByteArray(bytes);
    }
    return String.valueOf(value);
  }

  /// Attempts to produce a log-friendly representation of UTF-8 bytes.
  ///
  /// This method intentionally does not throw; it falls back to a best-effort string so sink
  /// logging never breaks the consumer processing path.
  private String formatByteArray(final byte[] bytes) {
    try {
      final var strValue = new String(bytes, StandardCharsets.UTF_8);
      if (isLikelyJson(strValue)) {
        try {
          final var trimmed = strValue.trim();
          final var json = trimmed.startsWith("[")
            ? DSL_JSON.deserialize(List.class, new ByteArrayInputStream(bytes))
            : DSL_JSON.deserialize(Map.class, new ByteArrayInputStream(bytes));
          try (final var out = new ByteArrayOutputStream()) {
            DSL_JSON.serialize(json, out);
            return out.toString(StandardCharsets.UTF_8);
          }
        } catch (final Exception e) {
          LOGGER.log(Level.ERROR, "Failed to parse/format as JSON, falling back to raw string", e);
          return strValue;
        }
      }
      return strValue;
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Failed to decode byte[] as UTF-8", e);
      return "";
    }
  }

  private boolean isLikelyJson(final String value) {
    final var trimmed = value.trim();
    return ((trimmed.startsWith("{") || trimmed.startsWith("[")) && (trimmed.endsWith("}") || trimmed.endsWith("]")));
  }
}
