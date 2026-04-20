package org.kpipe.consumer.sink;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.kpipe.sink.MessageSink;

/// A sink that logs processed messages with JSON formatting.
///
/// @param <T> The type of the processed object
public record JsonConsoleSink<T>() implements MessageSink<T> {
  private static final DslJson<Object> DSL_JSON = new DslJson<>();
  private static final Logger LOGGER = System.getLogger(JsonConsoleSink.class.getName());

  @Override
  public void accept(final T processedValue) {
    ConsoleSinkSupport.log(LOGGER, processedValue, this::formatValue);
  }

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
