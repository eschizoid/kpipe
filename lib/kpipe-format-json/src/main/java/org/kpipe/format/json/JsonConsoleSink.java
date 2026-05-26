package org.kpipe.format.json;

import com.alibaba.fastjson2.JSON;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import org.kpipe.sink.ConsoleSinkSupport;
import org.kpipe.sink.MessageSink;

/// A sink that logs processed messages with JSON formatting.
///
/// @param <T> The type of the processed object
public record JsonConsoleSink<T>() implements MessageSink<T> {
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
          // fastjson2's JSON.parse handles both object and array roots transparently — no
          // need to branch on the leading char.
          final var json = JSON.parse(bytes);
          return JSON.toJSONString(json);
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
