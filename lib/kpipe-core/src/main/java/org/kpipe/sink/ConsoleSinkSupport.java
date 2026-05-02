package org.kpipe.sink;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.function.Function;

/// Shared logging logic for console sinks across the format modules.
///
/// Each concrete sink delegates to [#log] with its own value formatter.
public final class ConsoleSinkSupport {

  private ConsoleSinkSupport() {}

  /// Logs a processed value using the provided logger and formatter.
  ///
  /// Output is a compact JSON object: `{"processedMessage": "<formatted>"}`. JSON escaping
  /// is hand-rolled to keep `kpipe-consumer` free of any JSON library dependency — sinks that
  /// need richer formatting can do it inside their formatter callback.
  ///
  /// @param <T> the value type
  /// @param logger the System.Logger for the calling sink
  /// @param processedValue the value to log
  /// @param formatter converts the value to a loggable string
  public static <T> void log(final Logger logger, final T processedValue, final Function<T, String> formatter) {
    try {
      if (!logger.isLoggable(Level.INFO)) return;
      final var formatted = formatter.apply(processedValue);
      logger.log(Level.INFO, "{\"processedMessage\":\"" + jsonEscape(formatted) + "\"}");
    } catch (final Exception e) {
      logger.log(Level.ERROR, "Error in console sink while processing message", e);
    }
  }

  /// Minimal JSON string-content escaping for the log payload.
  private static String jsonEscape(final String input) {
    if (input == null) return "";
    final var out = new StringBuilder(input.length() + 16);
    for (int i = 0; i < input.length(); i++) {
      final char c = input.charAt(i);
      switch (c) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        case '\b' -> out.append("\\b");
        case '\f' -> out.append("\\f");
        default -> {
          if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
          else out.append(c);
        }
      }
    }
    return out.toString();
  }
}
