package org.kpipe.consumer.sink;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.function.Function;

/// Shared logging logic for console sinks.
///
/// Each concrete sink delegates to [log] with its own value formatter.
final class ConsoleSinkSupport {

  private static final DslJson<Object> DSL_JSON = new DslJson<>();

  private ConsoleSinkSupport() {}

  /// Logs a processed value using the provided logger and formatter.
  ///
  /// @param <T> the value type
  /// @param logger the System.Logger for the calling sink
  /// @param processedValue the value to log
  /// @param formatter converts the value to a loggable string
  static <T> void log(final Logger logger, final T processedValue, final Function<T, String> formatter) {
    try {
      if (!logger.isLoggable(Level.INFO)) return;
      final var logData = LinkedHashMap.newLinkedHashMap(1);
      logData.put("processedMessage", formatter.apply(processedValue));

      try (final var out = new ByteArrayOutputStream()) {
        DSL_JSON.serialize(logData, out);
        logger.log(Level.INFO, out.toString(StandardCharsets.UTF_8));
      } catch (final IOException e) {
        logger.log(Level.WARNING, "Failed to serialize log data");
      }
    } catch (final Exception e) {
      logger.log(Level.ERROR, "Error in console sink while processing message", e);
    }
  }
}

