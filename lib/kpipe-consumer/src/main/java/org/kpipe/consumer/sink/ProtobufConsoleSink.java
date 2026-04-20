package org.kpipe.consumer.sink;

import com.dslplatform.json.DslJson;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import org.kpipe.sink.MessageSink;

/// A sink that logs processed messages with Protobuf formatting.
///
/// @param <T> The type of message to log
public record ProtobufConsoleSink<T>() implements MessageSink<T> {
  private static final DslJson<Object> DSL_JSON = new DslJson<>();
  private static final Logger LOGGER = System.getLogger(ProtobufConsoleSink.class.getName());
  private static final Level LOG_LEVEL = Level.INFO;
  private static final JsonFormat.Printer PROTO_PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

  @Override
  public void accept(final T processedValue) {
    try {
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
      LOGGER.log(Level.ERROR, "Error in ProtobufConsoleSink while processing message", e);
    }
  }

  private String formatValue(final T value) {
    if (value == null) return "null";
    if (value instanceof Message message) {
      try {
        return PROTO_PRINTER.print(message);
      } catch (final Exception e) {
        LOGGER.log(Level.ERROR, "Failed to format Protobuf message as JSON", e);
        return value.toString();
      }
    }
    return String.valueOf(value);
  }
}

