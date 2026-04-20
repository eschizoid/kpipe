
package org.kpipe.consumer.sink;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import org.kpipe.sink.MessageSink;

/// A sink that logs processed messages with Protobuf formatting.
///
/// @param <T> The type of the processed object
public record ProtobufConsoleSink<T>() implements MessageSink<T> {
  private static final Logger LOGGER = System.getLogger(ProtobufConsoleSink.class.getName());
  private static final JsonFormat.Printer PROTO_PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

  @Override
  public void accept(final T processedValue) {
    ConsoleSinkSupport.log(LOGGER, processedValue, this::formatValue);
  }

  private String formatValue(final T value) {
    if (value == null) return "null";
    if (value instanceof Message message) {
      try {
        return PROTO_PRINTER.print(message);
      } catch (final Exception e) {
        LOGGER.log(Level.ERROR, "Failed to format Protobuf message as JSON", e);
        return message.toString();
      }
    }
    if (value instanceof byte[] bytes) {
      if (bytes.length == 0) return "empty";
      return "[binary protobuf data]";
    }
    return String.valueOf(value);
  }
}
