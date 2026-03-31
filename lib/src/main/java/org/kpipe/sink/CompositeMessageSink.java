package org.kpipe.sink;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;

/// A [MessageSink] that broadcasts processed objects to multiple other sinks.
///
/// @param <T> the type of the processed object
/// @param sinks the list of sinks to which objects will be broadcast
public record CompositeMessageSink<T>(List<MessageSink<T>> sinks) implements MessageSink<T> {
  private static final Logger LOGGER = System.getLogger(CompositeMessageSink.class.getName());

  /// Constructs a CompositeMessageSink with the specified list of sinks.
  ///
  /// @param sinks the list of sinks to which objects will be broadcast
  public CompositeMessageSink {
    sinks = List.copyOf(sinks);
  }

  @Override
  public void accept(final T processedValue) {
    for (final var sink : sinks) {
      try {
        sink.accept(processedValue);
      } catch (final Exception e) {
        LOGGER.log(Level.ERROR, "Sink " + sink.getClass().getSimpleName() + " failed to process value", e);
      }
    }
  }
}
