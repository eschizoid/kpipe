package org.kpipe.sink;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// A [MessageSink] that broadcasts records to multiple other sinks.
///
/// This allows a single processing pipeline to deliver results to multiple destinations
/// (e.g., a database and a logging console) simultaneously.
///
/// Failures in one sink do not prevent other sinks from receiving the record.
/// Errors are caught and logged using [System.Logger].
///
/// @param <K> the type of the record key
/// @param <V> the type of the processed value
public record CompositeMessageSink<K, V>(List<MessageSink<K, V>> sinks) implements MessageSink<K, V> {
  private static final Logger LOGGER = System.getLogger(CompositeMessageSink.class.getName());

  public CompositeMessageSink {
    sinks = List.copyOf(sinks);
  }

  @Override
  public void send(final ConsumerRecord<K, V> record, final V processedValue) {
    for (final var sink : sinks) {
      try {
        sink.send(record, processedValue);
      } catch (final Exception e) {
        LOGGER.log(Level.ERROR, "Sink " + sink.getClass().getSimpleName() + " failed to process record", e);
      }
    }
  }
}
