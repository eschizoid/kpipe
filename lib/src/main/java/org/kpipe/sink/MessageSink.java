package org.kpipe.sink;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Functional interface representing a destination for processed Kafka messages.
 *
 * @param <K> The type of message key
 * @param <V> The type of message value
 */
@FunctionalInterface
public interface MessageSink<K, V> {
  /**
   * Sends a processed message to the sink.
   *
   * @param record The original Kafka record
   * @param processedValue The result of processing the record value
   */
  void send(final ConsumerRecord<K, V> record, final V processedValue);
}
