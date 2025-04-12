package org.kpipe.sink;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Interface for handling processed Kafka messages.
 *
 * @param <K> The type of message key
 * @param <V> The type of message value
 */
public interface MessageSink<K, V> {
  /**
   * Handles a processed message from a Kafka consumer.
   *
   * @param record The original Kafka record
   * @param processedValue The result of processing the record value
   */
  void accept(final ConsumerRecord<K, V> record, V processedValue);
}
