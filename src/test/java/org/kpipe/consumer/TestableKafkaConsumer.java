package org.kpipe.consumer;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TestableKafkaConsumer<K, V> extends FunctionalKafkaConsumer<K, V> {

  private final KafkaConsumer<K, V> mockConsumer;

  public TestableKafkaConsumer(
    final Properties kafkaProps,
    final String topic,
    final Function<V, V> processor,
    final KafkaConsumer<K, V> mockConsumer
  ) {
    super(kafkaProps, topic, processor);
    this.mockConsumer = mockConsumer;
    setMockConsumer();
  }

  public TestableKafkaConsumer(
    final Properties props,
    final String topic,
    final Function<V, V> processor,
    final KafkaConsumer<K, V> mockConsumer,
    final int maxRetries,
    final Duration retryBackoff,
    final Consumer<ProcessingError<K, V>> errorHandler
  ) {
    super(
      new Builder<K, V>()
        .withProperties(props)
        .withTopic(topic)
        .withProcessor(processor)
        .withRetry(maxRetries, retryBackoff)
        .withErrorHandler(errorHandler)
    );
    this.mockConsumer = mockConsumer;
    setMockConsumer();
  }

  private void setMockConsumer() {
    try {
      final var consumerField = FunctionalKafkaConsumer.class.getDeclaredField("consumer");
      consumerField.setAccessible(true);
      consumerField.set(this, mockConsumer);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set mock consumer", e);
    }
  }

  @Override
  protected KafkaConsumer<K, V> createConsumer(Properties kafkaProps) {
    return mockConsumer;
  }

  public void executeProcessRecords(ConsumerRecords<K, V> records) {
    processRecords(records);
  }
}
