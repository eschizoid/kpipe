package com.example.kafka;

import java.time.Duration;
import java.util.Properties;
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
    setConsumerField(mockConsumer);
  }

  public TestableKafkaConsumer(
    final Properties kafkaProps,
    final String topic,
    final Function<V, V> processor,
    final Duration pollTimeout,
    final KafkaConsumer<K, V> mockConsumer
  ) {
    super(kafkaProps, topic, processor, pollTimeout);
    this.mockConsumer = mockConsumer;
    setConsumerField(mockConsumer);
  }

  private void setConsumerField(final KafkaConsumer<K, V> mockConsumer) {
    try {
      final var consumerField = FunctionalKafkaConsumer.class.getDeclaredField("consumer");
      consumerField.setAccessible(true);
      consumerField.set(this, mockConsumer);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set mock consumer", e);
    }
  }

  @Override
  protected KafkaConsumer<K, V> createConsumer(final Properties kafkaProps) {
    return mockConsumer;
  }

  // Expose protected methods for testing
  public void executeProcessRecords(final ConsumerRecords<K, V> records) {
    processRecords(records);
  }
}
