package com.example.kafka;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TestableKafkaConsumer<K, V> extends FunctionalKafkaConsumer<K, V> {
  private final KafkaConsumer<K, V> mockConsumer;

  public TestableKafkaConsumer(
      Properties kafkaProps,
      String topic,
      Function<V, V> processor,
      KafkaConsumer<K, V> mockConsumer) {
    super(kafkaProps, topic, processor);
    this.mockConsumer = mockConsumer;
    setConsumerField(mockConsumer);
  }

  public TestableKafkaConsumer(
      Properties kafkaProps,
      String topic,
      Function<V, V> processor,
      Duration pollTimeout,
      KafkaConsumer<K, V> mockConsumer) {
    super(kafkaProps, topic, processor, pollTimeout);
    this.mockConsumer = mockConsumer;
    setConsumerField(mockConsumer);
  }

  private void setConsumerField(KafkaConsumer<K, V> mockConsumer) {
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

  // Expose protected methods for testing
  public void executeProcessRecords(ConsumerRecords<K, V> records) {
    processRecords(records);
  }
}
