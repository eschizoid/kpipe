package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KPipeDlqTest {

  private static final String TOPIC = "test-topic";
  private static final String DLQ_TOPIC = "test-dlq-topic";

  @Mock
  private Producer<String, String> mockProducer;

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldSendToDlqAfterMaxRetries() throws Exception {
    // Arrange
    final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value");
    final var future = CompletableFuture.completedFuture(mock(RecordMetadata.class));
    when(mockProducer.send(any(ProducerRecord.class))).thenReturn(future);

    final var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(v -> {
        throw new RuntimeException("fail");
      })
      .withRetry(1, Duration.ofMillis(1))
      .withDeadLetterTopic(DLQ_TOPIC)
      .withKafkaProducer(mockProducer)
      .build();

    // Act
    consumer.processRecord(record);

    // Assert
    verify(mockProducer, times(1)).send(
      argThat(producerRecord -> {
        assertEquals(DLQ_TOPIC, producerRecord.topic());
        assertEquals("key", producerRecord.key());
        assertEquals("value", producerRecord.value());
        return true;
      })
    );
    consumer.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldNotSendToDlqIfProcessingSucceedsAfterRetry() throws Exception {
    // Arrange
    final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value");
    final var attemptCount = new java.util.concurrent.atomic.AtomicInteger(0);

    final var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(v -> {
        if (attemptCount.getAndIncrement() == 0) {
          throw new RuntimeException("fail first time");
        }
        return v;
      })
      .withRetry(1, Duration.ofMillis(1))
      .withDeadLetterTopic(DLQ_TOPIC)
      .withKafkaProducer(mockProducer)
      .build();

    // Act
    consumer.processRecord(record);

    // Assert
    verify(mockProducer, never()).send(any(ProducerRecord.class));
    assertEquals(2, attemptCount.get());
    consumer.close();
  }
}
