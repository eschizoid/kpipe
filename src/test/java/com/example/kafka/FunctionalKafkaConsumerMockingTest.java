package com.example.kafka;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FunctionalKafkaConsumerMockingTest {

  private static final String TOPIC = "test-topic";

  @Mock
  private Function<String, String> processor;

  @Mock
  private KafkaConsumer<String, String> mockConsumer;

  @Captor
  private ArgumentCaptor<List<String>> topicCaptor;

  @Test
  void shouldSubscribeToTopic() {
    final var props = new Properties();
    final var consumer = new TestableKafkaConsumer<>(props, TOPIC, processor, mockConsumer);

    consumer.start();

    verify(mockConsumer).subscribe(topicCaptor.capture());
    assertEquals(List.of(TOPIC), topicCaptor.getValue());
  }

  @Test
  void shouldProcessRecordsWithProcessor() throws Exception {
    // Setup
    final var props = new Properties();

    // Create mock records
    final var partition = new TopicPartition(TOPIC, 0);
    final var recordsList = List.of(new ConsumerRecord<>(TOPIC, 0, 0L, "test-key", "test-value"));
    final var records = new ConsumerRecords<>(Map.of(partition, recordsList));

    // Create consumer with mock
    final var consumer = new TestableKafkaConsumer<>(props, TOPIC, processor, mockConsumer);

    // Create a CountDownLatch to wait for async processing
    final var latch = new CountDownLatch(1);

    // Combine both counting down the latch and returning a value
    doAnswer(invocation -> {
        latch.countDown();
        return "processed-value";
      })
      .when(processor)
      .apply("test-value");

    // Test
    consumer.executeProcessRecords(records);
    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    // Verify processor was called with correct value
    verify(processor).apply("test-value");
  }

  @Test
  void shouldHandleProcessorExceptions() throws Exception {
    // Setup
    final var props = new Properties();
    final var latch = new CountDownLatch(1);

    // Configure mock to throw exception and count down latch
    doAnswer(invocation -> {
        latch.countDown();
        throw new RuntimeException("Test exception");
      })
      .when(processor)
      .apply("test-value");

    // Create mock records
    final var partition = new TopicPartition(TOPIC, 0);
    final var recordsList = List.of(new ConsumerRecord<>(TOPIC, 0, 0L, "test-key", "test-value"));
    final var records = new ConsumerRecords<>(Map.of(partition, recordsList));

    // Create consumer with mock
    final var consumer = new TestableKafkaConsumer<>(props, TOPIC, processor, mockConsumer);

    // Test - should not throw exception
    assertDoesNotThrow(() -> consumer.executeProcessRecords(records));

    // Wait for async processing to complete
    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    // Verify processor was called
    verify(processor).apply("test-value");
  }

  @Test
  void shouldCloseKafkaConsumerWhenClosed() {
    final var props = new Properties();
    final var consumer = new TestableKafkaConsumer<>(props, TOPIC, processor, mockConsumer);

    consumer.close();

    verify(mockConsumer).wakeup();
  }
}
