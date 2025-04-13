package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FunctionalConsumerMockingTest {

  private static final String TOPIC = "test-topic";
  private Properties properties;

  @Mock
  private Function<String, String> processor;

  @Mock
  private KafkaConsumer<String, String> mockConsumer;

  @Mock
  private Consumer<FunctionalConsumer.ProcessingError<String, String>> errorHandler;

  @Captor
  private ArgumentCaptor<List<String>> topicCaptor;

  @Captor
  private ArgumentCaptor<FunctionalConsumer.ProcessingError<String, String>> errorCaptor;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  }

  @Test
  void shouldSubscribeToTopic() {
    final var props = new Properties();
    final var consumer = new TestableFunctionalConsumer<>(props, TOPIC, processor, mockConsumer);

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
    final var consumer = new TestableFunctionalConsumer<>(props, TOPIC, processor, mockConsumer);

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
    final var consumer = new TestableFunctionalConsumer<>(props, TOPIC, processor, mockConsumer);

    // Test - should not throw exception
    assertDoesNotThrow(() -> consumer.executeProcessRecords(records));

    // Wait for async processing to complete
    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    // Verify processor was called
    verify(processor).apply("test-value");
  }

  @Test
  void shouldCloseKafkaConsumerWhenClosed() throws Exception {
    // Setup
    final var props = new Properties();
    KafkaConsumer<String, String> mockConsumer = mock(KafkaConsumer.class);
    Function<String, String> processor = value -> value;

    final var consumer = new TestableFunctionalConsumer<>(props, TOPIC, processor, mockConsumer);
    consumer.start();

    // Test
    consumer.close();

    // Verify
    verify(mockConsumer).wakeup();

    // Use atLeastOnce() instead of the default times(1)
    verify(mockConsumer, atLeastOnce()).close();

    // Or, to be more specific about the exact number of times:
    // verify(mockConsumer, times(2)).close();

    assertFalse(consumer.isRunning());
  }

  @Test
  void shouldRetryProcessingOnFailureUpToMaxRetries() throws Exception {
    // Setup
    final var props = new Properties();
    final var latch = new CountDownLatch(3); // Expect 3 calls (initial + 2 retries)

    // Configure mock to always fail and count down latch
    doAnswer(inv -> {
        latch.countDown();
        throw new RuntimeException("Test exception");
      })
      .when(processor)
      .apply(anyString());

    // Create mock record
    final var record = new ConsumerRecord<>(TOPIC, 0, 0L, "key", "value");
    final var partition = new TopicPartition(TOPIC, 0);
    final var recordsList = List.of(record);
    final var records = new ConsumerRecords<>(Map.of(partition, recordsList));

    // Create consumer with retry config
    var consumer = new TestableFunctionalConsumer<>(
      props,
      TOPIC,
      processor,
      mockConsumer,
      2,
      Duration.ofMillis(10),
      errorHandler
    );

    // Process records
    consumer.executeProcessRecords(records);

    // Wait for all attempts to complete
    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    // Verify processor was called 3 times (initial + 2 retries)
    verify(processor, times(3)).apply(anyString());

    // Add timeout to handle async processing
    verify(errorHandler, timeout(1000)).accept(errorCaptor.capture());

    // Verify error details
    var error = errorCaptor.getValue();
    assertEquals(record, error.record());
    assertEquals(2, error.retryCount()); // 2 retries
    assertNotNull(error.exception());
  }

  @Test
  void shouldNotRetryWhenMaxRetriesIsZero() throws Exception {
    // Setup
    final var props = new Properties();
    final var latch = new CountDownLatch(1);

    // Configure mock to fail and count down latch
    doAnswer(inv -> {
        latch.countDown();
        throw new RuntimeException("Test exception");
      })
      .when(processor)
      .apply(anyString());

    // Create mock records
    final var record = new ConsumerRecord<>(TOPIC, 0, 0L, "key", "value");
    final var partition = new TopicPartition(TOPIC, 0);
    final var recordsList = List.of(record);
    final var records = new ConsumerRecords<>(Map.of(partition, recordsList));

    // Create consumer with no retries
    final var consumer = new TestableFunctionalConsumer<>(
      props,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler
    );

    // Process records
    consumer.executeProcessRecords(records);

    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    // Verify processor was called only once (no retries)
    verify(processor, times(1)).apply(anyString());

    // Verify error handler was called - with timeout to handle async processing
    verify(errorHandler, timeout(1000)).accept(errorCaptor.capture());

    // Verify error details
    var error = errorCaptor.getValue();
    assertEquals(0, error.retryCount()); // No retries
  }

  @Test
  void shouldPauseConsumerWhenPauseCalled() {
    // Setup
    final var props = new Properties();
    final var consumer = new TestableFunctionalConsumer<>(props, TOPIC, processor, mockConsumer);

    // Mock the assignment
    final var assignment = Set.of(new TopicPartition(TOPIC, 0));
    when(mockConsumer.assignment()).thenReturn(assignment);

    // Test
    consumer.pause();

    // Verify
    assertTrue(consumer.isPaused());
    verify(mockConsumer).pause(assignment);
  }

  @Test
  void shouldResumeConsumerWhenResumeCalled() {
    // Setup
    var props = new Properties();
    var consumer = new TestableFunctionalConsumer<>(props, TOPIC, processor, mockConsumer);

    // Mock the assignment
    var assignment = Set.of(new TopicPartition(TOPIC, 0));
    when(mockConsumer.assignment()).thenReturn(assignment);

    // Pause the consumer first
    consumer.pause();
    assertTrue(consumer.isPaused());

    // Test
    consumer.resume();

    // Verify
    assertFalse(consumer.isPaused());
    verify(mockConsumer).resume(assignment);
  }

  @Test
  void pauseAndResumeShouldBeIdempotent() {
    // Setup
    var props = new Properties();
    var consumer = new TestableFunctionalConsumer<>(props, TOPIC, processor, mockConsumer);

    // Mock the assignment
    var assignment = Set.of(new TopicPartition(TOPIC, 0));
    when(mockConsumer.assignment()).thenReturn(assignment);

    // Test pause idempotence
    consumer.pause();
    consumer.pause(); // Second call should have no effect

    // Verify pause was only called once
    verify(mockConsumer, times(1)).pause(any());

    // Test resume idempotence
    consumer.resume();
    consumer.resume(); // Second call should have no effect

    // Verify resume was only called once
    verify(mockConsumer, times(1)).resume(any());
  }

  @Test
  void shouldUpdateMetricsOnSuccessfulProcessing() throws Exception {
    // Setup
    final var props = new Properties();
    final var latch = new CountDownLatch(1);

    // Configure mock to return success and count down latch
    doAnswer(inv -> {
        latch.countDown();
        return "processed-value";
      })
      .when(processor)
      .apply(anyString());

    // Create mock records
    var partition = new TopicPartition(TOPIC, 0);
    var recordsList = List.of(new ConsumerRecord<>(TOPIC, 0, 0L, "key", "value"));
    var records = new ConsumerRecords<>(Map.of(partition, recordsList));

    // Create test consumer
    var consumer = new TestableFunctionalConsumer<>(props, TOPIC, processor, mockConsumer);

    // Process records
    consumer.executeProcessRecords(records);

    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    // Give virtual threads time to complete processing
    Thread.sleep(500);

    // Verify metrics
    final var metrics = consumer.getMetrics();
    assertEquals(1L, metrics.get("messagesReceived"));
    assertEquals(1L, metrics.get("messagesProcessed"));
    assertEquals(0L, metrics.get("processingErrors"));
    assertEquals(0L, metrics.get("retries"));
  }

  @Test
  void shouldUpdateMetricsOnProcessingError() throws Exception {
    // Setup
    final var props = new Properties();
    final var latch = new CountDownLatch(1);

    // Configure mock to throw exception and count down latch
    doAnswer(inv -> {
        latch.countDown();
        throw new RuntimeException("Test exception");
      })
      .when(processor)
      .apply(anyString());

    // Create mock records
    final var partition = new TopicPartition(TOPIC, 0);
    final var recordsList = List.of(new ConsumerRecord<>(TOPIC, 0, 0L, "key", "value"));
    final var records = new ConsumerRecords<>(Map.of(partition, recordsList));

    // Create test consumer
    final var consumer = new TestableFunctionalConsumer<>(props, TOPIC, processor, mockConsumer);

    // Process records
    consumer.executeProcessRecords(records);

    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    // Verify metrics
    final var metrics = consumer.getMetrics();
    assertEquals(1L, metrics.get("messagesReceived"));
    assertEquals(0L, metrics.get("messagesProcessed"));
    assertEquals(1L, metrics.get("processingErrors"));
  }

  @Test
  void shouldNotCollectMetricsWhenDisabled() {
    // Setup
    final var props = new Properties();
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("bootstrap.servers", "localhost:9092"); // Can be any value since we just test metrics

    // Create consumer with disabled metrics
    try (
      var consumer = new FunctionalConsumer.Builder<String, String>()
        .withProperties(props)
        .withTopic(TOPIC)
        .withProcessor(processor)
        .withMetrics(false)
        .build()
    ) {
      // Verify metrics are empty
      assertTrue(consumer.getMetrics().isEmpty());
    }
  }

  @Test
  void builderShouldCreateConsumerWithMinimalConfig() {
    var consumer = new FunctionalConsumer.Builder<String, String>()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(processor)
      .build();

    assertNotNull(consumer);
    assertTrue(consumer.isRunning());
  }

  @Test
  void builderShouldRespectAllOptions() {
    final var customPollTimeout = Duration.ofMillis(200);
    final var customRetryBackoff = Duration.ofMillis(300);
    final var maxRetries = 3;

    var consumer = new FunctionalConsumer.Builder<String, String>()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(processor)
      .withPollTimeout(customPollTimeout)
      .withErrorHandler(errorHandler)
      .withRetry(maxRetries, customRetryBackoff)
      .withMetrics(false)
      .build();

    assertNotNull(consumer);
    assertTrue(consumer.isRunning());
    assertTrue(consumer.getMetrics().isEmpty()); // Metrics disabled
  }

  @Test
  void builderShouldThrowNullPointerExceptionWhenMissingRequiredFields() {
    final var builder = new FunctionalConsumer.Builder<String, String>();

    assertThrows(NullPointerException.class, builder::build);

    builder.withProperties(properties);
    assertThrows(NullPointerException.class, builder::build);

    builder.withTopic(TOPIC);
    assertThrows(NullPointerException.class, builder::build);
  }

  public static class TestableFunctionalConsumer<K, V> extends FunctionalConsumer<K, V> {

    private final KafkaConsumer<K, V> mockConsumer;

    public TestableFunctionalConsumer(
      final Properties kafkaProps,
      final String topic,
      final Function<V, V> processor,
      final KafkaConsumer<K, V> mockConsumer
    ) {
      super(kafkaProps, topic, processor);
      this.mockConsumer = mockConsumer;
      setMockConsumer();
    }

    public TestableFunctionalConsumer(
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
        final var consumerField = FunctionalConsumer.class.getDeclaredField("consumer");
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

    public void executeProcessRecords(ConsumerRecords<K, V> records) {
      processRecords(records);
    }
  }
}
