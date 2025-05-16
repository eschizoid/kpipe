package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kpipe.consumer.enums.ConsumerCommand;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FunctionalConsumerMockingTest {

  private static final int PARTITION = 0;
  private static final String TOPIC = "test-topic";
  private Properties properties;

  @Mock
  private Function<String, String> processor;

  @Mock
  private KafkaConsumer<String, String> mockConsumer;

  @Mock
  private Consumer<FunctionalConsumer.ProcessingError<String, String>> errorHandler;

  @Mock
  private OffsetManager<String, String> offsetManager;

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
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    functionalConsumer.start();

    verify(mockConsumer).subscribe(topicCaptor.capture());
    assertEquals(List.of(TOPIC), topicCaptor.getValue());
  }

  @Test
  void shouldProcessRecordsWithProcessor() throws Exception {
    // Create mock records
    final var partition = new TopicPartition(TOPIC, PARTITION);
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var recordsList = List.of(new ConsumerRecord<>(TOPIC, PARTITION, 0L, "test-key", "test-value"));
    final var records = new ConsumerRecords<>(Map.of(partition, recordsList));
    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Create a CountDownLatch to wait for async processing
    final var latch = new CountDownLatch(1);

    // Combine both counting down the latch and returning a value
    doAnswer(invocation -> {
        latch.countDown();
        return "processed-value";
      })
      .when(processor)
      .apply(anyString());

    // Test
    functionalConsumer.executeProcessRecords(records);
    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    // Verify the processor was called with the correct value
    verify(processor).apply("test-value");
  }

  @Test
  void shouldHandleProcessorExceptions() throws Exception {
    // Setup
    final var latch = new CountDownLatch(1);
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    // Configure mock to throw exception and count down latch
    doAnswer(invocation -> {
        latch.countDown();
        throw new RuntimeException("Test exception");
      })
      .when(processor)
      .apply(anyString());

    // Create mock records
    final var partition = new TopicPartition(TOPIC, PARTITION);
    final var recordsList = List.of(new ConsumerRecord<>(TOPIC, PARTITION, 0L, "test-key", "test-value"));
    final var records = new ConsumerRecords<>(Map.of(partition, recordsList));
    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Test - should not throw exception
    assertDoesNotThrow(() -> functionalConsumer.executeProcessRecords(records));

    // Wait for async processing to complete
    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    // Verify processor was called
    verify(processor).apply("test-value");
  }

  @Test
  void shouldCloseKafkaConsumerWhenClosed() {
    // Setup
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );
    functionalConsumer.start();

    // Test
    functionalConsumer.close();

    // Verify
    verify(mockConsumer).wakeup();
    verify(mockConsumer, times(1)).close();
    assertFalse(functionalConsumer.isRunning());
  }

  @Test
  void shouldRetryProcessingOnFailureUpToMaxRetries() throws Exception {
    // Setup
    final var latch = new CountDownLatch(3); // Expect 3 calls (initial + 2 retries)
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    // Configure mock to always fail and count down latch
    doAnswer(inv -> {
        latch.countDown();
        throw new RuntimeException("Test exception");
      })
      .when(processor)
      .apply(anyString());

    // Create a mock record
    final var record = new ConsumerRecord<>(TOPIC, PARTITION, 0L, "key", "value");
    final var partition = new TopicPartition(TOPIC, PARTITION);
    final var recordsList = List.of(record);
    final var records = new ConsumerRecords<>(Map.of(partition, recordsList));
    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      2,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Process records
    functionalConsumer.executeProcessRecords(records);

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
    final var latch = new CountDownLatch(1);
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    // Configure mock to fail and count down latch
    doAnswer(inv -> {
        latch.countDown();
        throw new RuntimeException("Test exception");
      })
      .when(processor)
      .apply(anyString());

    // Create mock records
    final var record = new ConsumerRecord<>(TOPIC, PARTITION, 0L, "key", "value");
    final var partition = new TopicPartition(TOPIC, PARTITION);
    final var recordsList = List.of(record);
    final var records = new ConsumerRecords<>(Map.of(partition, recordsList));

    // Create a consumer with no retries
    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Process records
    functionalConsumer.executeProcessRecords(records);

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
    final var partitions = Set.of(new TopicPartition(TOPIC, PARTITION));
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    when(mockConsumer.assignment()).thenReturn(partitions);

    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Action
    functionalConsumer.pause();
    functionalConsumer.processCommands();

    // Verification
    verify(mockConsumer).pause(partitions);
  }

  @Test
  void shouldResumeConsumerWhenResumeCalled() {
    // Setup
    final var partition = new TopicPartition(TOPIC, PARTITION);
    final var partitions = Set.of(partition);
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    when(mockConsumer.assignment()).thenReturn(partitions);

    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Set up a paused state
    functionalConsumer.pause();
    functionalConsumer.processCommands(); // Process pause command

    // Clear any previous interactions with the mock
    reset(mockConsumer);
    when(mockConsumer.assignment()).thenReturn(partitions);

    // Action
    functionalConsumer.resume();
    functionalConsumer.processCommands(); // Process resume command

    // Verification
    verify(mockConsumer).resume(partitions);
  }

  @Test
  void pauseAndResumeShouldBeIdempotent() {
    // Setup
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var partitions = Set.of(new TopicPartition(TOPIC, PARTITION));
    when(mockConsumer.assignment()).thenReturn(partitions);

    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Action
    functionalConsumer.pause();
    functionalConsumer.processCommands(); // Process the pause command
    functionalConsumer.pause(); // The second call should be idempotent
    functionalConsumer.processCommands(); // Process the second command (should do nothing)

    // Verify
    verify(mockConsumer, times(1)).pause(any());
  }

  @Test
  void shouldUpdateMetricsOnSuccessfulProcessing() throws Exception {
    // Setup
    final var latch = new CountDownLatch(1);
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    // Configure mock to return success and count down latch
    doAnswer(inv -> {
        latch.countDown();
        return "processed-value";
      })
      .when(processor)
      .apply(anyString());

    // Create mock records
    var partition = new TopicPartition(TOPIC, PARTITION);
    var recordsList = List.of(new ConsumerRecord<>(TOPIC, PARTITION, 0L, "key", "value"));
    var records = new ConsumerRecords<>(Map.of(partition, recordsList));

    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Process records
    functionalConsumer.executeProcessRecords(records);

    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    // Give virtual threads time to complete processing
    Thread.sleep(500);

    // Verify metrics
    final var metrics = functionalConsumer.getMetrics();
    assertEquals(1L, metrics.get("messagesReceived"));
    assertEquals(1L, metrics.get("messagesProcessed"));
    assertEquals(0L, metrics.get("processingErrors"));
    assertEquals(0L, metrics.get("retries"));
  }

  @Test
  void shouldUpdateMetricsOnProcessingError() throws Exception {
    // Setup
    final var latch = new CountDownLatch(1);
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    // Configure mock to throw exception and count down latch
    doAnswer(inv -> {
        latch.countDown();
        throw new RuntimeException("Test exception");
      })
      .when(processor)
      .apply(anyString());

    // Create mock records
    final var partition = new TopicPartition(TOPIC, PARTITION);
    final var recordsList = List.of(new ConsumerRecord<>(TOPIC, PARTITION, 0L, "key", "value"));
    final var records = new ConsumerRecords<>(Map.of(partition, recordsList));
    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Process records
    functionalConsumer.executeProcessRecords(records);

    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    // Verify metrics
    final var metrics = functionalConsumer.getMetrics();
    assertEquals(1L, metrics.get("messagesReceived"));
    assertEquals(0L, metrics.get("messagesProcessed"));
    assertEquals(1L, metrics.get("processingErrors"));
  }

  @Test
  void shouldNotCollectMetricsWhenDisabled() {
    // Create consumer with disabled metrics
    try (
      final var consumer = FunctionalConsumer
        .<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(s -> s)
        .withMetrics(false)
        .build()
    ) {
      assertTrue(consumer.getMetrics().isEmpty(), "Metrics should be empty when disabled");
      assertNull(consumer.createMessageTracker(), "MessageTracker should be null when metrics are disabled");
    }
  }

  @Test
  void builderShouldRespectAllOptions() {
    // Setup
    final var pollTimeout = Duration.ofMillis(200);
    final Consumer<FunctionalConsumer.ProcessingError<String, String>> errorHandler = error -> {};
    final var maxRetries = 3;
    final var retryBackoff = Duration.ofMillis(100);
    final var enableMetrics = true;

    // Create a consumer with all options
    final var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(s -> s)
      .withPollTimeout(pollTimeout)
      .withErrorHandler(errorHandler)
      .withRetry(maxRetries, retryBackoff)
      .withMetrics(enableMetrics)
      .build();

    // Assert
    assertFalse(consumer.isRunning());
    assertFalse(consumer.isPaused());

    // Cleanup
    consumer.close();
  }

  @Test
  void builderShouldThrowNullPointerExceptionWhenMissingRequiredFields() {
    final var builder = FunctionalConsumer.<String, String>builder();

    assertThrows(NullPointerException.class, builder::build);

    builder.withProperties(properties);
    assertThrows(NullPointerException.class, builder::build);

    builder.withTopic(TOPIC);
    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  void shouldHandleEmptyRecordBatch() {
    // Setup
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Create empty records
    final var records = new ConsumerRecords<String, String>(Map.of());

    // Process records
    functionalConsumer.executeProcessRecords(records);

    // Verify processor was never called
    verifyNoInteractions(processor);

    // Verify metrics
    final var metrics = functionalConsumer.getMetrics();
    assertEquals(0L, metrics.get("messagesReceived"));
  }

  @Test
  void shouldHandleNullValueInRecord() throws Exception {
    // Setup
    final var latch = new CountDownLatch(1);
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    doAnswer(inv -> {
        latch.countDown();
        return null;
      })
      .when(processor)
      .apply(null);

    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Create record with null value
    final var partition = new TopicPartition(TOPIC, PARTITION);
    final var recordsList = List.of(new ConsumerRecord<String, String>(TOPIC, PARTITION, 0L, "key", null));
    final var records = new ConsumerRecords<>(Map.of(partition, recordsList));

    // Process records
    functionalConsumer.executeProcessRecords(records);

    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    // Verify the processor was called with null
    verify(processor).apply(null);
  }

  @Test
  void shouldNotInterruptShutdownWithInFlightMessages() throws Exception {
    // Setup - create a processor that takes time
    final var processingStarted = new CountDownLatch(1);
    final var processingBlocked = new AtomicBoolean(true);
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    Function<String, String> slowProcessor = value -> {
      processingStarted.countDown();
      while (processingBlocked.get()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return value;
    };

    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      slowProcessor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Initiate close in a separate thread
    final var closeFuture = CompletableFuture.runAsync(functionalConsumer::close);

    // Verify close doesn't complete immediately
    assertFalse(closeFuture.isDone(), "Close should wait for in-flight messages");

    // Verify close completes
    closeFuture.get(1, TimeUnit.SECONDS);
  }

  @Test
  void processCommandsShouldHandlePauseAndResume() {
    // Arrange
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    commandQueue.offer(ConsumerCommand.PAUSE);
    commandQueue.offer(ConsumerCommand.RESUME);

    // Act
    functionalConsumer.processCommands();

    // Assert
    verify(mockConsumer, atLeastOnce()).pause(any());
    verify(mockConsumer, atLeastOnce()).resume(any());
  }

  @Test
  void shouldTrackInFlightMessagesCorrectly() throws Exception {
    // Create a custom consumer with tracking methods
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Check initial state
    assertEquals(0, functionalConsumer.getProcessingCount(), "Initially no in-flight messages");

    // Create records
    final var partition = new TopicPartition(TOPIC, PARTITION);
    final var recordsList = List.of(
      new ConsumerRecord<>(TOPIC, PARTITION, 0L, "key1", "value1"),
      new ConsumerRecord<>(TOPIC, PARTITION, 1L, "key2", "value2")
    );
    final var records = new ConsumerRecords<>(Map.of(partition, recordsList));

    // Use CountDownLatch to control when processing completes
    final var startLatch = new CountDownLatch(2);
    final var completeLatch = new CountDownLatch(1);

    // Mock processor that counts down startLatch and waits on completeLatch
    when(processor.apply(anyString()))
      .thenAnswer(inv -> {
        startLatch.countDown();
        completeLatch.await(5, TimeUnit.SECONDS);
        return inv.getArgument(0);
      });

    // Process records
    CompletableFuture.runAsync(() -> functionalConsumer.executeProcessRecords(records));

    // Wait for processing to start
    assertTrue(startLatch.await(1, TimeUnit.SECONDS), "Processing did not start in time");

    // Verify the processing count
    assertEquals(2, functionalConsumer.getProcessingCount(), "Should have 2 in-flight messages");

    // Allow processing to complete
    completeLatch.countDown();

    // Wait for processing to complete
    Thread.sleep(300);

    // Verify count returns to 0
    assertEquals(0, functionalConsumer.getProcessingCount(), "Should have no in-flight messages after completion");
  }

  @Test
  void shouldHandleInterruptedPollOperation() throws Exception {
    // Setup - mock the behavior to first throw exception, then simulate wakeup
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    when(mockConsumer.poll(any(Duration.class)))
      .thenThrow(new RuntimeException("Poll interrupted"))
      .thenThrow(new RuntimeException("Poll interrupted again"));

    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Start in a separate thread
    final var executor = Executors.newSingleThreadExecutor();
    final var future = executor.submit(functionalConsumer::start);

    // Allow time for a poll to be called
    Thread.sleep(100);

    // Force stop the consumer
    functionalConsumer.close();

    // Wait for the thread to complete
    future.get(1, TimeUnit.SECONDS);

    // Verify consumer is no longer running
    assertFalse(functionalConsumer.isRunning(), "Consumer should stop after close");

    // Cleanup
    executor.shutdownNow();
  }

  @Test
  void stateShouldTransitionCorrectlyDuringLifecycle() throws InterruptedException {
    // Create consumer
    final var functionalConsumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(s -> s)
      .build();

    // Initial state
    assertFalse(functionalConsumer.isRunning(), "Should not be running initially");
    assertFalse(functionalConsumer.isPaused(), "Should not be paused initially");

    // Start consumer
    final var executor = Executors.newSingleThreadExecutor();
    executor.submit(functionalConsumer::start);

    // Wait for consumer to start
    Thread.sleep(100);

    // Check the running state
    assertTrue(functionalConsumer.isRunning(), "Should be running after start");
    assertFalse(functionalConsumer.isPaused(), "Should not be paused after start");

    // Pause consumer
    functionalConsumer.pause();
    Thread.sleep(50);

    // Check paused state
    assertTrue(functionalConsumer.isRunning(), "Should still be running when paused");
    assertTrue(functionalConsumer.isPaused(), "Should be paused after pause");

    // Resume consumer
    functionalConsumer.resume();
    Thread.sleep(50);

    // Check resumed state
    assertTrue(functionalConsumer.isRunning(), "Should be running after resume");
    assertFalse(functionalConsumer.isPaused(), "Should not be paused after resume");

    // Close consumer
    functionalConsumer.close();
    Thread.sleep(50);

    // Check closed state
    assertFalse(functionalConsumer.isRunning(), "Should not be running after close");

    // Cleanup
    executor.shutdownNow();
  }

  @Test
  void shouldMarkOffsetAsProcessedEvenWhenProcessingFails() throws Exception {
    // Setup
    final var latch = new CountDownLatch(1);
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    // Configure the processor to always fail
    doAnswer(inv -> {
        latch.countDown();
        throw new RuntimeException("Expected test exception");
      })
      .when(processor)
      .apply(anyString());

    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      processor,
      mockConsumer,
      2,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Create a record that will fail processing
    final var record = new ConsumerRecord<>(TOPIC, PARTITION, 123L, "key", "value");
    final var partition = new TopicPartition(TOPIC, PARTITION);
    final var records = new ConsumerRecords<>(Map.of(partition, List.of(record)));

    // Process record
    functionalConsumer.executeProcessRecords(records);

    // Wait for processing to complete
    assertTrue(latch.await(1, TimeUnit.SECONDS), "Processing did not complete in time");

    Thread.sleep(200);

    // Process the commands in the queue
    functionalConsumer.processCommands();

    // Verify offset manager interactions
    verify(offsetManager, times(1)).markOffsetProcessed(record);
    verify(offsetManager).markOffsetProcessed(record);

    // Verify error handler was called with the right retry count
    verify(errorHandler, timeout(500)).accept(errorCaptor.capture());
    assertEquals(2, errorCaptor.getValue().retryCount());
  }

  @Test
  void shouldIntegrateWithOffsetManager() {
    // Setup - create a consumer with offset manager
    when(offsetManager.createRebalanceListener()).thenReturn(mock(ConsumerRebalanceListener.class));

    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    properties.put("enable.auto.commit", "false");

    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      String::toUpperCase,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Start consumer
    functionalConsumer.start();

    // Verify offset manager is started
    verify(offsetManager).start();

    // Simulate a command to track offset
    var record = new ConsumerRecord<>(TOPIC, PARTITION, 123L, "key", "value");
    commandQueue.offer(ConsumerCommand.TRACK_OFFSET.withRecord(record));

    // Process commands
    functionalConsumer.processCommands();

    // Verify offset is tracked
    verify(offsetManager).trackOffset(record);

    // Simulate processing completion
    commandQueue.offer(ConsumerCommand.MARK_OFFSET_PROCESSED.withRecord(record));

    // Process commands
    functionalConsumer.processCommands();

    // Verify offset is marked as processed
    verify(offsetManager).markOffsetProcessed(record);
  }

  @Test
  void shouldCommitOffsetsViaCommandQueue() {
    // Setup - create consumer with offset manager
    when(offsetManager.createRebalanceListener()).thenReturn(mock(ConsumerRebalanceListener.class));

    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      String::toUpperCase,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Create an offset map to commit
    final var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
    final var commitId = "test-commit-id";
    offsets.put(new TopicPartition(TOPIC, PARTITION), new OffsetAndMetadata(123));

    // Send commit command
    commandQueue.offer(ConsumerCommand.COMMIT_OFFSETS.withOffsets(offsets).withCommitId(commitId));

    // Process commands
    functionalConsumer.processCommands();

    // Verify the commit was performed
    verify(mockConsumer).commitSync(offsets);

    // Verify notification of commit completion
    verify(offsetManager).notifyCommitComplete(commitId, true);
  }

  @Test
  void shouldHandleCommitFailure() {
    // Setup - create consumer with offset manager
    when(offsetManager.createRebalanceListener()).thenReturn(mock(ConsumerRebalanceListener.class));

    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      String::toUpperCase,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Configure mock to throw an exception on commit
    doThrow(new CommitFailedException("Commit failed")).when(mockConsumer).commitSync(anyMap());

    // Create an offset map to commit
    final var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
    final var commitId = "test-commit-id";
    offsets.put(new TopicPartition(TOPIC, PARTITION), new OffsetAndMetadata(123));

    // Send commit command
    commandQueue.offer(ConsumerCommand.COMMIT_OFFSETS.withOffsets(offsets).withCommitId(commitId));

    // Process commands
    functionalConsumer.processCommands();

    // Verify notification of commit failure
    verify(offsetManager).notifyCommitComplete(commitId, false);
  }

  @Test
  void shouldProcessRecordsConcurrently() throws Exception {
    // Setup - track concurrent execution
    final var startLatch = new CountDownLatch(3);
    final var completionLatch = new CountDownLatch(3);
    final var maxConcurrent = new AtomicInteger(0);
    final var currentConcurrent = new AtomicInteger(0);

    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    // Create a processor function separately
    Function<String, String> concurrentProcessor = value -> {
      // Count concurrent executions
      int current = currentConcurrent.incrementAndGet();
      maxConcurrent.updateAndGet(max -> Math.max(max, current));
      startLatch.countDown();

      try {
        // Wait for all processors to start to ensure they overlap
        startLatch.await(1, TimeUnit.SECONDS);
        Thread.sleep(100); // Ensure overlap
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        currentConcurrent.decrementAndGet();
        completionLatch.countDown();
      }
      return value.toUpperCase();
    };

    final var functionalConsumer = new TestableFunctionalConsumer<>(
      properties,
      TOPIC,
      concurrentProcessor,
      mockConsumer,
      0,
      Duration.ofMillis(10),
      errorHandler,
      commandQueue,
      offsetManager
    );

    // Create test records
    final var records = Arrays.asList(
      new ConsumerRecord<>(TOPIC, PARTITION, 1L, "key1", "value1"),
      new ConsumerRecord<>(TOPIC, PARTITION, 2L, "key2", "value2"),
      new ConsumerRecord<>(TOPIC, PARTITION, 3L, "key3", "value3")
    );

    // Create consumer records
    final var recordsMap = Map.of(new TopicPartition(TOPIC, PARTITION), records);
    final var consumerRecords = new ConsumerRecords<>(recordsMap);

    // Process records
    functionalConsumer.executeProcessRecords(consumerRecords);

    // Wait for all records to be processed
    assertTrue(completionLatch.await(3, TimeUnit.SECONDS), "Records processing timed out");

    // Verify concurrent execution
    assertTrue(maxConcurrent.get() > 1, "Records should be processed concurrently");
  }

  public static class TestableFunctionalConsumer<K, V> extends FunctionalConsumer<K, V> {

    private static final String METRIC_MESSAGES_RECEIVED = "messagesReceived";
    private static final String METRIC_MESSAGES_PROCESSED = "messagesProcessed";
    private static final String METRIC_PROCESSING_ERRORS = "processingErrors";

    public TestableFunctionalConsumer(
      final Properties props,
      final String topic,
      final Function<V, V> processor,
      final KafkaConsumer<K, V> mockConsumer,
      final int maxRetries,
      final Duration retryBackoff,
      final Consumer<ProcessingError<K, V>> errorHandler,
      final Queue<ConsumerCommand> mockCommandQueue,
      final OffsetManager<K, V> mockOffsetManager
    ) {
      super(
        FunctionalConsumer
          .<K, V>builder()
          .withProperties(props)
          .withTopic(topic)
          .withProcessor(processor)
          .withRetry(maxRetries, retryBackoff)
          .withErrorHandler(errorHandler)
          .withOffsetManager(mockOffsetManager)
          .withCommandQueue(mockCommandQueue)
          .withConsumer(() -> mockConsumer)
      );
    }

    private void executeProcessRecords(final ConsumerRecords<K, V> records) {
      processRecords(records);
      processCommands();
    }

    private int getProcessingCount() {
      final var metrics = getMetrics();
      final var received = metrics.getOrDefault(METRIC_MESSAGES_RECEIVED, 0L);
      final var processed = metrics.getOrDefault(METRIC_MESSAGES_PROCESSED, 0L);
      final var errors = metrics.getOrDefault(METRIC_PROCESSING_ERRORS, 0L);
      return (int) (received - processed - errors);
    }
  }
}
