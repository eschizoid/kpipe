package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kpipe.consumer.enums.ConsumerCommand;
import org.kpipe.sink.MessageSink;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FunctionalConsumerTest {

  private static final String TOPIC = "test-topic";
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

  @Mock
  private Function<String, String> mockProcessor;

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("enable.auto.commit", "true");
  }

  private FunctionalConsumer<String, String> createConsumer(Function<String, String> processor) {
    return FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(processor)
      .build();
  }

  private ConsumerRecord<String, String> createRecord(long offset, String key, String value) {
    return new ConsumerRecord<>(TOPIC, 0, offset, key, value);
  }

  @Test
  void constructorWithValidParametersShouldNotThrowException() {
    // Arrange & Act & Assert
    assertDoesNotThrow(() -> createConsumer(mockProcessor));
    assertDoesNotThrow(() ->
      FunctionalConsumer
        .<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(mockProcessor)
        .withPollTimeout(POLL_TIMEOUT)
        .build()
    );
  }

  @Test
  void constructorWithNullParametersShouldThrowNullPointerException() {
    // Arrange & Act & Assert
    assertThrows(NullPointerException.class, () -> FunctionalConsumer.<String, String>builder().build());
    assertThrows(
      NullPointerException.class,
      () ->
        FunctionalConsumer
          .<String, String>builder()
          .withProperties(null)
          .withTopic(TOPIC)
          .withProcessor(mockProcessor)
          .build()
    );
    assertThrows(
      NullPointerException.class,
      () ->
        FunctionalConsumer
          .<String, String>builder()
          .withProperties(properties)
          .withTopic(null)
          .withProcessor(mockProcessor)
          .build()
    );
    assertThrows(
      NullPointerException.class,
      () ->
        FunctionalConsumer
          .<String, String>builder()
          .withProperties(properties)
          .withTopic(TOPIC)
          .withProcessor(null)
          .build()
    );
  }

  @Test
  void isRunningShouldReturnFalseAfterConstruction() {
    // Arrange
    final var consumer = createConsumer(mockProcessor);

    // Act & Assert
    assertFalse(consumer.isRunning());
    consumer.close();
  }

  @Test
  void isRunningShouldReturnFalseAfterClose() {
    // Arrange
    final var consumer = createConsumer(mockProcessor);

    // Act
    consumer.close();

    // Assert
    assertFalse(consumer.isRunning());
  }

  @Test
  void closeCalledMultipleTimesShouldBeIdempotent() {
    // Arrange
    final var consumer = createConsumer(mockProcessor);

    // Act
    consumer.close();
    consumer.close();

    // Assert
    assertFalse(consumer.isRunning());
  }

  @Test
  void autoCloseableShouldCloseConsumerWhenExitingTryWithResources() {
    // Arrange & Act
    try (final var consumer = createConsumer(mockProcessor)) {
      // Assert
      assertFalse(consumer.isRunning());
    }
    // No exception means success
    assertTrue(true);
  }

  @Test
  void processorWithRetryEnabledShouldRetryFailedMessages() throws InterruptedException {
    // Arrange
    final var attempts = new AtomicInteger(0);
    final Function<String, String> retryProcessor = value -> {
      if (attempts.getAndIncrement() == 0) throw new RuntimeException("First attempt failure");
      return value.toUpperCase();
    };
    var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(retryProcessor)
      .withRetry(2, Duration.ofMillis(10))
      .withMetrics(true)
      .build();
    final var record = createRecord(0, "key", "hello");

    // Act
    consumer.processRecord(record);
    Thread.sleep(200);

    // Assert
    final var metrics = consumer.getMetrics();
    assertEquals(2, attempts.get());
    assertEquals(1, metrics.get("messagesProcessed"));
    assertEquals(1, metrics.get("retries"));
    assertEquals(0, metrics.get("processingErrors"));
    consumer.close();
  }

  @Test
  void processorWithMaxRetriesExceededShouldCallErrorHandler() throws InterruptedException {
    // Arrange
    final Function<String, String> failingProcessor = value -> {
      throw new RuntimeException("Always failing");
    };
    Consumer<FunctionalConsumer.ProcessingError<String, String>> errorHandler = mock(Consumer.class);
    var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(failingProcessor)
      .withRetry(2, Duration.ofMillis(10))
      .withErrorHandler(errorHandler)
      .withMetrics(true)
      .build();
    final var record = createRecord(0, "key", "hello");

    // Act
    consumer.processRecord(record);
    Thread.sleep(100);

    // Assert
    final var errorCaptor = ArgumentCaptor.forClass(FunctionalConsumer.ProcessingError.class);
    verify(errorHandler).accept(errorCaptor.capture());
    final var error = errorCaptor.getValue();
    assertEquals(record, error.record());
    assertEquals(2, error.retryCount());
    assertNotNull(error.exception());
    final var metrics = consumer.getMetrics();
    assertEquals(2, metrics.get("retries"));
    assertEquals(0, metrics.get("messagesProcessed"));
    assertEquals(1, metrics.get("processingErrors"));
    consumer.close();
  }

  @Test
  void metricsTrackingWithFailuresAndRetriesShouldIncrementCorrectly() throws InterruptedException {
    // Arrange
    final var counter = new AtomicInteger(0);
    final Function<String, String> intermittentProcessor = value -> {
      int count = counter.incrementAndGet();
      if (count % 3 != 0) throw new RuntimeException("Intermittent failure #" + count);
      return value.toUpperCase();
    };
    var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(intermittentProcessor)
      .withRetry(5, Duration.ofMillis(10))
      .withMetrics(true)
      .build();

    // Act
    for (int i = 0; i < 3; i++) {
      var record = createRecord(i, "key" + i, "value" + i);
      consumer.processRecord(record);
    }
    Thread.sleep(200);

    // Assert
    var metrics = consumer.getMetrics();
    assertEquals(3, metrics.get("messagesReceived"));
    assertEquals(3, metrics.get("messagesProcessed"));
    assertEquals(6, metrics.get("retries"));
    assertEquals(0, metrics.get("processingErrors"));
    consumer.close();
  }

  @Test
  void pauseShouldChangeStateAndEnqueuePauseCommand() {
    // Arrange
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(mockProcessor)
      .withCommandQueue(commandQueue)
      .build();

    // Act
    consumer.pause();

    // Assert
    assertTrue(consumer.isPaused());
    assertTrue(commandQueue.contains(ConsumerCommand.PAUSE));
  }

  @Test
  void resumeShouldChangeStateAndEnqueueResumeCommand() {
    // Arrange
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(mockProcessor)
      .withCommandQueue(commandQueue)
      .build();
    consumer.pause();

    // Act
    consumer.resume();

    // Assert
    assertFalse(consumer.isPaused());
    assertTrue(commandQueue.contains(ConsumerCommand.RESUME));
  }

  @Test
  void closeShouldEnqueueCloseCommand() {
    // Arrange
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(mockProcessor)
      .withCommandQueue(commandQueue)
      .build();

    consumer.start();

    // Act
    consumer.close();

    // Assert
    assertTrue(commandQueue.contains(ConsumerCommand.CLOSE));
  }

  @Test
  void processCommandsShouldHandleClose() {
    // Arrange
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var consumer = createConsumer(mockProcessor);
    commandQueue.offer(ConsumerCommand.CLOSE);

    // Act & Assert
    assertDoesNotThrow(consumer::processCommands);
  }

  @Test
  void customMessageSinkShouldReceiveProcessedMessages() {
    // Arrange
    @SuppressWarnings("unchecked")
    final var sink = mock(MessageSink.class);
    var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(s -> s + "-processed")
      .withMessageSink(sink)
      .build();
    var record = createRecord(1, "k", "v");

    // Act
    consumer.processRecord(record);

    // Assert
    verify(sink).send(eq(record), eq("v-processed"));
    consumer.close();
  }

  @Test
  void metricsShouldBeEmptyWhenDisabled() {
    // Arrange
    final var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(s -> s)
      .withMetrics(false)
      .build();
    final var record = createRecord(1, "k", "v");

    // Act
    consumer.processRecord(record);

    // Assert
    assertTrue(consumer.getMetrics().isEmpty());
    consumer.close();
  }

  @Test
  void sequentialProcessingShouldProcessInOrder() {
    // Arrange
    final var processed = new ArrayList<>();
    var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(s -> {
        processed.add(s);
        return s;
      })
      .withSequentialProcessing(true)
      .build();

    final var records = new ConsumerRecords<>(
      Map.of(
        new TopicPartition(TOPIC, 0),
        List.of(createRecord(0, "k1", "v1"), createRecord(1, "k2", "v2"), createRecord(2, "k3", "v3"))
      )
    );

    // Act
    consumer.processRecords(records);

    // Assert
    assertEquals(List.of("v1", "v2", "v3"), processed);
    consumer.close();
  }
}
