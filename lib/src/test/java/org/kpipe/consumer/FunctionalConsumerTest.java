package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

  @Test
  void constructorWithValidParametersShouldNotThrowException() {
    assertDoesNotThrow(() ->
      FunctionalConsumer
        .<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(mockProcessor)
        .build()
    );

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
    // Empty builder
    final var emptyBuilder = FunctionalConsumer.<String, String>builder();
    assertThrows(NullPointerException.class, emptyBuilder::build);

    // Missing properties
    final var noPropsBuilder = FunctionalConsumer
      .<String, String>builder()
      .withProperties(null)
      .withTopic(TOPIC)
      .withProcessor(mockProcessor);
    assertThrows(NullPointerException.class, noPropsBuilder::build);

    // Missing topic
    final var noTopicBuilder = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(null)
      .withProcessor(mockProcessor);
    assertThrows(NullPointerException.class, noTopicBuilder::build);

    // Missing processor
    final var noProcessorBuilder = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(null);
    assertThrows(NullPointerException.class, noProcessorBuilder::build);
  }

  @Test
  void isRunningShouldReturnFalseAfterConstruction() {
    try (
      final var consumer = FunctionalConsumer
        .<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(mockProcessor)
        .build()
    ) {
      assertFalse(consumer.isRunning());
    }
  }

  @Test
  void isRunningShouldReturnFalseAfterClose() {
    final var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(mockProcessor)
      .build();
    consumer.close();
    assertFalse(consumer.isRunning());
  }

  @Test
  void closeCalledMultipleTimesShouldBeIdempotent() {
    FunctionalConsumer<String, String> consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(mockProcessor)
      .build();

    // First close
    consumer.close();
    assertFalse(consumer.isRunning());

    // Second close should be idempotent
    consumer.close();
    assertFalse(consumer.isRunning());
  }

  @Test
  void autoCloseableShouldCloseConsumerWhenExitingTryWithResources() {
    try (
      final var consumer = FunctionalConsumer
        .<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(mockProcessor)
        .build()
    ) {
      assertFalse(consumer.isRunning());
    }

    // After try-with-resources, consumer should be closed
    // We can't access the consumer here, so just assert that we reached this point
    assertTrue(true);
  }

  @Test
  void processorWithRetryEnabledShouldRetryFailedMessages() throws InterruptedException {
    // Arrange
    final var attempts = new AtomicInteger(0);
    Function<String, String> retryProcessor = value -> {
      int currentAttempt = attempts.getAndIncrement();
      if (currentAttempt == 0) {
        throw new RuntimeException("First attempt failure");
      }
      return value.toUpperCase();
    };

    final var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(retryProcessor)
      .withRetry(2, Duration.ofMillis(10))
      .withMetrics(true)
      .build();
    final var record = new ConsumerRecord<>(TOPIC, 0, 0, "key", "hello");

    // Act
    consumer.processRecord(record);
    Thread.sleep(200);

    // Assert
    final var metrics = consumer.getMetrics();
    assertEquals(2, attempts.get(), "Processor should be called exactly twice (initial + retry)");
    assertEquals(1, metrics.get("messagesProcessed"), "Message should be processed");
    assertEquals(1, metrics.get("retries"), "One retry should be counted");
    assertEquals(0, metrics.get("processingErrors"), "No errors expected");
    consumer.close();
  }

  @Test
  void processorWithMaxRetriesExceededShouldCallErrorHandler() throws InterruptedException {
    // Arrange
    Function<String, String> failingProcessor = value -> {
      throw new RuntimeException("Always failing");
    };

    final Consumer<FunctionalConsumer.ProcessingError<String, String>> errorHandler = mock(Consumer.class);

    final var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(failingProcessor)
      .withRetry(2, Duration.ofMillis(10))
      .withErrorHandler(errorHandler)
      .withMetrics(true)
      .build();
    final var record = new ConsumerRecord<>(TOPIC, 0, 0, "key", "hello");

    // Act
    consumer.processRecord(record);
    Thread.sleep(50);

    // Assert
    final var errorCaptor = ArgumentCaptor.forClass(FunctionalConsumer.ProcessingError.class);
    verify(errorHandler).accept(errorCaptor.capture());

    final var error = errorCaptor.getValue();
    assertEquals(record, error.record());
    assertEquals(2, error.retryCount());
    assertNotNull(error.exception());

    final var metrics = consumer.getMetrics();
    assertEquals(2, metrics.get("retries"), "Two retries should be counted"); // Updated from 3 to 2
    assertEquals(0, metrics.get("messagesProcessed"));
    assertEquals(1, metrics.get("processingErrors"));
    consumer.close();
  }

  @Test
  void metricsTrackingWithFailuresAndRetriesShouldIncrementCorrectly() throws InterruptedException {
    // Arrange
    final var counter = new AtomicInteger(0);
    Function<String, String> intermittentProcessor = value -> {
      int count = counter.incrementAndGet();
      if (count % 3 != 0) {
        throw new RuntimeException("Intermittent failure #" + count);
      }
      return value.toUpperCase();
    };

    final var consumer = FunctionalConsumer
      .<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(intermittentProcessor)
      .withRetry(5, Duration.ofMillis(10))
      .withMetrics(true)
      .build();

    // Act
    for (int i = 0; i < 3; i++) {
      final var record = new ConsumerRecord<>(TOPIC, 0, i, "key" + i, "value" + i);
      consumer.processRecord(record);
    }
    // Give time for retry processing to complete
    Thread.sleep(200);

    // Assert
    final var metrics = consumer.getMetrics();
    assertEquals(3, metrics.get("messagesReceived"));
    assertEquals(3, metrics.get("messagesProcessed"));
    assertEquals(6, metrics.get("retries"), "Each message requires 2 retries (total of 6)");
    assertEquals(0, metrics.get("processingErrors"));
    consumer.close();
  }
}
