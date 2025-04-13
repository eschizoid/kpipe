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
  void constructor_WithValidParameters_ShouldNotThrowException() {
    assertDoesNotThrow(() -> new FunctionalConsumer<>(properties, TOPIC, mockProcessor));
    assertDoesNotThrow(() -> new FunctionalConsumer<>(properties, TOPIC, mockProcessor, POLL_TIMEOUT));
  }

  @Test
  void constructor_WithNullParameters_ShouldThrowNullPointerException() {
    assertThrows(NullPointerException.class, () -> new FunctionalConsumer<>(null, TOPIC, mockProcessor));
    assertThrows(NullPointerException.class, () -> new FunctionalConsumer<>(properties, null, mockProcessor));
    assertThrows(NullPointerException.class, () -> new FunctionalConsumer<>(properties, TOPIC, null));
    assertThrows(NullPointerException.class, () -> new FunctionalConsumer<>(properties, TOPIC, mockProcessor, null));
  }

  @Test
  void isRunningShouldReturnTrueAfterConstruction() {
    try (final var consumer = new FunctionalConsumer<>(properties, TOPIC, mockProcessor)) {
      assertTrue(consumer.isRunning());
    }
  }

  @Test
  void isRunningShouldReturnFalseAfterClose() {
    final var consumer = new FunctionalConsumer<>(properties, TOPIC, mockProcessor);
    consumer.close();
    assertFalse(consumer.isRunning());
  }

  @Test
  void closeCalledMultipleTimesShouldBeIdempotent() {
    final var consumer = new FunctionalConsumer<>(properties, TOPIC, mockProcessor);
    assertTrue(consumer.isRunning());
    consumer.close();
    assertFalse(consumer.isRunning());
    // Second close should not change state
    consumer.close();
    assertFalse(consumer.isRunning());
  }

  @Test
  void autoCloseableShouldCloseConsumerWhenExitingTryWithResources() {
    final FunctionalConsumer<String, String> consumer;
    try (FunctionalConsumer<String, String> c = new FunctionalConsumer<>(properties, TOPIC, mockProcessor)) {
      consumer = c;
      assertTrue(consumer.isRunning());
    }
    assertFalse(consumer.isRunning());
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

    final var consumer = new FunctionalConsumer.Builder<String, String>()
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

    final var consumer = new FunctionalConsumer.Builder<String, String>()
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

    final var consumer = new FunctionalConsumer.Builder<String, String>()
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
