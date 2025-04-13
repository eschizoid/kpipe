package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MessageTrackerTest {

  private Map<String, Long> metrics;
  private MessageTracker tracker;

  @BeforeEach
  void setUp() {
    metrics = new HashMap<>();
    metrics.put("received", 0L);
    metrics.put("processed", 0L);
    metrics.put("errors", 0L);

    tracker =
      MessageTracker
        .builder()
        .withMetricsSupplier(() -> metrics)
        .withReceivedMetricKey("received")
        .withProcessedMetricKey("processed")
        .withErrorsMetricKey("errors")
        .build();
  }

  @Test
  void shouldCalculateInFlightMessagesCorrectly() {
    // Arrange
    metrics.put("received", 10L);
    metrics.put("processed", 6L);
    metrics.put("errors", 1L);

    // Act
    long inFlight = tracker.getInFlightMessageCount();
    boolean hasInFlight = tracker.hasInFlightMessages();
    boolean isComplete = tracker.isProcessingComplete();

    // Assert
    assertEquals(3, inFlight);
    assertTrue(hasInFlight);
    assertFalse(isComplete);
  }

  @Test
  void shouldHandleNullAndMissingMetrics() {
    // Arrange
    Map<String, Long> incompleteMetrics = new HashMap<>();
    incompleteMetrics.put("received", 5L);
    // processed and errors are missing

    // Act
    long inFlight = tracker.calculateInFlightCount(incompleteMetrics);

    // Assert
    assertEquals(5, inFlight);
  }

  @Test
  void shouldHandleNegativeCalculatedInFlight() {
    // Arrange
    metrics.put("received", 5L);
    metrics.put("processed", 6L); // More processed than received

    // Act
    long inFlight = tracker.getInFlightMessageCount();

    // Assert
    assertEquals(0, inFlight, "In-flight count should never be negative");
  }

  @Test
  void shouldDetectProcessingCompletion() {
    // Arrange
    metrics.put("received", 10L);
    metrics.put("processed", 10L);

    // Act
    boolean isComplete = tracker.isProcessingComplete();

    // Assert
    assertTrue(isComplete);
  }

  @Test
  void shouldWaitUntilProcessingComplete() throws InterruptedException {
    // Arrange
    final AtomicReference<Map<String, Long>> metricsRef = new AtomicReference<>(new HashMap<>());
    metricsRef.get().put("received", 10L);
    metricsRef.get().put("processed", 5L);
    metricsRef.get().put("errors", 0L);

    MessageTracker dynamicTracker = MessageTracker
      .builder()
      .withMetricsSupplier(metricsRef::get)
      .withReceivedMetricKey("received")
      .withProcessedMetricKey("processed")
      .withErrorsMetricKey("errors")
      .build();

    // Simulate completion after delay
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      try {
        Thread.sleep(200);
        final Map<String, Long> updatedMetrics = new HashMap<>();
        updatedMetrics.put("received", 10L);
        updatedMetrics.put("processed", 10L);
        updatedMetrics.put("errors", 0L);
        metricsRef.set(updatedMetrics);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    // Act
    Optional<Boolean> result = dynamicTracker.waitForCompletion(1000);
    executor.shutdown();

    // Assert
    assertTrue(result.isPresent());
    assertTrue(result.get());
  }

  @Test
  void shouldTimeoutIfProcessingNotComplete() {
    // Arrange
    metrics.put("received", 10L);
    metrics.put("processed", 5L);

    // Act
    Optional<Boolean> result = tracker.waitForCompletion(100);

    // Assert
    assertTrue(result.isPresent());
    assertFalse(result.get());
  }

  @Test
  void shouldRequireAllBuilderProperties() {
    // Arrange & Act & Assert
    assertThrows(
      NullPointerException.class,
      () ->
        MessageTracker
          .builder()
          .withReceivedMetricKey("received")
          .withProcessedMetricKey("processed")
          .withErrorsMetricKey("errors")
          .build(),
      "Should require metricsSupplier"
    );

    assertThrows(
      NullPointerException.class,
      () ->
        MessageTracker
          .builder()
          .withMetricsSupplier(HashMap::new)
          .withProcessedMetricKey("processed")
          .withErrorsMetricKey("errors")
          .build(),
      "Should require receivedMetricKey"
    );

    assertThrows(
      NullPointerException.class,
      () ->
        MessageTracker
          .builder()
          .withMetricsSupplier(HashMap::new)
          .withReceivedMetricKey("received")
          .withErrorsMetricKey("errors")
          .build(),
      "Should require processedMetricKey"
    );

    assertThrows(
      NullPointerException.class,
      () ->
        MessageTracker
          .builder()
          .withMetricsSupplier(HashMap::new)
          .withReceivedMetricKey("received")
          .withProcessedMetricKey("processed")
          .build(),
      "Should require errorsMetricKey"
    );
  }
}
