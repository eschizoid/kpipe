package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kpipe.metrics.MetricsReporter;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConsumerRunnerTest {

  @Mock
  private FunctionalConsumer<String, String> mockConsumer;

  @Mock
  private MessageTracker mockTracker;

  @Mock
  private MetricsReporter mockReporter;

  private ConsumerRunner<FunctionalConsumer<String, String>> runner;

  @Test
  void shouldStartConsumer() {
    // Arrange
    when(mockConsumer.isRunning()).thenReturn(true);
    runner =
      ConsumerRunner
        .builder(mockConsumer)
        .withHealthCheck(FunctionalConsumer::isRunning) // Use the isRunning method in health check
        .build();

    // Act
    runner.start();

    // Assert
    verify(mockConsumer).start();
    assertTrue(runner.isHealthy()); // This will use the stubbed isRunning method
  }

  @Test
  void shouldHandleStartException() {
    // Arrange
    doThrow(new RuntimeException("Start failed")).when(mockConsumer).start();
    runner = ConsumerRunner.builder(mockConsumer).build();

    // Act & Assert
    assertThrows(RuntimeException.class, () -> runner.start());
    assertFalse(runner.isHealthy());
  }

  @Test
  void shouldCheckHealthCorrectly() {
    // Arrange
    final var healthCheckResult = new AtomicBoolean(true);
    runner = ConsumerRunner.builder(mockConsumer).withHealthCheck(c -> healthCheckResult.get()).build();
    runner.start();

    // Act & Assert - Initially healthy
    assertTrue(runner.isHealthy());

    // Act & Assert - Becomes unhealthy
    healthCheckResult.set(false);
    assertFalse(runner.isHealthy());
  }

  @Test
  void shouldShutdownGracefully() {
    // Arrange
    when(mockConsumer.createMessageTracker()).thenReturn(mockTracker);
    when(mockTracker.getInFlightMessageCount()).thenReturn(0L);
    runner = ConsumerRunner.builder(mockConsumer).build();
    runner.start();

    // Act
    final var result = runner.shutdownGracefully(1000);

    // Assert
    assertTrue(result);
    verify(mockConsumer).pause();
    verify(mockConsumer).createMessageTracker();
    verify(mockConsumer).close();
  }

  @Test
  void shouldWaitForInFlightMessagesOnShutdown() {
    // Arrange
    when(mockConsumer.createMessageTracker()).thenReturn(mockTracker);
    when(mockTracker.getInFlightMessageCount()).thenReturn(5L).thenReturn(0L);
    when(mockTracker.waitForCompletion(anyLong())).thenReturn(Optional.of(true));
    runner = ConsumerRunner.builder(mockConsumer).build();
    runner.start();

    // Act
    final var result = runner.shutdownGracefully(1000);

    // Assert
    assertTrue(result);
    verify(mockTracker).waitForCompletion(anyLong());
  }

  @Test
  void shouldTimeoutWhenInFlightMessagesRemain() {
    // Arrange
    when(mockConsumer.createMessageTracker()).thenReturn(mockTracker);
    when(mockTracker.getInFlightMessageCount()).thenReturn(5L).thenReturn(3L);
    when(mockTracker.waitForCompletion(anyLong())).thenReturn(Optional.of(false));
    runner = ConsumerRunner.builder(mockConsumer).build();
    runner.start();

    // Act
    final var result = runner.shutdownGracefully(1000);

    // Assert
    assertFalse(result);
  }

  @Test
  void shouldUseCustomGracefulShutdown() {
    // Arrange
    final var customShutdownCalled = new AtomicBoolean(false);
    final BiFunction<FunctionalConsumer<String, String>, Long, Boolean> customShutdown = (consumer, timeout) -> {
      customShutdownCalled.set(true);
      return true;
    };

    runner = ConsumerRunner.builder(mockConsumer).withGracefulShutdown(customShutdown).build();
    runner.start();

    // Act
    final var result = runner.shutdownGracefully(1000);

    // Assert
    assertTrue(result);
    assertTrue(customShutdownCalled.get());
  }

  @Test
  void shouldNotStartTwice() {
    // Arrange
    runner = ConsumerRunner.builder(mockConsumer).build();
    runner.start();
    reset(mockConsumer); // Reset to verify no more calls

    // Act
    runner.start();

    // Assert
    verifyNoInteractions(mockConsumer);
  }

  @Test
  void shouldCloseConsumerWhenClosed() {
    // Arrange
    runner = ConsumerRunner.builder(mockConsumer).build();
    runner.start();

    // Act
    runner.close();

    // Assert
    verify(mockConsumer).close();
    assertFalse(runner.isHealthy());
  }

  @Test
  void shouldHandleMultipleCloses() {
    // Arrange
    runner = ConsumerRunner.builder(mockConsumer).build();
    runner.start();
    runner.close();
    reset(mockConsumer);

    // Act
    runner.close();

    // Assert
    verifyNoInteractions(mockConsumer);
  }

  @Test
  void shouldAwaitShutdownSuccessfully() throws Exception {
    // Arrange
    when(mockConsumer.createMessageTracker()).thenReturn(mockTracker);
    when(mockTracker.getInFlightMessageCount()).thenReturn(0L);

    runner = ConsumerRunner.builder(mockConsumer).build();
    runner.start();

    // Create a thread to close the runner after a delay
    final var closeThread = new Thread(() -> {
      try {
        Thread.sleep(100);
        runner.close();
      } catch (Exception e) {
        // Ignore
      }
    });

    // Act
    closeThread.start();
    final var result = runner.awaitShutdown(1000);

    // Assert
    assertTrue(result);
  }

  @Test
  void shouldTimeoutWhenAwaitingShutdown() throws Exception {
    // Arrange
    runner = ConsumerRunner.builder(mockConsumer).build();
    runner.start();

    // Act
    final var result = runner.awaitShutdown(100);

    // Assert
    assertFalse(result);
  }

  @Test
  void shouldApplyCustomShutdownTimeout() {
    // Arrange
    final var customTimeout = 5000L;
    final var timeoutCaptured = new AtomicBoolean(false);
    final BiFunction<FunctionalConsumer<String, String>, Long, Boolean> timeoutCapturingShutdown = (
      consumer,
      timeout
    ) -> {
      timeoutCaptured.set(timeout == customTimeout);
      return true;
    };

    runner =
      ConsumerRunner
        .builder(mockConsumer)
        .withShutdownTimeout(customTimeout)
        .withGracefulShutdown(timeoutCapturingShutdown)
        .build();
    runner.start();

    // Act
    runner.shutdownGracefully(customTimeout);

    // Assert
    assertTrue(timeoutCaptured.get());
  }

  @Test
  void performGracefulConsumerShutdownShouldHandleNoInFlightMessages() {
    // Arrange
    when(mockConsumer.createMessageTracker()).thenReturn(mockTracker);
    when(mockTracker.getInFlightMessageCount()).thenReturn(0L);

    // Act
    final var result = ConsumerRunner.performGracefulConsumerShutdown(mockConsumer, 1000);

    // Assert
    assertTrue(result);
    verify(mockConsumer).pause();
    verify(mockTracker).getInFlightMessageCount();
    verify(mockTracker, never()).waitForCompletion(anyLong());
  }

  @Test
  void performGracefulConsumerShutdownShouldHandleInFlightMessages() {
    // Arrange
    when(mockConsumer.createMessageTracker()).thenReturn(mockTracker);
    when(mockTracker.getInFlightMessageCount()).thenReturn(5L).thenReturn(0L);
    when(mockTracker.waitForCompletion(anyLong())).thenReturn(Optional.of(true));

    // Act
    final var result = ConsumerRunner.performGracefulConsumerShutdown(mockConsumer, 1000);

    // Assert
    assertTrue(result);
    verify(mockConsumer).pause();
    verify(mockTracker, times(2)).getInFlightMessageCount();
    verify(mockTracker).waitForCompletion(anyLong());
  }

  @Test
  void performGracefulConsumerShutdownShouldHandlePartiallyProcessedMessages() {
    // Arrange
    when(mockConsumer.createMessageTracker()).thenReturn(mockTracker);
    when(mockTracker.getInFlightMessageCount()).thenReturn(5L).thenReturn(2L);
    when(mockTracker.waitForCompletion(anyLong())).thenReturn(Optional.of(false));

    // Act
    final var result = ConsumerRunner.performGracefulConsumerShutdown(mockConsumer, 1000);

    // Assert
    assertFalse(result);
    verify(mockConsumer).pause();
    verify(mockTracker, times(2)).getInFlightMessageCount();
    verify(mockTracker).waitForCompletion(anyLong());
    verify(mockConsumer).close();
  }

  @Test
  void performGracefulConsumerShutdownShouldHandleExceptionFromTracker() {
    // Arrange
    when(mockConsumer.createMessageTracker()).thenReturn(mockTracker);
    when(mockTracker.getInFlightMessageCount()).thenThrow(new RuntimeException("Tracker failure"));

    // Act
    final var result = ConsumerRunner.performGracefulConsumerShutdown(mockConsumer, 1000);

    // Assert
    assertFalse(result); // Expect false when an exception occurs
    verify(mockConsumer).close(); // Should still close consumer even when exception occurs
  }

  @Test
  void performGracefulConsumerShutdownShouldHandleEmptyCompletionResult() {
    // Arrange
    when(mockConsumer.createMessageTracker()).thenReturn(mockTracker);
    when(mockTracker.getInFlightMessageCount()).thenReturn(5L).thenReturn(3L);
    when(mockTracker.waitForCompletion(anyLong())).thenReturn(Optional.empty());

    // Act
    final var result = ConsumerRunner.performGracefulConsumerShutdown(mockConsumer, 1000);

    // Assert
    assertFalse(result);
    verify(mockTracker).waitForCompletion(anyLong());
    verify(mockConsumer).close();
  }

  @Test
  void performGracefulConsumerShutdownShouldHandleNullTracker() {
    // Arrange
    when(mockConsumer.createMessageTracker()).thenReturn(null);

    // Act
    final var result = ConsumerRunner.performGracefulConsumerShutdown(mockConsumer, 1000);

    // Assert
    assertTrue(result);
    verify(mockConsumer).pause();
    verify(mockConsumer).close();
  }

  @Test
  void shouldUseCustomStartAction() {
    // Arrange
    final var customStartActionCalled = new AtomicBoolean(false);
    final Consumer<FunctionalConsumer<String, String>> customStartAction = consumer -> {
      customStartActionCalled.set(true);
      consumer.start();
    };

    runner = ConsumerRunner.builder(mockConsumer).withStartAction(customStartAction).build();

    // Act
    runner.start();

    // Assert
    assertTrue(customStartActionCalled.get());
    verify(mockConsumer).start();
  }

  @Test
  void shouldAddShutdownHook() {
    // This test verifies that a shutdown hook is added when requested
    // Note: We can't directly test the Runtime shutdown hook registration,
    // but we can verify the behavior through reflection or functional testing

    // Arrange & Act - Create runner with shutdown hook
    runner = ConsumerRunner.builder(mockConsumer).withShutdownHook(true).build();
    // No direct assertion possible - this is mostly for coverage
    // The actual shutdown hook behavior would be tested in integration tests
  }

  @Test
  void shouldStartMetricsThreadWithReporters() throws Exception {
    // Arrange
    final long metricsInterval = 100; // Short interval for testing
    runner =
      ConsumerRunner
        .builder(mockConsumer)
        .withMetricsReporters(List.of(mockReporter))
        .withMetricsInterval(metricsInterval)
        .build();

    // Act
    runner.start();

    // Wait a bit to allow metrics thread to execute at least once
    Thread.sleep(metricsInterval * 2);

    // Assert
    verify(mockReporter, atLeastOnce()).reportMetrics();

    // Cleanup
    runner.close();
  }

  @Test
  void shouldNotStartMetricsThreadWithoutReporters() {
    // Arrange
    runner =
      ConsumerRunner
        .builder(mockConsumer)
        .withMetricsReporters(List.of()) // Empty list
        .withMetricsInterval(100)
        .build();

    // Act
    runner.start();
    // Assert - No exceptions means thread didn't start
    // This is primarily a coverage test
  }

  @Test
  void shouldNotStartMetricsThreadWithNegativeInterval() {
    // Arrange
    runner =
      ConsumerRunner
        .builder(mockConsumer)
        .withMetricsReporters(List.of(mockReporter))
        .withMetricsInterval(-1) // Negative interval
        .build();

    // Act
    runner.start();

    // Assert - No metrics reporting should happen
    verifyNoInteractions(mockReporter);
  }

  @Test
  void shouldStopMetricsThreadOnShutdown() throws Exception {
    // Arrange
    final long metricsInterval = 100;
    runner =
      ConsumerRunner
        .builder(mockConsumer)
        .withMetricsReporters(List.of(mockReporter))
        .withMetricsInterval(metricsInterval)
        .build();

    runner.start();

    // Wait to ensure metrics thread is running
    Thread.sleep(metricsInterval * 2);

    // Reset to clear previous interactions
    reset(mockReporter);

    // Act
    runner.close();

    // Wait to ensure the metrics thread would have reported if still running
    Thread.sleep(metricsInterval * 2);

    // Assert - No more metrics reports after close
    verifyNoInteractions(mockReporter);
  }

  @Test
  void shouldHandleErrorsInMetricsReporting() throws Exception {
    // Arrange
    final long metricsInterval = 100;
    doThrow(new RuntimeException("Metrics error")).when(mockReporter).reportMetrics();

    runner =
      ConsumerRunner
        .builder(mockConsumer)
        .withMetricsReporters(List.of(mockReporter))
        .withMetricsInterval(metricsInterval)
        .build();

    // Act
    runner.start();

    // Wait to ensure metrics thread executes
    Thread.sleep(metricsInterval * 2);

    // Assert - The thread should continue running despite errors
    verify(mockReporter, atLeastOnce()).reportMetrics();

    // Cleanup
    runner.close();
  }

  @Test
  void shouldSupportCustomConfigurationThroughWithMethod() {
    // Arrange
    final var configFunctionCalled = new AtomicBoolean(false);

    // Act
    runner =
      ConsumerRunner
        .builder(mockConsumer)
        .with(builder -> {
          configFunctionCalled.set(true);
          return builder.withShutdownTimeout(2000);
        })
        .build();

    // Assert
    assertTrue(configFunctionCalled.get());
  }

  @Test
  void shouldUseDefaultHealthCheckWhenNotSpecified() {
    // Arrange
    runner = ConsumerRunner.builder(mockConsumer).build();
    runner.start();

    // Act & Assert
    assertTrue(runner.isHealthy());
  }

  @Test
  void shouldNotBeHealthyWhenNotStarted() {
    // Arrange
    runner = ConsumerRunner.builder(mockConsumer).build();

    // Act & Assert - Consumer not started
    assertFalse(runner.isHealthy());
  }
}
