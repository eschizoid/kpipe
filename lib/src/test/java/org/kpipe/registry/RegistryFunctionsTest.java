package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class RegistryFunctionsTest {

  @Test
  void shouldCreateMetricsMapWithCorrectValues() {
    // Arrange
    final var operationCount = 10L;
    final var errorCount = 2L;
    final var totalTimeNs = 5000000L; // 5ms in nanoseconds

    // Act
    final var metrics = RegistryFunctions.createMetrics(operationCount, errorCount, totalTimeNs);

    // Assert
    assertEquals(operationCount, metrics.get("invocationCount"));
    assertEquals(errorCount, metrics.get("errorCount"));
    assertEquals(totalTimeNs / operationCount, metrics.get("averageProcessingTimeMs"));
  }

  @Test
  void shouldHandleZeroOperationsInMetrics() {
    // Arrange
    final var operationCount = 0L;
    final var errorCount = 0L;
    final var totalTimeNs = 0L;

    // Act
    final var metrics = RegistryFunctions.createMetrics(operationCount, errorCount, totalTimeNs);

    // Assert
    assertEquals(0L, metrics.get("invocationCount"));
    assertEquals(0L, metrics.get("errorCount"));
    assertEquals(0L, metrics.get("averageProcessingTimeMs"));
  }

  @Test
  void shouldReturnFunctionResultOnSuccess() {
    // Arrange
    final Function<String, Integer> operation = String::length;
    final var defaultValue = -1;
    final var logger = mock(System.Logger.class);

    // Act
    final var safeFunction = RegistryFunctions.withFunctionErrorHandling(operation, defaultValue, logger);
    final var result = safeFunction.apply("test");

    // Assert
    assertEquals(4, result);
    verify(logger, never()).log(any(System.Logger.Level.class), anyString());
    verify(logger, never()).log(any(System.Logger.Level.class), anyString(), any(Throwable.class));
  }

  @Test
  void shouldReturnDefaultValueOnFunctionError() {
    // Arrange
    final Function<String, Integer> operation = s -> {
      throw new RuntimeException("Test exception");
    };
    final var defaultValue = -1;
    final var logger = mock(System.Logger.class);

    // Act
    final var safeFunction = RegistryFunctions.withFunctionErrorHandling(operation, defaultValue, logger);
    final var result = safeFunction.apply("test");

    // Assert
    assertEquals(defaultValue, result);
    verify(logger, atLeastOnce()).log(any(System.Logger.Level.class), anyString(), any(Throwable.class));
  }

  @Test
  void shouldExecuteConsumerSuccessfully() {
    // Arrange
    final var counter = new AtomicLong(0);
    final BiConsumer<String, Integer> operation = (s, i) -> counter.incrementAndGet();
    final var logger = mock(System.Logger.class);

    // Act
    final var safeConsumer = RegistryFunctions.withConsumerErrorHandling(operation, logger);
    safeConsumer.accept("test", 42);

    // Assert
    assertEquals(1, counter.get());
    verify(logger, never()).log(any(System.Logger.Level.class), anyString());
    verify(logger, never()).log(any(System.Logger.Level.class), anyString(), any(Throwable.class));
  }

  @Test
  void shouldSuppressAndLogConsumerExceptions() {
    // Arrange
    final BiConsumer<String, Integer> operation = (s, i) -> {
      throw new IllegalArgumentException("Test consumer exception");
    };
    final var logger = mock(System.Logger.class);

    // Act
    final var safeConsumer = RegistryFunctions.withConsumerErrorHandling(operation, logger);
    safeConsumer.accept("test", 42);

    // Assert
    verify(logger, atLeastOnce()).log(any(System.Logger.Level.class), anyString(), any(Throwable.class));
  }

  @Test
  void shouldTrackMetricsOnSuccessfulExecution() {
    // Arrange
    final var counter = new AtomicLong(0);
    final var errorCounter = new AtomicLong(0);
    final var totalDurationNanos = new AtomicLong(0);

    // Act
    final var timedFunction = RegistryFunctions.timedExecution(
      counter::incrementAndGet,
      errorCounter::incrementAndGet,
      duration -> totalDurationNanos.addAndGet(duration.toNanos())
    );

    final var input = "test";
    final Function<String, Integer> operation = String::length;
    final var result = timedFunction.apply(input, (Function) operation);

    // Assert
    assertEquals(4, result);
    assertEquals(1, counter.get());
    assertEquals(0, errorCounter.get());
    assertTrue(totalDurationNanos.get() > 0);
  }

  @Test
  void shouldTrackErrorsOnFailedExecution() {
    // Arrange
    final var counter = new AtomicLong(0);
    final var errorCounter = new AtomicLong(0);
    final var totalDurationNanos = new AtomicLong(0);

    final var timedFunction = RegistryFunctions.timedExecution(
      counter::incrementAndGet,
      errorCounter::incrementAndGet,
      duration -> totalDurationNanos.addAndGet(duration.toNanos())
    );

    final var input = "test";
    final Function<String, Integer> operation = s -> {
      throw new RuntimeException("Test exception");
    };

    // Act & Assert
    assertThrows(RuntimeException.class, () -> timedFunction.apply(input, (Function) operation));
    assertEquals(0, counter.get());
    assertEquals(1, errorCounter.get());
  }

  @Test
  void shouldTransformRegistryValues() {
    // Arrange
    final var registry = new ConcurrentHashMap<String, TestEntry>();
    registry.put("key1", new TestEntry("value1"));
    registry.put("key2", new TestEntry("value2"));

    // Act
    final var result = RegistryFunctions.createUnmodifiableView(registry, entry -> ((TestEntry) entry).value());

    // Assert
    assertEquals(2, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value2", result.get("key2"));
  }

  @Test
  void shouldReturnEmptyMapForEmptyRegistry() {
    // Arrange
    final var registry = new ConcurrentHashMap<String, Object>();

    // Act
    final var result = RegistryFunctions.createUnmodifiableView(registry, Object::toString);

    // Assert
    assertTrue(result.isEmpty());
  }

  @Test
  void shouldReturnUnmodifiableMap() {
    // Arrange
    final var registry = new ConcurrentHashMap<String, String>();
    registry.put("key", "value");

    // Act
    final var result = RegistryFunctions.createUnmodifiableView(registry, s -> ((String) s).toUpperCase());

    // Assert
    assertThrows(UnsupportedOperationException.class, () -> result.put("newKey", "newValue"));
  }

  // Helper class for testing
  private record TestEntry(String value) {}
}
