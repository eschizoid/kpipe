package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class RegistryFunctionsTest {

  @Test
  void shouldExecuteConsumerSuccessfully() {
    // Arrange
    final var counter = new AtomicLong(0);
    final Consumer<String> operation = _ -> counter.incrementAndGet();
    final var logger = mock(System.Logger.class);

    // Act
    final var safeConsumer = RegistryFunctions.withConsumerErrorHandling(operation, logger);
    safeConsumer.accept("test");

    // Assert
    assertEquals(1, counter.get());
    verify(logger, never()).log(any(System.Logger.Level.class), anyString());
    verify(logger, never()).log(any(System.Logger.Level.class), anyString(), any(Throwable.class));
  }

  @Test
  void shouldSuppressAndLogConsumerExceptions() {
    // Arrange
    final Consumer<String> operation = _ -> {
      throw new IllegalArgumentException("Test consumer exception");
    };
    final var logger = mock(System.Logger.class);

    // Act
    final var safeConsumer = RegistryFunctions.withConsumerErrorHandling(operation, logger);
    safeConsumer.accept("test");

    // Assert
    verify(logger, atLeastOnce()).log(any(System.Logger.Level.class), anyString(), any(Throwable.class));
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
