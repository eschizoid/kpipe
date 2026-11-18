package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class RegistryFunctionsTest {

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
