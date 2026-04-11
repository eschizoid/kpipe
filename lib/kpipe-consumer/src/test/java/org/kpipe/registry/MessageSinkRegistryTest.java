package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.sink.MessageSink;

class MessageSinkRegistryTest {

  private MessageSinkRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new MessageSinkRegistry();
  }

  @Test
  void shouldHaveDefaultConsoleSink() {
    // Act
    final var allSinks = registry.getAll();

    // Assert
    assertTrue(allSinks.containsKey(MessageSinkRegistry.AVRO_LOGGING));
    assertTrue(allSinks.containsKey(MessageSinkRegistry.JSON_LOGGING));
    assertTrue(allSinks.get(MessageSinkRegistry.AVRO_LOGGING).contains("AvroConsoleSink"));
    assertTrue(allSinks.get(MessageSinkRegistry.JSON_LOGGING).contains("JsonConsoleSink"));
  }

  @Test
  void shouldRegisterAndRetrieveSink() {
    // Arrange
    final var testSink = mock(MessageSink.class);
    final var key = RegistryKey.of("testSink", Object.class);

    // Act
    registry.register(key, testSink);
    final var retrieved = registry.get(key);

    // Assert
    assertNotNull(retrieved);
    retrieved.accept("processed");
    verify(testSink).accept("processed");
  }

  @Test
  void shouldUnregisterSink() {
    // Arrange
    final var testSink = mock(MessageSink.class);
    final var key = RegistryKey.<Object>of("sinkToRemove", Object.class);
    registry.register(key, testSink);

    // Act & Assert
    assertTrue(registry.getAll().containsKey(key));
    final var removed = registry.unregister(key);

    assertTrue(removed);
    assertFalse(registry.getAll().containsKey(key));
  }

  @Test
  void shouldClearAllSinks() {
    // Arrange
    final var testSink = mock(MessageSink.class);
    registry.register(RegistryKey.of("testSink", Object.class), testSink);

    // Act
    registry.clear();

    // Assert
    assertTrue(registry.getAll().isEmpty());
  }

  @Test
  void shouldCreatePipelineThatSendsToMultipleSinks() {
    // Arrange
    final var sink1 = mock(MessageSink.class);
    final var sink2 = mock(MessageSink.class);

    final var key1 = RegistryKey.of("sink1", Object.class);
    final var key2 = RegistryKey.of("sink2", Object.class);

    registry.register(key1, sink1);
    registry.register(key2, sink2);

    // Act
    final var pipeline = registry.pipeline(key1, key2);
    pipeline.accept("processed");

    // Assert
    verify(sink1).accept("processed");
    verify(sink2).accept("processed");
  }

  @Test
  void shouldContinuePipelineWhenOneSinkThrowsException() {
    // Arrange
    final var failingSink = mock(MessageSink.class);
    doThrow(new RuntimeException("Test failure")).when(failingSink).accept(any());

    final var workingSink = mock(MessageSink.class);

    final var keyFailing = RegistryKey.of("failingSink", Object.class);
    final var keyWorking = RegistryKey.of("workingSink", Object.class);

    registry.register(keyFailing, failingSink);
    registry.register(keyWorking, workingSink);

    // Act
    final var pipeline = registry.pipeline(keyFailing, keyWorking);
    pipeline.accept("processed");

    // Assert
    verify(workingSink).accept("processed");
  }

  @Test
  void shouldTrackMetricsForSink() {
    // Arrange
    final var callCount = new java.util.concurrent.atomic.AtomicInteger(0);
    final MessageSink<Object> countingSink = value -> callCount.incrementAndGet();

    final var key = RegistryKey.of("countingSink", Object.class);
    registry.register(key, countingSink);
    final var sink = registry.get(key);

    // Act
    sink.accept("processed");
    sink.accept("processed");

    // Assert
    final var metrics = registry.getMetrics(key);
    assertEquals(2L, metrics.get("invocationCount"));
    assertEquals(0L, metrics.get("errorCount"));
  }

  @Test
  void shouldTrackErrorMetricsForFailingSink() {
    // Arrange
    final MessageSink<Object> failingSink = value -> {
      throw new RuntimeException("Test failure");
    };

    final var key = RegistryKey.of("failingSink", Object.class);
    registry.register(key, failingSink);
    final var sink = registry.get(key);

    // Act
    try {
      sink.accept("processed");
      fail("Should have thrown an exception");
    } catch (final RuntimeException e) {
      // Expected
    }

    // Assert
    final var metrics = registry.getMetrics(key);
    assertEquals(1L, metrics.get("errorCount"));
  }

  @Test
  void shouldWrapSinkWithErrorHandling() {
    // Arrange
    final MessageSink<Object> failingSink = value -> {
      throw new RuntimeException("Test failure");
    };

    // Act
    final var safeSink = MessageSinkRegistry.withErrorHandling(failingSink);

    // Assert - should not throw exception
    safeSink.accept("processed");
  }

  @Test
  void shouldReturnEmptyMetricsForNonExistentSink() {
    // Act
    final var metrics = registry.getMetrics(RegistryKey.of("nonExistent", Object.class));

    // Assert
    assertTrue(metrics.isEmpty());
  }

  @Test
  void shouldRejectNullOrEmptyName() {
    // Arrange
    final var testSink = mock(MessageSink.class);

    // Assert
    assertThrows(NullPointerException.class, () -> registry.register(null, testSink));
  }

  @Test
  void shouldRejectNullSink() {
    // Assert
    assertThrows(NullPointerException.class, () ->
      registry.register(RegistryKey.<Object>of("test", Object.class), null)
    );
  }

  @Test
  void shouldRegisterAndRetrieveTypedSink() {
    // Arrange
    final var key = RegistryKey.<String>of("typedSink", String.class);
    @SuppressWarnings("unchecked")
    final MessageSink<String> testSink = mock(MessageSink.class);
    registry.register(key, testSink);

    // Act
    final var retrieved = registry.get(key);
    retrieved.accept("processed");

    // Assert
    verify(testSink).accept("processed");
  }

  @Test
  void shouldThrowOnTypeMismatch() {
    // Arrange
    final var key = RegistryKey.<String>of("typedSink", String.class);
    registry.register(key, msg -> {});

    // Act & Assert
    assertThrows(ClassCastException.class, () -> {
      final MessageSink<Integer> retrieved = (MessageSink<Integer>) (MessageSink<?>) registry.get(key);
      retrieved.accept(123);
    });
  }
}
