package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
    // Assert
    final var allSinks = registry.getAll();
    assertTrue(allSinks.containsKey("logging"));
    assertEquals("ConsoleSink", allSinks.get("logging"));
  }

  @Test
  void shouldRegisterAndRetrieveSink() {
    // Arrange
    @SuppressWarnings("unchecked")
    final var testSink = mock(MessageSink.class);

    // Act
    registry.register("testSink", testSink);
    final var retrieved = registry.get("testSink");

    // Assert
    assertNotNull(retrieved);

    final var record = new ConsumerRecord<Object, Object>("topic", 0, 0, "key", "value");
    retrieved.send(record, "processed");

    verify(testSink).send(record, "processed");
  }

  @Test
  void shouldUnregisterSink() {
    // Arrange
    @SuppressWarnings("unchecked")
    final var testSink = mock(MessageSink.class);
    registry.register("sinkToRemove", testSink);

    // Act & Assert
    assertTrue(registry.getAll().containsKey("sinkToRemove"));
    final var removed = registry.unregister("sinkToRemove");

    assertTrue(removed);
    assertFalse(registry.getAll().containsKey("sinkToRemove"));
  }

  @Test
  void shouldClearAllSinks() {
    // Arrange
    @SuppressWarnings("unchecked")
    final var testSink = mock(MessageSink.class);
    registry.register("testSink", testSink);

    // Act
    registry.clear();

    // Assert
    assertTrue(registry.getAll().isEmpty());
  }

  @Test
  void shouldCreatePipelineThatSendsToMultipleSinks() {
    // Arrange
    @SuppressWarnings("unchecked")
    final var sink1 = mock(MessageSink.class);
    @SuppressWarnings("unchecked")
    final var sink2 = mock(MessageSink.class);

    registry.register("sink1", sink1);
    registry.register("sink2", sink2);

    // Act
    final var pipeline = registry.pipeline("sink1", "sink2");
    final var record = new ConsumerRecord<Object, Object>("topic", 0, 0, "key", "value");
    pipeline.send(record, "processed");

    // Assert
    verify(sink1).send(record, "processed");
    verify(sink2).send(record, "processed");
  }

  @Test
  void shouldContinuePipelineWhenOneSinkThrowsException() {
    // Arrange
    @SuppressWarnings("unchecked")
    final var failingSink = mock(MessageSink.class);
    doThrow(new RuntimeException("Test failure")).when(failingSink).send(any(), any());

    @SuppressWarnings("unchecked")
    final var workingSink = mock(MessageSink.class);

    registry.register("failingSink", failingSink);
    registry.register("workingSink", workingSink);

    // Act
    final var pipeline = registry.pipeline("failingSink", "workingSink");
    final var record = new ConsumerRecord<Object, Object>("topic", 0, 0, "key", "value");
    pipeline.send(record, "processed");

    // Assert
    verify(workingSink).send(record, "processed");
  }

  @Test
  void shouldTrackMetricsForSink() {
    // Arrange
    final var callCount = new AtomicInteger(0);
    final MessageSink<Object, Object> countingSink = (record, value) -> callCount.incrementAndGet();

    registry.register("countingSink", countingSink);
    final var sink = registry.get("countingSink");

    // Act
    final var record = new ConsumerRecord<Object, Object>("topic", 0, 0, "key", "value");
    sink.send(record, "processed");
    sink.send(record, "processed");

    // Assert
    final var metrics = registry.getMetrics("countingSink");
    assertEquals(2L, metrics.get("invocationCount"));
    assertEquals(0L, metrics.get("errorCount"));
  }

  @Test
  void shouldTrackErrorMetricsForFailingSink() {
    // Arrange
    final MessageSink<Object, Object> failingSink = (record, value) -> {
      throw new RuntimeException("Test failure");
    };

    registry.register("failingSink", failingSink);
    final var sink = registry.get("failingSink");

    // Act
    final var record = new ConsumerRecord<Object, Object>("topic", 0, 0, "key", "value");
    try {
      sink.send(record, "processed");
      fail("Should have thrown an exception");
    } catch (final RuntimeException e) {
      // Expected
    }

    // Assert
    final var metrics = registry.getMetrics("failingSink");
    assertEquals(0L, metrics.get("invocationCount"));
    assertEquals(1L, metrics.get("errorCount"));
  }

  @Test
  void shouldWrapSinkWithErrorHandling() {
    // Arrange
    final MessageSink<Object, Object> failingSink = (record, value) -> {
      throw new RuntimeException("Test failure");
    };

    // Act
    final var safeSink = MessageSinkRegistry.withErrorHandling(failingSink);
    final var record = new ConsumerRecord<Object, Object>("topic", 0, 0, "key", "value");

    // Assert - should not throw exception
    safeSink.send(record, "processed");
  }

  @Test
  void shouldReturnEmptyMetricsForNonExistentSink() {
    // Act
    final var metrics = registry.getMetrics("nonExistentSink");

    // Assert
    assertTrue(metrics.isEmpty());
  }

  @Test
  void shouldRejectNullOrEmptyName() {
    // Arrange
    @SuppressWarnings("unchecked")
    final var testSink = mock(MessageSink.class);

    // Assert
    assertThrows(NullPointerException.class, () -> registry.register(null, testSink));
    assertThrows(IllegalArgumentException.class, () -> registry.register("", testSink));
    assertThrows(IllegalArgumentException.class, () -> registry.register("  ", testSink));
  }

  @Test
  void shouldRejectNullSink() {
    // Assert
    assertThrows(NullPointerException.class, () -> registry.register("test", null));
  }
}
