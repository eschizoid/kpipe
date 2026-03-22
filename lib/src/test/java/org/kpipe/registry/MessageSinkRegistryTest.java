package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.sink.AvroConsoleSink;
import org.kpipe.sink.JsonConsoleSink;
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
    assertTrue(allSinks.containsKey(MessageSinkRegistry.AVRO_LOGGING));
    assertTrue(allSinks.containsKey(MessageSinkRegistry.JSON_LOGGING));
    assertTrue(allSinks.get(MessageSinkRegistry.AVRO_LOGGING).contains("AvroConsoleSink"));
    assertTrue(allSinks.get(MessageSinkRegistry.JSON_LOGGING).contains("JsonConsoleSink"));
  }

  @Test
  void shouldRegisterAndRetrieveSink() {
    // Arrange
    @SuppressWarnings("unchecked")
    final var testSink = mock(MessageSink.class);
    final var key = RegistryKey.<Object>of("testSink", Object.class);

    // Act
    registry.register(key, Object.class, testSink);
    final var retrieved = registry.get(key, Object.class);

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
    final var key = RegistryKey.<Object>of("sinkToRemove", Object.class);
    registry.register(key, Object.class, testSink);

    // Act & Assert
    assertTrue(registry.getAll().containsKey(key));
    final var removed = registry.unregister(key);

    assertTrue(removed);
    assertFalse(registry.getAll().containsKey(key));
  }

  @Test
  void shouldClearAllSinks() {
    // Arrange
    @SuppressWarnings("unchecked")
    final var testSink = mock(MessageSink.class);
    registry.register(RegistryKey.<Object>of("testSink", Object.class), Object.class, testSink);

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

    final var key1 = RegistryKey.<Object>of("sink1", Object.class);
    final var key2 = RegistryKey.<Object>of("sink2", Object.class);

    registry.register(key1, Object.class, sink1);
    registry.register(key2, Object.class, sink2);

    // Act
    final var pipeline = registry.pipeline(Object.class, key1, key2);
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

    final var keyFailing = RegistryKey.<Object>of("failingSink", Object.class);
    final var keyWorking = RegistryKey.<Object>of("workingSink", Object.class);

    registry.register(keyFailing, Object.class, failingSink);
    registry.register(keyWorking, Object.class, workingSink);

    // Act
    final var pipeline = registry.pipeline(Object.class, keyFailing, keyWorking);
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

    final var key = RegistryKey.<Object>of("countingSink", Object.class);
    registry.register(key, Object.class, countingSink);
    final var sink = registry.get(key, Object.class);

    // Act
    final var record = new ConsumerRecord<Object, Object>("topic", 0, 0, "key", "value");
    sink.send(record, "processed");
    sink.send(record, "processed");

    // Assert
    final var metrics = registry.getMetrics(key);
    assertEquals(2L, metrics.get("invocationCount"));
    assertEquals(0L, metrics.get("errorCount"));
  }

  @Test
  void shouldTrackErrorMetricsForFailingSink() {
    // Arrange
    final MessageSink<Object, Object> failingSink = (record, value) -> {
      throw new RuntimeException("Test failure");
    };

    final var key = RegistryKey.<Object>of("failingSink", Object.class);
    registry.register(key, Object.class, failingSink);
    final var sink = registry.get(key, Object.class);

    // Act
    final var record = new ConsumerRecord<Object, Object>("topic", 0, 0, "key", "value");
    try {
      sink.send(record, "processed");
      fail("Should have thrown an exception");
    } catch (final RuntimeException e) {
      // Expected
    }

    // Assert
    final var metrics = registry.getMetrics(key);
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
    final var metrics = registry.getMetrics(RegistryKey.<Object>of("nonExistentSink", Object.class));

    // Assert
    assertTrue(metrics.isEmpty());
  }

  @Test
  void shouldRejectNullOrEmptyName() {
    // Arrange
    @SuppressWarnings("unchecked")
    final var testSink = mock(MessageSink.class);

    // Assert
    assertThrows(NullPointerException.class, () -> registry.register(null, Object.class, testSink));
  }

  @Test
  void shouldRejectNullSink() {
    // Assert
    assertThrows(NullPointerException.class, () -> registry.register(RegistryKey.<Object>of("test", Object.class), Object.class, null));
  }

  @Test
  void shouldRegisterAndRetrieveTypedSink() {
    // Arrange
    final var key = RegistryKey.<String>of("typedSink", String.class);
    @SuppressWarnings("unchecked")
    final MessageSink<String, String> testSink = mock(MessageSink.class);
    registry.register(key, String.class, testSink);

    // Act
    final var retrieved = registry.get(key, String.class);
    final var record = new ConsumerRecord<String, String>("topic", 0, 0, "key", "value");
    retrieved.send(record, "processed");

    // Assert
    assertNotNull(retrieved);
    verify(testSink).send(record, "processed");
  }

  @Test
  void shouldThrowOnTypeMismatch() {
    // Arrange
    final var key = RegistryKey.<Integer>of("typeMismatchSink", Integer.class);
    registry.register(key, String.class, (record, value) -> {});

    // Act & Assert
    assertThrows(IllegalArgumentException.class, () -> registry.get(key, Integer.class));
  }
}
