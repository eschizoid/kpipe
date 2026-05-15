package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.sink.MessageSink;

/// Sink-namespace tests for [MessageProcessorRegistry]. The operator namespace is exercised
/// elsewhere (format-specific tests in each `kpipe-format-*` module).
class MessageProcessorRegistrySinksTest {

  private MessageProcessorRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new MessageProcessorRegistry();
  }

  @Test
  void shouldStartEmpty() {
    assertTrue(registry.getAllSinks().isEmpty());
  }

  @Test
  void shouldRegisterAndRetrieveSink() {
    final var testSink = mock(MessageSink.class);
    final var key = RegistryKey.of("testSink", Object.class);

    registry.register(key, testSink);
    final var retrieved = registry.getSink(key);

    assertNotNull(retrieved);
    retrieved.accept("processed");
    verify(testSink).accept("processed");
  }

  @Test
  void shouldUnregisterSink() {
    final var testSink = mock(MessageSink.class);
    final var key = RegistryKey.<Object>of("sinkToRemove", Object.class);
    registry.register(key, testSink);

    assertTrue(registry.getAllSinks().containsKey(key));
    final var removed = registry.unregisterSink(key);

    assertTrue(removed);
    assertFalse(registry.getAllSinks().containsKey(key));
  }

  @Test
  void shouldClearAllSinks() {
    final var testSink = mock(MessageSink.class);
    registry.register(RegistryKey.of("testSink", Object.class), testSink);

    registry.clearSinks();

    assertTrue(registry.getAllSinks().isEmpty());
  }

  @Test
  void shouldNotClearSinksWhenClearingOperators() {
    final var testSink = mock(MessageSink.class);
    final var sinkKey = RegistryKey.of("aSink", Object.class);
    registry.register(sinkKey, testSink);

    registry.clear(); // operator-side only

    assertTrue(registry.getAllSinks().containsKey(sinkKey), "clear() must not touch the sink namespace");
  }

  @Test
  void shouldDispatchCompositeSinkToEveryRegisteredKey() {
    final var sink1 = mock(MessageSink.class);
    final var sink2 = mock(MessageSink.class);

    final var key1 = RegistryKey.of("sink1", Object.class);
    final var key2 = RegistryKey.of("sink2", Object.class);

    registry.register(key1, sink1);
    registry.register(key2, sink2);

    registry.compositeSink(key1, key2).accept("processed");

    verify(sink1).accept("processed");
    verify(sink2).accept("processed");
  }

  @Test
  void shouldKeepDeliveringWhenOneCompositeSinkThrows() {
    final var failingSink = mock(MessageSink.class);
    doThrow(new RuntimeException("Test failure")).when(failingSink).accept(any());

    final var workingSink = mock(MessageSink.class);

    final var keyFailing = RegistryKey.of("failingSink", Object.class);
    final var keyWorking = RegistryKey.of("workingSink", Object.class);

    registry.register(keyFailing, failingSink);
    registry.register(keyWorking, workingSink);

    registry.compositeSink(keyFailing, keyWorking).accept("processed");

    verify(workingSink).accept("processed");
  }

  @Test
  void shouldTrackMetricsForSink() {
    final var callCount = new java.util.concurrent.atomic.AtomicInteger(0);
    final MessageSink<Object> countingSink = value -> callCount.incrementAndGet();

    final var key = RegistryKey.of("countingSink", Object.class);
    registry.register(key, countingSink);
    final var sink = registry.getSink(key);

    sink.accept("processed");
    sink.accept("processed");

    final var metrics = registry.getSinkMetrics(key);
    assertEquals(2L, metrics.get("invocationCount"));
    assertEquals(0L, metrics.get("errorCount"));
  }

  @Test
  void shouldTrackErrorMetricsForFailingSink() {
    final MessageSink<Object> failingSink = value -> {
      throw new RuntimeException("Test failure");
    };

    final var key = RegistryKey.of("failingSink", Object.class);
    registry.register(key, failingSink);
    final var sink = registry.getSink(key);

    try {
      sink.accept("processed");
      fail("Should have thrown an exception");
    } catch (final RuntimeException e) {
      // Expected
    }

    final var metrics = registry.getSinkMetrics(key);
    assertEquals(1L, metrics.get("errorCount"));
  }

  @Test
  void shouldWrapSinkWithErrorHandling() {
    final MessageSink<Object> failingSink = value -> {
      throw new RuntimeException("Test failure");
    };

    final var safeSink = MessageProcessorRegistry.withErrorHandling(failingSink);

    safeSink.accept("processed"); // must not throw
  }

  @Test
  void shouldReturnEmptyMetricsForNonExistentSink() {
    final var metrics = registry.getSinkMetrics(RegistryKey.of("nonExistent", Object.class));
    assertTrue(metrics.isEmpty());
  }

  @Test
  void shouldRejectNullKey() {
    final var testSink = mock(MessageSink.class);
    assertThrows(NullPointerException.class, () -> registry.register(null, testSink));
  }

  @Test
  void shouldRejectNullSink() {
    assertThrows(NullPointerException.class, () ->
      registry.register(RegistryKey.<Object>of("test", Object.class), (MessageSink<Object>) null)
    );
  }

  @Test
  void shouldRegisterAndRetrieveTypedSink() {
    final var key = RegistryKey.<String>of("typedSink", String.class);
    @SuppressWarnings("unchecked")
    final MessageSink<String> testSink = mock(MessageSink.class);
    registry.register(key, testSink);

    final var retrieved = registry.getSink(key);
    retrieved.accept("processed");

    verify(testSink).accept("processed");
  }

  @Test
  void shouldThrowOnTypeMismatch() {
    final var key = RegistryKey.<String>of("typedSink", String.class);
    registry.register(key, (MessageSink<String>) msg -> {});

    assertThrows(ClassCastException.class, () -> {
      @SuppressWarnings("unchecked")
      final MessageSink<Integer> retrieved = (MessageSink<Integer>) (MessageSink<?>) registry.getSink(key);
      retrieved.accept(123);
    });
  }
}
