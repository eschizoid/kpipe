package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.github.eschizoid.kpipe.sink.MessageSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    registry.registerSink(key, testSink);
    final var retrieved = registry.getSink(key);

    assertNotNull(retrieved);
    retrieved.accept("processed");
    verify(testSink).accept("processed");
  }

  @Test
  void shouldUnregisterSink() {
    final var testSink = mock(MessageSink.class);
    final var key = RegistryKey.of("sinkToRemove", Object.class);
    registry.registerSink(key, testSink);

    assertTrue(registry.getAllSinks().containsKey(key));
    final var removed = registry.unregisterSink(key);

    assertTrue(removed);
    assertFalse(registry.getAllSinks().containsKey(key));
  }

  @Test
  void shouldClearAllSinks() {
    final var testSink = mock(MessageSink.class);
    registry.registerSink(RegistryKey.of("testSink", Object.class), testSink);

    registry.clearSinks();

    assertTrue(registry.getAllSinks().isEmpty());
  }

  @Test
  void shouldNotClearSinksWhenClearingOperators() {
    final var testSink = mock(MessageSink.class);
    final var sinkKey = RegistryKey.of("aSink", Object.class);
    registry.registerSink(sinkKey, testSink);

    registry.clear(); // operator-side only

    assertTrue(registry.getAllSinks().containsKey(sinkKey), "clear() must not touch the sink namespace");
  }

  @Test
  void shouldDispatchCompositeSinkToEveryRegisteredKey() {
    final var sink1 = mock(MessageSink.class);
    final var sink2 = mock(MessageSink.class);

    final var key1 = RegistryKey.of("sink1", Object.class);
    final var key2 = RegistryKey.of("sink2", Object.class);

    registry.registerSink(key1, sink1);
    registry.registerSink(key2, sink2);

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

    registry.registerSink(keyFailing, failingSink);
    registry.registerSink(keyWorking, workingSink);

    registry.compositeSink(keyFailing, keyWorking).accept("processed");

    verify(workingSink).accept("processed");
  }

  @Test
  void shouldTrackMetricsForSink() {
    final var callCount = new java.util.concurrent.atomic.AtomicInteger(0);
    final MessageSink<Object> countingSink = _ -> callCount.incrementAndGet();

    final var key = RegistryKey.of("countingSink", Object.class);
    registry.registerSink(key, countingSink);
    final var sink = registry.getSink(key);

    sink.accept("processed");
    sink.accept("processed");

    final var metrics = registry.getSinkMetrics(key);
    assertEquals(2L, metrics.get("invocationCount"));
    assertEquals(0L, metrics.get("errorCount"));
  }

  @Test
  void shouldTrackErrorMetricsForFailingSink() {
    final MessageSink<Object> failingSink = _ -> {
      throw new RuntimeException("Test failure");
    };

    final var key = RegistryKey.of("failingSink", Object.class);
    registry.registerSink(key, failingSink);
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
    final MessageSink<Object> failingSink = _ -> {
      throw new RuntimeException("Test failure");
    };

    final var safeSink = MessageProcessorRegistry.withSinkErrorHandling(failingSink);

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
    assertThrows(NullPointerException.class, () -> registry.registerSink(null, testSink));
  }

  @Test
  void shouldRejectNullSink() {
    assertThrows(NullPointerException.class, () -> registry.registerSink(RegistryKey.of("test", Object.class), null));
  }

  @Test
  void shouldRegisterAndRetrieveTypedSink() {
    final var key = RegistryKey.of("typedSink", String.class);
    @SuppressWarnings("unchecked")
    final MessageSink<String> testSink = mock(MessageSink.class);
    registry.registerSink(key, testSink);

    final var retrieved = registry.getSink(key);
    retrieved.accept("processed");

    verify(testSink).accept("processed");
  }

  @Test
  void shouldThrowOnTypeMismatch() {
    final var key = RegistryKey.of("typedSink", String.class);
    registry.registerSink(key, _ -> {});

    assertThrows(ClassCastException.class, () -> {
      @SuppressWarnings("unchecked")
      final MessageSink<Integer> retrieved = (MessageSink<Integer>) (MessageSink<?>) registry.getSink(key);
      retrieved.accept(123);
    });
  }
}
