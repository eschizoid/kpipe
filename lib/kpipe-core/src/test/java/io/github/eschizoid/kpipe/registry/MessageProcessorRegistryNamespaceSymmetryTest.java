package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.sink.MessageSink;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Asserts the two [MessageProcessorRegistry] namespaces expose the same seven operations —
/// register / get / unregister / clear / keys / all / metrics — and that each acts on its own
/// namespace only. A key registered on both sides must survive removal or clearing of the
/// other side untouched.
class MessageProcessorRegistryNamespaceSymmetryTest {

  private MessageProcessorRegistry registry;
  private RegistryKey<Object> key;

  @BeforeEach
  void setUp() {
    registry = new MessageProcessorRegistry();
    key = RegistryKey.of("shared", Object.class);
    final UnaryOperator<Object> operator = value -> value;
    final MessageSink<Object> sink = _ -> {};
    registry.registerOperator(key, operator);
    registry.registerSink(key, sink);
  }

  @Test
  void keysAreTrackedPerNamespace() {
    assertTrue(registry.getOperatorKeys().contains(key));
    assertTrue(registry.getSinkKeys().contains(key));
  }

  @Test
  void allViewsMirrorEachOther() {
    assertTrue(registry.getAllOperators().containsKey(key));
    assertTrue(registry.getAllSinks().containsKey(key));
    assertNotNull(registry.getAllOperators().get(key));
    assertNotNull(registry.getAllSinks().get(key));
  }

  @Test
  void allViewsAreUnmodifiable() {
    assertThrows(UnsupportedOperationException.class, () -> registry.getAllOperators().remove(key));
    assertThrows(UnsupportedOperationException.class, () -> registry.getAllSinks().remove(key));
  }

  @Test
  void metricsAreTrackedPerNamespace() {
    registry.getOperator(key).apply("value");
    registry.getSink(key).accept("value");

    assertEquals(1L, registry.getOperatorMetrics(key).get("invocationCount"));
    assertEquals(1L, registry.getSinkMetrics(key).get("invocationCount"));
  }

  @Test
  void metricsAreEmptyForUnknownKeys() {
    final var unknown = RegistryKey.of("unknown", Object.class);
    assertTrue(registry.getOperatorMetrics(unknown).isEmpty());
    assertTrue(registry.getSinkMetrics(unknown).isEmpty());
  }

  @Test
  void unregisterOperatorLeavesSinkNamespaceUntouched() {
    assertTrue(registry.unregisterOperator(key));
    assertFalse(registry.unregisterOperator(key), "second removal must report nothing removed");

    assertFalse(registry.getOperatorKeys().contains(key));
    assertTrue(registry.getSinkKeys().contains(key));
  }

  @Test
  void unregisterSinkLeavesOperatorNamespaceUntouched() {
    assertTrue(registry.unregisterSink(key));
    assertFalse(registry.unregisterSink(key), "second removal must report nothing removed");

    assertFalse(registry.getSinkKeys().contains(key));
    assertTrue(registry.getOperatorKeys().contains(key));
  }

  @Test
  void clearOperatorsLeavesSinkNamespaceUntouched() {
    registry.clearOperators();

    assertTrue(registry.getOperatorKeys().isEmpty());
    assertTrue(registry.getAllOperators().isEmpty());
    assertTrue(registry.getSinkKeys().contains(key));
  }

  @Test
  void clearSinksLeavesOperatorNamespaceUntouched() {
    registry.clearSinks();

    assertTrue(registry.getSinkKeys().isEmpty());
    assertTrue(registry.getAllSinks().isEmpty());
    assertTrue(registry.getOperatorKeys().contains(key));
  }
}
