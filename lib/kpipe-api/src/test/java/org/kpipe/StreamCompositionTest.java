package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

/// Verifies the behavioral semantics of `pipe`, `filter`, `peek`, and `when` operators
/// composed via the facade. The pipeline is exercised by manually applying the accumulated
/// operators in order; this avoids spinning up a Kafka consumer while still proving that the
/// facade builds the correct logical pipeline.
class StreamCompositionTest {

  private static Properties props() {
    final var props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "test-group");
    return props;
  }

  private static Map<String, Object> apply(
    final DefaultStream<Map<String, Object>> stream,
    final Map<String, Object> input
  ) {
    Map<String, Object> v = input;
    for (final var op : stream.operators()) {
      v = op.apply(v);
      if (v == null) return null;
    }
    return v;
  }

  @Test
  void pipeAppendsTransformations() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props());
    stream.pipe(m -> {
      m.put("a", 1);
      return m;
    });
    stream.pipe(m -> {
      m.put("b", 2);
      return m;
    });

    final var result = apply(stream, new HashMap<>());
    assertEquals(1, result.get("a"));
    assertEquals(2, result.get("b"));
  }

  @Test
  void filterDropsMessagesByReturningNull() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props());
    stream.filter(m -> "active".equals(m.get("status")));

    final Map<String, Object> active = new HashMap<>();
    active.put("status", "active");
    final Map<String, Object> inactive = new HashMap<>();
    inactive.put("status", "inactive");

    assertSame(active, apply(stream, active));
    assertNull(apply(stream, inactive));
  }

  @Test
  void peekRunsSideEffectAndPassesThrough() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props());
    final var calls = new AtomicInteger();
    stream.peek(_ -> calls.incrementAndGet());

    final Map<String, Object> input = new HashMap<>();
    final var result = apply(stream, input);
    assertSame(input, result);
    assertEquals(1, calls.get());
  }

  @Test
  void whenSelectsBranchByPredicate() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props());
    final UnaryOperator<Map<String, Object>> markTrue = m -> {
      m.put("path", "true");
      return m;
    };
    final UnaryOperator<Map<String, Object>> markFalse = m -> {
      m.put("path", "false");
      return m;
    };
    stream.when(m -> Boolean.TRUE.equals(m.get("flag")), markTrue, markFalse);

    final Map<String, Object> a = new HashMap<>();
    a.put("flag", true);
    assertEquals("true", apply(stream, a).get("path"));

    final Map<String, Object> b = new HashMap<>();
    b.put("flag", false);
    assertEquals("false", apply(stream, b).get("path"));
  }

  @Test
  void mixedCompositionPreservesOrder() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props());
    stream
      .pipe(m -> {
        m.put("step1", true);
        return m;
      })
      .filter(m -> Boolean.TRUE.equals(m.get("step1")))
      .peek(m -> m.put("peeked", true))
      .pipe(m -> {
        m.put("step2", true);
        return m;
      });

    final var result = apply(stream, new HashMap<>());
    assertTrue((Boolean) result.get("step1"));
    assertTrue((Boolean) result.get("peeked"));
    assertTrue((Boolean) result.get("step2"));
  }

  @Test
  void filterShortCircuitsRemainingOperators() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props());
    final var afterFilterCalls = new AtomicInteger();

    stream
      .filter(_ -> false)
      .pipe(m -> {
        afterFilterCalls.incrementAndGet();
        return m;
      });

    assertNull(apply(stream, new HashMap<>()));
    assertEquals(0, afterFilterCalls.get());
  }
}
