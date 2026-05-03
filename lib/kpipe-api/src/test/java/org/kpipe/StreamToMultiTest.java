package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.kpipe.sink.CompositeMessageSink;
import org.kpipe.sink.MessageSink;

/// Verifies the fan-out semantics of `Stream<T>.toMulti(MessageSink<T>... sinks)`.
///
/// These tests do not call `start()` (which would require a Kafka broker). Instead, the
/// composition is exercised by reading the package-private accessor `DefaultSink<T>.terminalSink()`
/// and invoking the resulting [CompositeMessageSink] directly.
class StreamToMultiTest {

  private static Properties props() {
    final var props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "test-group");
    return props;
  }

  @Test
  void toMultiDeliversToEverySink() {
    final var captured1 = new CopyOnWriteArrayList<Map<String, Object>>();
    final var captured2 = new CopyOnWriteArrayList<Map<String, Object>>();
    final var captured3 = new CopyOnWriteArrayList<Map<String, Object>>();
    final MessageSink<Map<String, Object>> a = captured1::add;
    final MessageSink<Map<String, Object>> b = captured2::add;
    final MessageSink<Map<String, Object>> c = captured3::add;

    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props()).toMulti(a, b, c);
    assertTrue(sink.terminalSink() instanceof CompositeMessageSink<?>);

    final var payload = new HashMap<String, Object>();
    payload.put("k", "v");
    sink.terminalSink().accept(payload);

    assertEquals(List.of(payload), captured1);
    assertEquals(List.of(payload), captured2);
    assertEquals(List.of(payload), captured3);
    assertSame(payload, captured1.getFirst());
    assertSame(payload, captured2.getFirst());
    assertSame(payload, captured3.getFirst());
  }

  @Test
  void toMultiThrowingSinkDoesNotBlockOthers() {
    final var captured1 = new CopyOnWriteArrayList<Map<String, Object>>();
    final var captured2 = new CopyOnWriteArrayList<Map<String, Object>>();
    final MessageSink<Map<String, Object>> throwing = _ -> {
      throw new RuntimeException("boom");
    };
    final MessageSink<Map<String, Object>> a = captured1::add;
    final MessageSink<Map<String, Object>> b = captured2::add;

    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props()).toMulti(throwing, a, b);
    assertTrue(sink.terminalSink() instanceof CompositeMessageSink<?>);

    final var payload = new HashMap<String, Object>();
    payload.put("event", "ok");
    // CompositeMessageSink wraps each sink call in its own try/catch and logs failures
    // without rethrowing, so subsequent sinks must still receive the value.
    sink.terminalSink().accept(payload);

    assertEquals(1, captured1.size());
    assertEquals(1, captured2.size());
    assertSame(payload, captured1.getFirst());
    assertSame(payload, captured2.getFirst());
  }

  @Test
  void toMultiWithEmptyVarargsThrows() {
    final var stream = KPipe.json("topic", props());
    assertThrows(IllegalArgumentException.class, stream::toMulti);
  }

  @Test
  @SuppressWarnings("unchecked")
  void toMultiWithNullVarargsThrows() {
    final var stream = KPipe.json("topic", props());
    assertThrows(NullPointerException.class, () -> stream.toMulti((MessageSink<Map<String, Object>>[]) null));
  }
}
