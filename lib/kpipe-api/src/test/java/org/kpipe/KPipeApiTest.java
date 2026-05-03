package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

/// Compile-time witness that the 5-line "hello world" example from the facade brief actually
/// builds. The pipeline is constructed up to (but not through) `start()` so the test can run
/// without a real Kafka broker; the operator chain is then exercised manually to prove the
/// transformations are wired correctly.
class KPipeApiTest {

  /// Mimic of the `Operators.removeFields(...)` helper that the brief references — kept inline so
  /// this test does not need a new public API.
  private static UnaryOperator<Map<String, Object>> removeFields(final String... fields) {
    return m -> {
      for (final var f : fields) m.remove(f);
      return m;
    };
  }

  @Test
  void fiveLineHelloWorldCompilesAndComposes() {
    final var props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "hello-world");

    // 5-line hello world (chain only — `.start()` would open a real KafkaConsumer).
    final Stream<Map<String, Object>> stream = KPipe.json("events", props)
      .pipe(msg -> {
        msg.put("ts", 42L);
        return msg;
      })
      .pipe(removeFields("password"));
    final Sink<Map<String, Object>> sink = stream.toConsole();
    assertNotNull(sink);

    // Drive the operator chain by hand to assert behavior.
    final var ds = (DefaultStream<Map<String, Object>>) stream;
    Map<String, Object> v = new HashMap<>();
    v.put("password", "secret");
    for (final var op : ds.operators()) v = op.apply(v);

    assertEquals(42L, v.get("ts"));
    assertTrue(!v.containsKey("password"), "removeFields should drop the password field");
  }
}
