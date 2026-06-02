package org.kpipe;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.kpipe.registry.MessageFormat;

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
    assertFalse(v.containsKey("password"), "removeFields should drop the password field");
  }

  /// Pins the [KPipe#custom(String, Properties, MessageFormat)] facade entry point. A
  /// user-supplied `MessageFormat<T>` should plug in alongside the bundled formats and produce a
  /// `Stream<T>` with the same fluent surface. "Zero callers in the repo" should never be enough
  /// to justify deletion — this test makes that explicit by exercising the bundled-format
  /// surface against a custom format.
  @Test
  void kpipeCustomGivesUserFormatsTheSameFacadeAsBundledFormats() {
    final var props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "custom-format-test");

    // A minimal custom MessageFormat that uppercases on deserialize and lowercases on serialize.
    // Real implementations would do something useful (a binary protocol, MessagePack, etc.).
    final MessageFormat<String> upperFormat = new MessageFormat<>() {
      @Override
      public String deserialize(final byte[] data) {
        return new String(data, StandardCharsets.UTF_8).toUpperCase(Locale.ROOT);
      }

      @Override
      public byte[] serialize(final String data) {
        return data.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8);
      }
    };

    final Stream<String> single = KPipe.custom("orders", props, upperFormat).pipe(s -> s + "!");
    final Sink<String> singleSink = single.toCustom(v -> {});
    assertNotNull(singleSink);

    final var ds = (DefaultStream<String>) single;
    // topics() returns a Set; assert set equality so iteration order doesn't make the test flaky.
    assertEquals(Set.of("orders"), ds.topics());
    assertSame(upperFormat, ds.format(), "Stream must carry the supplied MessageFormat through to DefaultStream");

    // Multi-topic overload exists and accepts a Collection of topics.
    final Stream<String> multi = KPipe.custom(List.of("orders", "refunds"), props, upperFormat);
    final var multiDs = (DefaultStream<String>) multi;
    assertEquals(Set.of("orders", "refunds"), multiDs.topics());
    assertSame(upperFormat, multiDs.format());
  }
}
