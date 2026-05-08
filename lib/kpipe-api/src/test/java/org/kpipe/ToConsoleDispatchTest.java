package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Properties;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.kpipe.format.avro.AvroConsoleSink;
import org.kpipe.format.avro.AvroFormat;
import org.kpipe.format.json.JsonConsoleSink;
import org.kpipe.format.protobuf.ProtobufConsoleSink;
import org.kpipe.sink.CompositeMessageSink;
import org.kpipe.sink.MessageSink;

/// Verifies that `Stream<T>.toConsole()` dispatches to the format-appropriate sink type, and that
/// `toCustom` / `toMulti` produce the expected wrappers.
class ToConsoleDispatchTest {

  private static Properties props() {
    final var props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "test-group");
    return props;
  }

  /// Format → expected `toConsole()` sink type. `bytes` uses a lambda sink (no class to assert
  /// on), so its expected type is `null` and the test just verifies non-null.
  static java.util.stream.Stream<Arguments> consoleDispatchCases() {
    return java.util.stream.Stream.of(
      Arguments.of("json", (StreamFactory) KPipe::json, JsonConsoleSink.class),
      Arguments.of("avro", (StreamFactory) KPipe::avro, AvroConsoleSink.class),
      Arguments.of("protobuf", (StreamFactory) KPipe::protobuf, ProtobufConsoleSink.class),
      Arguments.of("bytes", (StreamFactory) KPipe::bytes, null)
    );
  }

  @ParameterizedTest(name = "{0}.toConsole() dispatches to expected sink")
  @MethodSource("consoleDispatchCases")
  void toConsoleDispatchesToFormatAppropriateSink(
    final String formatName,
    final StreamFactory factory,
    final Class<?> expectedSinkType
  ) {
    if ("avro".equals(formatName)) registerAvroSchema();
    try {
      final var sink = (DefaultSink<?>) factory.create("t", props()).toConsole();
      if (expectedSinkType == null) assertNotNull(sink.terminalSink());
      else assertTrue(expectedSinkType.isInstance(sink.terminalSink()), () ->
        "expected %s, got %s".formatted(expectedSinkType.getSimpleName(), sink.terminalSink().getClass())
      );
    } finally {
      if ("avro".equals(formatName)) AvroFormat.INSTANCE.clearSchemas();
    }
  }

  @Test
  void avroToConsoleThrowsWhenNoDefaultSchemaRegistered() {
    AvroFormat.INSTANCE.clearSchemas();
    final var stream = KPipe.avro("topic", props());
    final var ex = assertThrows(IllegalStateException.class, stream::toConsole);
    assertTrue(ex.getMessage().contains("Avro"), () -> "expected message to mention Avro: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("schema"), () -> "expected message to mention schema: " + ex.getMessage());
  }

  @Test
  void toCustomReturnsProvidedSink() {
    final MessageSink<Map<String, Object>> custom = _ -> {};
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("t", props()).toCustom(custom);
    assertSame(custom, sink.terminalSink());
  }

  @Test
  @SuppressWarnings("unchecked")
  void toMultiWrapsCompositeSink() {
    final MessageSink<Map<String, Object>> a = _ -> {};
    final MessageSink<Map<String, Object>> b = _ -> {};
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("t", props()).toMulti(a, b);
    assertInstanceOf(CompositeMessageSink.class, sink.terminalSink());
  }

  private static void registerAvroSchema() {
    AvroFormat.INSTANCE.clearSchemas();
    final var schema = SchemaBuilder.record("Test")
      .namespace("org.kpipe.test")
      .fields()
      .requiredString("id")
      .endRecord();
    AvroFormat.INSTANCE.addSchema("1", schema.toString());
  }

  @FunctionalInterface
  private interface StreamFactory {
    Stream<?> create(String topic, Properties props);
  }
}
