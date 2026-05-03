package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Message;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.kpipe.format.avro.AvroConsoleSink;
import org.kpipe.format.avro.AvroMessageProcessor;
import org.kpipe.format.json.JsonConsoleSink;
import org.kpipe.format.protobuf.ProtobufConsoleSink;
import org.kpipe.sink.MessageSink;

/// Verifies that `Stream<T>.toConsole()` dispatches to the format-appropriate sink type.
class ToConsoleDispatchTest {

  private static Properties props() {
    final var props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "test-group");
    return props;
  }

  @Test
  void jsonToConsoleProducesJsonConsoleSink() {
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("t", props()).toConsole();
    assertTrue(sink.terminalSink() instanceof JsonConsoleSink<?>);
  }

  @Test
  void avroToConsoleRequiresRegisteredSchema() {
    AvroMessageProcessor.clearSchemaRegistry();
    final var stream = KPipe.avro("t", props());
    assertThrows(IllegalStateException.class, stream::toConsole);
  }

  @Test
  void avroToConsoleSucceedsWithRegisteredSchema() {
    AvroMessageProcessor.clearSchemaRegistry();
    final var schema = SchemaBuilder.record("Test")
      .namespace("org.kpipe.test")
      .fields()
      .requiredString("id")
      .endRecord();
    AvroMessageProcessor.registerSchema("1", schema.toString());
    try {
      final var sink = (DefaultSink<GenericRecord>) KPipe.avro("t", props()).toConsole();
      assertTrue(sink.terminalSink() instanceof AvroConsoleSink<?>);
    } finally {
      AvroMessageProcessor.clearSchemaRegistry();
    }
  }

  @Test
  void protobufToConsoleProducesProtobufConsoleSink() {
    final var sink = (DefaultSink<Message>) KPipe.protobuf("t", props()).toConsole();
    assertTrue(sink.terminalSink() instanceof ProtobufConsoleSink<?>);
  }

  @Test
  void bytesToConsoleProducesNonNullSink() {
    final var sink = (DefaultSink<byte[]>) KPipe.bytes("t", props()).toConsole();
    assertNotNull(sink.terminalSink());
  }

  @Test
  void toCustomReturnsProvidedSink() {
    final MessageSink<Map<String, Object>> custom = _ -> {};
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("t", props()).toCustom(custom);
    assertTrue(sink.terminalSink() == custom);
  }

  @Test
  void toMultiWrapsCompositeSink() {
    final MessageSink<Map<String, Object>> a = _ -> {};
    final MessageSink<Map<String, Object>> b = _ -> {};
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("t", props()).toMulti(a, b);
    assertNotNull(sink.terminalSink());
    assertTrue(sink.terminalSink() instanceof org.kpipe.sink.CompositeMessageSink<?>);
  }

  /// Asserts that `KPipe.avro(...).toConsole()` fails fast when no default Avro schema has been
  /// registered under key `"1"` in [AvroMessageProcessor]. The schema registry is process-wide
  /// state, so we explicitly clear it before constructing the stream to make this test
  /// deterministic regardless of test ordering within the same JVM.
  @Test
  void avroToConsoleThrowsWhenNoDefaultSchemaRegistered() {
    AvroMessageProcessor.clearSchemaRegistry();
    final var stream = KPipe.avro("topic", props());
    final var ex = assertThrows(IllegalStateException.class, stream::toConsole);
    assertTrue(ex.getMessage().contains("Avro"), () -> "expected message to mention Avro: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("schema"), () -> "expected message to mention schema: " + ex.getMessage());
  }
}
