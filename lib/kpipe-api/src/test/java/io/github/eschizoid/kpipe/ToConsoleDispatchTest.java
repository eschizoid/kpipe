package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import io.github.eschizoid.kpipe.format.avro.AvroConsoleSink;
import io.github.eschizoid.kpipe.format.avro.AvroFormat;
import io.github.eschizoid.kpipe.format.json.JsonConsoleSink;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufConsoleSink;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.sink.CompositeMessageSink;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

/// Verifies that `Stream<T>.toConsole()` dispatches to the format-appropriate sink type, and that
/// `toCustom` / `toMulti` produce the expected wrappers.
class ToConsoleDispatchTest {

  private static Properties props() {
    final var props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "test-group");
    return props;
  }

  @Test
  void jsonToConsoleDispatchesToJsonConsoleSink() {
    final var sink = (DefaultSink<?>) KPipe.json("t", props()).toConsole();
    assertInstanceOf(JsonConsoleSink.class, sink.terminalSink());
  }

  @Test
  void avroToConsoleDispatchesToAvroConsoleSink() {
    final var format = new AvroFormat(
      SchemaBuilder.record("Test").namespace("io.github.eschizoid.kpipe.test").fields().requiredString("id").endRecord()
    );

    final var sink = (DefaultSink<?>) KPipe.avro(format, "t", props()).toConsole();
    assertInstanceOf(AvroConsoleSink.class, sink.terminalSink());
  }

  @Test
  void protobufToConsoleDispatchesToProtobufConsoleSink() {
    final var format = new ProtobufFormat(buildTestDescriptor());

    final var sink = (DefaultSink<?>) KPipe.protobuf(format, "t", props()).toConsole();
    assertInstanceOf(ProtobufConsoleSink.class, sink.terminalSink());
  }

  @Test
  void bytesToConsoleProducesNonNullSink() {
    final var sink = (DefaultSink<?>) KPipe.bytes("t", props()).toConsole();
    assertNotNull(sink.terminalSink());
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

  private static Descriptors.Descriptor buildTestDescriptor() {
    try {
      final var msg = DescriptorProtos.DescriptorProto.newBuilder()
        .setName("Test")
        .addField(
          DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("id")
            .setNumber(1)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
            .build()
        )
        .build();
      final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
        .setName("test.proto")
        .setSyntax("proto3")
        .addMessageType(msg)
        .build();
      return Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]).findMessageTypeByName(
        "Test"
      );
    } catch (final Descriptors.DescriptorValidationException e) {
      throw new RuntimeException(e);
    }
  }
}
