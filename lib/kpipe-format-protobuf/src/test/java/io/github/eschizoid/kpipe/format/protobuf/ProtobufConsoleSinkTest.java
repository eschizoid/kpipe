package io.github.eschizoid.kpipe.format.protobuf;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProtobufConsoleSinkTest {

  private ProtobufConsoleSink<Object> sink;
  private CapturingHandler handler;
  private Logger julLogger;

  @BeforeEach
  void setUp() {
    sink = new ProtobufConsoleSink<>();
    handler = new CapturingHandler();
    julLogger = Logger.getLogger(ProtobufConsoleSink.class.getName());
    julLogger.addHandler(handler);
    julLogger.setUseParentHandlers(false);
  }

  @AfterEach
  void tearDown() {
    julLogger.removeHandler(handler);
    julLogger.setUseParentHandlers(true);
  }

  private String output() {
    return handler.toString();
  }

  @Test
  void shouldLogProcessedMessageKey() {
    sink.accept("value1");
    assertTrue(output().contains("processedMessage"), "Expected processedMessage key in log output");
  }

  @Test
  void shouldOutputValidJsonStructure() {
    sink.accept("my-value");
    final var out = output();
    assertTrue(out.contains("\"processedMessage\":\"my-value\""), "Expected processedMessage value");
  }

  @Test
  void shouldHandleNullValue() {
    sink.accept(null);
    assertTrue(output().contains("null"), "Expected 'null' in log output");
  }

  @Test
  void shouldFormatProtobufMessageAsJson() throws Exception {
    final var descriptor = buildTestDescriptor();
    final var message = DynamicMessage.newBuilder(descriptor)
      .setField(descriptor.findFieldByName("id"), 42L)
      .setField(descriptor.findFieldByName("name"), "TestUser")
      .build();

    sink.accept(message);
    final var out = output();
    assertTrue(out.contains("42"), "Expected id value in JSON output");
    assertTrue(out.contains("TestUser"), "Expected name value in JSON output");
  }

  @Test
  void shouldFormatProtobufMessageWithDefaultValues() throws Exception {
    final var descriptor = buildTestDescriptor();
    final var message = DynamicMessage.newBuilder(descriptor).build();

    sink.accept(message);
    final var out = output();
    assertTrue(out.contains("processedMessage"), "Expected processedMessage key");
  }

  @Test
  void shouldHandlePlainStringValue() {
    sink.accept("plain text");
    assertTrue(output().contains("plain text"), "Expected plain text in output");
  }

  @Test
  void shouldHandleLargeMessage() throws Exception {
    final var descriptor = buildTestDescriptor();
    final var largeName = "x".repeat(10_000);
    final var message = DynamicMessage.newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), largeName)
      .build();

    sink.accept(message);
    assertTrue(output().contains("x"), "Expected large message content in output");
  }

  private static Descriptors.Descriptor buildTestDescriptor() throws Descriptors.DescriptorValidationException {
    final var msg = DescriptorProtos.DescriptorProto.newBuilder()
      .setName("TestMessage")
      .addField(
        DescriptorProtos.FieldDescriptorProto.newBuilder()
          .setName("id")
          .setNumber(1)
          .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
          .build()
      )
      .addField(
        DescriptorProtos.FieldDescriptorProto.newBuilder()
          .setName("name")
          .setNumber(2)
          .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
          .build()
      )
      .build();

    final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
      .setName("test.proto")
      .setPackage("test")
      .setSyntax("proto3")
      .addMessageType(msg)
      .build();

    return Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]).findMessageTypeByName(
      "TestMessage"
    );
  }

  /// JUL [Handler] that appends each [LogRecord]'s raw message to an in-memory buffer.
  /// No formatting, no parameter substitution, no separator — assertions just look for
  /// substrings in the concatenated output.
  private static final class CapturingHandler extends Handler {

    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    @Override
    public void publish(final LogRecord record) {
      if (record.getMessage() != null) {
        buffer.writeBytes(record.getMessage().getBytes(StandardCharsets.UTF_8));
      }
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    @Override
    public String toString() {
      return buffer.toString(StandardCharsets.UTF_8);
    }
  }
}
