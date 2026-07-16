package io.github.eschizoid.kpipe.format.protobuf;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Protobuf edge cases: oversized payloads, truncated wire bytes mid-field, and the 6-byte
/// single-message Confluent envelope boundary driven through the real built pipeline. An
/// off-by-one on the envelope strips the wrong bytes and corrupts every downstream field.
class ProtobufFormatEdgeCasesTest {

  private Descriptors.Descriptor descriptor;
  private ProtobufFormat format;
  private MessageProcessorRegistry registry;

  @BeforeEach
  void setUp() throws Exception {
    descriptor = buildTestDescriptor();
    format = new ProtobufFormat(descriptor);
    registry = new MessageProcessorRegistry();
  }

  /// Drives `bytes` through deserialize -> process -> serialize, re-throwing a failure cause.
  private static <T> byte[] roundTrip(final MessagePipeline<T> pipeline, final byte[] bytes) {
    final var deserialized = pipeline.deserializeOrFail(bytes);
    return switch (pipeline.process(deserialized)) {
      case Result.Passed<T> p -> pipeline.serialize(p.value());
      case Result.Filtered<T> _ -> null;
      case Result.Failed<T> f -> {
        if (f.cause() instanceof RuntimeException re) throw re;
        if (f.cause() instanceof Error err) throw err;
        throw new RuntimeException(f.cause());
      }
    };
  }

  /// Prepends the 6-byte single-message Confluent envelope: 1-byte magic 0x00 + 4-byte
  /// big-endian schema id + 1-byte message-index header (0x00 for the first message).
  private static byte[] withEnvelope(final int schemaId, final byte[] payload) {
    final var framed = new byte[6 + payload.length];
    framed[0] = 0x00;
    framed[1] = (byte) ((schemaId >>> 24) & 0xff);
    framed[2] = (byte) ((schemaId >>> 16) & 0xff);
    framed[3] = (byte) ((schemaId >>> 8) & 0xff);
    framed[4] = (byte) (schemaId & 0xff);
    framed[5] = 0x00;
    System.arraycopy(payload, 0, framed, 6, payload.length);
    return framed;
  }

  private DynamicMessage message(final long id, final String name) {
    return DynamicMessage.newBuilder(descriptor)
      .setField(descriptor.findFieldByName("id"), id)
      .setField(descriptor.findFieldByName("name"), name)
      .build();
  }

  @Test
  void oversizedPayloadRoundTrips() {
    final var big = "z".repeat(2_000_000);
    final var bytes = format.serialize(message(1L, big));

    final var pipeline = registry.pipeline(format).build();
    final var result = roundTrip(pipeline, bytes);

    assertNotNull(result);
    final var decoded = format.deserialize(result);
    assertEquals(big, decoded.getField(descriptor.findFieldByName("name")));
  }

  @Test
  void truncatedFieldBytesThrow() {
    // Tag byte for field 2 (name, wire type 2 = length-delimited) says "10 bytes follow" but
    // the buffer ends early. A partial decode must throw, not return a half-built message.
    final var truncated = new byte[] { 0x12, 0x0a, 'A', 'l', 'i' };
    assertThrows(IllegalStateException.class, () -> format.deserialize(truncated));
  }

  @Test
  void skipBytesSixStripsTheEnvelopeAndDecodesCleanly() {
    final var payload = format.serialize(message(7L, "framed"));
    final var framed = withEnvelope(99, payload);

    final var pipeline = registry.pipeline(format).skipBytes(6).build();
    final var decoded = pipeline.deserializeOrFail(framed);

    assertNotNull(decoded);
    assertEquals(7L, decoded.getField(descriptor.findFieldByName("id")));
    assertEquals("framed", decoded.getField(descriptor.findFieldByName("name")));
  }

  @Test
  void offByOneSkipFiveCorruptsOrFails() {
    // Leaving the message-index byte in front shifts the protobuf tag parse — corruption.
    final var payload = format.serialize(message(7L, "framed"));
    final var framed = withEnvelope(99, payload);

    final var pipeline = registry.pipeline(format).skipBytes(5).build();

    var corrupted = false;
    try {
      final var decoded = pipeline.deserializeOrFail(framed);
      corrupted =
        decoded == null ||
        !"framed".equals(decoded.getField(descriptor.findFieldByName("name"))) ||
        !Long.valueOf(7L).equals(decoded.getField(descriptor.findFieldByName("id")));
    } catch (final RuntimeException e) {
      // A throw is an acceptable corruption signal, but require the protobuf decoder's own
      // failure — not an unrelated error that would green this test vacuously.
      assertTrue(
        e.getMessage() != null && e.getMessage().contains("ProtobufFormat.deserialize failed"),
        () -> "expected a protobuf decode failure, got: " + e
      );
      corrupted = true;
    }
    assertTrue(corrupted, "skip(5) must not decode to the clean message");
  }

  @Test
  void emptyMessageSerializesToZeroLengthAndDeserializeTreatsItAsNull() {
    // A proto3 message with all-default fields encodes to zero bytes. Serialize yields an
    // empty array; the empty array is the null/empty deserialize case, not a parse error.
    final var empty = DynamicMessage.newBuilder(descriptor).build();
    final var bytes = format.serialize(empty);

    assertArrayEquals(new byte[0], bytes, "an all-default proto3 message encodes to zero bytes");
    assertNull(format.deserialize(bytes));
  }

  private static Descriptors.Descriptor buildTestDescriptor() throws Descriptors.DescriptorValidationException {
    final var msg = DescriptorProtos.DescriptorProto.newBuilder()
      .setName("TestMessage")
      .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
      .addField(field("name", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
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

  private static DescriptorProtos.FieldDescriptorProto field(
    final String name,
    final int number,
    final DescriptorProtos.FieldDescriptorProto.Type type
  ) {
    return DescriptorProtos.FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type).build();
  }
}
