package org.kpipe.format.protobuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProtobufFormatTest {

  private Descriptors.Descriptor descriptor;
  private ProtobufFormat format;

  @BeforeEach
  void setUp() throws Exception {
    descriptor = buildTestDescriptor();
    format = new ProtobufFormat(descriptor);
  }

  @Test
  void constructorRejectsNullDescriptor() {
    assertThrows(NullPointerException.class, () -> new ProtobufFormat(null));
  }

  @Test
  void descriptorAccessorReturnsBoundDescriptor() {
    assertSame(descriptor, format.descriptor());
  }

  @Test
  void consoleSinkIsNonNull() {
    assertNotNull(format.consoleSink());
  }

  @Test
  void shouldSerializeAndDeserializeRoundTrip() {
    final var original = DynamicMessage.newBuilder(descriptor)
      .setField(descriptor.findFieldByName("id"), 99L)
      .setField(descriptor.findFieldByName("name"), "RoundTrip")
      .build();

    final var bytes = format.serialize(original);
    assertNotNull(bytes);
    assertTrue(bytes.length > 0);

    final var deserialized = format.deserialize(bytes);
    assertNotNull(deserialized);

    final var desc = deserialized.getDescriptorForType();
    assertEquals(99L, deserialized.getField(desc.findFieldByName("id")));
    assertEquals("RoundTrip", deserialized.getField(desc.findFieldByName("name")));
  }

  @Test
  void shouldSerializeNullReturnsNull() {
    assertNull(format.serialize(null));
  }

  @Test
  void shouldDeserializeNullReturnsNull() {
    assertNull(format.deserialize(null));
  }

  @Test
  void shouldDeserializeEmptyReturnsNull() {
    assertNull(format.deserialize(new byte[0]));
  }

  @Test
  void shouldThrowOnInvalidProtobufBytes() {
    assertThrows(RuntimeException.class, () -> format.deserialize(new byte[] { (byte) 0xFF, (byte) 0xFF }));
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
}
