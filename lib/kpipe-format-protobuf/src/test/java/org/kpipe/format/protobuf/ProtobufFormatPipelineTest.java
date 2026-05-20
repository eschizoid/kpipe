package org.kpipe.format.protobuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.registry.MessageProcessorRegistry;

class ProtobufFormatPipelineTest {

  private Descriptors.Descriptor descriptor;
  private ProtobufFormat format;
  private MessageProcessorRegistry registry;

  @BeforeEach
  void setUp() throws Exception {
    descriptor = buildTestDescriptor();
    format = new ProtobufFormat(descriptor);
    registry = new MessageProcessorRegistry(format);
  }

  private DynamicMessage createTestMessage() {
    return DynamicMessage.newBuilder(descriptor)
      .setField(descriptor.findFieldByName("id"), 42L)
      .setField(descriptor.findFieldByName("name"), "Alice")
      .setField(descriptor.findFieldByName("email"), "alice@example.com")
      .setField(descriptor.findFieldByName("active"), true)
      .build();
  }

  @Test
  void shouldChainInlineOperatorsViaPipeline() {
    final UnaryOperator<Message> setName = msg ->
      msg.toBuilder().setField(msg.getDescriptorForType().findFieldByName("name"), "Processed").build();
    final UnaryOperator<Message> deactivate = msg ->
      msg.toBuilder().setField(msg.getDescriptorForType().findFieldByName("active"), false).build();

    registry.registerOperator(ProtobufRegistryKey.of("setName"), setName);
    registry.registerOperator(ProtobufRegistryKey.of("deactivate"), deactivate);

    final var pipeline = registry
      .pipeline(format)
      .add(ProtobufRegistryKey.of("setName"))
      .add(ProtobufRegistryKey.of("deactivate"))
      .build();

    final var input = format.serialize(createTestMessage());
    final var output = pipeline.apply(input);
    assertNotNull(output);

    final var result = format.deserialize(output);
    final var desc = result.getDescriptorForType();
    assertEquals("Processed", result.getField(desc.findFieldByName("name")));
    assertEquals(false, result.getField(desc.findFieldByName("active")));
    assertEquals(42L, result.getField(desc.findFieldByName("id")));
  }

  @Test
  void shouldHandleNullInputInPipeline() {
    final var pipeline = registry.pipeline(format).build();
    assertThrows(RuntimeException.class, () -> pipeline.apply(null));
  }

  @Test
  void shouldHandleEmptyInputInPipeline() {
    final var pipeline = registry.pipeline(format).build();
    assertThrows(RuntimeException.class, () -> pipeline.apply(new byte[0]));
  }

  private static Descriptors.Descriptor buildTestDescriptor() throws Descriptors.DescriptorValidationException {
    final var msg = DescriptorProtos.DescriptorProto.newBuilder()
      .setName("TestMessage")
      .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
      .addField(field("name", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
      .addField(field("email", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
      .addField(field("active", 4, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL))
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
