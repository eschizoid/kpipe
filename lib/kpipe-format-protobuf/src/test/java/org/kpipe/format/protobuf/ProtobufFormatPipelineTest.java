package org.kpipe.format.protobuf;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.registry.MessageProcessorRegistry;

class ProtobufFormatPipelineTest {

  private Descriptors.Descriptor descriptor;
  private MessageProcessorRegistry registry;

  @BeforeEach
  void setUp() throws Exception {
    descriptor = buildTestDescriptor();
    registry = new MessageProcessorRegistry(ProtobufFormat.INSTANCE);

    final var protoFormat = ProtobufFormat.INSTANCE;
    protoFormat.addDescriptor("test", descriptor);
    protoFormat.withDefaultDescriptor("test");
  }

  @AfterEach
  void tearDown() {
    ProtobufFormat.INSTANCE.clearSchemas();
    ProtobufFormat.INSTANCE.clearSchemas();
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
  void shouldRegisterAndGetDescriptor() {
    final var retrieved = ProtobufFormat.INSTANCE.getDescriptor("test");
    assertNotNull(retrieved);
    assertEquals("TestMessage", retrieved.getName());
  }

  @Test
  void shouldReturnNullForUnregisteredDescriptor() {
    assertNull(ProtobufFormat.INSTANCE.getDescriptor("nonexistent"));
  }

  @Test
  void shouldClearDescriptorRegistry() {
    assertNotNull(ProtobufFormat.INSTANCE.getDescriptor("test"));
    ProtobufFormat.INSTANCE.clearSchemas();
    assertNull(ProtobufFormat.INSTANCE.getDescriptor("test"));
  }

  @Test
  void shouldChainInlineOperatorsViaPipeline() {
    // Users now write inline lambdas using the native Protobuf builder API
    // rather than relying on operator helpers.
    final UnaryOperator<Message> setName = msg ->
      msg.toBuilder().setField(msg.getDescriptorForType().findFieldByName("name"), "Processed").build();
    final UnaryOperator<Message> deactivate = msg ->
      msg.toBuilder().setField(msg.getDescriptorForType().findFieldByName("active"), false).build();

    registry.register(ProtobufRegistryKey.of("setName"), setName);
    registry.register(ProtobufRegistryKey.of("deactivate"), deactivate);

    final var pipeline = registry
      .pipeline(ProtobufFormat.INSTANCE)
      .add(ProtobufRegistryKey.of("setName"))
      .add(ProtobufRegistryKey.of("deactivate"))
      .build();

    final var input = ProtobufFormat.INSTANCE.serialize(createTestMessage());
    final var output = pipeline.apply(input);
    assertNotNull(output);

    final var result = ProtobufFormat.INSTANCE.deserialize(output);
    final var desc = result.getDescriptorForType();
    assertEquals("Processed", result.getField(desc.findFieldByName("name")));
    assertEquals(false, result.getField(desc.findFieldByName("active")));
    assertEquals(42L, result.getField(desc.findFieldByName("id")));
  }

  @Test
  void shouldHandleNullInputInPipeline() {
    // Per MessagePipeline contract, null input is a contract violation; throw rather than swallow.
    final var pipeline = registry.pipeline(ProtobufFormat.INSTANCE).build();
    assertThrows(RuntimeException.class, () -> pipeline.apply(null));
  }

  @Test
  void shouldHandleEmptyInputInPipeline() {
    // Empty bytes are not a valid Protobuf message; throw rather than swallow.
    final var pipeline = registry.pipeline(ProtobufFormat.INSTANCE).build();
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
