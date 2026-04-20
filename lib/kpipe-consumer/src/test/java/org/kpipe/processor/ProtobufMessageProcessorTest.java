package org.kpipe.processor;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.RegistryKey;

class ProtobufMessageProcessorTest {

  private Descriptors.Descriptor descriptor;
  private MessageProcessorRegistry registry;

  @BeforeEach
  void setUp() throws Exception {
    descriptor = buildTestDescriptor();
    registry = new MessageProcessorRegistry("test-app", MessageFormat.PROTOBUF);

    final var protoFormat = MessageFormat.PROTOBUF;
    protoFormat.addDescriptor("test", descriptor);
    protoFormat.withDefaultDescriptor("test");
  }

  @AfterEach
  void tearDown() {
    ProtobufMessageProcessor.clearDescriptorRegistry();
    MessageFormat.PROTOBUF.clearSchemas();
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
  void shouldAddField() {
    final var operator = ProtobufMessageProcessor.addFieldOperator("name", "Bob");
    final var result = operator.apply(createTestMessage());

    assertEquals("Bob", result.getField(descriptor.findFieldByName("name")));
    assertEquals(42L, result.getField(descriptor.findFieldByName("id")));
  }

  @Test
  void shouldAddFieldIgnoreNonExistentField() {
    final var operator = ProtobufMessageProcessor.addFieldOperator("nonexistent", "value");
    final var result = operator.apply(createTestMessage());

    // Should return original message unchanged
    assertEquals("Alice", result.getField(descriptor.findFieldByName("name")));
  }

  @Test
  void shouldAddMultipleFields() {
    final var fields = Map.<String, Object>of("name", "Charlie", "active", false);
    final var operator = ProtobufMessageProcessor.addFieldsOperator(fields);
    final var result = operator.apply(createTestMessage());

    assertEquals("Charlie", result.getField(descriptor.findFieldByName("name")));
    assertEquals(false, result.getField(descriptor.findFieldByName("active")));
    assertEquals(42L, result.getField(descriptor.findFieldByName("id")));
  }

  @Test
  void shouldAddTimestamp() {
    final var before = System.currentTimeMillis();
    final var operator = ProtobufMessageProcessor.addTimestampOperator("id");
    final var result = operator.apply(createTestMessage());
    final var after = System.currentTimeMillis();

    final var timestamp = (long) result.getField(descriptor.findFieldByName("id"));
    assertTrue(timestamp >= before && timestamp <= after, "Timestamp should be within test bounds");
  }

  @Test
  void shouldAddTimestampIgnoreNonExistentField() {
    final var original = createTestMessage();
    final var operator = ProtobufMessageProcessor.addTimestampOperator("nonexistent");
    final var result = operator.apply(original);

    assertEquals(42L, result.getField(descriptor.findFieldByName("id")));
  }

  @Test
  void shouldRemoveFields() {
    final var operator = ProtobufMessageProcessor.removeFieldsOperator("name", "email");
    final var result = operator.apply(createTestMessage());

    // Cleared fields should have default values (empty string for proto3 strings)
    assertEquals("", result.getField(descriptor.findFieldByName("name")));
    assertEquals("", result.getField(descriptor.findFieldByName("email")));
    // Other fields should be preserved
    assertEquals(42L, result.getField(descriptor.findFieldByName("id")));
    assertEquals(true, result.getField(descriptor.findFieldByName("active")));
  }

  @Test
  void shouldRemoveNonExistentFieldsGracefully() {
    final var operator = ProtobufMessageProcessor.removeFieldsOperator("nonexistent");
    final var result = operator.apply(createTestMessage());

    assertEquals("Alice", result.getField(descriptor.findFieldByName("name")));
  }

  @Test
  void shouldTransformField() {
    final var operator = ProtobufMessageProcessor.transformFieldOperator("name", value ->
      ((String) value).toUpperCase()
    );
    final var result = operator.apply(createTestMessage());

    assertEquals("ALICE", result.getField(descriptor.findFieldByName("name")));
    assertEquals(42L, result.getField(descriptor.findFieldByName("id")));
  }

  @Test
  void shouldTransformNumericField() {
    final var operator = ProtobufMessageProcessor.transformFieldOperator("id", value -> ((Long) value) * 2);
    final var result = operator.apply(createTestMessage());

    assertEquals(84L, result.getField(descriptor.findFieldByName("id")));
  }

  @Test
  void shouldTransformNonExistentFieldGracefully() {
    final var original = createTestMessage();
    final var operator = ProtobufMessageProcessor.transformFieldOperator("nonexistent", value -> "x");
    final var result = operator.apply(original);

    assertEquals("Alice", result.getField(descriptor.findFieldByName("name")));
  }

  @Test
  void shouldRegisterAndGetDescriptor() {
    final var retrieved = ProtobufMessageProcessor.getDescriptor("test");
    assertNotNull(retrieved);
    assertEquals("TestMessage", retrieved.getName());
  }

  @Test
  void shouldReturnNullForUnregisteredDescriptor() {
    assertNull(ProtobufMessageProcessor.getDescriptor("nonexistent"));
  }

  @Test
  void shouldClearDescriptorRegistry() {
    assertNotNull(ProtobufMessageProcessor.getDescriptor("test"));
    ProtobufMessageProcessor.clearDescriptorRegistry();
    assertNull(ProtobufMessageProcessor.getDescriptor("test"));
  }

  @Test
  void shouldChainOperatorsViaPipeline() {
    registry.register(RegistryKey.protobuf("setName"), ProtobufMessageProcessor.addFieldOperator("name", "Processed"));
    registry.register(RegistryKey.protobuf("deactivate"), ProtobufMessageProcessor.addFieldOperator("active", false));

    final var pipeline = registry
      .pipeline(MessageFormat.PROTOBUF)
      .add(RegistryKey.protobuf("setName"))
      .add(RegistryKey.protobuf("deactivate"))
      .build();

    final var input = MessageFormat.PROTOBUF.serialize(createTestMessage());
    final var output = pipeline.apply(input);
    assertNotNull(output);

    final var result = MessageFormat.PROTOBUF.deserialize(output);
    final var desc = result.getDescriptorForType();
    assertEquals("Processed", result.getField(desc.findFieldByName("name")));
    assertEquals(false, result.getField(desc.findFieldByName("active")));
    assertEquals(42L, result.getField(desc.findFieldByName("id")));
  }

  @Test
  void shouldHandleNullInputInPipeline() {
    final var pipeline = registry.pipeline(MessageFormat.PROTOBUF).build();
    assertNull(pipeline.apply(null));
  }

  @Test
  void shouldHandleEmptyInputInPipeline() {
    final var pipeline = registry.pipeline(MessageFormat.PROTOBUF).build();
    assertNull(pipeline.apply(new byte[0]));
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
