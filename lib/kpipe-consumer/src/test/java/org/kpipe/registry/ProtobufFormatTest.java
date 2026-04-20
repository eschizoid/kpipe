package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.processor.ProtobufMessageProcessor;

class ProtobufFormatTest {

  private ProtobufFormat format;
  private Descriptors.Descriptor descriptor;

  @BeforeEach
  void setUp() throws Exception {
    format = MessageFormat.PROTOBUF;
    descriptor = buildTestDescriptor();
  }

  @AfterEach
  void tearDown() {
    format.clearSchemas();
    ProtobufMessageProcessor.clearDescriptorRegistry();
  }

  @Test
  void shouldSerializeAndDeserializeRoundTrip() {
    format.addDescriptor("test", descriptor);
    format.withDefaultDescriptor("test");

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
    format.addDescriptor("test", descriptor);
    format.withDefaultDescriptor("test");
    assertNull(format.deserialize(null));
  }

  @Test
  void shouldDeserializeEmptyReturnsNull() {
    format.addDescriptor("test", descriptor);
    format.withDefaultDescriptor("test");
    assertNull(format.deserialize(new byte[0]));
  }

  @Test
  void shouldThrowWhenNoDefaultDescriptor() {
    assertThrows(UnsupportedOperationException.class, () -> format.deserialize(new byte[] { 1, 2, 3 }));
  }

  @Test
  void shouldThrowWhenDescriptorKeyNotFound() {
    format.withDefaultDescriptor("missing");
    assertThrows(IllegalArgumentException.class, () -> format.deserialize(new byte[] { 1, 2, 3 }));
  }

  @Test
  void shouldThrowOnInvalidProtobufBytes() {
    format.addDescriptor("test", descriptor);
    format.withDefaultDescriptor("test");
    // Invalid protobuf bytes — field tag with wrong wire type
    assertThrows(RuntimeException.class, () -> format.deserialize(new byte[] { (byte) 0xFF, (byte) 0xFF }));
  }

  @Test
  void shouldAddDescriptorAndRegisterSchema() {
    format.addDescriptor("user", descriptor);

    final var schema = format.findSchema("user");
    assertTrue(schema.isPresent());
    assertEquals("test.TestMessage", schema.get().fullyQualifiedName());
  }

  @Test
  void shouldGetDescriptorByKey() {
    format.addDescriptor("myKey", descriptor);

    final var retrieved = format.getDescriptor("myKey");
    assertNotNull(retrieved);
    assertEquals("TestMessage", retrieved.getName());
  }

  @Test
  void shouldReturnNullForUnregisteredDescriptor() {
    assertNull(format.getDescriptor("nonexistent"));
  }

  @Test
  void shouldAddSchemaMetadataOnly() {
    format.addSchema("proto1", "com.example.Msg", "/path/to/file.proto");

    final var schema = format.findSchema("proto1");
    assertTrue(schema.isPresent());
    assertEquals("com.example.Msg", schema.get().fullyQualifiedName());
    assertEquals("/path/to/file.proto", schema.get().location());
  }

  @Test
  void shouldGetAllSchemas() {
    format.addDescriptor("a", descriptor);
    format.addSchema("b", "com.example.B", "b.proto");

    final var schemas = format.getSchemas();
    assertEquals(2, schemas.size());
  }

  @Test
  void shouldFindSchemasByPredicate() {
    format.addDescriptor("test", descriptor);
    format.addSchema("other", "com.other.Msg", "other.proto");

    final var results = format.findSchemas(s -> s.fullyQualifiedName().startsWith("test."));
    assertEquals(1, results.size());
  }

  @Test
  void shouldClearSchemasAndDescriptors() {
    format.addDescriptor("test", descriptor);
    format.withDefaultDescriptor("test");

    format.clearSchemas();

    assertTrue(format.getSchemas().isEmpty());
    assertNull(format.getDescriptor("test"));
  }

  @Test
  void shouldReturnEmptyOptionalForMissingSchema() {
    assertFalse(format.findSchema("nonexistent").isPresent());
  }

  @Test
  void shouldAlsoRegisterWithProcessorDescriptorRegistry() {
    format.addDescriptor("sync", descriptor);

    // ProtobufMessageProcessor should also have the descriptor
    final var fromProcessor = ProtobufMessageProcessor.getDescriptor("sync");
    assertNotNull(fromProcessor);
    assertEquals("TestMessage", fromProcessor.getName());
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
