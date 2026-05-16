package org.kpipe.format.protobuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProtobufDescriptorCatalogTest {

  private Descriptors.Descriptor descriptor;

  @BeforeEach
  void setUp() throws Exception {
    descriptor = buildTestDescriptor();
  }

  @Test
  void addStoresDescriptorByKey() {
    final var catalog = new ProtobufDescriptorCatalog();
    catalog.add("test", descriptor);

    final var retrieved = catalog.get("test");
    assertNotNull(retrieved);
    assertEquals("TestMessage", retrieved.getName());
  }

  @Test
  void getReturnsNullForMissingKey() {
    final var catalog = new ProtobufDescriptorCatalog();
    assertNull(catalog.get("missing"));
  }

  @Test
  void findReturnsEmptyForMissingKey() {
    final var catalog = new ProtobufDescriptorCatalog();
    assertTrue(catalog.find("missing").isEmpty());
  }

  @Test
  void findReturnsPresentForRegisteredKey() {
    final var catalog = new ProtobufDescriptorCatalog().add("test", descriptor);
    assertTrue(catalog.find("test").isPresent());
  }

  @Test
  void allReturnsRegisteredEntries() {
    final var catalog = new ProtobufDescriptorCatalog()
      .add("test", descriptor)
      .add("test2", descriptor);
    assertEquals(2, catalog.all().size());
  }

  @Test
  void allReturnsUnmodifiableView() {
    final var catalog = new ProtobufDescriptorCatalog().add("test", descriptor);
    assertThrows(UnsupportedOperationException.class, () -> catalog.all().clear());
  }

  @Test
  void clearRemovesAllEntries() {
    final var catalog = new ProtobufDescriptorCatalog().add("test", descriptor);
    catalog.clear();
    assertTrue(catalog.all().isEmpty());
  }

  @Test
  void addRejectsNullKey() {
    final var catalog = new ProtobufDescriptorCatalog();
    assertThrows(NullPointerException.class, () -> catalog.add(null, descriptor));
  }

  @Test
  void addRejectsNullDescriptor() {
    final var catalog = new ProtobufDescriptorCatalog();
    assertThrows(NullPointerException.class, () -> catalog.add("test", null));
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
