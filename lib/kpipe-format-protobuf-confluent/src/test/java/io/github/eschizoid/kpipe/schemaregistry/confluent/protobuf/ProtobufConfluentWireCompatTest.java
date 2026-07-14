package io.github.eschizoid.kpipe.schemaregistry.confluent.protobuf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.util.Map;
import org.junit.jupiter.api.Test;

/// The strongest proof of Confluent wire compatibility: bytes are produced by Confluent's OWN
/// [KafkaProtobufSerializer] against a [MockSchemaRegistryClient] (real magic + schema id + real
/// message-index encoding), and then decoded by `ProtobufFormat.withRegistry(...)`. If the
/// envelope/message-index parsing in the base format diverged from Confluent's, this round-trip
/// would fail.
class ProtobufConfluentWireCompatTest {

  private static FileDescriptor twoMessageFile() throws Exception {
    final var proto = FileDescriptorProto.newBuilder()
      .setName("catalog.proto")
      .setSyntax("proto3")
      .setPackage("com.kpipe.catalog")
      .addMessageType(
        DescriptorProto.newBuilder()
          .setName("Customer")
          .addField(field("id", 1, Type.TYPE_INT64))
          .addField(field("name", 2, Type.TYPE_STRING))
      )
      .addMessageType(DescriptorProto.newBuilder().setName("Order").addField(field("order_id", 1, Type.TYPE_INT64)))
      .build();
    return FileDescriptor.buildFrom(proto, new FileDescriptor[0]);
  }

  private static FieldDescriptorProto field(final String name, final int number, final Type type) {
    return FieldDescriptorProto.newBuilder()
      .setName(name)
      .setNumber(number)
      .setType(type)
      .setLabel(Label.LABEL_OPTIONAL)
      .build();
  }

  /// A resolver backed by the mock registry — returns the exact `.proto` text Confluent registered
  /// for a schema id, so our format compiles against the real writer schema.
  private static SchemaResolver resolverFor(final MockSchemaRegistryClient client) {
    return schemaId -> {
      try {
        return ((ProtobufSchema) client.getSchemaById(schemaId)).canonicalString();
      } catch (final Exception e) {
        throw new RuntimeException("mock registry lookup failed for id " + schemaId, e);
      }
    };
  }

  @Test
  void decodesBytesProducedByConfluentSerializer() throws Exception {
    final var file = twoMessageFile();
    final var customer = file.getMessageTypes().get(0);
    final var message = DynamicMessage.newBuilder(customer)
      .setField(customer.findFieldByName("id"), 42L)
      .setField(customer.findFieldByName("name"), "Mariano")
      .build();

    final var registry = new MockSchemaRegistryClient();
    final var serializer = new KafkaProtobufSerializer<DynamicMessage>(registry);
    serializer.configure(Map.of("schema.registry.url", "mock://wire-compat", "auto.register.schemas", true), false);
    final var confluentBytes = serializer.serialize("catalog-topic", message);

    final var format = ProtobufFormat.withRegistry(resolverFor(registry));
    final var decoded = format.deserialize(confluentBytes);

    assertEquals("Customer", decoded.getDescriptorForType().getName());
    assertEquals(42L, decoded.getField(decoded.getDescriptorForType().findFieldByName("id")));
    assertEquals("Mariano", decoded.getField(decoded.getDescriptorForType().findFieldByName("name")));
  }

  @Test
  void decodesSecondMessageWireBytesFromConfluentSerializer() throws Exception {
    final var file = twoMessageFile();
    final var order = file.getMessageTypes().get(1);
    final var message = DynamicMessage.newBuilder(order).setField(order.findFieldByName("order_id"), 7L).build();

    final var registry = new MockSchemaRegistryClient();
    final var serializer = new KafkaProtobufSerializer<DynamicMessage>(registry);
    serializer.configure(Map.of("schema.registry.url", "mock://wire-compat", "auto.register.schemas", true), false);
    // Confluent encodes the non-zero message index (Order is the 2nd message) into the envelope.
    final var confluentBytes = serializer.serialize("order-topic", message);

    final var format = ProtobufFormat.withRegistry(resolverFor(registry));
    final var decoded = format.deserialize(confluentBytes);

    assertEquals("Order", decoded.getDescriptorForType().getName());
    assertEquals(7L, decoded.getField(decoded.getDescriptorForType().findFieldByName("order_id")));
  }
}
