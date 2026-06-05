package io.github.eschizoid.kpipe;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import io.github.eschizoid.kpipe.consumer.config.AppConfig;
import io.github.eschizoid.kpipe.consumer.config.KafkaConsumerConfig;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

/// Minimal Protobuf consumer demonstrating the KPipe facade. Builds a `Customer` descriptor
/// programmatically (no protoc codegen), registers it with `ProtobufFormat`, and starts a
/// `KPipe.protobuf(...)` stream that logs every payload to the console.
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private App() {}

  static void main() {
    final var config = AppConfig.fromEnv();

    final var format = new ProtobufFormat(buildCustomerDescriptor());

    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    try (final var handle = KPipe.protobuf(format, config.topic(), props).toConsole().start()) {
      LOGGER.log(Level.INFO, "Protobuf consumer started for topic {0}", config.topic());
      handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in Protobuf consumer", e);
      System.exit(1);
    }
  }

  /// Builds the `Customer` descriptor programmatically so the example can run without a
  /// protoc-generated class on the classpath. Exposed package-private so integration tests
  /// can reuse it when producing test payloads.
  static Descriptors.Descriptor buildCustomerDescriptor() {
    try {
      final var customerMsg = DescriptorProtos.DescriptorProto.newBuilder()
        .setName("Customer")
        .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
        .addField(field("name", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
        .addField(field("email", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
        .addField(field("active", 4, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL))
        .addField(field("registration_date", 5, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
        .build();

      final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
        .setName("customer.proto")
        .setPackage("com.kpipe.customer")
        .setSyntax("proto3")
        .addMessageType(customerMsg)
        .build();

      final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
      return fileDescriptor.findMessageTypeByName("Customer");
    } catch (final Descriptors.DescriptorValidationException e) {
      throw new RuntimeException("Failed to build Customer descriptor", e);
    }
  }

  private static DescriptorProtos.FieldDescriptorProto field(
    final String name,
    final int number,
    final DescriptorProtos.FieldDescriptorProto.Type type
  ) {
    return DescriptorProtos.FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type).build();
  }
}
