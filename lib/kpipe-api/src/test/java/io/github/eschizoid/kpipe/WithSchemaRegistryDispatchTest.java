package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/// Pins the `Stream.withSchemaRegistry(...)` format-dispatch in [DefaultStream]: a `ProtobufFormat`
/// must route to the Protobuf registry branch, and an unsupported format must be rejected with the
/// dedicated message. A regression that dropped the Protobuf branch (so it fell through to the
/// unsupported-format `else`) would be caught here — the format-level `withRegistry` tests cannot
/// see the dispatch.
class WithSchemaRegistryDispatchTest {

  private static final SchemaResolver RESOLVER = schemaId -> "x";

  private static Properties props() {
    final var props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "test-group");
    return props;
  }

  private static ProtobufFormat staticProtobufFormat() throws DescriptorValidationException {
    final var proto = FileDescriptorProto.newBuilder()
      .setName("dispatch.proto")
      .setSyntax("proto3")
      .addMessageType(DescriptorProto.newBuilder().setName("M"))
      .build();
    return new ProtobufFormat(FileDescriptor.buildFrom(proto, new FileDescriptor[0]).getMessageTypes().get(0));
  }

  @Test
  void protobufFormatRoutesToTheProtobufRegistryBranch() throws DescriptorValidationException {
    // kpipe-api's test path has no kpipe-format-protobuf-confluent, so ProtobufFormat.withRegistry
    // fails ServiceLoader lookup with a "ProtobufDescriptorCompiler" message — reachable ONLY if
    // the
    // dispatch took the Protobuf branch. The generic "not supported for format" (else branch) would
    // mean the Protobuf branch was lost.
    final var stream = KPipe.protobuf("topic", props(), staticProtobufFormat());
    final var ex = assertThrows(IllegalStateException.class, () -> stream.withSchemaRegistry(RESOLVER));
    assertTrue(
      ex.getMessage().contains("ProtobufDescriptorCompiler"),
      "expected the Protobuf branch's ServiceLoader error, got: " + ex.getMessage()
    );
  }

  @Test
  void unsupportedFormatIsRejectedWithTheDedicatedMessage() {
    final var stream = KPipe.json("topic", props());
    final var ex = assertThrows(UnsupportedOperationException.class, () -> stream.withSchemaRegistry(RESOLVER));
    assertTrue(
      ex.getMessage().contains("not supported"),
      "expected the unsupported-format message, got: " + ex.getMessage()
    );
  }
}
