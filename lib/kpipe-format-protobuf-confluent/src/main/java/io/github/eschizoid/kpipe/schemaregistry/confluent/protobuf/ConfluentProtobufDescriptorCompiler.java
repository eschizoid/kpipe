package io.github.eschizoid.kpipe.schemaregistry.confluent.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufDescriptorCompiler;
import java.util.Objects;

/// Confluent-backed [ProtobufDescriptorCompiler]: compiles `.proto` source text to a protobuf
/// [Descriptor] using Confluent's `ProtobufSchema` (the reference parser for Confluent Protobuf
/// Schema Registry), then resolves the message type selected by the wire message-index path.
///
/// This is the heavy, opt-in impl module (`kpipe-format-protobuf-confluent`) — the analogue of
/// `OtelTracer` / `kpipe-metrics-otel`. It ships as a shaded jar (Square Wire relocated) so the
/// Confluent parser never leaks its split-package onto a consumer's module path.
public final class ConfluentProtobufDescriptorCompiler implements ProtobufDescriptorCompiler {

  /// Compiles `protoText` and resolves the message type at `messageIndex`.
  ///
  /// The Confluent message-index path is depth-based: index 0 selects a top-level message by its
  /// declaration order in the file; each further index selects a nested type by declaration order.
  ///
  /// @param protoText the `.proto` source for a schema id
  /// @param messageIndex the wire message-index path (`[0]` = first top-level message)
  /// @return the resolved message descriptor
  @Override
  public Descriptor compile(final String protoText, final int[] messageIndex) {
    Objects.requireNonNull(protoText, "protoText cannot be null");
    Objects.requireNonNull(messageIndex, "messageIndex cannot be null");
    if (messageIndex.length == 0) throw new IllegalArgumentException("messageIndex must not be empty");

    final var schema = new ProtobufSchema(protoText);
    final var file = schema.toDescriptor().getFile();

    final var topLevel = file.getMessageTypes();
    if (messageIndex[0] < 0 || messageIndex[0] >= topLevel.size()) throw new IllegalArgumentException(
      "message-index[0]=" + messageIndex[0] + " out of range for " + topLevel.size() + " top-level messages"
    );
    var descriptor = topLevel.get(messageIndex[0]);
    for (var depth = 1; depth < messageIndex.length; depth++) {
      final var nested = descriptor.getNestedTypes();
      final var idx = messageIndex[depth];
      if (idx < 0 || idx >= nested.size()) throw new IllegalArgumentException(
        "message-index[" +
          depth +
          "]=" +
          idx +
          " out of range for " +
          nested.size() +
          " nested types of " +
          descriptor.getFullName()
      );
      descriptor = nested.get(idx);
    }
    return descriptor;
  }
}
