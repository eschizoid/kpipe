package io.github.eschizoid.kpipe.format.protobuf;

import com.google.protobuf.Descriptors.Descriptor;

/// SPI that turns a Confluent Schema-Registry Protobuf schema — `.proto` source text plus the
/// wire message-index path — into the resolved message [Descriptor].
///
/// This seam keeps `kpipe-format-protobuf` dependency-light: the base module speaks only
/// `protobuf-java` types, while the real `.proto` compilation (which needs Confluent's
/// `ProtobufSchema` and its Square-Wire parser) lives in the opt-in impl module
/// `kpipe-format-protobuf-confluent`. Same SPI/impl split as `Tracer` vs `OtelTracer`
/// (`kpipe-tracing-otel`) and `kpipe-metrics` vs `kpipe-metrics-otel`: bring the heavy runtime.
///
/// Discovered via [java.util.ServiceLoader] — `ProtobufFormat.withRegistry(resolver)` finds the
/// impl on the runtime path, so the base's registry API stays a single-param mirror of
/// `AvroFormat.withRegistry(SchemaResolver)`.
@FunctionalInterface
public interface ProtobufDescriptorCompiler {
  /// Compiles `.proto` source text and resolves the message type named by `messageIndex`.
  ///
  /// @param protoText the `.proto` source returned by the schema registry for a schema id
  /// @param messageIndex the Confluent wire message-index path selecting a message type within the
  ///     file (`[0]` for the first top-level message); never null, never empty
  /// @return the resolved message descriptor
  /// @throws RuntimeException if the text fails to compile or the index does not resolve
  Descriptor compile(String protoText, int[] messageIndex);
}
