package io.github.eschizoid.kpipe.format.protobuf;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.github.eschizoid.kpipe.registry.ConfluentEnvelope;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.SchemaResolver;
import io.github.eschizoid.kpipe.registry.WireDiagnostics;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/// Protobuf codec for KPipe — a `MessageFormat<Message>` in one of two modes, decided at
/// construction and fixed for the instance's lifetime:
///
///   * **static** — `new ProtobufFormat(descriptor)`: one descriptor for every record.
///   * **registry** — [#withRegistry]: per-record Confluent Schema-Registry auto-lookup. The
///     descriptor comes from each record's wire envelope, resolved by a [SchemaResolver] and
///     a [ProtobufDescriptorCompiler].
///
/// Mirrors `AvroFormat` (static vs registry). Registry mode is consumer-first: `serialize` is
/// unsupported (bring a static descriptor if you need writer-side encoding).
///
/// ```java
/// final var format = new ProtobufFormat(UserProto.getDescriptor());
/// ```
public final class ProtobufFormat implements MessageFormat<Message> {

  private static final System.Logger LOGGER = System.getLogger(ProtobufFormat.class.getName());

  /// Non-null in static mode, null in registry mode.
  private final Descriptor descriptor;

  /// Non-null in registry mode, null in static mode.
  private final SchemaResolver resolver;

  private final ProtobufDescriptorCompiler compiler;

  /// Registry-mode cache of compiled descriptors, keyed by schema id + message-index path.
  /// The resolver caches the `.proto` text; this caches the compiled [Descriptor] so a repeat
  /// (id, index) pair skips both the lookup and the compile. Null in static mode.
  private final ConcurrentHashMap<DescriptorKey, Descriptor> resolvedDescriptors;

  /// The `[0]` path — Confluent's single-`0x00`-byte shorthand for "the first top-level message".
  private static final int[] FIRST_MESSAGE_INDEX = { 0 };

  /// Constructs a codec bound to `descriptor` (static mode).
  ///
  /// There is no `of(String)` factory (unlike `AvroFormat.of(json)`): protobuf-java compiles a
  /// [Descriptor] only from a binary `FileDescriptorProto`, not from `.proto` source text, so a
  /// descriptor must be supplied. For per-record schema lookup from `.proto` text, use
  /// [#withRegistry(SchemaResolver)] with `kpipe-format-protobuf-confluent`.
  ///
  /// @param descriptor the Protobuf message descriptor used for deserialization (must be non-null)
  public ProtobufFormat(final Descriptor descriptor) {
    this.descriptor = Objects.requireNonNull(descriptor, "descriptor cannot be null");
    this.resolver = null;
    this.compiler = null;
    this.resolvedDescriptors = null;
  }

  private ProtobufFormat(final SchemaResolver resolver, final ProtobufDescriptorCompiler compiler) {
    this.descriptor = null;
    this.resolver = Objects.requireNonNull(resolver, "resolver cannot be null");
    this.compiler = Objects.requireNonNull(compiler, "compiler cannot be null");
    this.resolvedDescriptors = new ConcurrentHashMap<>();
  }

  /// Registry-mode factory taking an explicit compiler, bypassing [ServiceLoader]. Package-private:
  /// exists so the base module's tests can exercise the envelope + message-index logic with a fake
  /// compiler, without depending on `kpipe-format-protobuf-confluent`. The public surface stays the
  /// single-param [#withRegistry(SchemaResolver)] mirror of `AvroFormat`.
  static ProtobufFormat withRegistry(final SchemaResolver resolver, final ProtobufDescriptorCompiler compiler) {
    return new ProtobufFormat(resolver, compiler);
  }

  /// Creates a registry-mode codec that resolves each record's descriptor from its Confluent wire
  /// envelope. The `.proto` text is fetched by id through `resolver` (wrap it in a
  /// `CachedSchemaResolver`); the `.proto` compiler is discovered via [ServiceLoader], so add
  /// `kpipe-format-protobuf-confluent` to your runtime (like bringing the OTel SDK for metrics).
  ///
  /// Exact mirror of `AvroFormat.withRegistry(SchemaResolver)` — single param.
  /// Do NOT combine registry mode with `skipBytes(6)`: the format consumes the envelope itself.
  ///
  /// @param resolver resolves a schema id to its `.proto` source text (must be non-null)
  /// @return a registry-mode Protobuf codec
  /// @throws IllegalStateException if no [ProtobufDescriptorCompiler] is on the runtime path
  public static ProtobufFormat withRegistry(final SchemaResolver resolver) {
    final var compilers = ServiceLoader.load(ProtobufDescriptorCompiler.class).stream().toList();
    if (compilers.isEmpty()) throw new IllegalStateException(
      "No ProtobufDescriptorCompiler found — add kpipe-format-protobuf-confluent to your runtime"
    );
    final var compiler = compilers.getFirst().get();
    if (compilers.size() > 1) LOGGER.log(
      System.Logger.Level.WARNING,
      "Found {0} ProtobufDescriptorCompiler implementations on the path — using {1}",
      compilers.size(),
      compiler.getClass().getName()
    );
    return new ProtobufFormat(resolver, compiler);
  }

  /// Returns the descriptor this codec is bound to.
  ///
  /// @return the bound descriptor in static mode; `null` in Schema-Registry mode (each record's
  ///     descriptor is resolved per-envelope, so there is no single bound descriptor)
  public Descriptor descriptor() {
    return descriptor;
  }

  /// Creates a new [ProtobufConsoleSink] for [Message] payloads.
  ///
  /// @return a new console sink
  public ProtobufConsoleSink<Message> consoleSink() {
    return new ProtobufConsoleSink<>();
  }

  /// Serializes a Protobuf [Message] to bytes. Unsupported in registry mode (consumer-first).
  ///
  /// @param data the message to serialize
  /// @return the binary-encoded bytes, or null if `data` is null
  @Override
  public byte[] serialize(final Message data) {
    if (resolver != null) throw new UnsupportedOperationException(
      "registry-mode ProtobufFormat is consumer-first — construct a static ProtobufFormat(descriptor) to serialize"
    );
    if (data == null) return null;
    return data.toByteArray();
  }

  /// Deserializes bytes to a Protobuf [Message]. In static mode, decodes against the bound
  /// descriptor. In registry mode, reads the Confluent wire envelope (schema id + message index),
  /// resolves the descriptor via the [SchemaResolver] + [ProtobufDescriptorCompiler], and decodes
  /// the payload against it.
  ///
  /// @param data the binary-encoded bytes
  /// @return the decoded message (a [DynamicMessage]), or null if `data` is null/empty
  @Override
  public Message deserialize(final byte[] data) {
    if (data == null || data.length == 0) return null;
    if (resolver != null) return deserializeFromEnvelope(data);
    try {
      return DynamicMessage.parseFrom(descriptor, data);
    } catch (final InvalidProtocolBufferException e) {
      throw new IllegalStateException(
        "ProtobufFormat.deserialize failed on " +
          data.length +
          " bytes for descriptor " +
          descriptor.getFullName() +
          " (first bytes " +
          WireDiagnostics.hexPreview(data) +
          ") — if using Confluent wire format, check skipBytes(6)",
        e
      );
    }
  }

  /// Registry-mode deserialization: read the Confluent Protobuf wire envelope (1-byte magic +
  /// 4-byte big-endian schema id + message-index array), resolve the message descriptor, then
  /// decode the trailing payload against it.
  ///
  /// The message-index array selects which message type in the `.proto` file the payload was
  /// written with (Confluent files can hold several). It is a Kafka-style zig-zag varint `size`
  /// followed by `size` zig-zag varint path elements — with the shorthand that `size == 0` means
  /// the array `[0]` (the first top-level message), the common single-message case.
  private Message deserializeFromEnvelope(final byte[] data) {
    if (data.length < ConfluentEnvelope.HEADER_LENGTH + 1) throw new IllegalStateException(
      "Record too short for Confluent Protobuf wire envelope: " + data.length + " bytes; expected at least 6"
    );
    final int schemaId = ConfluentEnvelope.readSchemaId(data);

    final var cursor = new VarintCursor(data, ConfluentEnvelope.HEADER_LENGTH);
    final var size = cursor.readZigZagVarint();
    final int[] messageIndex;
    if (size == 0) {
      messageIndex = FIRST_MESSAGE_INDEX;
    } else if (size < 0) {
      throw new IllegalStateException("Negative message-index size " + size + " for schema id " + schemaId);
    } else if (size > data.length - cursor.offset()) {
      // Each index entry needs at least one byte, so a size larger than the bytes that remain is a
      // corrupt envelope — reject before allocating (guards against a huge int[] from a bad
      // varint).
      throw new IllegalStateException(
        "Message-index size " + size + " exceeds remaining envelope bytes for schema id " + schemaId
      );
    } else {
      messageIndex = new int[size];
      for (var i = 0; i < size; i++) messageIndex[i] = cursor.readZigZagVarint();
    }
    final var payloadOffset = cursor.offset();

    final var messageDescriptor = resolvedDescriptors.computeIfAbsent(
      new DescriptorKey(schemaId, messageIndex),
      key -> {
        final var protoText = resolver.lookupById(key.schemaId());
        if (protoText == null || protoText.isBlank()) throw new IllegalStateException(
          "Schema resolver returned empty schema for id " + key.schemaId()
        );
        return compiler.compile(protoText, key.messageIndex());
      }
    );

    try {
      final var payload = CodedInputStream.newInstance(data, payloadOffset, data.length - payloadOffset);
      return DynamicMessage.parseFrom(messageDescriptor, payload);
    } catch (final IOException e) {
      throw new IllegalStateException(
        "Failed to decode Protobuf record under Confluent wire envelope (schema id " + schemaId + ")",
        e
      );
    }
  }

  /// Reads Kafka-style zig-zag varints from a byte array — the encoding Confluent uses for the
  /// Protobuf message-index array (via `org.apache.kafka.common.utils.ByteUtils`). Not thread-safe;
  /// a fresh cursor is used per record.
  private static final class VarintCursor {

    private final byte[] data;
    private int offset;

    private VarintCursor(final byte[] data, final int offset) {
      this.data = data;
      this.offset = offset;
    }

    private int offset() {
      return offset;
    }

    /// Reads one zig-zag-encoded base-128 varint and advances the cursor.
    private int readZigZagVarint() {
      var value = 0;
      var shift = 0;
      int b;
      do {
        if (offset >= data.length) throw new IllegalStateException("Truncated varint in Confluent message-index array");
        if (shift > 28) throw new IllegalStateException("Malformed varint (over 5 bytes) in message-index array");
        b = data[offset++] & 0xff;
        value |= (b & 0x7f) << shift;
        shift += 7;
      } while ((b & 0x80) != 0);
      return (value >>> 1) ^ -(value & 1);
    }
  }

  /// Cache key for a resolved descriptor: schema id plus the message-index path. Uses a
  /// [List] path (not the raw `int[]`) so equality and hashing are value-based.
  private record DescriptorKey(int schemaId, List<Integer> path) {
    private DescriptorKey(final int schemaId, final int[] messageIndex) {
      this(schemaId, toList(messageIndex));
    }

    private int[] messageIndex() {
      final var out = new int[path.size()];
      for (var i = 0; i < out.length; i++) out[i] = path.get(i);
      return out;
    }

    private static List<Integer> toList(final int[] messageIndex) {
      final var list = new ArrayList<Integer>(messageIndex.length);
      for (final var i : messageIndex) list.add(i);
      return List.copyOf(list);
    }
  }

}
