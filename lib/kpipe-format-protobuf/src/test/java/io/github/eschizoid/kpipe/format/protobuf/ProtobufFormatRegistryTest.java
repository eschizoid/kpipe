package io.github.eschizoid.kpipe.format.protobuf;

import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/// Tests the Schema-Registry mode of [ProtobufFormat] — the Confluent wire-envelope parse
/// (magic + schema id + message-index array), the `(id, index)` descriptor cache, and payload
/// decode — with a FAKE [ProtobufDescriptorCompiler], so the base module's tests need no
/// `kpipe-format-protobuf-confluent` dependency. The real `.proto` compilation is covered by that
/// module's own test.
class ProtobufFormatRegistryTest {

  /// A two-message `.proto`, built programmatically so the fake compiler can resolve a
  /// message-index path exactly as the real one does: `[0]` → Customer, `[1]` → Order.
  private static final FileDescriptor CATALOG = catalog();

  private static final Descriptor CUSTOMER = CATALOG.getMessageTypes().get(0);
  private static final Descriptor ORDER = CATALOG.getMessageTypes().get(1);

  private static FileDescriptor catalog() {
    try {
      final var proto = FileDescriptorProto.newBuilder()
        .setName("catalog.proto")
        .setSyntax("proto3")
        .setPackage("com.kpipe.catalog")
        .addMessageType(
          DescriptorProto.newBuilder()
            .setName("Customer")
            .addField(field("id", 1, TYPE_INT64))
            .addField(field("name", 2, TYPE_STRING))
        )
        .addMessageType(DescriptorProto.newBuilder().setName("Order").addField(field("orderId", 1, TYPE_INT64)))
        .build();
      return FileDescriptor.buildFrom(proto, new FileDescriptor[0]);
    } catch (final DescriptorValidationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static FieldDescriptorProto field(final String name, final int number, final FieldDescriptorProto.Type type) {
    return FieldDescriptorProto.newBuilder()
      .setName(name)
      .setNumber(number)
      .setType(type)
      .setLabel(LABEL_OPTIONAL)
      .build();
  }

  /// Fake compiler that resolves the message-index path against [#CATALOG] (mirroring the real
  /// impl's walk) and records the `(protoText, index)` pairs it was asked to compile.
  private static final class FakeCompiler implements ProtobufDescriptorCompiler {

    final AtomicInteger calls = new AtomicInteger();
    volatile String lastProtoText;
    volatile int[] lastIndex;

    @Override
    public Descriptor compile(final String protoText, final int[] messageIndex) {
      calls.incrementAndGet();
      lastProtoText = protoText;
      lastIndex = messageIndex.clone();
      var descriptor = CATALOG.getMessageTypes().get(messageIndex[0]);
      for (var depth = 1; depth < messageIndex.length; depth++) {
        descriptor = descriptor.getNestedTypes().get(messageIndex[depth]);
      }
      return descriptor;
    }
  }

  /// Hands out `.proto` text by id (content is opaque to the fake compiler) and counts lookups.
  private static final class FakeResolver implements SchemaResolver {

    final Map<Integer, String> schemas = new HashMap<>();
    final AtomicInteger calls = new AtomicInteger();

    FakeResolver put(final int id, final String protoText) {
      schemas.put(id, protoText);
      return this;
    }

    @Override
    public String lookupById(final int schemaId) {
      calls.incrementAndGet();
      final var text = schemas.get(schemaId);
      if (text == null) throw new RuntimeException("Unknown schema id " + schemaId);
      return text;
    }
  }

  /// Zig-zag base-128 varint encode (Kafka `ByteUtils` form) — matches the format's parser.
  private static void writeZigZagVarint(final ByteArrayOutputStream out, final int value) {
    var v = (value << 1) ^ (value >> 31);
    while ((v & ~0x7f) != 0) {
      out.write((v & 0x7f) | 0x80);
      v >>>= 7;
    }
    out.write(v);
  }

  /// Builds a Confluent Protobuf wire envelope: magic 0x00 + 4-byte big-endian id + message-index
  /// array (single `0x00` shorthand for `[0]`, else zig-zag size then elements) + payload.
  private static byte[] envelope(final int schemaId, final int[] messageIndex, final byte[] payload) {
    final var out = new ByteArrayOutputStream();
    out.write(0x00);
    out.writeBytes(ByteBuffer.allocate(4).putInt(schemaId).array());
    if (messageIndex.length == 1 && messageIndex[0] == 0) {
      out.write(0x00);
    } else {
      writeZigZagVarint(out, messageIndex.length);
      for (final var i : messageIndex) writeZigZagVarint(out, i);
    }
    out.writeBytes(payload);
    return out.toByteArray();
  }

  private static byte[] customer(final long id, final String name) {
    return DynamicMessage.newBuilder(CUSTOMER)
      .setField(CUSTOMER.findFieldByName("id"), id)
      .setField(CUSTOMER.findFieldByName("name"), name)
      .build()
      .toByteArray();
  }

  private static byte[] order(final long orderId) {
    return DynamicMessage.newBuilder(ORDER).setField(ORDER.findFieldByName("orderId"), orderId).build().toByteArray();
  }

  @Test
  void decodesFirstMessageUnderZeroIndexShorthand() {
    final var resolver = new FakeResolver().put(101, "catalog.proto text");
    final var format = ProtobufFormat.withRegistry(resolver, new FakeCompiler());

    final var decoded = format.deserialize(envelope(101, new int[] { 0 }, customer(42, "Mariano")));

    assertEquals(42L, decoded.getField(decoded.getDescriptorForType().findFieldByName("id")));
    assertEquals("Mariano", decoded.getField(decoded.getDescriptorForType().findFieldByName("name")));
    assertEquals("Customer", decoded.getDescriptorForType().getName());
  }

  @Test
  void messageIndexSelectsTheRightMessageType() {
    final var compiler = new FakeCompiler();
    final var resolver = new FakeResolver().put(101, "catalog.proto text");
    final var format = ProtobufFormat.withRegistry(resolver, compiler);

    final var decoded = format.deserialize(envelope(101, new int[] { 1 }, order(7)));

    assertEquals("Order", decoded.getDescriptorForType().getName());
    assertEquals(7L, decoded.getField(decoded.getDescriptorForType().findFieldByName("orderId")));
    assertArrayEquals(new int[] { 1 }, compiler.lastIndex);
  }

  @Test
  void largeMessageIndexParsesMultiByteVarint() {
    // Build a 128-message file so index 100 forces a multi-byte zig-zag varint (zigzag(100)=200).
    final var fileProto = FileDescriptorProto.newBuilder().setName("big.proto").setSyntax("proto3");
    for (var i = 0; i < 128; i++) {
      fileProto.addMessageType(DescriptorProto.newBuilder().setName("M" + i).addField(field("v", 1, TYPE_INT64)));
    }
    final FileDescriptor big;
    try {
      big = FileDescriptor.buildFrom(fileProto.build(), new FileDescriptor[0]);
    } catch (final DescriptorValidationException e) {
      throw new IllegalStateException(e);
    }
    final var m100 = big.getMessageTypes().get(100);
    final ProtobufDescriptorCompiler compiler = (text, index) -> big.getMessageTypes().get(index[0]);
    final var format = ProtobufFormat.withRegistry(new FakeResolver().put(5, "big"), compiler);

    final var payload = DynamicMessage.newBuilder(m100).setField(m100.findFieldByName("v"), 99L).build().toByteArray();
    final var decoded = format.deserialize(envelope(5, new int[] { 100 }, payload));

    assertEquals("M100", decoded.getDescriptorForType().getName());
    assertEquals(99L, decoded.getField(m100.findFieldByName("v")));
  }

  @Test
  void descriptorIsCachedPerIdAndIndex() {
    final var compiler = new FakeCompiler();
    final var resolver = new FakeResolver().put(101, "catalog.proto text");
    final var format = ProtobufFormat.withRegistry(resolver, compiler);

    format.deserialize(envelope(101, new int[] { 0 }, customer(1, "a")));
    format.deserialize(envelope(101, new int[] { 0 }, customer(2, "b")));
    format.deserialize(envelope(101, new int[] { 0 }, customer(3, "c")));

    assertEquals(1, resolver.calls.get(), "resolver must be hit once per (id, index)");
    assertEquals(1, compiler.calls.get(), "compiler must be hit once per (id, index)");
  }

  @Test
  void distinctIndexesUnderSameIdResolveIndependently() {
    final var compiler = new FakeCompiler();
    final var resolver = new FakeResolver().put(101, "catalog.proto text");
    final var format = ProtobufFormat.withRegistry(resolver, compiler);

    final var asCustomer = format.deserialize(envelope(101, new int[] { 0 }, customer(1, "a")));
    final var asOrder = format.deserialize(envelope(101, new int[] { 1 }, order(9)));

    assertEquals("Customer", asCustomer.getDescriptorForType().getName());
    assertEquals("Order", asOrder.getDescriptorForType().getName());
    // Same id, two different index paths → two cache entries → two compiles, but the resolver's
    // text is looked up once per (id, index) key too (the resolver itself caches by id in prod).
    assertEquals(2, compiler.calls.get(), "each distinct (id, index) compiles once");
  }

  @Test
  void mixedSchemaIdsResolveIndependently() {
    final var resolver = new FakeResolver().put(101, "one").put(202, "two");
    final var format = ProtobufFormat.withRegistry(resolver, new FakeCompiler());

    final var a = format.deserialize(envelope(101, new int[] { 0 }, customer(1, "a")));
    final var b = format.deserialize(envelope(202, new int[] { 0 }, customer(2, "b")));

    assertEquals("a", a.getField(CUSTOMER.findFieldByName("name")));
    assertEquals("b", b.getField(CUSTOMER.findFieldByName("name")));
    assertEquals(2, resolver.calls.get(), "two distinct ids = two lookups");
  }

  @Test
  void skipBytesLikeDoubleStripIsNotOurConcernButBadIndexSizeThrows() {
    // A malformed (huge) message-index size must fail loudly, not silently mis-slice the payload.
    final var format = ProtobufFormat.withRegistry(new FakeResolver().put(1, "x"), new FakeCompiler());
    // magic + id=1 + zig-zag size 0x04 (=2) but truncated before the two index elements.
    final var truncated = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x01, 0x04 };
    assertThrows(IllegalStateException.class, () -> format.deserialize(truncated));
  }

  @Test
  void overlongMessageIndexVarintThrowsAtFiveByteCap() {
    // Six 0x80 continuation bytes never terminate a varint: the parser must trip the 5-byte cap
    // rather than keep shifting garbage into the value (or walk into the payload).
    final var format = ProtobufFormat.withRegistry(new FakeResolver().put(1, "x"), new FakeCompiler());
    // magic + id=1 + an unterminated varint of six continuation bytes where the index size goes.
    final var overlong = new byte[] {
      0x00,
      0x00,
      0x00,
      0x00,
      0x01,
      (byte) 0x80,
      (byte) 0x80,
      (byte) 0x80,
      (byte) 0x80,
      (byte) 0x80,
      (byte) 0x80,
    };
    final var ex = assertThrows(IllegalStateException.class, () -> format.deserialize(overlong));
    assertTrue(ex.getMessage().contains("varint"), "must mention varint; got: " + ex.getMessage());
  }

  @Test
  void wrongMagicByteThrows() {
    final var format = ProtobufFormat.withRegistry(new FakeResolver().put(1, "x"), new FakeCompiler());
    final var bad = new byte[] { 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x42 };
    final var ex = assertThrows(IllegalStateException.class, () -> format.deserialize(bad));
    assertTrue(ex.getMessage().contains("magic byte"), "must mention magic byte; got: " + ex.getMessage());
  }

  @Test
  void payloadShorterThanEnvelopeThrows() {
    final var format = ProtobufFormat.withRegistry(new FakeResolver(), new FakeCompiler());
    final var tooShort = new byte[] { 0x00, 0x00, 0x00 };
    final var ex = assertThrows(IllegalStateException.class, () -> format.deserialize(tooShort));
    assertTrue(ex.getMessage().contains("envelope"));
  }

  @Test
  void resolverReturningEmptyThrows() {
    final SchemaResolver resolver = schemaId -> "";
    final var format = ProtobufFormat.withRegistry(resolver, new FakeCompiler());
    final var ex = assertThrows(IllegalStateException.class, () ->
      format.deserialize(envelope(1, new int[] { 0 }, customer(1, "a")))
    );
    assertTrue(ex.getMessage().contains("empty schema"));
  }

  @Test
  void registryModeSerializeThrows() {
    final var format = ProtobufFormat.withRegistry(new FakeResolver(), new FakeCompiler());
    final var msg = DynamicMessage.newBuilder(CUSTOMER).build();
    assertThrows(UnsupportedOperationException.class, () -> format.serialize(msg));
  }

  @Test
  void emptyOrNullPayloadReturnsNull() {
    final var format = ProtobufFormat.withRegistry(new FakeResolver(), new FakeCompiler());
    assertNull(format.deserialize(new byte[0]));
    assertNull(format.deserialize(null));
  }

  @Test
  void rejectsNullResolver() {
    assertThrows(NullPointerException.class, () -> ProtobufFormat.withRegistry(null, new FakeCompiler()));
  }

  @Test
  void aThrowingResolveIsNotCachedSoTheNextRecordRetries() {
    // computeIfAbsent does not cache when the mapping function throws — a transiently-down registry
    // recovers on the next record rather than poisoning the (id, index) cache forever.
    final var attempts = new AtomicInteger();
    final SchemaResolver flaky = schemaId -> {
      if (attempts.incrementAndGet() == 1) throw new RuntimeException("registry temporarily down");
      return "catalog.proto text";
    };
    final var format = ProtobufFormat.withRegistry(flaky, new FakeCompiler());
    final var env = envelope(101, new int[] { 0 }, customer(1, "a"));

    // The resolver throws its own RuntimeException; the format propagates it as-is (a resolver/IO
    // failure is not a malformed-envelope condition to reclassify), so this stays RuntimeException.
    assertThrows(RuntimeException.class, () -> format.deserialize(env));
    final var decoded = format.deserialize(env);

    assertEquals("Customer", decoded.getDescriptorForType().getName());
    assertEquals(2, attempts.get(), "a throwing resolve must not be cached — the next record retries");
  }

  @Test
  void multiElementMessageIndexResolvesNestedType() throws DescriptorValidationException {
    // A genuine multi-element index path [0,1] → Outer.B, exercising the `for i < size` parse loop
    // with size > 1 (the single-element base tests never do).
    final var proto = FileDescriptorProto.newBuilder()
      .setName("nested.proto")
      .setSyntax("proto3")
      .addMessageType(
        DescriptorProto.newBuilder()
          .setName("Outer")
          .addNestedType(DescriptorProto.newBuilder().setName("A").addField(field("x", 1, TYPE_INT64)))
          .addNestedType(DescriptorProto.newBuilder().setName("B").addField(field("y", 1, TYPE_INT64)))
      )
      .build();
    final var nested = FileDescriptor.buildFrom(proto, new FileDescriptor[0]);
    final var outerB = nested.getMessageTypes().get(0).getNestedTypes().get(1);
    final var seen = new int[1][];
    final ProtobufDescriptorCompiler compiler = (text, index) -> {
      seen[0] = index.clone();
      var descriptor = nested.getMessageTypes().get(index[0]);
      for (var depth = 1; depth < index.length; depth++) descriptor = descriptor.getNestedTypes().get(index[depth]);
      return descriptor;
    };
    final var format = ProtobufFormat.withRegistry(new FakeResolver().put(7, "nested"), compiler);
    final var payload = DynamicMessage.newBuilder(outerB)
      .setField(outerB.findFieldByName("y"), 55L)
      .build()
      .toByteArray();

    final var decoded = format.deserialize(envelope(7, new int[] { 0, 1 }, payload));

    assertArrayEquals(new int[] { 0, 1 }, seen[0], "the full multi-element index path must reach the compiler");
    assertEquals("B", decoded.getDescriptorForType().getName());
    assertEquals(55L, decoded.getField(outerB.findFieldByName("y")));
  }
}
