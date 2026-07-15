package io.github.eschizoid.kpipe.format.protobuf.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.DynamicMessage;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/// End-to-end Protobuf Schema-Registry decode through the REAL Confluent compiler, discovered via
/// [java.util.ServiceLoader] by `ProtobufFormat.withRegistry(resolver)` — exactly the production
/// path an SR consumer runs. Feeds Confluent-shaped wire envelopes (magic + schema id +
/// message-index array + payload) and asserts the [DynamicMessage] round-trips, including a
/// multi-message `.proto` selected by a non-zero message index.
class ProtobufFormatConfluentE2ETest {

  private static final String CATALOG_PROTO = """
    syntax = "proto3";
    package com.kpipe.catalog;
    message Customer { int64 id = 1; string name = 2; }
    message Order { int64 order_id = 1; string sku = 2; }
    """;

  private final ConfluentProtobufDescriptorCompiler compiler = new ConfluentProtobufDescriptorCompiler();

  /// Kafka `ByteUtils` zig-zag base-128 varint — the encoding Confluent uses for message indexes.
  private static void writeZigZagVarint(final ByteArrayOutputStream out, final int value) {
    var v = (value << 1) ^ (value >> 31);
    while ((v & ~0x7f) != 0) {
      out.write((v & 0x7f) | 0x80);
      v >>>= 7;
    }
    out.write(v);
  }

  /// Confluent Protobuf wire envelope: magic 0x00 + 4-byte big-endian id + message-index array
  /// (single 0x00 shorthand for `[0]`, else zig-zag size then elements) + protobuf payload.
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

  @Test
  void firstMessageRoundTripsThroughTheRealCompiler() {
    final var customer = compiler.compile(CATALOG_PROTO, new int[] { 0 });
    final var payload = DynamicMessage.newBuilder(customer)
      .setField(customer.findFieldByName("id"), 42L)
      .setField(customer.findFieldByName("name"), "Mariano")
      .build()
      .toByteArray();

    final SchemaResolver resolver = schemaId -> CATALOG_PROTO;
    final var format = ProtobufFormat.withRegistry(resolver);

    final var decoded = format.deserialize(envelope(1, new int[] { 0 }, payload));

    assertEquals("Customer", decoded.getDescriptorForType().getName());
    assertEquals(42L, decoded.getField(decoded.getDescriptorForType().findFieldByName("id")));
    assertEquals("Mariano", decoded.getField(decoded.getDescriptorForType().findFieldByName("name")));
  }

  @Test
  void secondMessageResolvedByNonZeroMessageIndex() {
    final var order = compiler.compile(CATALOG_PROTO, new int[] { 1 });
    final var payload = DynamicMessage.newBuilder(order)
      .setField(order.findFieldByName("order_id"), 7L)
      .setField(order.findFieldByName("sku"), "ABC-9")
      .build()
      .toByteArray();

    final var format = ProtobufFormat.withRegistry(schemaId -> CATALOG_PROTO);

    final var decoded = format.deserialize(envelope(1, new int[] { 1 }, payload));

    assertEquals("Order", decoded.getDescriptorForType().getName());
    assertEquals(7L, decoded.getField(decoded.getDescriptorForType().findFieldByName("order_id")));
    assertEquals("ABC-9", decoded.getField(decoded.getDescriptorForType().findFieldByName("sku")));
  }
}
