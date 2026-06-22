package io.github.eschizoid.kpipe.format.avro;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.Result;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

/// Avro edge cases that live on the static-mode + `skipBytes` path: the exact 5-byte Confluent
/// envelope boundary, off-by-one wire-prefix stripping, and oversized payloads. These exercise
/// the real pipeline built from the registry, so the slice fed to the codec is whatever the
/// builder produced — the surface where an envelope off-by-one corrupts silently.
class AvroFormatEnvelopeEdgeCasesTest {

  private static final String SCHEMA_JSON = """
    {
      "type": "record",
      "name": "Simple",
      "fields": [
        {"name": "value", "type": "string"}
      ]
    }""";

  private static final MessageProcessorRegistry REGISTRY = new MessageProcessorRegistry();

  /// Drives `bytes` through deserialize -> process -> serialize, re-throwing a failure cause.
  private static <T> byte[] roundTrip(final MessagePipeline<T> pipeline, final byte[] bytes) {
    final var deserialized = pipeline.deserializeOrFail(bytes);
    return switch (pipeline.process(deserialized)) {
      case Result.Passed<T> p -> pipeline.serialize(p.value());
      case Result.Filtered<T> _ -> null;
      case Result.Failed<T> f -> {
        if (f.cause() instanceof RuntimeException re) throw re;
        if (f.cause() instanceof Error err) throw err;
        throw new RuntimeException(f.cause());
      }
    };
  }

  /// Encodes a record's raw Avro binary (no envelope).
  private static byte[] encode(final Schema schema, final String value) throws IOException {
    final var record = new GenericData.Record(schema);
    record.put("value", value);
    try (final var out = new ByteArrayOutputStream()) {
      final var writer = new GenericDatumWriter<GenericRecord>(schema);
      final var encoder = EncoderFactory.get().binaryEncoder(out, null);
      writer.write(record, encoder);
      encoder.flush();
      return out.toByteArray();
    }
  }

  /// Prepends the 5-byte Confluent envelope (1-byte magic 0x00 + 4-byte big-endian schema id).
  private static byte[] withEnvelope(final int schemaId, final byte[] payload) {
    final var framed = new byte[5 + payload.length];
    framed[0] = 0x00;
    ByteBuffer.wrap(framed, 1, 4).putInt(schemaId);
    System.arraycopy(payload, 0, framed, 5, payload.length);
    return framed;
  }

  @Test
  void skipBytesFiveStripsTheEnvelopeAndDecodesCleanly() throws IOException {
    final var format = AvroFormat.of(SCHEMA_JSON);
    final var payload = encode(format.schema(), "hello");
    final var framed = withEnvelope(42, payload);

    final var pipeline = REGISTRY.pipeline(format).skipBytes(5).build();
    final var decoded = pipeline.deserializeOrFail(framed);

    assertNotNull(decoded);
    assertEquals("hello", decoded.get("value").toString());
  }

  @Test
  void offByOneSkipFourCorruptsOrFails() throws IOException {
    // Leaving the trailing schema-id byte in front of the payload either makes Avro throw or
    // decode a different string — never the clean value. Pins that exactly 5 is required.
    final var format = AvroFormat.of(SCHEMA_JSON);
    final var payload = encode(format.schema(), "hello");
    final var framed = withEnvelope(42, payload);

    final var pipeline = REGISTRY.pipeline(format).skipBytes(4).build();

    var corrupted = false;
    try {
      final var decoded = pipeline.deserializeOrFail(framed);
      corrupted = decoded == null || !"hello".equals(decoded.get("value").toString());
    } catch (final RuntimeException e) {
      // A throw is an acceptable "did not silently match" outcome — but require it to be the
      // format's decode failure, not an unrelated NPE/wiring error that would green vacuously.
      assertDecodeFailure(e);
      corrupted = true;
    }
    assertTrue(corrupted, "skip(4) must not decode to the clean value");
  }

  @Test
  void offByOneSkipSixCorruptsOrFails() throws IOException {
    // Eating the first payload byte shifts every downstream field into corruption.
    final var format = AvroFormat.of(SCHEMA_JSON);
    final var payload = encode(format.schema(), "hello");
    final var framed = withEnvelope(42, payload);

    final var pipeline = REGISTRY.pipeline(format).skipBytes(6).build();

    var corrupted = false;
    try {
      final var decoded = pipeline.deserializeOrFail(framed);
      corrupted = decoded == null || !"hello".equals(decoded.get("value").toString());
    } catch (final RuntimeException e) {
      assertDecodeFailure(e);
      corrupted = true;
    }
    assertTrue(corrupted, "skip(6) must not decode to the clean value");
  }

  @Test
  void oversizedPayloadRoundTripsWithoutTruncation() throws IOException {
    // A multi-megabyte string value exercises the codec's buffer growth on a large record.
    final var format = AvroFormat.of(SCHEMA_JSON);
    final var big = "x".repeat(2_000_000);
    final var bytes = encode(format.schema(), big);

    final var pipeline = REGISTRY.pipeline(format).build();
    final var result = roundTrip(pipeline, bytes);

    assertNotNull(result);
    assertArrayEquals(bytes, result, "an oversized payload must round-trip byte-for-byte");
    assertEquals(big, format.deserialize(result).get("value").toString());
  }

  @Test
  void registryModeDecodesValidEnvelopeWithoutSkipBytes() throws IOException {
    // The complement to skipBytes-5: registry mode consumes the envelope itself, so the raw
    // framed record decodes straight through with no skip configured.
    final var schema = new Schema.Parser().parse(SCHEMA_JSON);
    final var payload = encode(schema, "via-registry");
    final var framed = withEnvelope(7, payload);

    final var format = AvroFormat.withRegistry(id -> id == 7 ? SCHEMA_JSON : null);
    final var decoded = format.deserialize(framed);

    assertEquals("via-registry", decoded.get("value").toString());
  }

  /// Confirms a caught throw is the Avro codec's own decode failure — not an unrelated
  /// NPE/wiring error — so an off-by-one test cannot pass on the wrong exception.
  private static void assertDecodeFailure(final RuntimeException e) {
    assertTrue(
      e.getMessage() != null && e.getMessage().contains("AvroFormat.deserialize failed"),
      () -> "expected an Avro decode failure, got: " + e
    );
  }
}
