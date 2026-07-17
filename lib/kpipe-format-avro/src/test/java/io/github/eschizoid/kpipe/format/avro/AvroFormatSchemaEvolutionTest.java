package io.github.eschizoid.kpipe.format.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

/// End-to-end schema-evolution behaviour for [AvroFormat].
///
/// The point of registry-backed decoding is that every record is decoded against the *exact*
/// writer schema that produced it — resolved per record from the wire envelope — rather than a
/// single schema chosen once at startup. That is what keeps a consumer correct across producer
/// schema rolls. These tests prove three things directly:
///
/// 1. A registry-backed format decodes records written under different schema versions (added
///    field, removed field) correctly, because the writer schema is looked up per record.
/// 2. A static-mode format bound to an *old* schema silently mis-decodes bytes written under a
///    newer, incompatible schema — the corruption the registry path exists to prevent. This is
///    the contrast that justifies per-record lookup even when the registry enforces compatible
///    evolution.
/// 3. Stripping the Confluent envelope before a registry-backed format sees it (the
///    `withSchemaRegistry(...)` + `skipBytes(5)` misconfiguration) surfaces a decode error rather
///    than quietly returning wrong data.
class AvroFormatSchemaEvolutionTest {

  /// v1: id + name.
  private static final String USER_V1 = """
    {"type":"record","name":"User","fields":[
      {"name":"id","type":"string"},
      {"name":"name","type":"string"}
    ]}""";

  /// v2: appends an optional `email` (union with null default) — BACKWARD/FORWARD compatible.
  private static final String USER_V2 = """
    {"type":"record","name":"User","fields":[
      {"name":"id","type":"string"},
      {"name":"name","type":"string"},
      {"name":"email","type":["null","string"],"default":null}
    ]}""";

  /// v3: drops `name`, keeps `id` + optional `email`. A field removal — the kind of non-append
  /// evolution a static v1-bound reader cannot decode safely.
  private static final String USER_V3 = """
    {"type":"record","name":"User","fields":[
      {"name":"id","type":"string"},
      {"name":"email","type":["null","string"],"default":null}
    ]}""";

  /// Two records with a leading int field, used to show concretely what goes wrong when a reader
  /// decodes payload framed by a different writer schema: the field a static reader lands on no
  /// longer matches the bytes the writer laid down.
  private static final String EVENT_WIDE = """
    {"type":"record","name":"Event","fields":[
      {"name":"seq","type":"long"},
      {"name":"flag","type":"boolean"},
      {"name":"label","type":"string"}
    ]}""";

  private static final String EVENT_NARROW = """
    {"type":"record","name":"Event","fields":[
      {"name":"seq","type":"long"},
      {"name":"label","type":"string"}
    ]}""";

  /// Encodes `record` under the Confluent wire envelope: 1-byte magic (0x00) + 4-byte big-endian
  /// schema id + Avro binary payload written with the record's own schema.
  private static byte[] envelope(final int schemaId, final GenericRecord record) throws IOException {
    final var bytes = new ByteArrayOutputStream();
    bytes.write(0x00);
    bytes.write(ByteBuffer.allocate(4).putInt(schemaId).array());
    final var writer = new GenericDatumWriter<GenericRecord>(record.getSchema());
    final var encoder = EncoderFactory.get().binaryEncoder(bytes, null);
    writer.write(record, encoder);
    encoder.flush();
    return bytes.toByteArray();
  }

  /// Raw Avro binary payload (no envelope) for the static-mode corruption contrast.
  private static byte[] payload(final GenericRecord record) throws IOException {
    final var bytes = new ByteArrayOutputStream();
    final var writer = new GenericDatumWriter<GenericRecord>(record.getSchema());
    final var encoder = EncoderFactory.get().binaryEncoder(bytes, null);
    writer.write(record, encoder);
    encoder.flush();
    return bytes.toByteArray();
  }

  /// Serves preregistered schemas by id and counts lookups.
  private static final class FakeResolver implements SchemaResolver {

    final Map<Integer, String> schemas = new HashMap<>();
    final AtomicInteger calls = new AtomicInteger();

    FakeResolver put(final int id, final String json) {
      schemas.put(id, json);
      return this;
    }

    @Override
    public String lookupById(final int schemaId) {
      calls.incrementAndGet();
      final var json = schemas.get(schemaId);
      if (json == null) throw new RuntimeException("Unknown schema id " + schemaId);
      return json;
    }
  }

  @Test
  void registryDecodesAddedFieldRecordAgainstItsOwnWriterSchema() throws IOException {
    final var v2 = new Schema.Parser().parse(USER_V2);
    final var newRecord = new GenericData.Record(v2);
    newRecord.put("id", "u-2");
    newRecord.put("name", "New");
    newRecord.put("email", "new@example.com");

    final var resolver = new FakeResolver().put(1, USER_V1).put(2, USER_V2);
    final var format = AvroFormat.withRegistry(resolver);

    final var decoded = format.deserialize(envelope(2, newRecord));

    // The appended field is present because the v2 writer schema was resolved for this record;
    // a v1-bound static reader would never surface it.
    assertEquals("u-2", decoded.get("id").toString());
    assertEquals("New", decoded.get("name").toString());
    assertEquals("new@example.com", decoded.get("email").toString());
  }

  @Test
  void registryDecodesRemovedFieldRecordAgainstItsOwnWriterSchema() throws IOException {
    final var v3 = new Schema.Parser().parse(USER_V3);
    final var record = new GenericData.Record(v3);
    record.put("id", "u-3");
    record.put("email", "drop@example.com");

    final var resolver = new FakeResolver().put(1, USER_V1).put(3, USER_V3);
    final var format = AvroFormat.withRegistry(resolver);

    final var decoded = format.deserialize(envelope(3, record));

    // `name` was removed in v3; decoding against the v3 writer schema yields a record that has no
    // `name` field at all rather than mis-reading the following bytes as a string.
    assertEquals("u-3", decoded.get("id").toString());
    assertEquals("drop@example.com", decoded.get("email").toString());
    assertNull(
      decoded.getSchema().getField("name"),
      "v3 writer schema has no name field, so the decoded record must not either"
    );
  }

  @Test
  void registryDecodesInterleavedSchemaVersionsCorrectly() throws IOException {
    // A single format instance consuming a stream that interleaves v1, v2 and v3 records — the
    // realistic during-a-roll case. Each record carries its own schema id and must decode against
    // its own writer schema, independent of arrival order.
    final var v1 = new Schema.Parser().parse(USER_V1);
    final var v2 = new Schema.Parser().parse(USER_V2);
    final var v3 = new Schema.Parser().parse(USER_V3);

    final var r1 = new GenericData.Record(v1);
    r1.put("id", "a");
    r1.put("name", "Alice");

    final var r2 = new GenericData.Record(v2);
    r2.put("id", "b");
    r2.put("name", "Bob");
    r2.put("email", "bob@example.com");

    final var r3 = new GenericData.Record(v3);
    r3.put("id", "c");
    r3.put("email", "carol@example.com");

    final var resolver = new FakeResolver().put(1, USER_V1).put(2, USER_V2).put(3, USER_V3);
    final var format = AvroFormat.withRegistry(resolver);

    final var d2 = format.deserialize(envelope(2, r2));
    final var d1 = format.deserialize(envelope(1, r1));
    final var d3 = format.deserialize(envelope(3, r3));

    assertEquals("Alice", d1.get("name").toString());
    assertNull(d1.getSchema().getField("email"), "v1 record has no email field");

    assertEquals("Bob", d2.get("name").toString());
    assertEquals("bob@example.com", d2.get("email").toString());

    assertEquals("c", d3.get("id").toString());
    assertEquals("carol@example.com", d3.get("email").toString());
    assertNull(d3.getSchema().getField("name"), "v3 record has no name field");
  }

  @Test
  void staticReaderBoundToOldSchemaMisDecodesNewerIncompatibleBytes() throws IOException {
    // The corruption the registry path prevents. EVENT_WIDE writes [seq, flag, label]; a static
    // reader bound to EVENT_NARROW expects [seq, label]. After `seq` the narrow reader treats the
    // boolean `flag` byte as the start of `label`'s length-prefixed string — so `label` decodes to
    // garbage (or the decode blows up), never the value the producer wrote. No exception is
    // guaranteed: silent corruption is the whole danger.
    final var wide = new Schema.Parser().parse(EVENT_WIDE);
    final var record = new GenericData.Record(wide);
    record.put("seq", 7L);
    record.put("flag", true);
    record.put("label", "real-label");
    final var wideBytes = payload(record);

    final var staticNarrow = new AvroFormat(new Schema.Parser().parse(EVENT_NARROW));

    // Either the mis-framed decode throws, or it returns a record whose `label` is NOT the value
    // the producer actually wrote. Both outcomes prove a static old-schema reader cannot be
    // trusted on newer incompatible bytes.
    String corruptLabel = null;
    try {
      final var misdecoded = staticNarrow.deserialize(wideBytes);
      corruptLabel = String.valueOf(misdecoded.get("label"));
    } catch (final RuntimeException expected) {
      // Mis-framing surfaced as a decode error — also an acceptable "did not silently succeed",
      // but require it to be the codec's own failure so an unrelated error can't green this.
      assertTrue(
        expected.getMessage() != null && expected.getMessage().contains("AvroFormat.deserialize failed"),
        () -> "expected an Avro decode failure, got: " + expected
      );
      return;
    }
    assertNotEquals(
      "real-label",
      corruptLabel,
      "a static narrow reader must not reproduce the wide writer's label — that would mean the " +
        "corruption went undetected"
    );
  }

  @Test
  void registryDecodesNewerBytesThatStaticOldReaderWouldCorrupt() throws IOException {
    // Same wide-vs-narrow framing, but now the wide bytes carry an envelope and the registry
    // resolves the wide writer schema per record. The label decodes to exactly what the producer
    // wrote — proving the registry path is correct precisely where the static old reader corrupts.
    final var wide = new Schema.Parser().parse(EVENT_WIDE);
    final var record = new GenericData.Record(wide);
    record.put("seq", 7L);
    record.put("flag", true);
    record.put("label", "real-label");

    final var resolver = new FakeResolver().put(9, EVENT_WIDE);
    final var format = AvroFormat.withRegistry(resolver);

    final var decoded = format.deserialize(envelope(9, record));

    assertEquals(7L, decoded.get("seq"));
    assertEquals(true, decoded.get("flag"));
    assertEquals("real-label", decoded.get("label").toString());
  }

  @Test
  void skipBytesBeforeRegistryFormatStripsEnvelopeAndForcesDecodeError() throws IOException {
    // Combining withSchemaRegistry(...) with skipBytes(5) double-strips: the 5-byte envelope is
    // removed upstream, so the format sees the bare Avro payload where it expects the magic byte +
    // schema id. Simulate skipBytes(5) by hand-stripping the envelope, then feed it to a
    // registry-backed format. The first payload byte is no longer 0x00, so the format must reject
    // it rather than read a bogus schema id and silently decode wrong data.
    final var v2 = new Schema.Parser().parse(USER_V2);
    final var record = new GenericData.Record(v2);
    record.put("id", "u-2");
    record.put("name", "New");
    record.put("email", "new@example.com");

    final var framed = envelope(2, record);
    final var stripped = new byte[framed.length - 5];
    System.arraycopy(framed, 5, stripped, 0, stripped.length);

    final var resolver = new FakeResolver().put(2, USER_V2);
    final var format = AvroFormat.withRegistry(resolver);

    final var ex = assertThrows(IllegalStateException.class, () -> format.deserialize(stripped));
    // The Avro payload for this record does not begin with the 0x00 magic byte, so the envelope
    // check fires before any schema lookup happens.
    assertTrue(
      ex.getMessage().contains("magic byte") || ex.getMessage().contains("envelope"),
      "double-stripped record must fail the envelope check, not decode silently; got: " + ex.getMessage()
    );
    assertEquals(0, resolver.calls.get(), "envelope must be rejected before any schema lookup");
  }

  @Test
  void skipBytesShorteningEnvelopeBelowFiveBytesIsRejected() throws IOException {
    // Degenerate variant: the record is short enough that stripping 5 bytes leaves fewer than the
    // envelope length, so the too-short guard fires instead of the magic-byte guard. Still a hard
    // error, never silent.
    final var v1 = new Schema.Parser().parse(USER_V1);
    final var record = new GenericData.Record(v1);
    record.put("id", "");
    record.put("name", "");

    final var framed = envelope(1, record);
    final var stripped = new byte[Math.max(0, framed.length - 5)];
    System.arraycopy(framed, 5, stripped, 0, stripped.length);

    final var format = AvroFormat.withRegistry(new FakeResolver().put(1, USER_V1));

    // Empty-string fields encode to two zero-length varints, so the stripped payload is 2 bytes —
    // below ConfluentEnvelope.HEADER_LENGTH. deserialize returns null only for null/empty input;
    // a 2-byte input
    // reaches the too-short envelope guard.
    if (stripped.length == 0) {
      assertNull(format.deserialize(stripped));
    } else {
      final var ex = assertThrows(IllegalStateException.class, () -> format.deserialize(stripped));
      assertTrue(
        ex.getMessage().contains("envelope") || ex.getMessage().contains("magic byte"),
        "short double-stripped record must be rejected; got: " + ex.getMessage()
      );
    }
  }
}
