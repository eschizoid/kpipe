package io.github.eschizoid.kpipe.format.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

/// Tests for the Schema-Registry-backed mode of [AvroFormat]. Produces records framed in
/// Confluent wire-envelope shape so the format's envelope-stripping and per-record schema
/// lookup are exercised end-to-end.
class AvroFormatRegistryTest {

  private static final String USER_V1 = """
    {"type":"record","name":"User","fields":[
      {"name":"id","type":"string"},
      {"name":"name","type":"string"}
    ]}""";

  private static final String USER_V2 = """
    {"type":"record","name":"User","fields":[
      {"name":"id","type":"string"},
      {"name":"name","type":"string"},
      {"name":"email","type":["null","string"],"default":null}
    ]}""";

  /// Encodes a record under the Confluent wire envelope: 1-byte magic (0x00) + 4-byte
  /// big-endian schema id + Avro binary payload.
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

  /// Hands out preregistered schemas by id and counts lookups.
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
  void registryBackedFormatDecodesRecordsAgainstResolvedSchema() throws IOException {
    final var schema = new Schema.Parser().parse(USER_V1);
    final var record = new GenericData.Record(schema);
    record.put("id", "u-1");
    record.put("name", "Mariano");

    final var resolver = new FakeResolver().put(101, USER_V1);
    final var format = AvroFormat.withRegistry(resolver);

    final var decoded = format.deserialize(envelope(101, record));

    assertEquals("u-1", decoded.get("id").toString());
    assertEquals("Mariano", decoded.get("name").toString());
  }

  @Test
  void schemaIsCachedAfterFirstLookup() throws IOException {
    final var schema = new Schema.Parser().parse(USER_V1);
    final var record = new GenericData.Record(schema);
    record.put("id", "u-1");
    record.put("name", "Mariano");

    final var resolver = new FakeResolver().put(101, USER_V1);
    final var format = AvroFormat.withRegistry(resolver);

    format.deserialize(envelope(101, record));
    format.deserialize(envelope(101, record));
    format.deserialize(envelope(101, record));

    assertEquals(1, resolver.calls.get(), "the format must cache parsed schemas by id");
  }

  @Test
  void mixedSchemaIdsResolveIndependently() throws IOException {
    final var v1 = new Schema.Parser().parse(USER_V1);
    final var v2 = new Schema.Parser().parse(USER_V2);

    final var oldRecord = new GenericData.Record(v1);
    oldRecord.put("id", "u-1");
    oldRecord.put("name", "OldName");

    final var newRecord = new GenericData.Record(v2);
    newRecord.put("id", "u-2");
    newRecord.put("name", "NewName");
    newRecord.put("email", "new@example.com");

    final var resolver = new FakeResolver().put(101, USER_V1).put(102, USER_V2);
    final var format = AvroFormat.withRegistry(resolver);

    final var decodedOld = format.deserialize(envelope(101, oldRecord));
    final var decodedNew = format.deserialize(envelope(102, newRecord));

    assertEquals("OldName", decodedOld.get("name").toString());
    assertEquals("NewName", decodedNew.get("name").toString());
    assertEquals("new@example.com", decodedNew.get("email").toString());
    assertEquals(2, resolver.calls.get(), "two distinct ids = two lookups");
  }

  @Test
  void wrongMagicByteThrows() {
    final var resolver = new FakeResolver().put(1, USER_V1);
    final var format = AvroFormat.withRegistry(resolver);

    final var bad = new byte[] { 0x01, 0x00, 0x00, 0x00, 0x01, 0x42 };
    final var ex = assertThrows(RuntimeException.class, () -> format.deserialize(bad));
    assertTrue(ex.getMessage().contains("magic byte"), "must mention magic byte; got: " + ex.getMessage());
  }

  @Test
  void payloadShorterThanEnvelopeThrows() {
    final var resolver = new FakeResolver();
    final var format = AvroFormat.withRegistry(resolver);

    final var tooShort = new byte[] { 0x00, 0x00, 0x00 };
    final var ex = assertThrows(RuntimeException.class, () -> format.deserialize(tooShort));
    assertTrue(ex.getMessage().contains("envelope"));
  }

  @Test
  void registryBackedSerializeThrows() {
    final var format = AvroFormat.withRegistry(new FakeResolver());
    final var schema = new Schema.Parser().parse(USER_V1);
    final var record = new GenericData.Record(schema);
    record.put("id", "u-1");
    record.put("name", "Mariano");

    assertThrows(UnsupportedOperationException.class, () -> format.serialize(record));
  }

  @Test
  void registryBackedConsoleSinkThrows() {
    final var format = AvroFormat.withRegistry(new FakeResolver());
    assertThrows(IllegalStateException.class, format::consoleSink);
  }

  @Test
  void registryBackedSchemaIsNull() {
    assertNull(AvroFormat.withRegistry(new FakeResolver()).schema());
  }

  @Test
  void rejectsNullResolver() {
    assertThrows(NullPointerException.class, () -> AvroFormat.withRegistry(null));
  }

  @Test
  void emptyPayloadReturnsNull() {
    final var format = AvroFormat.withRegistry(new FakeResolver());
    assertNull(format.deserialize(new byte[0]));
    assertNull(format.deserialize(null));
  }

  @Test
  void resolverReturningNullThrows() {
    final var resolver = new SchemaResolver() {
      @Override
      public String lookupById(final int schemaId) {
        return null;
      }
    };
    final var format = AvroFormat.withRegistry(resolver);
    final var anyEnvelope = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x01, 0x42, 0x00 };
    final var ex = assertThrows(RuntimeException.class, () -> format.deserialize(anyEnvelope));
    assertTrue(ex.getMessage().contains("empty schema"));
  }
}
