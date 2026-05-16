package org.kpipe.format.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.junit.jupiter.api.Test;

class AvroFormatBehaviorTest {

  private static final String USER_SCHEMA_JSON = """
    {
      "type": "record",
      "name": "User",
      "namespace": "com.example",
      "fields": [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"}
      ]
    }
    """;

  @Test
  void constructorRejectsNullSchema() {
    assertThrows(NullPointerException.class, () -> new AvroFormat((Schema) null));
  }

  @Test
  void ofRejectsNullSchemaJson() {
    assertThrows(NullPointerException.class, () -> AvroFormat.of(null));
  }

  @Test
  void ofParsesSchemaJson() {
    final var format = AvroFormat.of(USER_SCHEMA_JSON);
    assertEquals("com.example.User", format.schema().getFullName());
  }

  @Test
  void schemaAccessorReturnsBoundSchema() {
    final var schema = new Schema.Parser().parse(USER_SCHEMA_JSON);
    final var format = new AvroFormat(schema);
    assertEquals(schema, format.schema());
  }

  @Test
  void ofRejectsInvalidSchemaJson() {
    assertThrows(SchemaParseException.class, () -> AvroFormat.of("{invalid json}"));
  }

  @Test
  void consoleSinkIsBoundToFormatSchema() {
    final var format = AvroFormat.of(USER_SCHEMA_JSON);
    final var sink = format.consoleSink();
    assertNotNull(sink);
    assertEquals(format.schema(), sink.schema());
  }

  @Test
  void serializeRoundTripsThroughDeserialize() {
    final var format = AvroFormat.of(USER_SCHEMA_JSON);
    final var record = new org.apache.avro.generic.GenericData.Record(format.schema());
    record.put("id", "1");
    record.put("name", "Alice");
    record.put("email", "alice@example.com");

    final var bytes = format.serialize(record);
    final var decoded = format.deserialize(bytes);

    assertEquals("1", decoded.get("id").toString());
    assertEquals("Alice", decoded.get("name").toString());
    assertEquals("alice@example.com", decoded.get("email").toString());
  }

  @Test
  void deserializeReturnsNullForNullOrEmptyBytes() {
    final var format = AvroFormat.of(USER_SCHEMA_JSON);
    assertEquals(null, format.deserialize(null));
    assertEquals(null, format.deserialize(new byte[0]));
  }

  @Test
  void serializeReturnsNullForNullData() {
    final var format = AvroFormat.of(USER_SCHEMA_JSON);
    assertEquals(null, format.serialize(null));
  }

  @Test
  void deserializeThrowsForGarbageBytes() {
    final var format = AvroFormat.of(USER_SCHEMA_JSON);
    assertThrows(RuntimeException.class, () -> format.deserialize(new byte[] { 0x7f, 0x7f, 0x7f }));
  }
}
