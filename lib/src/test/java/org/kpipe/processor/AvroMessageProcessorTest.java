package org.kpipe.processor;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AvroMessageProcessorTest {

  @AfterEach
  public void clearSchemaRegistry() {
    AvroMessageProcessor.clearSchemaRegistry();
  }

  @Test
  void testParseAvroInvalidRecord() {
    // Arrange
    final var schemaJson =
      """
      {
        "type": "record",
        "name": "Simple",
        "fields": [
          {"name": "value", "type": "string"}
        ]
      }""";
    byte[] invalidAvroBytes = "invalid avro".getBytes(StandardCharsets.UTF_8);
    AvroMessageProcessor.registerSchema("simpleSchema", schemaJson);
    final var schema = AvroMessageProcessor.getSchema("simpleSchema");

    // Act
    byte[] result = AvroMessageProcessor.processAvro(invalidAvroBytes, schema, record -> record);

    // Assert
    assertEquals(0, result.length);
  }

  @Test
  void testSimpleAvroProcessing() throws IOException {
    // Arrange
    final var schemaJson =
      """
      {
        "type": "record",
        "name": "Simple",
        "fields": [
          {"name": "value", "type": "string"}
        ]
      }""";
    final var schema = new Schema.Parser().parse(schemaJson);
    final var record = new GenericData.Record(schema);
    record.put("value", "test");
    byte[] avroBytes;
    try (final var outputStream = new ByteArrayOutputStream()) {
      final var writer = new GenericDatumWriter<>(schema);
      final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      writer.write(record, encoder);
      encoder.flush();
      avroBytes = outputStream.toByteArray();
    }
    AvroMessageProcessor.registerSchema("simpleSchema", schemaJson);

    // Act
    final var result = AvroMessageProcessor.processAvro(avroBytes, schema, record1 -> record1);

    // Assert
    assertNotNull(result);
    assertEquals(avroBytes.length, result.length, "Result should have the same length as input");
  }

  @Test
  void testParseAvroValidRecord() throws IOException {
    // Arrange
    final var schemaJson =
      """
      {
        "type": "record",
        "name": "Simple",
        "fields": [
          {"name": "value", "type": "string"}
        ]
      }""";
    final var schema = new Schema.Parser().parse(schemaJson);
    final var record = new GenericData.Record(schema);
    record.put("value", "test-value");
    final byte[] avroBytes;
    try (final var outputStream = new ByteArrayOutputStream()) {
      final var writer = new GenericDatumWriter<GenericRecord>(schema);
      final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      writer.write(record, encoder);
      encoder.flush();
      avroBytes = outputStream.toByteArray();
    }
    AvroMessageProcessor.registerSchema("testSchema", schemaJson);

    // Act
    final var result = AvroMessageProcessor.processAvro(avroBytes, schema, record1 -> record1);

    // Assert
    assertNotNull(result);
    assertEquals(avroBytes.length, result.length);
    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var inputStream = new ByteArrayInputStream(result);
    final var resultRecord = reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));
    assertEquals("test-value", resultRecord.get("value").toString());
  }

  @Test
  void testAddFieldValidRecord() throws IOException {
    // Arrange
    final var schemaJson =
      """
      {
        "type": "record",
        "name": "Simple",
        "fields": [
          {"name": "value", "type": "string"},
          {"name": "source", "type": ["null", "string"], "default": null}
        ]
      }""";
    final var schema = new Schema.Parser().parse(schemaJson);
    final var record = new GenericData.Record(schema);
    record.put("value", "test-value");
    final byte[] avroBytes;
    try (final var outputStream = new ByteArrayOutputStream()) {
      final var writer = new GenericDatumWriter<GenericRecord>(schema);
      final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      writer.write(record, encoder);
      encoder.flush();
      avroBytes = outputStream.toByteArray();
    }
    AvroMessageProcessor.registerSchema("sourceSchema", schemaJson);

    // Act
    final var result = AvroMessageProcessor.processAvro(
      avroBytes,
      schema,
      AvroMessageProcessor.addFieldOperator("source", "test-app")
    );

    // Assert
    assertNotNull(result);
    assertTrue(result.length > 0);
    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var inputStream = new ByteArrayInputStream(result);
    final var resultRecord = reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));
    assertEquals("test-value", resultRecord.get("value").toString());
    assertEquals("test-app", resultRecord.get("source").toString());
  }

  @Test
  void testAddTimestamp() throws IOException {
    // Arrange
    final var schemaJson =
      """
      {
        "type": "record",
        "name": "Simple",
        "fields": [
          {"name": "value", "type": "string"},
          {"name": "timestamp", "type": ["null", "long"], "default": null}
        ]
      }""";
    final var schema = new Schema.Parser().parse(schemaJson);
    final var record = new GenericData.Record(schema);
    record.put("value", "test-value");
    byte[] avroBytes;
    try (final var outputStream = new ByteArrayOutputStream()) {
      final var writer = new GenericDatumWriter<>(schema);
      var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      writer.write(record, encoder);
      encoder.flush();
      avroBytes = outputStream.toByteArray();
    }
    AvroMessageProcessor.registerSchema("timestampSchema", schemaJson);

    // Act
    final var result = AvroMessageProcessor.processAvro(
      avroBytes,
      schema,
      AvroMessageProcessor.addTimestampOperator("timestamp")
    );

    // Assert
    assertNotNull(result);
    assertTrue(result.length > 0);
    try (final var inputStream = new ByteArrayInputStream(result)) {
      final var reader = new GenericDatumReader<GenericRecord>(schema);
      GenericRecord resultRecord = reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));
      assertEquals("test-value", resultRecord.get("value").toString());
      assertInstanceOf(Long.class, resultRecord.get("timestamp"));
      assertTrue((Long) resultRecord.get("timestamp") > 0L);
    }
  }

  @Test
  void testRemoveFields() throws IOException {
    // Arrange
    final var schemaJson =
      """
      {
        "type": "record",
        "name": "TestRecord",
        "fields": [
          {"name": "id", "type": "int"},
          {"name": "source", "type": "string"},
          {"name": "remove_source", "type": ["null", "string"], "default": null}
        ]
      }""";
    final var schema = new Schema.Parser().parse(schemaJson);
    AvroMessageProcessor.registerSchema("testSchema", schemaJson);
    final var record = new GenericData.Record(schema);
    record.put("id", 123);
    record.put("source", "test-source");
    record.put("remove_source", "remove-source");
    byte[] avroBytes;
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      final var writer = new GenericDatumWriter<>(schema);
      var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      writer.write(record, encoder);
      encoder.flush();
      avroBytes = outputStream.toByteArray();
    }

    // Act
    byte[] result = AvroMessageProcessor.processAvro(
      avroBytes,
      schema,
      AvroMessageProcessor.removeFieldsOperator(schema, "source", "remove_source")
    );

    // Assert
    assertNotNull(result);
    assertTrue(result.length > 0);
    try (final var inputStream = new ByteArrayInputStream(result)) {
      final var reader = new GenericDatumReader<GenericRecord>(schema);
      final var resultRecord = reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));
      assertEquals(123, resultRecord.get("id"));
      assertEquals("", resultRecord.get("source").toString());
      assertNull(resultRecord.get("remove_source"));
    }
  }

  @Test
  void testTransformField() throws IOException {
    // Arrange
    final var simpleSchemaJson =
      """
      {
        "type": "record",
        "name": "Simple",
        "fields": [
          {"name": "value", "type": "string"}
        ]
      }""";
    AvroMessageProcessor.registerSchema("transformSchema", simpleSchemaJson);
    final var schema = AvroMessageProcessor.getSchema("transformSchema");
    final var record = new GenericData.Record(schema);
    record.put("value", "test-value");
    final var outputStream = new ByteArrayOutputStream();
    final var writer = new GenericDatumWriter<GenericRecord>(schema);
    final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    writer.write(record, encoder);
    encoder.flush();
    final var avroBytes = outputStream.toByteArray();

    // Act
    final var result = AvroMessageProcessor.processAvro(
      avroBytes,
      schema,
      AvroMessageProcessor.transformFieldOperator(
        schema,
        "value",
        value -> value instanceof String ? ((String) value).toUpperCase() : value
      )
    );

    // Assert
    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var resultRecord = reader.read(
      null,
      DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(result), null)
    );
    assertEquals("TEST-VALUE", resultRecord.get("value").toString());
  }

  @Test
  void testTransformNumericField() throws IOException {
    // Arrange
    final var simpleSchemaJson =
      """
      {
        "type": "record",
        "name": "Simple",
        "fields": [
          {"name": "value", "type": "string"},
          {"name": "age", "type": "int"}
        ]
      }""";
    AvroMessageProcessor.registerSchema("ageSchema", simpleSchemaJson);
    final var schema = AvroMessageProcessor.getSchema("ageSchema");
    final var record = new GenericData.Record(schema);
    record.put("value", "test-value");
    record.put("age", 30);
    final var outputStream = new ByteArrayOutputStream();
    final var writer = new GenericDatumWriter<GenericRecord>(schema);
    final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    writer.write(record, encoder);
    encoder.flush();
    final var avroBytes = outputStream.toByteArray();

    // Act
    final var result = AvroMessageProcessor.processAvro(
      avroBytes,
      schema,
      AvroMessageProcessor.transformFieldOperator(
        schema,
        "age",
        value -> value instanceof Integer ? ((Integer) value) * 2 : value
      )
    );

    // Assert
    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var resultRecord = reader.read(
      null,
      DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(result), null)
    );
    assertEquals(60, resultRecord.get("age"));
  }

  @Test
  void testTransformUnionField() throws IOException {
    // Arrange
    final var unionSchemaJson =
      """
      {
        "type": "record",
        "name": "UnionRecord",
        "fields": [
          {"name": "id", "type": "int"},
          {"name": "comment", "type": ["null", "string"], "default": null}
        ]
      }""";
    AvroMessageProcessor.registerSchema("unionSchema", unionSchemaJson);
    final var schema = AvroMessageProcessor.getSchema("unionSchema");
    final var record = new GenericData.Record(schema);
    record.put("id", 100);
    record.put("comment", "some comment");
    final var outputStream = new ByteArrayOutputStream();
    final var writer = new GenericDatumWriter<GenericRecord>(schema);
    final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    writer.write(record, encoder);
    encoder.flush();
    final var avroBytes = outputStream.toByteArray();

    // Act
    final var result = AvroMessageProcessor.processAvro(
      avroBytes,
      schema,
      AvroMessageProcessor.transformFieldOperator(
        schema,
        "comment",
        value -> value instanceof String ? ((String) value).toUpperCase() : value
      )
    );

    // Assert
    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var resultRecord = reader.read(
      null,
      DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(result), null)
    );
    assertEquals("SOME COMMENT", resultRecord.get("comment").toString());
  }

  @Test
  void testAddFields() throws IOException {
    // Arrange
    final var schemaJson =
      """
      {
        "type": "record",
        "name": "MultiField",
        "fields": [
          {"name": "id", "type": "int"},
          {"name": "source", "type": ["null", "string"], "default": null},
          {"name": "environment", "type": ["null", "string"], "default": null},
          {"name": "version", "type": ["null", "string"], "default": null}
        ]
      }""";
    AvroMessageProcessor.registerSchema("multiFieldSchema", schemaJson);
    final var schema = AvroMessageProcessor.getSchema("multiFieldSchema");
    final var record = new GenericData.Record(schema);
    record.put("id", 42);

    final var outputStream = new ByteArrayOutputStream();
    final var writer = new GenericDatumWriter<GenericRecord>(schema);
    final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    writer.write(record, encoder);
    encoder.flush();
    final var avroBytes = outputStream.toByteArray();

    // Create a map of fields to add
    final var fieldsToAdd = Map.<String, Object>of(
      "source",
      "test-app",
      "environment",
      "development",
      "version",
      "1.0.0"
    );

    // Act
    final var result = AvroMessageProcessor.processAvro(
      avroBytes,
      schema,
      AvroMessageProcessor.addFieldsOperator(fieldsToAdd)
    );

    // Assert
    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var resultRecord = reader.read(
      null,
      DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(result), null)
    );

    assertEquals(42, resultRecord.get("id"));
    assertEquals("test-app", resultRecord.get("source").toString());
    assertEquals("development", resultRecord.get("environment").toString());
    assertEquals("1.0.0", resultRecord.get("version").toString());
  }
}
