package org.kpipe.processor;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Function;
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

    // Act
    byte[] result = AvroMessageProcessor.parseAvro("simpleSchema").apply(invalidAvroBytes);

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
    final var result = AvroMessageProcessor.parseAvro("simpleSchema").apply(avroBytes);

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
    final var result = AvroMessageProcessor.parseAvro("testSchema").apply(avroBytes);

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
    final var result = AvroMessageProcessor.addField("sourceSchema", "source", "test-app").apply(avroBytes);

    // Assert
    assertNotNull(result);
    assertTrue(result.length > 0);
    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var inputStream = new ByteArrayInputStream(result);
    final var resultRecord = reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));
    assertEquals("test-value", resultRecord.get("value").toString());
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
    final var result = AvroMessageProcessor.addTimestamp("timestampSchema", "timestamp").apply(avroBytes);

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
    byte[] result = AvroMessageProcessor.removeFields("testSchema", "source", "remove_source").apply(avroBytes);

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
    final var result = AvroMessageProcessor
      .transformField(
        "transformSchema",
        "value",
        value -> value instanceof String ? ((String) value).toUpperCase() : value
      )
      .apply(avroBytes);

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
    final var result = AvroMessageProcessor
      .transformField("ageSchema", "age", value -> value instanceof Integer ? ((Integer) value) * 2 : value)
      .apply(avroBytes);

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
    final var result = AvroMessageProcessor
      .transformField(
        "unionSchema",
        "comment",
        value -> value instanceof String ? ((String) value).toUpperCase() : value
      )
      .apply(avroBytes);

    // Assert
    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var resultRecord = reader.read(
      null,
      DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(result), null)
    );
    assertEquals("SOME COMMENT", resultRecord.get("comment").toString());
  }

  @Test
  void testCompose() throws IOException {
    // Arrange
    final var schemaJson =
      """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"},
                {"name": "verified", "type": "boolean", "default": false},
                {"name": "timestamp", "type": ["null", "long"], "default": null}
              ]
            }""";
    AvroMessageProcessor.registerSchema("userSchema", schemaJson);
    final var schema = AvroMessageProcessor.getSchema("userSchema");
    final var record = new GenericData.Record(schema);
    record.put("name", "john");
    record.put("age", 25);
    record.put("verified", false);

    final var outputStream = new ByteArrayOutputStream();
    final var writer = new GenericDatumWriter<GenericRecord>(schema);
    final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    writer.write(record, encoder);
    encoder.flush();
    final var avroBytes = outputStream.toByteArray();

    // Create individual processors
    final var uppercaseName = AvroMessageProcessor.transformField(
      "userSchema",
      "name",
      value -> value instanceof String ? ((String) value).toUpperCase() : value
    );
    final var doubleAge = AvroMessageProcessor.transformField(
      "userSchema",
      "age",
      value -> value instanceof Integer ? ((Integer) value) * 2 : value
    );
    final var verify = AvroMessageProcessor.transformField("userSchema", "verified", value -> true);
    final var addTimestamp = AvroMessageProcessor.addTimestamp("userSchema", "timestamp");

    // Act
    final var pipeline = AvroMessageProcessor.compose(uppercaseName, doubleAge, verify, addTimestamp);
    final var result = pipeline.apply(avroBytes);

    // Assert
    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var resultRecord = reader.read(
      null,
      DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(result), null)
    );

    assertEquals("JOHN", resultRecord.get("name").toString());
    assertEquals(50, resultRecord.get("age"));
    assertEquals(true, resultRecord.get("verified"));
    assertNotNull(resultRecord.get("timestamp"));
  }

  @Test
  void testProcessBatch() throws IOException {
    // Arrange
    final var schemaJson =
      """
            {
              "type": "record",
              "name": "Message",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "content", "type": "string"}
              ]
            }""";
    AvroMessageProcessor.registerSchema("messageSchema", schemaJson);
    final var schema = AvroMessageProcessor.getSchema("messageSchema");

    // Create three records
    final var batch = new ArrayList<byte[]>();
    for (int i = 1; i <= 3; i++) {
      final var record = new GenericData.Record(schema);
      record.put("id", i);
      record.put("content", "message-" + i);

      final var outputStream = new ByteArrayOutputStream();
      final var writer = new GenericDatumWriter<GenericRecord>(schema);
      final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      writer.write(record, encoder);
      encoder.flush();
      batch.add(outputStream.toByteArray());
    }

    // Create processor to uppercase content
    final var uppercaseContent = AvroMessageProcessor.transformField(
      "messageSchema",
      "content",
      value -> value instanceof String ? ((String) value).toUpperCase() : value
    );

    // Act
    final var results = AvroMessageProcessor.processBatch(batch, uppercaseContent);

    // Assert
    assertEquals(3, results.size());
    for (int i = 0; i < results.size(); i++) {
      final var reader = new GenericDatumReader<GenericRecord>(schema);
      final var resultRecord = reader.read(
        null,
        DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(results.get(i)), null)
      );

      assertEquals(i + 1, resultRecord.get("id"));
      assertEquals("MESSAGE-%d".formatted(i + 1), resultRecord.get("content").toString());
    }
  }

  @Test
  void testProcessWithResult() throws IOException {
    // Arrange
    final var schemaJson =
      """
                  {
                    "type": "record",
                    "name": "Processor",
                    "fields": [
                      {"name": "data", "type": "string"},
                      {"name": "status", "type": ["null", "string"], "default": null}
                    ]
                  }""";
    AvroMessageProcessor.registerSchema("processorSchema", schemaJson);
    final var schema = AvroMessageProcessor.getSchema("processorSchema");
    final var record = new GenericData.Record(schema);
    record.put("data", "important data");

    final var outputStream = new ByteArrayOutputStream();
    final var writer = new GenericDatumWriter<GenericRecord>(schema);
    final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    writer.write(record, encoder);
    encoder.flush();
    final var avroBytes = outputStream.toByteArray();

    // Create a processor that works on GenericRecord directly
    final Function<GenericRecord, GenericRecord> recordProcessor = genericRecord -> {
      genericRecord.put("status", "processed");
      return genericRecord;
    };

    // Act
    final var result = AvroMessageProcessor.processWithResult("processorSchema", avroBytes, recordProcessor);

    // Assert
    assertTrue(result.success());
    assertNotNull(result.data());

    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var resultRecord = reader.read(
      null,
      DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(result.data()), null)
    );

    assertEquals("important data", resultRecord.get("data").toString());
    assertEquals("processed", resultRecord.get("status").toString());
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
    final var result = AvroMessageProcessor.addFields("multiFieldSchema", fieldsToAdd).apply(avroBytes);

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

  @Test
  void testParseAvroWithMagicBytes() throws IOException {
    // Arrange
    final var schemaJson =
      """
        {
          "type": "record",
          "name": "TestRecord",
          "namespace": "test",
          "fields": [
            {"name": "field1", "type": "string"},
            {"name": "field2", "type": "int"}
          ]
        }
        """;
    final var schemaKey = "testMagicSchema";
    AvroMessageProcessor.registerSchema(schemaKey, schemaJson);

    // Create Avro data
    final var schema = new Schema.Parser().parse(schemaJson);
    final var record = new GenericData.Record(schema);
    record.put("field1", "test value");
    record.put("field2", 42);

    // Serialize the record using Avro's binary encoding
    final var out = new ByteArrayOutputStream();
    final var datumWriter = new GenericDatumWriter<>(schema);
    final var encoder = EncoderFactory.get().binaryEncoder(out, null);
    datumWriter.write(record, encoder);
    encoder.flush();
    final var avroData = out.toByteArray();

    // Verify we have actual serialized data
    assertTrue(avroData.length > 0, "Serialized Avro data should not be empty");

    // Add magic byte prefix (typical Confluent format with 0 byte + 4-byte schema ID)
    final var dataWithMagicBytes = new byte[avroData.length + 5];
    // Magic byte
    dataWithMagicBytes[0] = 0;
    // 4 bytes for schema ID
    dataWithMagicBytes[1] = 0;
    dataWithMagicBytes[2] = 0;
    dataWithMagicBytes[3] = 0;
    dataWithMagicBytes[4] = 1;
    // Copy the actual Avro data after the magic bytes
    System.arraycopy(avroData, 0, dataWithMagicBytes, 5, avroData.length);

    // Act
    final var processor = AvroMessageProcessor.parseAvroWithMagicBytes(schemaKey, 5);

    // Process the data
    final var result = processor.apply(dataWithMagicBytes);

    // Verify the result is not empty
    assertNotNull(result);
    assertTrue(result.length > 0);

    // Parse and verify the processed result
    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var decoder = DecoderFactory.get().binaryDecoder(result, null);
    final var parsedRecord = reader.read(null, decoder);

    // Verify fields match our original record
    assertEquals("test value", parsedRecord.get("field1").toString());
    assertEquals(42, parsedRecord.get("field2"));
  }
}
