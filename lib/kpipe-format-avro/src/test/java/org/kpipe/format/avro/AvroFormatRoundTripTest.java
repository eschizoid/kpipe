package org.kpipe.format.avro;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kpipe.registry.MessageProcessorRegistry;

class AvroFormatRoundTripTest {

  private static final MessageProcessorRegistry REGISTRY = new MessageProcessorRegistry("test-app");

  @AfterEach
  public void clearSchemaRegistry() {
    AvroFormat.INSTANCE.clearSchemas();
  }

  @Test
  void testParseAvroInvalidRecord() {
    // Arrange
    final var schemaJson = """
      {
        "type": "record",
        "name": "Simple",
        "fields": [
          {"name": "value", "type": "string"}
        ]
      }""";
    byte[] invalidAvroBytes = "invalid avro".getBytes(StandardCharsets.UTF_8);
    AvroFormat.INSTANCE.addSchema("simpleSchema", schemaJson);
    final var schema = AvroFormat.INSTANCE.getSchema("simpleSchema");

    // Act
    final var format = AvroFormat.INSTANCE.withDefaultSchema("simpleSchema");
    final var pipeline = REGISTRY.pipeline(format).build();

    // Assert — per MessagePipeline contract, malformed Avro input throws.
    assertThrows(RuntimeException.class, () -> pipeline.apply(invalidAvroBytes));
  }

  @Test
  void testSimpleAvroProcessing() throws IOException {
    // Arrange
    final var schemaJson = """
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
    AvroFormat.INSTANCE.addSchema("simpleSchema", schemaJson);

    // Act
    final var format = AvroFormat.INSTANCE.withDefaultSchema("simpleSchema");
    final var pipeline = REGISTRY.pipeline(format).build();
    final var result = pipeline.apply(avroBytes);

    // Assert
    assertNotNull(result);
    assertEquals(avroBytes.length, result.length, "Result should have the same length as input");
  }

  @Test
  void testParseAvroValidRecord() throws IOException {
    // Arrange
    final var schemaJson = """
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
    AvroFormat.INSTANCE.addSchema("testSchema", schemaJson);

    // Act
    final var format = AvroFormat.INSTANCE.withDefaultSchema("testSchema");
    final var pipeline = REGISTRY.pipeline(format).build();
    final var result = pipeline.apply(avroBytes);

    // Assert
    assertNotNull(result);
    assertEquals(avroBytes.length, result.length);
    final var reader = new GenericDatumReader<GenericRecord>(schema);
    final var inputStream = new ByteArrayInputStream(result);
    final var resultRecord = reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));
    assertEquals("test-value", resultRecord.get("value").toString());
  }
}
