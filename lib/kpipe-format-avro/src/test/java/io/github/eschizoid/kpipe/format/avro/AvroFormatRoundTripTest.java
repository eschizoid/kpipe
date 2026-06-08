package io.github.eschizoid.kpipe.format.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.Result;
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
import org.junit.jupiter.api.Test;

class AvroFormatRoundTripTest {

  /// Test helper: drives `bytes` through the full pipeline cycle (deserialize → process →
  /// serialize) and returns the resulting bytes. Filtered records return null; failures
  /// re-throw the cause. Replaces the byte-level `apply(byte[])` entry point that was removed
  /// from `MessagePipeline` so production callers couldn't accidentally rely on its
  /// null-for-filter / rethrow-for-failure semantics.
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

  private static final String SIMPLE_SCHEMA_JSON = """
    {
      "type": "record",
      "name": "Simple",
      "fields": [
        {"name": "value", "type": "string"}
      ]
    }""";

  private static final MessageProcessorRegistry REGISTRY = new MessageProcessorRegistry();

  @Test
  void testParseAvroInvalidRecord() {
    final var format = AvroFormat.of(SIMPLE_SCHEMA_JSON);
    final var pipeline = REGISTRY.pipeline(format).build();
    final var invalidAvroBytes = "invalid avro".getBytes(StandardCharsets.UTF_8);

    assertThrows(RuntimeException.class, () -> roundTrip(pipeline, invalidAvroBytes));
  }

  @Test
  void testSimpleAvroProcessing() throws IOException {
    final var format = AvroFormat.of(SIMPLE_SCHEMA_JSON);
    final var avroBytes = encode(format.schema(), "test");

    final var pipeline = REGISTRY.pipeline(format).build();
    final var result = roundTrip(pipeline, avroBytes);

    assertNotNull(result);
    assertEquals(avroBytes.length, result.length, "Result should have the same length as input");
  }

  @Test
  void testParseAvroValidRecord() throws IOException {
    final var format = AvroFormat.of(SIMPLE_SCHEMA_JSON);
    final var avroBytes = encode(format.schema(), "test-value");

    final var pipeline = REGISTRY.pipeline(format).build();
    final var result = roundTrip(pipeline, avroBytes);

    assertNotNull(result);
    assertEquals(avroBytes.length, result.length);
    final var reader = new GenericDatumReader<GenericRecord>(format.schema());
    final var inputStream = new ByteArrayInputStream(result);
    final var resultRecord = reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));
    assertEquals("test-value", resultRecord.get("value").toString());
  }

  private static byte[] encode(final Schema schema, final String value) throws IOException {
    final var record = new GenericData.Record(schema);
    record.put("value", value);
    try (final var outputStream = new ByteArrayOutputStream()) {
      final var writer = new GenericDatumWriter<GenericRecord>(schema);
      final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      writer.write(record, encoder);
      encoder.flush();
      return outputStream.toByteArray();
    }
  }
}
