package org.kpipe.sink;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AvroConsoleSinkTest {

  private static final Schema TEST_SCHEMA = new Schema.Parser().parse(
    """
    {
      "type": "record",
      "name": "Test",
      "fields": [
        {"name": "id", "type": "string"},
        {"name": "value", "type": "string"}
      ]
    }
    """
  );

  private AvroConsoleSink<Object> sink;
  private CapturingHandler handler;
  private Logger julLogger;

  @BeforeEach
  void setUp() {
    sink = new AvroConsoleSink<>(TEST_SCHEMA);
    handler = new CapturingHandler();
    julLogger = Logger.getLogger(AvroConsoleSink.class.getName());
    julLogger.addHandler(handler);
    julLogger.setUseParentHandlers(false);
  }

  @AfterEach
  void tearDown() {
    julLogger.removeHandler(handler);
    julLogger.setUseParentHandlers(true);
  }

  private String output() {
    return handler.toString();
  }

  @Test
  void shouldLogTopicInOutput() {
    sink.accept("value1");
    assertTrue(output().contains("processedMessage"), "Expected log output");
  }

  @Test
  void shouldOutputValidJsonStructure() {
    sink.accept("my-value");
    final var out = output();
    assertTrue(out.contains("\"processedMessage\":\"my-value\""), "Expected processedMessage value");
  }

  @Test
  void shouldHandleNullValue() {
    sink.accept(null);
    assertTrue(output().contains("null"), "Expected 'null' in log output");
  }

  @Test
  void shouldHandleEmptyByteArray() {
    sink.accept(new byte[0]);
    assertTrue(output().contains("empty"), "Expected 'empty' for zero-length byte array");
  }

  @Test
  void shouldHandleInvalidAvroDataWithoutThrowing() {
    final var bytes = "not avro".getBytes(StandardCharsets.UTF_8);
    sink.accept(bytes);
    final var out = output();
    assertTrue(out.contains("\"processedMessage\":\"\""), "Expected empty processedMessage on Avro parse failure");
  }

  @Test
  void shouldFormatValidAvroByteArrayAsJson() throws Exception {
    final var avroBytes = createAvroBytes();
    sink.accept(avroBytes);
    final var out = output();
    // Avro JSON encoder encodes string fields as {"string":"value"}
    assertTrue(out.contains("test-id"), "Expected decoded Avro 'id' value");
    assertTrue(out.contains("test-value"), "Expected decoded Avro 'value' value");
  }

  private byte[] createAvroBytes() throws Exception {
    final var avroRecord = new GenericData.Record(TEST_SCHEMA);
    avroRecord.put("id", "test-id");
    avroRecord.put("value", "test-value");
    final var out = new ByteArrayOutputStream();
    final var writer = new GenericDatumWriter<GenericRecord>(TEST_SCHEMA);
    final var encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(avroRecord, encoder);
    encoder.flush();
    return out.toByteArray();
  }
}
