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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AvroConsoleSinkTest {

  private static final Schema TEST_SCHEMA = new Schema.Parser()
    .parse(
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

  private AvroConsoleSink<String, Object> sink;
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
    final var record = new ConsumerRecord<>("my-topic", 0, 0L, "key1", (Object) "value1");
    sink.send(record, "value1");
    assertTrue(output().contains("my-topic"), "Expected topic in log output");
  }

  @Test
  void shouldLogKeyInOutput() {
    final var record = new ConsumerRecord<>("test-topic", 0, 0L, "my-key", (Object) "value1");
    sink.send(record, "value1");
    assertTrue(output().contains("my-key"), "Expected key in log output");
  }

  @Test
  void shouldLogPartitionAndOffsetInOutput() {
    final var record = new ConsumerRecord<>("test-topic", 3, 42L, "key1", (Object) "value1");
    sink.send(record, "value1");
    final var out = output();
    assertTrue(out.contains("\"partition\":3"), "Expected partition:3 in log output");
    assertTrue(out.contains("\"offset\":42"), "Expected offset:42 in log output");
  }

  @Test
  void shouldOutputValidJsonStructure() {
    final var record = new ConsumerRecord<>("test-topic", 2, 7L, "my-key", (Object) "my-value");
    sink.send(record, "my-value");
    final var out = output();
    assertTrue(out.contains("\"topic\":\"test-topic\""), "Expected topic value");
    assertTrue(out.contains("\"partition\":2"), "Expected partition value");
    assertTrue(out.contains("\"offset\":7"), "Expected offset value");
    assertTrue(out.contains("\"key\":\"my-key\""), "Expected key value");
    assertTrue(out.contains("\"processedMessage\":\"my-value\""), "Expected processedMessage value");
  }

  @Test
  void shouldHandleNullValue() {
    final var record = new ConsumerRecord<>("test-topic", 0, 1L, "key1", (Object) null);
    sink.send(record, null);
    assertTrue(output().contains("null"), "Expected 'null' in log output");
  }

  @Test
  void shouldHandleEmptyByteArray() {
    final var record = new ConsumerRecord<String, Object>("test-topic", 0, 2L, "key1", new byte[0]);
    sink.send(record, new byte[0]);
    assertTrue(output().contains("empty"), "Expected 'empty' for zero-length byte array");
  }

  @Test
  void shouldHandleNullKey() {
    final var record = new ConsumerRecord<String, Object>("test-topic", 0, 3L, null, "value");
    sink.send(record, "value");
    assertTrue(output().contains("null"), "Expected 'null' key in output");
  }

  @Test
  void shouldHandleInvalidAvroDataWithoutThrowing() {
    final var bytes = "not avro".getBytes(StandardCharsets.UTF_8);
    final var record = new ConsumerRecord<String, Object>("test-topic", 0, 5L, "key1", bytes);
    sink.send(record, bytes);
    final var out = output();
    assertTrue(out.contains("\"topic\":\"test-topic\""), "Expected topic value");
    assertTrue(out.contains("\"processedMessage\":\"\""), "Expected empty processedMessage on Avro parse failure");
  }

  @Test
  void shouldFormatValidAvroByteArrayAsJson() throws Exception {
    final var avroBytes = createAvroBytes();
    final var record = new ConsumerRecord<String, Object>("test-topic", 0, 4L, "key1", avroBytes);
    sink.send(record, avroBytes);
    final var out = output();
    assertTrue(out.contains("\"topic\":\"test-topic\""), "Expected topic value");
    assertTrue(out.contains("\"key\":\"key1\""), "Expected key value");
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
