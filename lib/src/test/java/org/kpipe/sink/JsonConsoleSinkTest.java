package org.kpipe.sink;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonConsoleSinkTest {

  private JsonConsoleSink<String, Object> sink;
  private CapturingHandler handler;
  private Logger julLogger;

  @BeforeEach
  void setUp() {
    sink = new JsonConsoleSink<>();
    handler = new CapturingHandler();
    julLogger = Logger.getLogger(JsonConsoleSink.class.getName());
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
  void shouldFormatJsonByteArray() {
    final var json = """
        {"field":"value"}
        """.strip().getBytes(StandardCharsets.UTF_8);
    final var record = new ConsumerRecord<String, Object>("test-topic", 0, 3L, "key1", json);
    sink.send(record, json);
    final var out = output();
    assertTrue(out.contains("field") && out.contains("value"), "Expected JSON content in output");
    assertFalse(out.contains("Failed to parse/format JSON content"), "Did not expect JSON parse error logs");
  }

  @Test
  void shouldHandleNonJsonByteArray() {
    final var bytes = """
        plain text
        """.strip().getBytes(StandardCharsets.UTF_8);
    final var record = new ConsumerRecord<String, Object>("test-topic", 0, 4L, "key1", bytes);
    sink.send(record, bytes);
    assertTrue(output().contains("plain text"), "Expected raw string for non-JSON bytes");
  }

  @Test
  void shouldHandleJsonArray() {
    final var json = """
        [{"a":1},{"b":2}]
        """.strip().getBytes(StandardCharsets.UTF_8);
    final var record = new ConsumerRecord<String, Object>("test-topic", 0, 7L, "key1", json);
    sink.send(record, json);
    final var out = output();
    assertTrue(out.contains("test-topic"), "Expected topic in output for JSON array");
    assertFalse(out.contains("Failed to parse/format JSON content"), "Did not expect JSON parse error logs");
  }

  @Test
  void shouldHandleInvalidJsonByteArray() {
    final var bytes = """
        {invalid json
        """.strip().getBytes(StandardCharsets.UTF_8);
    final var record = new ConsumerRecord<String, Object>("test-topic", 0, 8L, "key1", bytes);
    sink.send(record, bytes);
    assertTrue(output().contains("invalid json"), "Expected fallback raw string for invalid JSON");
  }

  @Test
  void shouldHandleLargeMessage() {
    final var large = "x".repeat(10_000);
    final var record = new ConsumerRecord<>("test-topic", 0, 9L, "key1", (Object) large);
    sink.send(record, large);
    assertTrue(output().contains("x"), "Expected large message content in output");
  }

  @Test
  void shouldHandleNullKey() {
    final var record = new ConsumerRecord<String, Object>("test-topic", 0, 10L, null, "value");
    sink.send(record, "value");
    assertTrue(output().contains("null"), "Expected 'null' key in output");
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
}
