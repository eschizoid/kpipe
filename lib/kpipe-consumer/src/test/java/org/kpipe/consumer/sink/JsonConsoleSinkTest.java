package org.kpipe.consumer.sink;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonConsoleSinkTest {

  private JsonConsoleSink<Object> sink;
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
    sink.accept("value1");
    assertTrue(output().contains("processedMessage"), "Expected log output");
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
  void shouldFormatJsonByteArray() {
    final var json = """
      {"field":"value"}
      """.strip()
      .getBytes(StandardCharsets.UTF_8);
    sink.accept(json);
    final var out = output();
    assertTrue(out.contains("field") && out.contains("value"), "Expected JSON content in output");
    assertFalse(out.contains("Failed to parse/format JSON content"), "Did not expect JSON parse error logs");
  }

  @Test
  void shouldHandleNonJsonByteArray() {
    final var bytes = """
      plain text
      """.strip()
      .getBytes(StandardCharsets.UTF_8);
    sink.accept(bytes);
    assertTrue(output().contains("plain text"), "Expected raw string for non-JSON bytes");
  }

  @Test
  void shouldHandleJsonArray() {
    final var json = """
      [{"a":1},{"b":2}]
      """.strip()
      .getBytes(StandardCharsets.UTF_8);
    sink.accept(json);
    final var out = output();
    assertFalse(out.contains("Failed to parse/format JSON content"), "Did not expect JSON parse error logs");
  }

  @Test
  void shouldHandleInvalidJsonByteArray() {
    final var bytes = """
      {invalid json
      """.strip()
      .getBytes(StandardCharsets.UTF_8);
    sink.accept(bytes);
    assertTrue(output().contains("invalid json"), "Expected fallback raw string for invalid JSON");
  }

  @Test
  void shouldHandleLargeMessage() {
    final var large = "x".repeat(10_000);
    sink.accept(large);
    assertTrue(output().contains("x"), "Expected large message content in output");
  }

  @Test
  void shouldOutputValidJsonStructure() {
    sink.accept("my-value");
    final var out = output();
    assertTrue(out.contains("\"processedMessage\":\"my-value\""), "Expected processedMessage value");
  }
}
