package io.github.eschizoid.kpipe.format.json;

import static org.junit.jupiter.api.Assertions.*;

import com.alibaba.fastjson2.JSON;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.Result;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class JsonFormatPipelineTest {

  private static final MessageProcessorRegistry REGISTRY = new MessageProcessorRegistry();

  /// Test helper: drives `bytes` through the full pipeline cycle (deserialize → process →
  /// serialize) and returns the resulting bytes. Filtered records return null; failures
  /// re-throw the cause. Replaces the byte-level `apply(byte[])` entry point that was removed
  /// from `MessagePipeline` so production callers couldn't accidentally rely on its
  /// null-for-filter / rethrow-for-failure semantics.
  private static <T> byte[] roundTrip(final MessagePipeline<T> pipeline, final byte[] bytes) {
    final var deserialized = pipeline.deserializeOrFail(bytes);
    return switch (pipeline.process(deserialized)) {
      case Result.Passed<T> p -> pipeline.serialize(p.value());
      case Result.Filtered<T> __ -> null;
      case Result.Failed<T> f -> {
        if (f.cause() instanceof RuntimeException re) throw re;
        if (f.cause() instanceof Error err) throw err;
        throw new RuntimeException(f.cause());
      }
    };
  }

  /// Round-trips the input through fastjson2 so the test compares canonical
  /// representations rather than raw byte-equality (which would break on whitespace).
  private static String normalizeJson(final String json) {
    try {
      return JSON.toJSONString(JSON.parse(json.getBytes(StandardCharsets.UTF_8)));
    } catch (final Exception e) {
      return "{}";
    }
  }

  @Test
  void testParseJsonValidJson() {
    // Arrange
    final var json = """
      {
        "key":"value"
      }
      """;

    // Act
    final var pipeline = REGISTRY.pipeline(JsonFormat.INSTANCE).build();
    final var result = roundTrip(pipeline, json.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(normalizeJson(json), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testParseJsonInvalidJson() {
    // Arrange
    final var invalidJson = "invalid json";

    // Act
    final var pipeline = REGISTRY.pipeline(JsonFormat.INSTANCE).build();

    // Assert — per MessagePipeline contract, malformed input throws.
    assertThrows(RuntimeException.class, () -> roundTrip(pipeline, invalidJson.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void testParseJsonEmptyJson() {
    // Arrange
    final var emptyJson = "{}";

    // Act
    final var pipeline = REGISTRY.pipeline(JsonFormat.INSTANCE).build();
    final var result = roundTrip(pipeline, emptyJson.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(emptyJson, new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testParseJsonNullInput() {
    // Arrange
    // Act
    final var pipeline = REGISTRY.pipeline(JsonFormat.INSTANCE).build();

    // Assert — null input is a contract violation; throw rather than swallow.
    assertThrows(RuntimeException.class, () -> roundTrip(pipeline, null));
  }
}
