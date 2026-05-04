package org.kpipe.format.json;

import static org.junit.jupiter.api.Assertions.*;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.kpipe.registry.MessageProcessorRegistry;

class JsonFormatPipelineTest {

  private static final DslJson<Map<String, Object>> DSL_JSON = new DslJson<>();
  private static final MessageProcessorRegistry REGISTRY = new MessageProcessorRegistry("test-app");

  private static String normalizeJson(String json) {
    try (
      final var input = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
      final var output = new ByteArrayOutputStream()
    ) {
      final var parsed = DSL_JSON.deserialize(Map.class, input);
      DSL_JSON.serialize(parsed, output);
      return output.toString(StandardCharsets.UTF_8);
    } catch (final IOException e) {
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
    final var result = pipeline.apply(json.getBytes(StandardCharsets.UTF_8));

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
    assertThrows(RuntimeException.class, () -> pipeline.apply(invalidJson.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void testParseJsonEmptyJson() {
    // Arrange
    final var emptyJson = "{}";

    // Act
    final var pipeline = REGISTRY.pipeline(JsonFormat.INSTANCE).build();
    final var result = pipeline.apply(emptyJson.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(emptyJson, new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testParseJsonNullInput() {
    // Arrange
    // Act
    final var pipeline = REGISTRY.pipeline(JsonFormat.INSTANCE).build();

    // Assert — null input is a contract violation; throw rather than swallow.
    assertThrows(RuntimeException.class, () -> pipeline.apply(null));
  }
}
