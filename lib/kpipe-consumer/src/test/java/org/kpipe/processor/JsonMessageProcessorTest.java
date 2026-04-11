package org.kpipe.processor;

import static org.junit.jupiter.api.Assertions.*;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.MessageProcessorRegistry;

class JsonMessageProcessorTest {

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
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON).build();
    final var result = pipeline.apply(json.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(normalizeJson(json), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testParseJsonInvalidJson() {
    // Arrange
    final var invalidJson = "invalid json";

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON).build();
    final var result = pipeline.apply(invalidJson.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertNull(result);
  }

  @Test
  void testAddFieldValidJson() {
    // Arrange
    final var json = """
      {
        "key":"value"
      }
      """;
    final var expectedJson = """
        {
          "key":"value",
          "newKey":"newValue"
        }
      """;

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(JsonMessageProcessor.addFieldOperator("newKey", "newValue"))
      .build();
    final var result = pipeline.apply(json.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertNotNull(result);
    assertEquals(normalizeJson(expectedJson), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testAddFieldInvalidJson() {
    // Arrange
    final var invalidJson = "invalid json";

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(JsonMessageProcessor.addFieldOperator("newKey", "newValue"))
      .build();
    final var result = pipeline.apply(invalidJson.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertNull(result);
  }

  @Test
  void testAddFieldNonObjectJson() {
    // Arrange
    final var jsonArray = """
          ["value1", "value2"]
      """;

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(JsonMessageProcessor.addFieldOperator("newKey", "newValue"))
      .build();
    final var result = pipeline.apply(jsonArray.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertNull(result);
  }

  @Test
  void testParseJsonEmptyJson() {
    // Arrange
    final var emptyJson = "{}";

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON).build();
    final var result = pipeline.apply(emptyJson.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(emptyJson, new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testParseJsonNullInput() {
    // Arrange
    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON).build();
    final var result = pipeline.apply(null);

    // Assert
    assertNull(result);
  }

  @Test
  void testAddFieldToEmptyJson() {
    // Arrange
    final var emptyJson = "{}";
    final var expectedJson = """
      {
        "newKey":"newValue"
      }
      """;

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(JsonMessageProcessor.addFieldOperator("newKey", "newValue"))
      .build();
    final var result = pipeline.apply(emptyJson.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(normalizeJson(expectedJson), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testAddTimestamp() {
    // Arrange
    final var json = """
      {
        "key":"value"
      }
      """;

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(JsonMessageProcessor.addTimestampOperator("timestamp"))
      .build();
    final var result = pipeline.apply(json.getBytes(StandardCharsets.UTF_8));
    final var resultMap = parseJsonToMap(result);

    // Assert
    assertEquals("value", resultMap.get("key"));
    assertNotNull(resultMap.get("timestamp"));
    assertInstanceOf(Number.class, resultMap.get("timestamp"));
  }

  @Test
  void testAddTimestampToInvalidJson() {
    // Arrange
    final var invalidJson = "invalid json";

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(JsonMessageProcessor.addTimestampOperator("timestamp"))
      .build();
    final var result = pipeline.apply(invalidJson.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertNull(result);
  }

  @Test
  void testRemoveFields() {
    // Arrange
    final var json = """
      {
        "field1":"value1",
        "field2":"value2",
        "field3":"value3"
      }
      """;
    final var expected = """
      {
        "field2":"value2"
      }
      """;

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(JsonMessageProcessor.removeFieldsOperator("field1", "field3"))
      .build();
    final var result = pipeline.apply(json.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(normalizeJson(expected), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testRemoveNonExistingFields() {
    // Arrange
    final var json = """
      {
        "field1":"value1"
      }
      """;

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(JsonMessageProcessor.removeFieldsOperator("field2"))
      .build();
    final var result = pipeline.apply(json.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(normalizeJson(json), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testTransformField() {
    // Arrange
    final var json = """
      {
        "message":"hello",
        "count":5
      }
      """;
    final var expected = """
      {
        "message":"HELLO",
        "count":5
      }
      """;

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(
        JsonMessageProcessor.transformFieldOperator("message", value ->
          value instanceof String ? ((String) value).toUpperCase() : value
        )
      )
      .build();
    final var result = pipeline.apply(json.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(normalizeJson(expected), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testTransformNumericField() {
    // Arrange
    final var json = """
      {
        "message":"hello",
        "count":5
      }
      """;
    final var expected = """
      {
        "message":"hello",
        "count":10
      }
      """;

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(
        JsonMessageProcessor.transformFieldOperator("count", value ->
          value instanceof Number ? ((Number) value).intValue() * 2 : value
        )
      )
      .build();
    final var result = pipeline.apply(json.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(normalizeJson(expected), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testTransformNonExistingField() {
    // Arrange
    final var json = """
      {
        "message":"hello"
      }
      """;

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(JsonMessageProcessor.transformFieldOperator("nonExisting", value -> "transformed"))
      .build();
    final var result = pipeline.apply(json.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(normalizeJson(json), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testMergeWith() {
    // Arrange
    final var json = """
      {
        "original":"value"
      }
      """;

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(JsonMessageProcessor.mergeWithOperator(Map.of("added1", "value1", "added2", 42)))
      .build();
    final var result = pipeline.apply(json.getBytes(StandardCharsets.UTF_8));
    final var resultMap = parseJsonToMap(result);

    // Assert
    assertEquals("value", resultMap.get("original"));
    assertEquals("value1", resultMap.get("added1"));
    assertEquals(42, ((Number) resultMap.get("added2")).intValue());
  }

  @Test
  void testMergeWithOverlappingKeys() {
    // Arrange
    final var json = """
      {
        "key":"originalValue"
      }
      """;

    // Act
    final var pipeline = REGISTRY.pipeline(MessageFormat.JSON)
      .add(JsonMessageProcessor.mergeWithOperator(Map.of("key", "newValue")))
      .build();
    final var result = pipeline.apply(json.getBytes(StandardCharsets.UTF_8));
    final var resultMap = parseJsonToMap(result);

    // Assert
    assertEquals("newValue", resultMap.get("key"));
  }

  // Helper method to parse JSON bytes to Map
  private Map<String, Object> parseJsonToMap(byte[] jsonBytes) {
    try (var input = new ByteArrayInputStream(jsonBytes)) {
      return DSL_JSON.deserialize(Map.class, input);
    } catch (IOException e) {
      return Map.of();
    }
  }
}
