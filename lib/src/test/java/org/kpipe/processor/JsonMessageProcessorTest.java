package org.kpipe.processor;

import static org.junit.jupiter.api.Assertions.*;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonMessageProcessorTest {

  private static final DslJson<Map<String, Object>> DSL_JSON = new DslJson<>();

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
    final var json =
      """
                    {
                      "key":"value"
                    }
                    """;

    // Act
    final var result = JsonMessageProcessor.processJson(json.getBytes(StandardCharsets.UTF_8), obj -> obj);

    // Assert
    assertEquals(normalizeJson(json), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testParseJsonInvalidJson() {
    // Arrange
    final var invalidJson = "invalid json";

    // Act
    final var result = JsonMessageProcessor.processJson(invalidJson.getBytes(StandardCharsets.UTF_8), obj -> obj);

    // Assert
    assertEquals("{}", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testAddFieldValidJson() {
    // Arrange
    final var json = """
                  {
                    "key":"value"
                  }
                  """;
    final var expectedJson =
      """
            {
              "key":"value",
              "newKey":"newValue"
            }
          """;

    // Act
    final var result = JsonMessageProcessor.processJson(
      json.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.addFieldOperator("newKey", "newValue")
    );

    // Assert
    assertEquals(normalizeJson(expectedJson), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testAddFieldInvalidJson() {
    // Arrange
    final var invalidJson = "invalid json";

    // Act
    final var result = JsonMessageProcessor.processJson(
      invalidJson.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.addFieldOperator("newKey", "newValue")
    );

    // Assert
    assertEquals("{}", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testAddFieldNonObjectJson() {
    // Arrange
    final var jsonArray = """
                    ["value1", "value2"]
                """;

    // Act
    final var result = JsonMessageProcessor.processJson(
      jsonArray.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.addFieldOperator("newKey", "newValue")
    );

    // Assert
    assertEquals("{}", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testParseJsonEmptyJson() {
    // Arrange
    final var emptyJson = "{}";

    // Act
    final var result = JsonMessageProcessor.processJson(emptyJson.getBytes(StandardCharsets.UTF_8), obj -> obj);

    // Assert
    assertEquals(emptyJson, new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testParseJsonNullInput() {
    // Arrange
    // Act
    final var result = JsonMessageProcessor.processJson(null, obj -> obj);

    // Assert
    assertEquals("{}", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testAddFieldToEmptyJson() {
    // Arrange
    final var emptyJson = "{}";
    final var expectedJson =
      """
                      {
                        "newKey":"newValue"
                      }
                      """;

    // Act
    final var result = JsonMessageProcessor.processJson(
      emptyJson.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.addFieldOperator("newKey", "newValue")
    );

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
    final var result = JsonMessageProcessor.processJson(
      json.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.addTimestampOperator("timestamp")
    );
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
    final var result = JsonMessageProcessor.processJson(
      invalidJson.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.addTimestampOperator("timestamp")
    );

    // Assert
    assertEquals("{}", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testRemoveFields() {
    // Arrange
    final var json =
      """
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
    final var result = JsonMessageProcessor.processJson(
      json.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.removeFieldsOperator("field1", "field3")
    );

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
    final var result = JsonMessageProcessor.processJson(
      json.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.removeFieldsOperator("field2")
    );

    // Assert
    assertEquals(normalizeJson(json), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testTransformField() {
    // Arrange
    final var json =
      """
            {
              "message":"hello",
              "count":5
            }
            """;
    final var expected =
      """
            {
              "message":"HELLO",
              "count":5
            }
            """;

    // Act
    final var result = JsonMessageProcessor.processJson(
      json.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.transformFieldOperator(
        "message",
        value -> value instanceof String ? ((String) value).toUpperCase() : value
      )
    );

    // Assert
    assertEquals(normalizeJson(expected), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testTransformNumericField() {
    // Arrange
    final var json =
      """
            {
              "message":"hello",
              "count":5
            }
            """;
    final var expected =
      """
            {
              "message":"hello",
              "count":10
            }
            """;

    // Act
    final var result = JsonMessageProcessor.processJson(
      json.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.transformFieldOperator(
        "count",
        value -> value instanceof Number ? ((Number) value).intValue() * 2 : value
      )
    );

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
    final var result = JsonMessageProcessor.processJson(
      json.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.transformFieldOperator("nonExisting", value -> "transformed")
    );

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
    final var result = JsonMessageProcessor.processJson(
      json.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.mergeWithOperator(Map.of("added1", "value1", "added2", 42))
    );
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
    final var result = JsonMessageProcessor.processJson(
      json.getBytes(StandardCharsets.UTF_8),
      JsonMessageProcessor.mergeWithOperator(Map.of("key", "newValue"))
    );
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
