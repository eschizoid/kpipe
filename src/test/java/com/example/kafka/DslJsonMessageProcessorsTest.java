package com.example.kafka;

import static org.junit.jupiter.api.Assertions.*;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DslJsonMessageProcessorsTest {

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
    final var parseJson = DslJsonMessageProcessors.parseJson();

    // Act
    final var result = parseJson.apply(json.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(normalizeJson(json), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testParseJsonInvalidJson() {
    // Arrange
    final var invalidJson = "invalid json";
    final var parseJson = DslJsonMessageProcessors.parseJson();

    // Act
    final var result = parseJson.apply(invalidJson.getBytes(StandardCharsets.UTF_8));

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
    final var addField = DslJsonMessageProcessors.addField("newKey", "newValue");

    // Act
    final var result = addField.apply(json.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(normalizeJson(expectedJson), normalizeJson(new String(result, StandardCharsets.UTF_8)));
  }

  @Test
  void testAddFieldInvalidJson() {
    // Arrange
    final var invalidJson = "invalid json";
    final var addField = DslJsonMessageProcessors.addField("newKey", "newValue");

    // Act
    final var result = addField.apply(invalidJson.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals("{}", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testAddFieldNonObjectJson() {
    // Arrange
    final var jsonArray = """
                    ["value1", "value2"]
                """;
    final var addField = DslJsonMessageProcessors.addField("newKey", "newValue");

    // Act
    final var result = addField.apply(jsonArray.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals("{}", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testParseJsonEmptyJson() {
    // Arrange
    final var emptyJson = "{}";
    final var parseJson = DslJsonMessageProcessors.parseJson();

    // Act
    final var result = parseJson.apply(emptyJson.getBytes(StandardCharsets.UTF_8));

    // Assert
    assertEquals(emptyJson, new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testParseJsonNullInput() {
    // Arrange
    final var parseJson = DslJsonMessageProcessors.parseJson();

    // Act
    final var result = parseJson.apply(null);

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
    final var addField = DslJsonMessageProcessors.addField("newKey", "newValue");

    // Act
    final var result = addField.apply(emptyJson.getBytes(StandardCharsets.UTF_8));

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
    final var addTimestamp = DslJsonMessageProcessors.addTimestamp("timestamp");

    // Act
    final var result = addTimestamp.apply(json.getBytes(StandardCharsets.UTF_8));
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
    final var addTimestamp = DslJsonMessageProcessors.addTimestamp("timestamp");

    // Act
    final var result = addTimestamp.apply(invalidJson.getBytes(StandardCharsets.UTF_8));

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
    final var removeFields = DslJsonMessageProcessors.removeFields("field1", "field3");
    final var expected = """
            {
              "field2":"value2"
            }
            """;

    // Act
    final var result = removeFields.apply(json.getBytes(StandardCharsets.UTF_8));

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
    final var removeFields = DslJsonMessageProcessors.removeFields("field2");

    // Act
    final var result = removeFields.apply(json.getBytes(StandardCharsets.UTF_8));

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
    final var transformField = DslJsonMessageProcessors.transformField(
      "message",
      value -> value instanceof String ? ((String) value).toUpperCase() : value
    );
    final var expected =
      """
            {
              "message":"HELLO",
              "count":5
            }
            """;

    // Act
    final var result = transformField.apply(json.getBytes(StandardCharsets.UTF_8));

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
    final var transformField = DslJsonMessageProcessors.transformField(
      "count",
      value -> value instanceof Number ? ((Number) value).intValue() * 2 : value
    );
    final var expected =
      """
            {
              "message":"hello",
              "count":10
            }
            """;

    // Act
    final var result = transformField.apply(json.getBytes(StandardCharsets.UTF_8));

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
    final var transformField = DslJsonMessageProcessors.transformField("nonExisting", value -> "transformed");

    // Act
    final var result = transformField.apply(json.getBytes(StandardCharsets.UTF_8));

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
    final var mergeWith = DslJsonMessageProcessors.mergeWith(Map.of("added1", "value1", "added2", 42));

    // Act
    final var result = mergeWith.apply(json.getBytes(StandardCharsets.UTF_8));
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
    final var mergeWith = DslJsonMessageProcessors.mergeWith(Map.of("key", "newValue"));

    // Act
    final var result = mergeWith.apply(json.getBytes(StandardCharsets.UTF_8));
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
