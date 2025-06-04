package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kpipe.processor.AvroMessageProcessor;

class MessageFormatTest {

  @TempDir
  private Path tempDir;

  private String userSchemaJson;

  @BeforeEach
  void setUp() {
    // Create Avro schema
    userSchemaJson =
      """
        {
          "type": "record",
          "name": "User",
          "namespace": "com.example",
          "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"}
          ]
        }
        """;
  }

  @AfterEach
  void tearDown() {
    AvroMessageProcessor.clearSchemaRegistry();
    MessageFormat.AVRO.clearSchemas();
    MessageFormat.JSON.clearSchemas();
    MessageFormat.PROTOBUF.clearSchemas();
  }

  @Test
  void shouldAddAndRetrieveSchema() {
    // Arrange
    final var schemaKey = "user";
    final var schemaName = "com.example.User";

    // Act
    MessageFormat.AVRO.addSchema(schemaKey, schemaName, userSchemaJson);
    final var schema = MessageFormat.AVRO.findSchema(schemaKey);

    // Assert
    assertTrue(schema.isPresent());
    assertEquals(schemaName, schema.get().fullyQualifiedName());
    assertEquals(userSchemaJson, schema.get().location());
  }

  @Test
  void shouldReturnEmptyOptionalForMissingSchema() {
    // Arrange
    final var nonExistentKey = "nonExistent";

    // Act
    final var result = MessageFormat.AVRO.findSchema(nonExistentKey);

    // Assert
    assertFalse(result.isPresent());
  }

  @Test
  void shouldFindSchemasByPredicate() {
    // Arrange
    MessageFormat.AVRO.addSchema("user", "com.example.User", userSchemaJson);

    // Act
    final var userSchemas = MessageFormat.AVRO.findSchemas(schema ->
      schema.fullyQualifiedName().startsWith("com.example.User")
    );

    // Assert
    assertEquals(1, userSchemas.size());
    assertTrue(userSchemas.stream().anyMatch(s -> s.fullyQualifiedName().equals("com.example.User")));
  }

  @Test
  void shouldGetDefaultJSONProcessors() {
    // Arrange
    final var sourceAppName = "test-app";

    // Act
    final var processors = MessageFormat.JSON.getDefaultProcessors(sourceAppName);

    // Assert
    assertTrue(processors.containsKey("parseJson"));
    assertTrue(processors.containsKey("addSource"));
    assertTrue(processors.containsKey("markProcessed"));
    assertTrue(processors.containsKey("addTimestamp"));
  }

  @Test
  void shouldGetEmptyDefaultProcessorsForAVRO() {
    // Arrange
    final var sourceAppName = "test-app";

    // Act
    final var processors = MessageFormat.AVRO.getDefaultProcessors(sourceAppName);

    // Assert
    assertTrue(processors.isEmpty());
  }

  @Test
  void shouldGetSchemaProcessorsForAVRO() throws Exception {
    // Arrange
    final var schemaKey = "user";
    final var schemaName = "com.example.User";
    final var sourceAppName = "test-app";
    MessageFormat.AVRO.addSchema(schemaKey, schemaName, userSchemaJson);

    // Act
    final var processors = MessageFormat.AVRO.getSchemaProcessors(schemaKey, sourceAppName);

    // Assert
    assertFalse(processors.isEmpty());
    assertTrue(processors.containsKey("parseAvro_user"));
    assertTrue(processors.containsKey("addSource_user"));
    assertTrue(processors.containsKey("markProcessed_user"));
    assertTrue(processors.containsKey("addTimestamp_user"));
  }

  @Test
  void shouldGetEmptySchemaProcessorsForMissingSchema() {
    // Arrange
    final var nonExistentKey = "nonExistent";
    final var sourceAppName = "test-app";

    // Act
    final var processors = MessageFormat.AVRO.getSchemaProcessors(nonExistentKey, sourceAppName);

    // Assert
    assertTrue(processors.isEmpty());
  }

  @Test
  void shouldReadSchemaFromFile() throws Exception {
    // Arrange
    final var schemaFile = tempDir.resolve("user.avsc");
    Files.writeString(schemaFile, userSchemaJson);
    final var schemaKey = "userFromFile";
    final var schemaName = "com.example.UserFromFile";

    // Act
    MessageFormat.AVRO.addSchema(schemaKey, schemaName, schemaFile.toString());
    final var schema = MessageFormat.AVRO.findSchema(schemaKey);

    // Assert
    assertTrue(schema.isPresent());
    assertEquals(schemaName, schema.get().fullyQualifiedName());
  }

  @Test
  void shouldReadSchemaFromExplicitFileProtocol() throws Exception {
    // Arrange
    final var schemaFile = tempDir.resolve("user_protocol.avsc");
    Files.writeString(schemaFile, userSchemaJson);
    final var schemaKey = "userFromFileProtocol";
    final var schemaName = "com.example.UserFromFileProtocol";
    final var fileUri = "file://" + schemaFile.toAbsolutePath();

    // Act
    MessageFormat.AVRO.addSchema(schemaKey, schemaName, fileUri);
    final var schema = MessageFormat.AVRO.findSchema(schemaKey);

    // Assert
    assertTrue(schema.isPresent());
    assertEquals(schemaName, schema.get().fullyQualifiedName());
  }

  @Test
  void shouldThrowExceptionForInvalidSchema() {
    // Arrange
    final var invalidJson = "{invalid json}";

    // Act & Assert
    final var exception = assertThrows(
      IllegalArgumentException.class,
      () -> MessageFormat.AVRO.addSchema("invalid", "com.example.Invalid", invalidJson)
    );
    assertTrue(exception.getMessage().contains("Failed to register Avro schema"));
  }

  @Test
  void shouldAllowSchemaInfoCreation() {
    // Arrange
    final var name = "com.example.Test";
    final var location = "test.avsc";

    // Act
    final var schemaInfo = new MessageFormat.SchemaInfo(name, location);

    // Assert
    assertEquals(name, schemaInfo.fullyQualifiedName());
    assertEquals(location, schemaInfo.location());
  }

  @Test
  void shouldNotAcceptNullSchemaValues() {
    // Assert
    assertThrows(NullPointerException.class, () -> new MessageFormat.SchemaInfo(null, "location"));
    assertThrows(NullPointerException.class, () -> new MessageFormat.SchemaInfo("name", null));
  }

  @Test
  void shouldGetAllSchemas() {
    // Arrange
    MessageFormat.AVRO.addSchema("schema", "com.example.Schema", userSchemaJson);

    // Act
    final var schemas = MessageFormat.AVRO.getSchemas();

    // Assert
    assertEquals(1, schemas.size());
  }
}
