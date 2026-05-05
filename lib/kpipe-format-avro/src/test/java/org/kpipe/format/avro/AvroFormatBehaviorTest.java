package org.kpipe.format.avro;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kpipe.registry.SchemaAwareFormat;

class AvroFormatBehaviorTest {

  @TempDir
  private Path tempDir;

  private String userSchemaJson;

  @BeforeEach
  void setUp() {
    // Create Avro schema
    userSchemaJson = """
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
    AvroFormat.INSTANCE.clearSchemas();
    AvroFormat.INSTANCE.clearSchemas();
  }

  @Test
  void shouldAddAndRetrieveSchema() {
    // Arrange
    final var schemaKey = "user";
    final var schemaName = "com.example.User";

    // Act
    AvroFormat.INSTANCE.addSchema(schemaKey, schemaName, userSchemaJson);
    final var schema = AvroFormat.INSTANCE.findSchema(schemaKey);

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
    final var result = AvroFormat.INSTANCE.findSchema(nonExistentKey);

    // Assert
    assertFalse(result.isPresent());
  }

  @Test
  void shouldFindSchemasByPredicate() {
    // Arrange
    AvroFormat.INSTANCE.addSchema("user", "com.example.User", userSchemaJson);

    // Act
    final var userSchemas = AvroFormat.INSTANCE.findSchemas(schema ->
      schema.fullyQualifiedName().startsWith("com.example.User")
    );

    // Assert
    assertEquals(1, userSchemas.size());
    assertTrue(userSchemas.stream().anyMatch(s -> s.fullyQualifiedName().equals("com.example.User")));
  }

  @Test
  void shouldReadSchemaFromFile() throws Exception {
    // Arrange
    final var schemaFile = tempDir.resolve("user.avsc");
    Files.writeString(schemaFile, userSchemaJson);
    final var schemaKey = "userFromFile";
    final var schemaName = "com.example.UserFromFile";

    // Act
    AvroFormat.INSTANCE.addSchema(schemaKey, schemaName, schemaFile.toString());
    final var schema = AvroFormat.INSTANCE.findSchema(schemaKey);

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
    AvroFormat.INSTANCE.addSchema(schemaKey, schemaName, fileUri);
    final var schema = AvroFormat.INSTANCE.findSchema(schemaKey);

    // Assert
    assertTrue(schema.isPresent());
    assertEquals(schemaName, schema.get().fullyQualifiedName());
  }

  @Test
  void shouldThrowExceptionForInvalidSchema() {
    // Arrange
    final var invalidJson = "{invalid json}";

    // Act & Assert
    final var exception = assertThrows(IllegalArgumentException.class, () ->
      AvroFormat.INSTANCE.addSchema("invalid", "com.example.Invalid", invalidJson)
    );
    assertTrue(exception.getMessage().contains("Failed to register Avro schema"));
  }

  @Test
  void shouldAllowSchemaInfoCreation() {
    // Arrange
    final var name = "com.example.Test";
    final var location = "test.avsc";

    // Act
    final var schemaInfo = new SchemaAwareFormat.SchemaInfo(name, location);

    // Assert
    assertEquals(name, schemaInfo.fullyQualifiedName());
    assertEquals(location, schemaInfo.location());
  }

  @Test
  void shouldNotAcceptNullSchemaValues() {
    // Assert
    assertThrows(NullPointerException.class, () -> new SchemaAwareFormat.SchemaInfo(null, "location"));
    assertThrows(NullPointerException.class, () -> new SchemaAwareFormat.SchemaInfo("name", null));
  }

  @Test
  void shouldGetAllSchemas() {
    // Arrange
    AvroFormat.INSTANCE.addSchema("schema", "com.example.Schema", userSchemaJson);

    // Act
    final var schemas = AvroFormat.INSTANCE.getSchemas();

    // Assert
    assertEquals(1, schemas.size());
  }
}
