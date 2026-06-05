package io.github.eschizoid.kpipe.format.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AvroSchemaCatalogTest {

  private static final String USER_SCHEMA_JSON = """
    {
      "type": "record",
      "name": "User",
      "namespace": "com.example",
      "fields": [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"}
      ]
    }
    """;

  @Test
  void addStoresSchemaByKey() {
    final var catalog = new AvroSchemaCatalog();
    catalog.add("user", USER_SCHEMA_JSON);

    final var schema = catalog.get("user");
    assertNotNull(schema);
    assertEquals("com.example.User", schema.getFullName());
  }

  @Test
  void getReturnsNullForMissingKey() {
    final var catalog = new AvroSchemaCatalog();
    assertNull(catalog.get("missing"));
  }

  @Test
  void findReturnsEmptyForMissingKey() {
    final var catalog = new AvroSchemaCatalog();
    assertTrue(catalog.find("missing").isEmpty());
  }

  @Test
  void findReturnsPresentForRegisteredKey() {
    final var catalog = new AvroSchemaCatalog().add("user", USER_SCHEMA_JSON);
    assertTrue(catalog.find("user").isPresent());
  }

  @Test
  void allReturnsRegisteredEntries() {
    final var catalog = new AvroSchemaCatalog().add("user", USER_SCHEMA_JSON).add("user2", USER_SCHEMA_JSON);
    assertEquals(2, catalog.all().size());
  }

  @Test
  void allReturnsUnmodifiableView() {
    final var catalog = new AvroSchemaCatalog().add("user", USER_SCHEMA_JSON);
    assertThrows(UnsupportedOperationException.class, () -> catalog.all().clear());
  }

  @Test
  void clearRemovesAllEntries() {
    final var catalog = new AvroSchemaCatalog().add("user", USER_SCHEMA_JSON);
    catalog.clear();
    assertTrue(catalog.all().isEmpty());
  }

  @Test
  void addByLocationReadsFromFile(@TempDir final Path tempDir) throws Exception {
    final var schemaFile = tempDir.resolve("user.avsc");
    Files.writeString(schemaFile, USER_SCHEMA_JSON);

    final var catalog = new AvroSchemaCatalog();
    catalog.add("user", "com.example.User", schemaFile.toString());

    assertEquals("com.example.User", catalog.get("user").getFullName());
  }

  @Test
  void addByLocationSupportsFileProtocol(@TempDir final Path tempDir) throws Exception {
    final var schemaFile = tempDir.resolve("user.avsc");
    Files.writeString(schemaFile, USER_SCHEMA_JSON);

    final var catalog = new AvroSchemaCatalog();
    catalog.add("user", "com.example.User", "file://" + schemaFile.toAbsolutePath());

    assertEquals("com.example.User", catalog.get("user").getFullName());
  }

  @Test
  void addByLocationRejectsHttpUrls() {
    final var catalog = new AvroSchemaCatalog();
    final var ex = assertThrows(IllegalArgumentException.class, () ->
      catalog.add("user", "com.example.User", "http://schema-registry:8081/foo")
    );
    assertTrue(ex.getMessage().contains("HTTP"));
  }

  @Test
  void addRejectsNullKey() {
    final var catalog = new AvroSchemaCatalog();
    assertThrows(NullPointerException.class, () -> catalog.add(null, USER_SCHEMA_JSON));
  }

  @Test
  void addRejectsNullSchemaJson() {
    final var catalog = new AvroSchemaCatalog();
    assertThrows(NullPointerException.class, () -> catalog.add("user", (String) null));
  }

  @Test
  void customReaderIsInvoked() {
    final var catalog = new AvroSchemaCatalog(loc -> USER_SCHEMA_JSON);
    catalog.add("user", "com.example.User", "any-location");
    assertEquals("com.example.User", catalog.get("user").getFullName());
  }
}
