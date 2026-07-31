package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/// Pins the [SchemaResolver#lookupRequired(int)] default method: a resolver that violates the
/// no-null/no-blank contract fails with the one uniform message, and a well-behaved resolver's
/// text passes through unchanged. Format modules rely on this instead of re-checking locally.
class SchemaResolverLookupRequiredTest {

  @Test
  void nullSchemaTextThrowsIllegalStateWithUniformMessage() {
    final SchemaResolver resolver = schemaId -> null;
    final var ex = assertThrows(IllegalStateException.class, () -> resolver.lookupRequired(42));
    assertEquals("Schema resolver returned empty schema for id 42", ex.getMessage());
  }

  @Test
  void blankSchemaTextThrowsIllegalStateWithUniformMessage() {
    final SchemaResolver resolver = schemaId -> "   ";
    final var ex = assertThrows(IllegalStateException.class, () -> resolver.lookupRequired(7));
    assertEquals("Schema resolver returned empty schema for id 7", ex.getMessage());
  }

  @Test
  void nonBlankSchemaTextPassesThroughUnchanged() {
    final SchemaResolver resolver = schemaId -> "{\"type\":\"string\"} for " + schemaId;
    assertEquals("{\"type\":\"string\"} for 5", resolver.lookupRequired(5));
  }

  @Test
  void resolverExceptionPropagatesUnchanged() {
    final SchemaResolver resolver = schemaId -> {
      throw new RuntimeException("registry down");
    };
    final var ex = assertThrows(RuntimeException.class, () -> resolver.lookupRequired(1));
    assertTrue(ex.getMessage().contains("registry down"));
  }
}
