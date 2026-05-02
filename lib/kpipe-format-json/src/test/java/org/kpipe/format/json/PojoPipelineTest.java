package org.kpipe.format.json;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.RegistryKey;

public class PojoPipelineTest {

  @Test
  void shouldProcessPojoPipeline() {
    final var registry = new MessageProcessorRegistry("test-app");

    // Register a POJO operator
    final RegistryKey<UserRecord> userMaskerKey = RegistryKey.of("userMasker", UserRecord.class);
    registry.register(userMaskerKey, (UserRecord user) -> new UserRecord(user.id(), "MASKED", user.email()));

    // Build pipeline
    final var pipeline = registry.pipeline(JsonFormat.pojo(UserRecord.class)).add(userMaskerKey).build();

    // Initial data
    final var user = new UserRecord("1", "John Doe", "john@example.com");
    final var format = JsonFormat.pojo(UserRecord.class);
    final byte[] inputBytes = format.serialize(user);

    // Process
    final byte[] resultBytes = pipeline.apply(inputBytes);

    // Verify
    final UserRecord result = format.deserialize(resultBytes);
    assertEquals("1", result.id());
    assertEquals("MASKED", result.name());
    assertEquals("john@example.com", result.email());
  }

  @Test
  void shouldHandleMixedOperatorsInPojoPipeline() {
    final var registry = new MessageProcessorRegistry("test-app");

    final var user = new UserRecord("1", "John Doe", "john@example.com");
    final var format = JsonFormat.pojo(UserRecord.class);
    final byte[] inputBytes = format.serialize(user);

    final var pipeline = registry
      .pipeline(JsonFormat.pojo(UserRecord.class))
      .add(u -> new UserRecord(u.id(), u.name().toUpperCase(), u.email()))
      .add(u -> new UserRecord(u.id(), u.name(), "PROTECTED"))
      .build();

    final byte[] resultBytes = pipeline.apply(inputBytes);
    final UserRecord result = format.deserialize(resultBytes);

    assertEquals("JOHN DOE", result.name());
    assertEquals("PROTECTED", result.email());
  }
}
