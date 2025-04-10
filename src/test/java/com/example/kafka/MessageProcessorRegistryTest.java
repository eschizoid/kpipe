package com.example.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MessageProcessorRegistryTest {

  private MessageProcessorRegistry registry;

  @BeforeEach
  void setUp() {
    // Create a fresh registry instance before each test
    registry = new MessageProcessorRegistry("test-app");
  }

  @Test
  void shouldContainDefaultProcessors() {
    // Arrange
    final var processors = registry.getAll();

    // Act
    assertTrue(processors.containsKey("parseJson"));
    assertTrue(processors.containsKey("addSource"));
    assertTrue(processors.containsKey("markProcessed"));
    assertTrue(processors.containsKey("addTimestamp"));
  }

  @Test
  void shouldRegisterAndRetrieveProcessor() {
    // Arrange
    final var processorName = "testProcessor";
    final var input = "input".getBytes();
    final var output = "output".getBytes();
    Function<byte[], byte[]> testProcessor = message -> output;

    // Act
    registry.register(processorName, testProcessor);
    Function<byte[], byte[]> retrievedProcessor = registry.get(processorName);

    // Assert
    assertArrayEquals(output, retrievedProcessor.apply(input));
  }

  @Test
  void shouldReturnIdentityForMissingProcessor() {
    // Arrange
    Function<byte[], byte[]> processor = registry.get("nonExistentProcessor");

    // Act
    final var input = "test".getBytes();

    // Assert
    assertArrayEquals(input, processor.apply(input));
  }

  @Test
  void shouldComposeProcessorPipeline() {
    // Arrange
    registry.register(
      "doubleKey",
      message -> {
        String json = new String(message);
        return json.replace("key", "keykey").getBytes();
      }
    );

    // Act
    Function<byte[], byte[]> pipeline = registry.pipeline("parseJson", "doubleKey");

    final var result = pipeline.apply(
      """
            {
              "key":"value"
            }
            """.getBytes()
    );
    final var resultJson = new String(result);

    // Assert
    assertEquals("""
            {"keykey":"value"}""", resultJson);
  }

  @Test
  void shouldCreatePipelineFromListOfProcessors() {
    // Arrange
    Function<byte[], byte[]> processor1 = message -> "test1-".getBytes();
    Function<byte[], byte[]> processor2 = message -> {
      byte[] suffix = "suffix".getBytes();
      byte[] result = new byte[message.length + suffix.length];
      System.arraycopy(message, 0, result, 0, message.length);
      System.arraycopy(suffix, 0, result, message.length, suffix.length);
      return result;
    };

    // Act
    Function<byte[], byte[]> pipeline = registry.pipeline(List.of(processor1, processor2));

    final var result = pipeline.apply("input".getBytes());

    // Assert
    assertEquals("test1-suffix", new String(result));
  }

  @Test
  void shouldHandleErrorsGracefully() {
    // Arrange
    Function<byte[], byte[]> processor = message -> {
      throw new RuntimeException("Test exception");
    };

    Function<byte[], byte[]> safeProcessor = MessageProcessorRegistry.withErrorHandling(
      processor,
      "fallback".getBytes()
    );

    // Act
    final var result = safeProcessor.apply("any input".getBytes());

    // Assert
    assertEquals("fallback", new String(result));
  }

  @Test
  void shouldApplyConditionBasedProcessing() {
    // Arrange
    Function<byte[], byte[]> trueProcessor = message -> "true".getBytes();
    Function<byte[], byte[]> falseProcessor = message -> "false".getBytes();

    // Act
    Function<byte[], byte[]> conditionalProcessor = MessageProcessorRegistry.when(
      message -> message.length > 5,
      trueProcessor,
      falseProcessor
    );

    // Assert
    final var longMessage = "123456".getBytes();
    assertArrayEquals("true".getBytes(), conditionalProcessor.apply(longMessage));

    // Test condition false
    final var shortMessage = "1234".getBytes();
    assertArrayEquals("false".getBytes(), conditionalProcessor.apply(shortMessage));
  }

  @Test
  void shouldTrackRegisteredProcessors() {
    // Arrange
    final var uniqueName = "unique" + System.currentTimeMillis();
    Function<byte[], byte[]> processor = message -> message;

    // Act
    registry.register(uniqueName, processor);
    Map<String, Function<byte[], byte[]>> allProcessors = registry.getAll();

    // Assert
    assertTrue(allProcessors.containsKey(uniqueName));
  }

  @Test
  void shouldUnregisterProcessor() {
    // Arrange
    final var name = "processorToRemove";

    // Act
    registry.register(name, msg -> msg);

    // Assert
    assertTrue(registry.getAll().containsKey(name));

    // Act
    boolean removed = registry.unregister(name);

    // Assert
    assertTrue(removed);
    assertFalse(registry.getAll().containsKey(name));
  }

  @Test
  void shouldTrackMetrics() {
    // Arrange
    final var name = "metricsTest";
    registry.register(name, msg -> msg);

    // Act
    final var processor = registry.get(name);
    processor.apply("test".getBytes());
    processor.apply("test2".getBytes());

    // Assert
    final var metrics = registry.getMetrics(name);
    assertEquals(2L, metrics.get("invocationCount"));
    assertEquals(0L, metrics.get("errorCount"));
  }
}
