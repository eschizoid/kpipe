package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;

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
  void shouldContainDefaultOperators() {
    // Act
    var pipeline = registry.jsonPipeline("addSource", "addTimestamp", "markProcessed");
    var input = "{}".getBytes();
    var result = new String(pipeline.apply(input));

    // Assert
    assertTrue(result.contains("\"source\":\"test-app\""));
    assertTrue(result.contains("\"timestamp\":"));
    assertTrue(result.contains("\"processed\":\"true\""));
  }

  @Test
  void shouldRegisterAndRetrieveJsonOperator() {
    // Arrange
    final var operatorName = "testOperator";
    registry.registerJsonOperator(
      operatorName,
      obj -> {
        obj.put("test", "value");
        return obj;
      }
    );

    // Act
    var pipeline = registry.jsonPipeline(operatorName);
    var result = new String(pipeline.apply("{}".getBytes()));

    // Assert
    assertTrue(result.contains("\"test\":\"value\""));
  }

  @Test
  void shouldComposeJsonOperatorPipeline() {
    // Arrange
    registry.registerJsonOperator(
      "op1",
      obj -> {
        obj.put("op1", "val1");
        return obj;
      }
    );
    registry.registerJsonOperator(
      "op2",
      obj -> {
        obj.put("op2", "val2");
        return obj;
      }
    );

    // Act
    final var pipeline = registry.jsonPipeline("op1", "op2");
    final var result = new String(pipeline.apply("{}".getBytes()));

    // Assert
    assertTrue(result.contains("\"op1\":\"val1\""));
    assertTrue(result.contains("\"op2\":\"val2\""));
  }

  @Test
  void shouldHandleErrorsGracefully() {
    // Arrange
    Function<byte[], byte[]> processor = message -> {
      throw new RuntimeException("Test exception");
    };

    final var safeProcessor = MessageProcessorRegistry.withErrorHandling(processor, "fallback".getBytes());

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
    final var conditionalProcessor = MessageProcessorRegistry.when(
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
