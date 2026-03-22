package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;
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
    var pipeline = registry
      .jsonPipelineBuilder()
      .add(MessageProcessorRegistry.JSON_ADD_SOURCE)
      .add(MessageProcessorRegistry.JSON_ADD_TIMESTAMP)
      .add(MessageProcessorRegistry.JSON_MARK_PROCESSED)
      .build();
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
    final var key = RegistryKey.json("testOperator");
    registry.registerOperator(
      key,
      obj -> {
        obj.put("test", "value");
        return obj;
      }
    );

    // Act
    var pipeline = registry.jsonPipelineBuilder().add(key).build();
    var result = new String(pipeline.apply("{}".getBytes()));

    // Assert
    assertTrue(result.contains("\"test\":\"value\""));
  }

  @Test
  void shouldComposeJsonOperatorPipeline() {
    // Arrange
    final var op1 = RegistryKey.json("op1");
    final var op2 = RegistryKey.json("op2");

    registry.registerOperator(
      op1,
      obj -> {
        obj.put("op1", "val1");
        return obj;
      }
    );
    registry.registerOperator(
      op2,
      obj -> {
        obj.put("op2", "val2");
        return obj;
      }
    );

    // Act
    final var pipeline = registry.jsonPipelineBuilder().add(op1).add(op2).build();
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
    final var key = RegistryKey.<Function<byte[], byte[]>>of("unique" + System.currentTimeMillis(), Function.class);
    Function<byte[], byte[]> processor = message -> message;

    // Act
    registry.register(key, processor);
    Map<RegistryKey<?>, Function<byte[], byte[]>> allProcessors = registry.getAll();

    // Assert
    assertTrue(allProcessors.containsKey(key));
  }

  @Test
  void shouldUnregisterProcessor() {
    // Arrange
    final var key = RegistryKey.<Function<byte[], byte[]>>of("processorToRemove", Function.class);

    // Act
    registry.register(key, msg -> msg);

    // Assert
    assertTrue(registry.getAll().containsKey(key));

    // Act
    boolean removed = registry.unregister(key);

    // Assert
    assertTrue(removed);
    assertFalse(registry.getAll().containsKey(key));
  }

  @Test
  void shouldTrackMetrics() {
    // Arrange
    final var key = RegistryKey.<Function<byte[], byte[]>>of("metricsTest", Function.class);
    registry.register(key, msg -> msg);

    // Act
    final var processor = registry.get(key);
    processor.apply("test".getBytes());
    processor.apply("test2".getBytes());

    // Assert
    final var metrics = registry.getMetrics(key);
    assertEquals(2L, metrics.get("invocationCount"));
    assertEquals(0L, metrics.get("errorCount"));
  }

  @Test
  void shouldRegisterAndRetrieveTypedOperator() {
    // Arrange
    final var key = RegistryKey.json("typedOp");
    registry.registerOperator(
      key,
      obj -> {
        obj.put("typed", "success");
        return obj;
      }
    );

    // Act
    final var retrieved = registry.getOperator(key);
    final var pipeline = registry.jsonPipelineBuilder().add(key).build();
    final var result = new String(pipeline.apply("{}".getBytes()));

    // Assert
    assertNotNull(retrieved);
    assertTrue(result.contains("\"typed\":\"success\""));
  }

  @Test
  void shouldComposePipelineUsingBuilder() {
    // Arrange
    final var key1 = RegistryKey.json("builderOp1");
    registry.registerOperator(
      key1,
      obj -> {
        obj.put("b1", "v1");
        return obj;
      }
    );

    // Act
    final var pipeline = registry
      .jsonPipelineBuilder()
      .add(key1)
      .add(obj -> {
        obj.put("b2", "v2");
        return obj;
      })
      .add(MessageProcessorRegistry.JSON_ADD_SOURCE)
      .build();

    final var result = new String(pipeline.apply("{}".getBytes()));

    // Assert
    assertTrue(result.contains("\"b1\":\"v1\""));
    assertTrue(result.contains("\"b2\":\"v2\""));
    assertTrue(result.contains("\"source\":\"test-app\""));
  }

  private enum TestOperators implements UnaryOperator<Map<String, Object>> {
    ENUM_OP1(obj -> {
      obj.put("enum1", "v1");
      return obj;
    }),
    ENUM_OP2(obj -> {
      obj.put("enum2", "v2");
      return obj;
    });

    private final UnaryOperator<Map<String, Object>> op;

    TestOperators(UnaryOperator<Map<String, Object>> op) {
      this.op = op;
    }

    @Override
    public Map<String, Object> apply(Map<String, Object> t) {
      return op.apply(t);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldRegisterOperatorsFromEnum() {
    // Act
    registry.registerEnum((Class<Map<String, Object>>) (Class<?>) Map.class, TestOperators.class);

    // Assert
    final var key1 = RegistryKey.json("ENUM_OP1");
    final var key2 = RegistryKey.json("ENUM_OP2");

    assertNotNull(registry.getOperator(key1));
    assertNotNull(registry.getOperator(key2));

    final var pipeline = registry.jsonPipelineBuilder().add(key1).add(key2).build();

    final var result = new String(pipeline.apply("{}".getBytes()));
    assertTrue(result.contains("\"enum1\":\"v1\""));
    assertTrue(result.contains("\"enum2\":\"v2\""));
  }
}
