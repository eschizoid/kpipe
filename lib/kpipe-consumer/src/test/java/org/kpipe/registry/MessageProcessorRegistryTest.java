package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.sink.MessageSink;

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
      .pipeline(MessageFormat.JSON)
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
    registry.register(key, obj -> {
      obj.put("test", "value");
      return obj;
    });

    // Act
    var pipeline = registry.pipeline(MessageFormat.JSON).add(key).build();
    var result = new String(pipeline.apply("{}".getBytes()));

    // Assert
    assertTrue(result.contains("\"test\":\"value\""));
  }

  @Test
  void shouldComposeJsonOperatorPipeline() {
    // Arrange
    final var op1 = RegistryKey.json("op1");
    final var op2 = RegistryKey.json("op2");

    registry.register(op1, obj -> {
      obj.put("op1", "val1");
      return obj;
    });
    registry.register(op2, obj -> {
      obj.put("op2", "val2");
      return obj;
    });

    // Act
    final var pipeline = registry.pipeline(MessageFormat.JSON).add(op1).add(op2).build();
    final var result = new String(pipeline.apply("{}".getBytes()));

    // Assert
    assertTrue(result.contains("\"op1\":\"val1\""));
    assertTrue(result.contains("\"op2\":\"val2\""));
  }

  @Test
  void shouldHandleErrorsGracefully() {
    // Arrange
    UnaryOperator<Map<String, Object>> operator = message -> {
      throw new RuntimeException("Test exception");
    };

    final var pipeline = registry.pipeline(MessageFormat.JSON).add(operator).build();

    // Act & Assert
    // Pipeline itself catches errors in TypedPipelineBuilder's implementation
    assertNull(pipeline.apply("{}".getBytes()));
  }

  @Test
  void shouldWrapOperatorWithErrorHandling() {
    // Arrange
    final UnaryOperator<Map<String, Object>> operator = message -> {
      throw new RuntimeException("Test exception");
    };
    final var safeOperator = MessageProcessorRegistry.withErrorHandling(operator);

    // Act
    final var input = new java.util.HashMap<String, Object>();
    final var result = safeOperator.apply(input);

    // Assert
    assertSame(input, result);
  }

  @Test
  void shouldWrapSinkWithErrorHandling() {
    // Arrange
    final MessageSink<Map<String, Object>> sink = message -> {
      throw new RuntimeException("Test exception");
    };
    final var safeSink = MessageSinkRegistry.withErrorHandling(sink);

    // Act & Assert
    // Should not throw exception
    assertDoesNotThrow(() -> safeSink.accept(new java.util.HashMap<>()));
  }

  @Test
  void shouldApplyConditionBasedProcessing() {
    // Arrange
    UnaryOperator<Map<String, Object>> trueOp = message -> {
      message.put("result", "true");
      return message;
    };
    UnaryOperator<Map<String, Object>> falseOp = message -> {
      message.put("result", "false");
      return message;
    };

    // Act
    final var pipeline = registry
      .pipeline(MessageFormat.JSON)
      .when(obj -> !obj.isEmpty(), trueOp, falseOp)
      .build();

    // Assert
    final var nonEmpty = "{\"key\":\"val\"}".getBytes();
    assertTrue(new String(pipeline.apply(nonEmpty)).contains("\"result\":\"true\""));

    final var empty = "{}".getBytes();
    assertTrue(new String(pipeline.apply(empty)).contains("\"result\":\"false\""));
  }

  @Test
  void shouldTrackRegisteredProcessors() {
    // Arrange
    final var key = RegistryKey.json("p1");
    final UnaryOperator<Map<String, Object>> op = obj -> obj;
    registry.register(key, op);

    // Act
    var keys = registry.getKeys();

    // Assert
    assertTrue(keys.contains(key));
  }

  @Test
  void shouldUnregisterProcessor() {
    // Arrange
    final var key = RegistryKey.json("p1");
    registry.register(key, obj -> obj);

    // Act
    assertTrue(registry.getKeys().contains(key));
    boolean removed = registry.unregister(key);

    // Assert
    assertTrue(removed);
    assertFalse(registry.getKeys().contains(key));
  }

  @Test
  void shouldTrackMetrics() {
    // Arrange
    final var key = RegistryKey.json("metricsTest");
    registry.register(key, obj -> obj);

    final var pipeline = registry.pipeline(MessageFormat.JSON).add(key).build();

    // Act
    pipeline.apply("{}".getBytes());
    pipeline.apply("{}".getBytes());

    // Assert
    final var metrics = registry.getMetrics(key);
    assertEquals(2L, metrics.get("invocationCount"));
  }

  @Test
  void shouldRegisterAndRetrieveTypedOperator() {
    // Arrange
    final var key = RegistryKey.json("typedOp");
    registry.register(key, obj -> {
      obj.put("typed", "success");
      return obj;
    });

    // Act
    final var retrieved = registry.getOperator(key);
    final var pipeline = registry.pipeline(MessageFormat.JSON).add(key).build();
    final var result = new String(pipeline.apply("{}".getBytes()));

    // Assert
    assertNotNull(retrieved);
    assertTrue(result.contains("\"typed\":\"success\""));
  }

  @Test
  void shouldComposePipelineUsingBuilder() {
    // Arrange
    final var key1 = RegistryKey.json("builderOp1");
    registry.register(key1, obj -> {
      obj.put("b1", "v1");
      return obj;
    });

    // Act
    final var pipeline = registry
      .pipeline(MessageFormat.JSON)
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
  void shouldRegisterFromEnum() {
    // Act
    registry.registerEnum((Class<Map<String, Object>>) (Class<?>) Map.class, TestOperators.class);

    // Assert
    final var key1 = RegistryKey.json("ENUM_OP1");
    final var key2 = RegistryKey.json("ENUM_OP2");

    assertNotNull(registry.getOperator(key1));
    assertNotNull(registry.getOperator(key2));

    final var pipeline = registry.pipeline(MessageFormat.JSON).add(key1).add(key2).build();

    final var result = new String(pipeline.apply("{}".getBytes()));
    assertTrue(result.contains("\"enum1\":\"v1\""));
    assertTrue(result.contains("\"enum2\":\"v2\""));
  }
}
