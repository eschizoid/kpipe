package org.kpipe.format.json;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.RegistryKey;
import org.kpipe.sink.MessageSink;

class MessageProcessorRegistryJsonTest {

  private MessageProcessorRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new MessageProcessorRegistry();
  }

  @Test
  void shouldComposeJsonProcessorChain() {
    final var pipeline = registry
      .pipeline(JsonFormat.INSTANCE)
      .add(obj -> {
        obj.put("source", "test-app");
        return obj;
      })
      .add(obj -> {
        obj.put("timestamp", System.currentTimeMillis());
        return obj;
      })
      .add(obj -> {
        obj.put("processed", "true");
        return obj;
      })
      .build();

    final var result = new String(pipeline.apply("{}".getBytes()));

    assertTrue(result.contains("\"source\":\"test-app\""));
    assertTrue(result.contains("\"timestamp\":"));
    assertTrue(result.contains("\"processed\":\"true\""));
  }

  @Test
  void shouldRegisterAndRetrieveJsonOperator() {
    final var key = RegistryKey.json("testOperator");
    registry.register(key, obj -> {
      obj.put("test", "value");
      return obj;
    });

    final var pipeline = registry.pipeline(JsonFormat.INSTANCE).add(key).build();
    final var result = new String(pipeline.apply("{}".getBytes()));

    assertTrue(result.contains("\"test\":\"value\""));
  }

  @Test
  void shouldComposeJsonOperatorPipeline() {
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

    final var pipeline = registry.pipeline(JsonFormat.INSTANCE).add(op1).add(op2).build();
    final var result = new String(pipeline.apply("{}".getBytes()));

    assertTrue(result.contains("\"op1\":\"val1\""));
    assertTrue(result.contains("\"op2\":\"val2\""));
  }

  @Test
  void shouldPropagateExceptionsFromOperators() {
    final UnaryOperator<Map<String, Object>> operator = message -> {
      throw new RuntimeException("Test exception");
    };

    final var pipeline = registry.pipeline(JsonFormat.INSTANCE).add(operator).build();

    final var ex = assertThrows(RuntimeException.class, () -> pipeline.apply("{}".getBytes()));
    assertEquals("Test exception", ex.getMessage());
  }

  @Test
  void shouldWrapOperatorWithErrorHandling() {
    final UnaryOperator<Map<String, Object>> operator = message -> {
      throw new RuntimeException("Test exception");
    };
    final var safeOperator = MessageProcessorRegistry.withErrorHandling(operator);

    final var input = new java.util.HashMap<String, Object>();
    final var result = safeOperator.apply(input);

    assertSame(input, result);
  }

  @Test
  void shouldWrapSinkWithErrorHandling() {
    final MessageSink<Map<String, Object>> sink = message -> {
      throw new RuntimeException("Test exception");
    };
    final var safeSink = MessageProcessorRegistry.withErrorHandling(sink);

    assertDoesNotThrow(() -> safeSink.accept(new java.util.HashMap<>()));
  }

  @Test
  void shouldApplyConditionBasedProcessing() {
    final UnaryOperator<Map<String, Object>> trueOp = message -> {
      message.put("result", "true");
      return message;
    };
    final UnaryOperator<Map<String, Object>> falseOp = message -> {
      message.put("result", "false");
      return message;
    };

    final var pipeline = registry
      .pipeline(JsonFormat.INSTANCE)
      .when(obj -> !obj.isEmpty(), trueOp, falseOp)
      .build();

    final var nonEmpty = "{\"key\":\"val\"}".getBytes();
    assertTrue(new String(pipeline.apply(nonEmpty)).contains("\"result\":\"true\""));

    final var empty = "{}".getBytes();
    assertTrue(new String(pipeline.apply(empty)).contains("\"result\":\"false\""));
  }

  @Test
  void shouldTrackRegisteredProcessors() {
    final var key = RegistryKey.json("p1");
    registry.register(key, obj -> obj);

    assertTrue(registry.getKeys().contains(key));
  }

  @Test
  void shouldUnregisterProcessor() {
    final var key = RegistryKey.json("p1");
    registry.register(key, obj -> obj);

    assertTrue(registry.getKeys().contains(key));
    final var removed = registry.unregister(key);

    assertTrue(removed);
    assertFalse(registry.getKeys().contains(key));
  }

  @Test
  void shouldTrackMetrics() {
    final var key = RegistryKey.json("metricsTest");
    registry.register(key, obj -> obj);

    final var pipeline = registry.pipeline(JsonFormat.INSTANCE).add(key).build();

    pipeline.apply("{}".getBytes());
    pipeline.apply("{}".getBytes());

    final var metrics = registry.getMetrics(key);
    assertEquals(2L, metrics.get("invocationCount"));
  }

  @Test
  void shouldRegisterAndRetrieveTypedOperator() {
    final var key = RegistryKey.json("typedOp");
    registry.register(key, obj -> {
      obj.put("typed", "success");
      return obj;
    });

    final var retrieved = registry.getOperator(key);
    final var pipeline = registry.pipeline(JsonFormat.INSTANCE).add(key).build();
    final var result = new String(pipeline.apply("{}".getBytes()));

    assertNotNull(retrieved);
    assertTrue(result.contains("\"typed\":\"success\""));
  }

  @Test
  void shouldComposePipelineUsingBuilder() {
    final var key1 = RegistryKey.json("builderOp1");
    registry.register(key1, obj -> {
      obj.put("b1", "v1");
      return obj;
    });

    final var pipeline = registry
      .pipeline(JsonFormat.INSTANCE)
      .add(key1)
      .add(obj -> {
        obj.put("b2", "v2");
        return obj;
      })
      .add(obj -> {
        obj.put("source", "test-app");
        return obj;
      })
      .build();

    final var result = new String(pipeline.apply("{}".getBytes()));

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

    TestOperators(final UnaryOperator<Map<String, Object>> op) {
      this.op = op;
    }

    @Override
    public Map<String, Object> apply(final Map<String, Object> t) {
      return op.apply(t);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldRegisterFromEnum() {
    registry.registerEnum((Class<Map<String, Object>>) (Class<?>) Map.class, TestOperators.class);

    final var key1 = RegistryKey.json("ENUM_OP1");
    final var key2 = RegistryKey.json("ENUM_OP2");

    assertNotNull(registry.getOperator(key1));
    assertNotNull(registry.getOperator(key2));

    final var pipeline = registry.pipeline(JsonFormat.INSTANCE).add(key1).add(key2).build();

    final var result = new String(pipeline.apply("{}".getBytes()));
    assertTrue(result.contains("\"enum1\":\"v1\""));
    assertTrue(result.contains("\"enum2\":\"v2\""));
  }
}
