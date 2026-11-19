package io.github.eschizoid.kpipe.format.json;

import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.Result;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

/// JSON pipelines built with [MessagePipeline#from] compose their operators in order.
class JsonPipelineCompositionTest {

  /// Test helper: drives `bytes` through the full pipeline cycle (deserialize → process →
  /// serialize) and returns the resulting bytes. Filtered records return null; failures
  /// re-throw the cause.
  private static <T> byte[] roundTrip(
    final MessageFormat<T> format,
    final MessagePipeline<T> pipeline,
    final byte[] bytes
  ) {
    final var deserialized = pipeline.deserializeOrFail(bytes);
    return switch (pipeline.process(deserialized)) {
      case Result.Passed<T> p -> format.serialize(p.value());
      case Result.Filtered<T> _ -> null;
      case Result.Failed<T> f -> {
        if (f.cause() instanceof RuntimeException re) throw re;
        if (f.cause() instanceof Error err) throw err;
        throw new RuntimeException(f.cause());
      }
    };
  }

  @Test
  void shouldComposeJsonProcessorChain() {
    final var pipeline = MessagePipeline.from(JsonFormat.INSTANCE)
      .pipe(obj -> {
        obj.put("source", "test-app");
        return obj;
      })
      .pipe(obj -> {
        obj.put("timestamp", System.currentTimeMillis());
        return obj;
      })
      .pipe(obj -> {
        obj.put("processed", "true");
        return obj;
      })
      .build();

    final var result = new String(roundTrip(JsonFormat.INSTANCE, pipeline, "{}".getBytes()));

    assertTrue(result.contains("\"source\":\"test-app\""));
    assertTrue(result.contains("\"timestamp\":"));
    assertTrue(result.contains("\"processed\":\"true\""));
  }

  @Test
  void shouldPropagateExceptionsFromOperators() {
    final UnaryOperator<Map<String, Object>> operator = _ -> {
      throw new RuntimeException("Test exception");
    };

    final var pipeline = MessagePipeline.from(JsonFormat.INSTANCE).pipe(operator).build();

    final var ex = assertThrows(RuntimeException.class, () ->
      roundTrip(JsonFormat.INSTANCE, pipeline, "{}".getBytes())
    );
    assertEquals("Test exception", ex.getMessage());
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

    final var pipeline = MessagePipeline.from(JsonFormat.INSTANCE)
      .when(obj -> !obj.isEmpty(), trueOp, falseOp)
      .build();

    final var nonEmpty = "{\"key\":\"val\"}".getBytes();
    assertTrue(new String(roundTrip(JsonFormat.INSTANCE, pipeline, nonEmpty)).contains("\"result\":\"true\""));

    final var empty = "{}".getBytes();
    assertTrue(new String(roundTrip(JsonFormat.INSTANCE, pipeline, empty)).contains("\"result\":\"false\""));
  }

  @Test
  void shouldComposeSeveralOperatorsInOrder() {
    final var pipeline = MessagePipeline.from(JsonFormat.INSTANCE)
      .pipe(obj -> {
        obj.put("b1", "v1");
        return obj;
      })
      .pipe(obj -> {
        obj.put("b2", obj.get("b1") + "-then-b2");
        return obj;
      })
      .pipe(obj -> {
        obj.put("b3", obj.get("b2") + "-then-b3");
        return obj;
      })
      .build();

    final var result = new String(roundTrip(JsonFormat.INSTANCE, pipeline, "{}".getBytes()));

    assertTrue(result.contains("\"b3\":\"v1-then-b2-then-b3\""), result);
  }
}
