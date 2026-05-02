package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.kpipe.sink.MessageSink;

/// Verifies the [MessagePipeline] error contract:
/// - exceptions in any phase propagate (no silent swallowing)
/// - [MessagePipeline#process] returning null is treated as an intentional filter
/// - [MessagePipeline#deserialize] returning null is a contract violation
class MessagePipelineContractTest {

  private static final byte[] INPUT = new byte[] { 1, 2, 3 };

  @Test
  void shouldPropagateExceptionFromDeserialize() {
    final var pipeline = pipelineWithDeserialize(bytes -> {
      throw new IllegalArgumentException("malformed");
    });
    final var ex = assertThrows(IllegalArgumentException.class, () -> pipeline.apply(INPUT));
    assertEquals("malformed", ex.getMessage());
  }

  @Test
  void shouldPropagateExceptionFromProcess() {
    final var pipeline = pipelineWithProcess(_ -> {
      throw new IllegalStateException("schema mismatch");
    });
    final var ex = assertThrows(IllegalStateException.class, () -> pipeline.apply(INPUT));
    assertEquals("schema mismatch", ex.getMessage());
  }

  @Test
  void shouldPropagateExceptionFromSerialize() {
    final var pipeline = pipelineWithSerialize(_ -> {
      throw new RuntimeException("serializer down");
    });
    final var ex = assertThrows(RuntimeException.class, () -> pipeline.apply(INPUT));
    assertEquals("serializer down", ex.getMessage());
  }

  @Test
  void shouldReturnNullWhenProcessReturnsNull() {
    final var pipeline = pipelineWithProcess(_ -> null);
    assertNull(pipeline.apply(INPUT), "process() returning null is an intentional filter");
  }

  @Test
  void shouldThrowWhenDeserializeReturnsNull() {
    final var pipeline = pipelineWithDeserialize(_ -> null);
    final var ex = assertThrows(IllegalStateException.class, () -> pipeline.apply(INPUT));
    assertEquals("deserialize() returned null — implementations must throw on malformed input", ex.getMessage());
  }

  @Test
  void shouldRoundTripSuccessfully() {
    final byte[] output = new byte[] { 9, 9, 9 };
    final var pipeline = new TestPipeline<String>(_ -> "v", s -> s, s -> output, null);
    assertSame(output, pipeline.apply(INPUT));
  }

  @Test
  void processToSinkShouldDeliverProcessedValueToSink() {
    final var captured = new AtomicReference<String>();
    final MessageSink<String> sink = captured::set;
    final var pipeline = new TestPipeline<>(_ -> "raw", s -> s + "-processed", _ -> INPUT, sink);

    pipeline.processToSink(INPUT);

    assertEquals("raw-processed", captured.get());
  }

  @Test
  void processToSinkShouldNotInvokeSinkWhenProcessReturnsNull() {
    final var invoked = new AtomicReference<>(false);
    final MessageSink<String> sink = _ -> invoked.set(true);
    final var pipeline = new TestPipeline<String>(_ -> "raw", _ -> null, _ -> INPUT, sink);

    pipeline.processToSink(INPUT);

    assertFalse(invoked.get(), "sink must not be invoked for filtered messages");
  }

  @Test
  void processToSinkShouldPropagateExceptionFromSink() {
    final MessageSink<String> sink = _ -> {
      throw new RuntimeException("sink failed");
    };
    final var pipeline = new TestPipeline<>(_ -> "raw", s -> s, _ -> INPUT, sink);

    final var ex = assertThrows(RuntimeException.class, () -> pipeline.processToSink(INPUT));
    assertEquals("sink failed", ex.getMessage());
  }

  @Test
  void processToSinkShouldThrowWhenDeserializeReturnsNull() {
    final var pipeline = new TestPipeline<String>(_ -> null, s -> s, _ -> INPUT, _ -> {});

    final var ex = assertThrows(IllegalStateException.class, () -> pipeline.processToSink(INPUT));
    assertTrue(ex.getMessage().contains("deserialize() returned null"));
  }

  @Test
  void processToSinkShouldNoopWhenSinkAbsent() {
    // No sink configured; processToSink runs deserialize/process and returns silently.
    final var pipeline = new TestPipeline<String>(_ -> "v", s -> s, _ -> INPUT, null);

    // Just verifying no exception — implicit assertion.
    pipeline.processToSink(INPUT);
  }

  private interface DeserializeFn<T> {
    T apply(byte[] bytes);
  }

  private interface ProcessFn<T> {
    T apply(T value);
  }

  private interface SerializeFn<T> {
    byte[] apply(T value);
  }

  private static MessagePipeline<String> pipelineWithDeserialize(final DeserializeFn<String> fn) {
    return new TestPipeline<>(fn, s -> s, s -> INPUT, null);
  }

  private static MessagePipeline<String> pipelineWithProcess(final ProcessFn<String> fn) {
    return new TestPipeline<>(bytes -> "v", fn, s -> INPUT, null);
  }

  private static MessagePipeline<String> pipelineWithSerialize(final SerializeFn<String> fn) {
    return new TestPipeline<>(bytes -> "v", s -> s, fn, null);
  }

  /// Minimal record-style pipeline used to drive the contract tests.
  private record TestPipeline<T>(
    DeserializeFn<T> deserializer,
    ProcessFn<T> processor,
    SerializeFn<T> serializer,
    MessageSink<T> sink
  ) implements MessagePipeline<T> {
    @Override
    public T deserialize(final byte[] data) {
      return deserializer.apply(data);
    }

    @Override
    public byte[] serialize(final T data) {
      return serializer.apply(data);
    }

    @Override
    public T process(final T data) {
      return processor.apply(data);
    }

    @Override
    public MessageSink<T> getSink() {
      return sink;
    }
  }
}
