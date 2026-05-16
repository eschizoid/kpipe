package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.kpipe.sink.MessageSink;

/// Verifies the [MessagePipeline] error contract (1.13.0+):
/// - exceptions thrown by `deserialize` propagate (no silent swallowing) — Result is reserved
///   for the `process()` boundary.
/// - [MessagePipeline#process] returns `Result<T>`: `Passed`, `Filtered`, or `Failed`.
///   Byte-level entry points (`apply`, `processToSink`, `processToValue`) unwrap the Result:
///   `Passed.value` flows through, `Filtered` yields null/skip, `Failed.cause` is re-thrown.
/// - [MessagePipeline#deserialize] returning null is a contract violation.
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
  void shouldRethrowFailedCauseFromApply() {
    final var cause = new IllegalStateException("schema mismatch");
    final var pipeline = pipelineWithProcess(_ -> Result.failed(cause));
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
  void shouldReturnNullWhenProcessReturnsFiltered() {
    final var pipeline = pipelineWithProcess(_ -> Result.filtered());
    assertNull(pipeline.apply(INPUT), "Filtered is an intentional drop");
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
    final var pipeline = new TestPipeline<String>(_ -> "v", Result::passed, s -> output, null);
    assertSame(output, pipeline.apply(INPUT));
  }

  @Test
  void processToSinkShouldDeliverProcessedValueToSink() {
    final var captured = new AtomicReference<String>();
    final MessageSink<String> sink = captured::set;
    final var pipeline = new TestPipeline<>(_ -> "raw", s -> Result.passed(s + "-processed"), _ -> INPUT, sink);

    pipeline.processToSink(INPUT);

    assertEquals("raw-processed", captured.get());
  }

  @Test
  void processToSinkShouldNotInvokeSinkOnFiltered() {
    final var invoked = new AtomicReference<>(false);
    final MessageSink<String> sink = _ -> invoked.set(true);
    final var pipeline = new TestPipeline<String>(_ -> "raw", _ -> Result.filtered(), _ -> INPUT, sink);

    pipeline.processToSink(INPUT);

    assertFalse(invoked.get(), "sink must not be invoked for filtered messages");
  }

  @Test
  void processToSinkShouldRethrowFailedCause() {
    final MessageSink<String> sink = _ -> {
      // unreachable — Failed short-circuits before reaching the sink
    };
    final var cause = new RuntimeException("upstream failed");
    final var pipeline = new TestPipeline<String>(_ -> "raw", _ -> Result.failed(cause), _ -> INPUT, sink);

    final var ex = assertThrows(RuntimeException.class, () -> pipeline.processToSink(INPUT));
    assertEquals("upstream failed", ex.getMessage());
  }

  @Test
  void processToSinkShouldPropagateExceptionFromSink() {
    final MessageSink<String> sink = _ -> {
      throw new RuntimeException("sink failed");
    };
    final var pipeline = new TestPipeline<>(_ -> "raw", Result::passed, _ -> INPUT, sink);

    final var ex = assertThrows(RuntimeException.class, () -> pipeline.processToSink(INPUT));
    assertEquals("sink failed", ex.getMessage());
  }

  @Test
  void processToSinkShouldThrowWhenDeserializeReturnsNull() {
    final var pipeline = new TestPipeline<String>(_ -> null, Result::passed, _ -> INPUT, _ -> {});

    final var ex = assertThrows(IllegalStateException.class, () -> pipeline.processToSink(INPUT));
    assertTrue(ex.getMessage().contains("deserialize() returned null"));
  }

  @Test
  void thenShouldChainProcessOfBothPipelines() {
    final var first = new TestPipeline<>(_ -> "raw", s -> Result.passed(s + "-A"), _ -> INPUT, null);
    final var second = new TestPipeline<>(_ -> "raw", s -> Result.passed(s + "-B"), _ -> INPUT, null);

    final var composed = first.then(second);

    assertEquals(Result.passed("raw-A-B"), composed.process("raw"));
  }

  @Test
  void thenShouldShortCircuitOnFilterFromFirstPipeline() {
    final var captured = new AtomicReference<String>();
    final var first = new TestPipeline<String>(_ -> "raw", _ -> Result.filtered(), _ -> INPUT, null);
    final var second = new TestPipeline<>(
      _ -> "raw",
      s -> {
        captured.set("should-not-run");
        return Result.passed(s);
      },
      _ -> INPUT,
      null
    );

    final var composed = first.then(second);

    assertInstanceOf(Result.Filtered.class, composed.process("raw"));
    assertNull(captured.get(), "second pipeline must not run when first filters");
  }

  @Test
  void thenShouldShortCircuitOnFailedFromFirstPipeline() {
    final var cause = new RuntimeException("first failed");
    final var captured = new AtomicReference<String>();
    final var first = new TestPipeline<String>(_ -> "raw", _ -> Result.failed(cause), _ -> INPUT, null);
    final var second = new TestPipeline<>(
      _ -> "raw",
      s -> {
        captured.set("should-not-run");
        return Result.passed(s);
      },
      _ -> INPUT,
      null
    );

    final var composed = first.then(second);

    final var result = composed.process("raw");
    final var failed = assertInstanceOf(Result.Failed.class, result);
    assertSame(cause, failed.cause());
    assertNull(captured.get(), "second pipeline must not run when first fails");
  }

  @Test
  void thenShouldComposeSinksWhenBothPresent() {
    final var firstCalled = new AtomicReference<>(false);
    final var secondCalled = new AtomicReference<>(false);
    final var first = new TestPipeline<>(_ -> "raw", Result::passed, _ -> INPUT, _ -> firstCalled.set(true));
    final var second = new TestPipeline<>(_ -> "raw", Result::passed, _ -> INPUT, _ -> secondCalled.set(true));

    final var composed = first.then(second);

    composed.processToSink(INPUT);
    assertTrue(firstCalled.get(), "first sink must run");
    assertTrue(secondCalled.get(), "second sink must run");
  }

  @Test
  void processToSinkShouldNoopWhenSinkAbsent() {
    // No sink configured; processToSink runs deserialize/process and returns silently.
    final var pipeline = new TestPipeline<String>(_ -> "v", Result::passed, _ -> INPUT, null);

    // Just verifying no exception — implicit assertion.
    pipeline.processToSink(INPUT);
  }

  private interface DeserializeFn<T> {
    T apply(byte[] bytes);
  }

  private interface ProcessFn<T> {
    Result<T> apply(T value);
  }

  private interface SerializeFn<T> {
    byte[] apply(T value);
  }

  private static MessagePipeline<String> pipelineWithDeserialize(final DeserializeFn<String> fn) {
    return new TestPipeline<>(fn, Result::passed, s -> INPUT, null);
  }

  private static MessagePipeline<String> pipelineWithProcess(final ProcessFn<String> fn) {
    return new TestPipeline<>(bytes -> "v", fn, s -> INPUT, null);
  }

  private static MessagePipeline<String> pipelineWithSerialize(final SerializeFn<String> fn) {
    return new TestPipeline<>(bytes -> "v", Result::passed, fn, null);
  }

  /// Minimal record-style pipeline used to drive the contract tests. `process` returns
  /// `Result<T>` directly; callers construct `Result.passed(...)` / `Result.filtered()` /
  /// `Result.failed(...)` explicitly.
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
    public Result<T> process(final T data) {
      return processor.apply(data);
    }

    @Override
    public MessageSink<T> getSink() {
      return sink;
    }
  }
}
