package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.sink.MessageSink;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/// Verifies the [MessagePipeline] error contract:
/// - exceptions thrown by `deserialize` propagate (no silent swallowing) — `Result` is reserved
///   for the `process()` boundary.
/// - [MessagePipeline#process] returns `Result<T>`: `Passed`, `Filtered`, or `Failed`. Callers
///   `switch` on the variant directly; there are no byte-level convenience entry points that
///   unwrap to `null`/throw.
/// - [MessagePipeline#deserializeOrFail] throws if `deserialize` violates the no-null contract.
/// - [MessagePipeline#then] composes two pipelines, short-circuiting on `Filtered`/`Failed`.
class MessagePipelineContractTest {

  private static final byte[] INPUT = new byte[] { 1, 2, 3 };

  @Test
  void shouldPropagateExceptionFromDeserialize() {
    final var pipeline = pipelineWithDeserialize(_ -> {
      throw new IllegalArgumentException("malformed");
    });
    final var ex = assertThrows(IllegalArgumentException.class, () -> pipeline.deserialize(INPUT));
    assertEquals("malformed", ex.getMessage());
  }

  @Test
  void deserializeOrFailShouldThrowWhenDeserializeReturnsNull() {
    final var pipeline = pipelineWithDeserialize(_ -> null);
    final var ex = assertThrows(IllegalStateException.class, () -> pipeline.deserializeOrFail(INPUT));
    assertEquals("deserialize() returned null — implementations must throw on malformed input", ex.getMessage());
  }

  @Test
  void deserializeOrFailShouldReturnValueWhenNonNull() {
    final var pipeline = pipelineWithDeserialize(_ -> "v");
    assertEquals("v", pipeline.deserializeOrFail(INPUT));
  }

  @Test
  void processShouldReturnPassedForNormalFlow() {
    final var pipeline = pipelineWithProcess(Result::passed);
    final var result = pipeline.process("v");
    final var passed = assertInstanceOf(Result.Passed.class, result);
    assertEquals("v", passed.value());
  }

  @Test
  void processShouldReturnFilteredWhenImplementationFiltered() {
    final var pipeline = pipelineWithProcess(_ -> Result.filtered());
    assertInstanceOf(Result.Filtered.class, pipeline.process("v"));
  }

  @Test
  void processShouldReturnFailedWhenImplementationCapturesFailure() {
    final var cause = new IllegalStateException("schema mismatch");
    final var pipeline = pipelineWithProcess(_ -> Result.failed(cause));
    final var failed = assertInstanceOf(Result.Failed.class, pipeline.process("v"));
    assertSame(cause, failed.cause());
  }

  @Test
  void shouldRoundTripSuccessfully() {
    final byte[] output = new byte[] { 9, 9, 9 };
    final var pipeline = new TestPipeline<String>(_ -> "v", Result::passed, _ -> output, null);

    final var deserialized = pipeline.deserializeOrFail(INPUT);
    final var result = pipeline.process(deserialized);
    final var passed = assertInstanceOf(Result.Passed.class, result);
    assertSame(output, pipeline.serialize((String) passed.value()));
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

    // Composed sink runs the first then the second when both are present.
    final var composedSink = composed.getSink();
    assertNotNull(composedSink, "composed sink should be non-null when both pipelines have sinks");
    composedSink.accept("v");
    assertTrue(firstCalled.get(), "first sink must run");
    assertTrue(secondCalled.get(), "second sink must run");
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
    return new TestPipeline<>(fn, Result::passed, _ -> INPUT, null);
  }

  private static MessagePipeline<String> pipelineWithProcess(final ProcessFn<String> fn) {
    return new TestPipeline<>(_ -> "v", fn, _ -> INPUT, null);
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
