package io.github.eschizoid.kpipe.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.sink.MessageSink;
import org.junit.jupiter.api.Test;

/// Verifies the [Pipeline] error contract:
/// - exceptions thrown by `deserialize` propagate (no silent swallowing) — `Result` is reserved
///   for the `process()` boundary.
/// - [Pipeline#process] returns `Result<T>`: `Passed`, `Filtered`, or `Failed`. Callers
///   `switch` on the variant directly; there are no byte-level convenience entry points that
///   unwrap to `null`/throw.
/// - [Pipeline#deserializeOrFail] throws if `deserialize` violates the no-null contract.
class PipelineContractTest {

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
    final var ex = assertThrows(MalformedRecordException.class, () -> pipeline.deserializeOrFail(INPUT));
    assertEquals(
      "deserialize() returned null for a " + INPUT.length + "-byte value instead of throwing",
      ex.getMessage()
    );
  }

  @Test
  void deserializeOrFailNamesANullValueAsATombstone() {
    final var pipeline = pipelineWithDeserialize(_ -> null);
    final var ex = assertThrows(MalformedRecordException.class, () -> pipeline.deserializeOrFail(null));
    assertTrue(ex.getMessage().contains("tombstone"), ex.getMessage());
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
  void aDeserializedValuePassesThroughProcess() {
    final var pipeline = new TestPipeline<String>(_ -> "v", Result::passed, null);

    final var deserialized = pipeline.deserializeOrFail(INPUT);
    final var passed = assertInstanceOf(Result.Passed.class, pipeline.process(deserialized));
    assertEquals("v", passed.value());
  }

  private interface DeserializeFn<T> {
    T apply(byte[] bytes);
  }

  private interface ProcessFn<T> {
    Result<T> apply(T value);
  }

  private static Pipeline<String> pipelineWithDeserialize(final DeserializeFn<String> fn) {
    return new TestPipeline<>(fn, Result::passed, null);
  }

  private static Pipeline<String> pipelineWithProcess(final ProcessFn<String> fn) {
    return new TestPipeline<>(_ -> "v", fn, null);
  }

  /// Minimal record-style pipeline used to drive the contract tests. `process` returns
  /// `Result<T>` directly; callers construct `Result.passed(...)` / `Result.filtered()` /
  /// `Result.failed(...)` explicitly.
  private record TestPipeline<T>(
    DeserializeFn<T> deserializer,
    ProcessFn<T> processor,
    MessageSink<T> sink
  ) implements Pipeline<T> {
    @Override
    public T deserialize(final byte[] data) {
      return deserializer.apply(data);
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
