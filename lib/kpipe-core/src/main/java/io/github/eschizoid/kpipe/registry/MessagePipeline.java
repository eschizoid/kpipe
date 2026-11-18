package io.github.eschizoid.kpipe.registry;

import io.github.eschizoid.kpipe.sink.MessageSink;

/// A unified pipeline interface for the lifecycle:
/// byte[] (Kafka) -> T (deserialized) -> T (processed) -> sink.
///
/// ## Error contract
///
/// `process(T)` returns a [Result] that distinguishes three outcomes at the type level:
/// - **`Passed<T>`** — non-null processed value flows to the sink.
/// - **`Filtered<T>`** — intentional drop. The caller treats the message as handled (e.g. commits
///   the offset) but skips downstream sinks.
/// - **`Failed<T>`** — non-fatal exception captured. The caller routes the cause to retry / DLQ /
///   error-handler logic.
///
/// `deserialize(byte[])` throws [MalformedRecordException] on bytes it cannot decode. `Result` is
/// reserved for the `process()` boundary, where the filter-vs-fail distinction lives. Callers do
/// `deserializeOrFail(bytes)`, then `switch (process(value))`, and handle the three `Result` cases
/// per their own semantics.
///
/// @param <T> the type of the object in the pipeline.
public interface MessagePipeline<T> {
  /// Deserializes the raw byte array into a typed object.
  ///
  /// Implementations must throw rather than return `null`: [MalformedRecordException] for bytes
  /// that cannot be decoded, including a null or empty value, and any other exception for a
  /// failure a retry may fix, such as a schema registry that is down.
  ///
  /// @param data the raw data from Kafka
  /// @return the deserialized object (never null)
  T deserialize(byte[] data);

  /// Applies the chain of transformations to the typed object.
  ///
  /// Returns a [Result] distinguishing Passed / Filtered / Failed at the type level. The
  /// implementation may throw fatal exceptions (`Error`, `InterruptedException` etc.) — only
  /// non-fatal `RuntimeException`s should be captured into `Failed`.
  ///
  /// @param data the deserialized object
  /// @return a `Result<T>` describing the outcome
  Result<T> process(T data);

  /// Returns the terminal sink configured for this pipeline, if any.
  ///
  /// @return the message sink, or null if none is configured
  default MessageSink<T> getSink() {
    return null;
  }

  /// Calls [#deserialize] and throws [MalformedRecordException] when it returns null, which a
  /// pipeline must not do. A pipeline built by [TypedPipelineBuilder] never returns null; this
  /// guards a hand-written one.
  ///
  /// @param data the raw data from Kafka
  /// @return the deserialized object (never null)
  /// @throws MalformedRecordException if `deserialize` returns null
  default T deserializeOrFail(byte[] data) {
    final var deserialized = deserialize(data);
    if (deserialized == null) throw new MalformedRecordException(
      data == null
        ? "The record value is null (a tombstone), so there is nothing to deserialize"
        : "deserialize() returned null for a " + data.length + "-byte value instead of throwing"
    );
    return deserialized;
  }
}
