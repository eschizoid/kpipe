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
/// `deserialize(byte[])` throws on malformed input — `Result` is reserved for the `process()`
/// boundary where the filter-vs-fail distinction lives. Callers do `deserializeOrFail(bytes)`
/// then `switch (process(value))` and handle the three `Result` cases per their own semantics.
///
/// @param <T> the type of the object in the pipeline.
public interface MessagePipeline<T> {
  /// Deserializes the raw byte array into a typed object.
  ///
  /// Implementations MUST throw on malformed input rather than return `null`.
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

  /// Convenience wrapper that calls [#deserialize] and throws `IllegalStateException` if the
  /// implementation violates the no-null contract. Most callers want this — they're going to
  /// `switch (process(value))` next and would otherwise repeat the same null-check.
  ///
  /// @param data the raw data from Kafka
  /// @return the deserialized object (never null)
  /// @throws IllegalStateException if `deserialize` returns null
  default T deserializeOrFail(byte[] data) {
    final var deserialized = deserialize(data);
    if (deserialized == null) throw new IllegalStateException(
      "deserialize() returned null — implementations must throw on malformed input"
    );
    return deserialized;
  }
}
