package org.kpipe.registry;

import org.kpipe.sink.MessageSink;

/// A unified pipeline interface for the lifecycle:
/// byte[] (Kafka) -> T (deserialized) -> T (processed) -> byte[] (Kafka).
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

  /// Serializes the typed object back into a byte array.
  ///
  /// @param data the processed object
  /// @return the serialized data to be sent to Kafka
  byte[] serialize(T data);

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

  /// Composes this pipeline with another of the same type T, running this pipeline's processors
  /// first then `next`'s. Both pipelines must operate on the same domain type. The composed
  /// pipeline reuses this pipeline's deserializer/serializer; only the [#process] and [#getSink]
  /// steps from `next` are chained.
  ///
  /// If this pipeline's `process` returns `Filtered` or `Failed`, `next` is not invoked and the
  /// composed result is the first outcome.
  ///
  /// If `next` has a sink, it runs after this pipeline's sink.
  ///
  /// @param next the pipeline to run after this one
  /// @return a composed MessagePipeline driving both stages
  default MessagePipeline<T> then(final MessagePipeline<T> next) {
    final MessagePipeline<T> self = this;
    return new MessagePipeline<>() {
      @Override
      public T deserialize(final byte[] data) {
        return self.deserialize(data);
      }

      @Override
      public byte[] serialize(final T data) {
        return self.serialize(data);
      }

      @Override
      public Result<T> process(final T data) {
        return switch (self.process(data)) {
          case Result.Passed<T> p -> next.process(p.value());
          case Result.Filtered<T> filtered -> filtered;
          case Result.Failed<T> failed -> failed;
        };
      }

      @Override
      public MessageSink<T> getSink() {
        final var firstSink = self.getSink();
        final var nextSink = next.getSink();
        if (firstSink == null) return nextSink;
        if (nextSink == null) return firstSink;
        return value -> {
          firstSink.accept(value);
          nextSink.accept(value);
        };
      }
    };
  }
}
