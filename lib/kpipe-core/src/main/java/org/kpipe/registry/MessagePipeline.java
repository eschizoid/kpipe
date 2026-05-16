package org.kpipe.registry;

import java.util.function.UnaryOperator;
import org.kpipe.sink.MessageSink;

/// A unified pipeline interface that encapsulates the lifecycle:
/// byte[] (Kafka) -> T (Deserialized Object) -> T (Processed Object) -> byte[] (Kafka).
///
/// ## Error contract (1.13.0+)
///
/// `process(T)` returns a [Result] that distinguishes three outcomes at the type level:
/// - **`Passed<T>`** — non-null processed value flows to the sink.
/// - **`Filtered<T>`** — intentional drop. The caller treats the message as handled (e.g. commits
///   the offset) but skips downstream sinks.
/// - **`Failed<T>`** — non-fatal exception captured. The caller routes the cause to retry / DLQ /
///   error-handler logic.
///
/// `deserialize(byte[])` still throws on malformed input — Result is reserved for the
/// `process()` boundary where the filter-vs-fail distinction lives. The byte-level entry points
/// [#apply], [#processToSink], and [#processToValue] preserve their pre-1.13.0 contracts
/// (returning null / void for the filter case, propagating exceptions for failures) by
/// unwrapping the Result internally — so users on the byte boundary need not change. New code
/// that wants typed handling calls [#process] directly.
///
/// @param <T> The type of the object in the pipeline.
public interface MessagePipeline<T> extends UnaryOperator<byte[]> {
  /// Deserializes the raw byte array into a typed object.
  ///
  /// Implementations MUST throw on malformed input rather than return `null`.
  ///
  /// @param data The raw data from Kafka.
  /// @return The deserialized object (never null).
  T deserialize(byte[] data);

  /// Serializes the typed object back into a byte array.
  ///
  /// @param data The processed object.
  /// @return The serialized data to be sent to Kafka.
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
  /// @return The message sink, or null if none is configured.
  default MessageSink<T> getSink() {
    return null;
  }

  /// Executes the full pipeline lifecycle and returns the serialized bytes.
  ///
  /// Use this when you want bytes back (e.g. forwarding to another topic). Use
  /// [#processToSink] when you only care about the side-effect of [#getSink].
  ///
  /// Preserves the pre-1.13.0 byte-level contract: `null` for intentional filters, exceptions
  /// propagate for failures. The Result is unwrapped internally so byte-level callers don't see
  /// the type.
  ///
  /// @param data The input bytes.
  /// @return The output bytes, or `null` if the message was intentionally filtered.
  /// @throws IllegalStateException if [#deserialize] returns `null` (contract violation).
  /// @throws RuntimeException re-throws the cause from a `Result.Failed` outcome.
  @Override
  default byte[] apply(byte[] data) {
    final var deserialized = deserialize(data);
    if (deserialized == null) throw new IllegalStateException(
      "deserialize() returned null — implementations must throw on malformed input"
    );
    return switch (process(deserialized)) {
      case Result.Passed<T> p -> serialize(p.value());
      case Result.Filtered<T> __ -> null;
      case Result.Failed<T> f -> throw rethrow(f.cause());
    };
  }

  /// Executes deserialize → process → sink without serializing back to bytes.
  ///
  /// Use this when the sink is the terminal step (the common consumer pattern) — it
  /// avoids the wasted serialize() call that [#apply] performs.
  ///
  /// Intentionally filtered messages return silently without invoking the sink; failures
  /// re-throw the captured cause.
  ///
  /// @param data The input bytes.
  /// @throws IllegalStateException if [#deserialize] returns `null` (contract violation).
  /// @throws RuntimeException re-throws the cause from a `Result.Failed` outcome.
  default void processToSink(byte[] data) {
    final var deserialized = deserialize(data);
    if (deserialized == null) throw new IllegalStateException(
      "deserialize() returned null — implementations must throw on malformed input"
    );
    switch (process(deserialized)) {
      case Result.Passed<T> p -> {
        final var sink = getSink();
        if (sink != null) sink.accept(p.value());
      }
      case Result.Filtered<T> __ -> {
        /* intentional filter — no sink invocation */
      }
      case Result.Failed<T> f -> throw rethrow(f.cause());
    }
  }

  /// Executes deserialize → process and returns the resulting value without invoking any sink
  /// or re-serializing. This is the entry point used by batch-mode pipelines so the consumer
  /// can buffer the deserialized value alongside the originating Kafka record before flushing
  /// in batches.
  ///
  /// Preserves the pre-1.13.0 contract: `null` return = intentional filter; exceptions for
  /// failures.
  ///
  /// @param data the input bytes
  /// @return the processed value, or `null` if the message was intentionally filtered
  /// @throws IllegalStateException if [#deserialize] returns `null` (contract violation)
  /// @throws RuntimeException re-throws the cause from a `Result.Failed` outcome.
  default T processToValue(byte[] data) {
    final var deserialized = deserialize(data);
    if (deserialized == null) throw new IllegalStateException(
      "deserialize() returned null — implementations must throw on malformed input"
    );
    return switch (process(deserialized)) {
      case Result.Passed<T> p -> p.value();
      case Result.Filtered<T> __ -> null;
      case Result.Failed<T> f -> throw rethrow(f.cause());
    };
  }

  /// Composes this pipeline with another of the same type T, running this pipeline's
  /// processors first then `next`'s. Both pipelines must operate on the same domain
  /// type. The composed pipeline reuses this pipeline's deserializer/serializer; only
  /// the [#process] and [#getSink] steps from `next` are chained.
  ///
  /// If this pipeline's `process` returns `Filtered` or `Failed`, the next pipeline is not
  /// invoked and the composed result is the first outcome.
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

  /// Re-throws a captured cause as an unchecked exception. `RuntimeException` and `Error` pass
  /// through directly; checked exceptions are wrapped in `RuntimeException` (the byte-level
  /// callers don't declare checked throws). This preserves the pre-1.13.0 propagation behavior
  /// at the byte-level entry points.
  private static RuntimeException rethrow(final Throwable cause) {
    if (cause instanceof RuntimeException re) return re;
    if (cause instanceof Error err) throw err;
    return new RuntimeException(cause);
  }
}
