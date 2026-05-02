package org.kpipe.registry;

import java.util.function.UnaryOperator;
import org.kpipe.sink.MessageSink;

/// A unified pipeline interface that encapsulates the lifecycle:
/// byte[] (Kafka) -> T (Deserialized Object) -> T (Processed Object) -> byte[] (Kafka).
///
/// ## Error contract
///
/// `apply()` distinguishes three outcomes:
/// - **Success** — non-null bytes returned.
/// - **Intentional filter** — `null` returned. The caller should treat the message as
///   handled (e.g. commit the offset) but skip downstream sinks.
/// - **Failure** — any exception is propagated. Implementations of [#deserialize],
///   [#process], or [#serialize] that fail (malformed input, schema mismatch, etc.)
///   MUST throw rather than return `null`. The caller is responsible for routing
///   exceptions to error metrics, retry logic, or DLQ.
///
/// Implementations MUST NOT use `null` as a generic error signal — `null` is reserved
/// for the intentional-filter case from [#process]. [#deserialize] returning `null`
/// is treated as a contract violation.
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
  /// Returning `null` signals an intentional filter — the caller should skip the
  /// message without treating it as an error. Throw to signal a real failure.
  ///
  /// @param data The deserialized object.
  /// @return The processed object, or `null` to filter the message.
  T process(T data);

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
  /// Exceptions propagate; `null` indicates an intentional filter from [#process].
  /// See the interface-level error contract.
  ///
  /// @param data The input bytes.
  /// @return The output bytes, or `null` if the message was intentionally filtered.
  /// @throws IllegalStateException if [#deserialize] returns `null` (contract violation).
  @Override
  default byte[] apply(byte[] data) {
    final var deserialized = deserialize(data);
    if (deserialized == null) throw new IllegalStateException(
      "deserialize() returned null — implementations must throw on malformed input"
    );
    final var processed = process(deserialized);
    if (processed == null) return null;
    return serialize(processed);
  }

  /// Executes deserialize → process → sink without serializing back to bytes.
  ///
  /// Use this when the sink is the terminal step (the common consumer pattern) — it
  /// avoids the wasted serialize() call that [#apply] performs.
  ///
  /// Exceptions propagate; intentionally filtered messages return silently without
  /// invoking the sink. See the interface-level error contract.
  ///
  /// @param data The input bytes.
  /// @throws IllegalStateException if [#deserialize] returns `null` (contract violation).
  default void processToSink(byte[] data) {
    final var deserialized = deserialize(data);
    if (deserialized == null) throw new IllegalStateException(
      "deserialize() returned null — implementations must throw on malformed input"
    );
    final var processed = process(deserialized);
    if (processed == null) return;
    final var sink = getSink();
    if (sink != null) sink.accept(processed);
  }
}
