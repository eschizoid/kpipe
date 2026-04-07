package org.kpipe.registry;

import java.util.function.UnaryOperator;
import org.kpipe.sink.MessageSink;

/// A unified pipeline interface that encapsulates the lifecycle:
/// byte[] (Kafka) -> T (Deserialized Object) -> T (Processed Object) -> byte[] (Kafka).
///
/// @param <T> The type of the object in the pipeline.
public interface MessagePipeline<T> extends UnaryOperator<byte[]> {

  /// Deserializes the raw byte array into a typed object.
  ///
  /// @param data The raw data from Kafka.
  /// @return The deserialized object.
  T deserialize(byte[] data);

  /// Serializes the typed object back into a byte array.
  ///
  /// @param data The processed object.
  /// @return The serialized data to be sent to Kafka.
  byte[] serialize(T data);

  /// Applies the chain of transformations to the typed object.
  ///
  /// @param data The deserialized object.
  /// @return The processed object.
  T process(T data);

  /// Returns the terminal sink configured for this pipeline, if any.
  ///
  /// @return The message sink, or null if none is configured.
  default MessageSink<T> getSink() {
    return null;
  }

  /// Implementation of UnaryOperator.apply that executes the full pipeline lifecycle.
  ///
  /// @param data The input bytes.
  /// @return The output bytes after processing.
  @Override
  default byte[] apply(byte[] data) {
    try {
      final var deserialized = deserialize(data);
      if (deserialized == null) return null;
      final var processed = process(deserialized);
      if (processed == null) return null;
      return serialize(processed);
    } catch (final Exception e) {
      return null;
    }
  }
}
