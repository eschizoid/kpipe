package org.kpipe;

/// Terminal type returned by `Stream<T>.toXxx(...)`. A `Sink<T>` is fully configured but not yet
/// started; calling [#start] launches the underlying KPipe consumer + runner and returns a
/// runtime [Handle].
///
/// @param <T> the deserialized message type flowing through the pipeline
/// @since 1.11.0
public interface Sink<T> {
  /// Starts consuming and returns a runtime handle.
  ///
  /// @return a [Handle] for monitoring and shutdown
  Handle start();
}
