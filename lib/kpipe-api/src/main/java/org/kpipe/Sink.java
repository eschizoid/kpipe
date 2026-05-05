package org.kpipe;

/// Terminal type returned by `Stream<T>.toXxx(...)`. A `Sink<T>` carries the fully-configured
/// pipeline but has not started consuming yet — call [#start] to launch the underlying KPipe
/// consumer + runner and obtain a runtime [Handle].
///
/// A `Sink<T>` is single-shot: calling `start()` more than once produces undefined behavior
/// (the underlying consumer cannot be restarted). To run multiple pipelines, build separate
/// `Sink<T>` instances from independent `Stream<T>` chains.
///
/// @param <T> the deserialized message type flowing through the pipeline
public interface Sink<T> {
  /// Starts the pipeline. The returned [Handle] is the only way to query metrics, check
  /// health, or shut down the running consumer. The underlying `KPipeRunner` registers a JVM
  /// shutdown hook by default, so the consumer will close cleanly even if the handle is not
  /// closed explicitly — but using try-with-resources on the [Handle] (which is
  /// `AutoCloseable`) is the recommended pattern.
  ///
  /// @return a [Handle] for monitoring and shutdown
  Handle start();
}
