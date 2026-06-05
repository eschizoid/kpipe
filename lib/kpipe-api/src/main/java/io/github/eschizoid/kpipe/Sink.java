package io.github.eschizoid.kpipe;

/// Terminal type returned by `Stream<T>.toXxx(...)`. A `Sink<T>` carries the fully-configured
/// pipeline but has not started consuming yet — call [#start] to launch the underlying KPipe
/// consumer and obtain a runtime [Handle].
///
/// A `Sink<T>` is single-shot: calling `start()` more than once produces undefined behavior
/// (the underlying consumer cannot be restarted). To run multiple pipelines, build separate
/// `Sink<T>` instances from independent `Stream<T>` chains.
///
/// **Phantom-but-load-bearing `T`:** no method on this interface uses `T`, but the parameter
/// tightens the [MultiBuilder] configurator signatures
/// (`Function<Stream<T>, Sink<T>>` — the configurator can't return a sink derived from the wrong
/// stream). Dropping it would silently weaken that compile-time check. The `@SuppressWarnings`
/// below documents the deliberate phantom.
///
/// @param <T> the deserialized message type flowing through the pipeline
@SuppressWarnings("unused")
public interface Sink<T> {
  /// Starts the pipeline. The returned [Handle] is the only way to query metrics, check
  /// health, or shut down the running consumer. The handle is `AutoCloseable` — wrap the call in
  /// try-with-resources to guarantee shutdown. For long-running hosts, opt in to a JVM shutdown
  /// hook via `KPipeConsumer.Builder.withShutdownHook(true)` on the explicit-API path.
  ///
  /// @return a [Handle] for monitoring and shutdown
  Handle start();
}
