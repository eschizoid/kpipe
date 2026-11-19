package io.github.eschizoid.kpipe.consumer;

/// Why a consumer is paused. A consumer is paused while at least one source holds a pause, and it
/// resumes only when the last one lets go, so sources never override each other.
public enum PauseSource {
  /// [KPipeConsumer#pause()] was called, and [KPipeConsumer#resume()] has not been called since.
  MANUAL,

  /// The backpressure metric crossed its high watermark and has not yet fallen to the low one.
  BACKPRESSURE,

  /// The circuit breaker is open.
  CIRCUIT_BREAKER,
}
