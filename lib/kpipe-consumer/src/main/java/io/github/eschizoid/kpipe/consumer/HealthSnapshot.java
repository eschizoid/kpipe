package io.github.eschizoid.kpipe.consumer;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/// A consumer's health at one moment, taken with [KPipeConsumer#health()]; every component comes
/// from the same moment.
///
/// [#healthy()] is the value a liveness or readiness probe should serve. A consumer paused by its
/// circuit breaker is running but not healthy, since the breaker pauses consumption because
/// downstream is failing; a manual or backpressure pause is normal flow control and leaves it
/// healthy. How much work is in flight is a load figure, not a health fact, so it lives in
/// [MetricsSnapshot#inFlight()].
///
/// @param running        the consumer has started and is not shutting down, paused or not
/// @param pauseSources   the sources holding a pause, in declaration order; empty when not paused
/// @param circuitBreaker the circuit breaker's state; `CLOSED` when no breaker is configured
public record HealthSnapshot(boolean running, Set<PauseSource> pauseSources, CircuitBreakerState circuitBreaker) {
  /// Copies `pauseSources` into an unmodifiable set that keeps declaration order.
  ///
  /// @throws NullPointerException if `pauseSources` or `circuitBreaker` is null
  public HealthSnapshot {
    Objects.requireNonNull(pauseSources, "pauseSources");
    Objects.requireNonNull(circuitBreaker, "circuitBreaker");
    pauseSources = pauseSources.isEmpty() ? Set.of() : Collections.unmodifiableSet(EnumSet.copyOf(pauseSources));
  }

  /// @return `true` when at least one source holds a pause
  public boolean paused() {
    return !pauseSources.isEmpty();
  }

  /// @return `true` when the consumer is running and its circuit breaker is not `OPEN`
  public boolean healthy() {
    return running && circuitBreaker != CircuitBreakerState.OPEN;
  }
}
