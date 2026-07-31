package io.github.eschizoid.kpipe.consumer;

import java.util.Set;

/// Point-in-time health view of a running consumer, taken via [KPipeConsumer#health()].
///
/// This is the one place the "is this consumer healthy" question is answered. A consumer that
/// paused itself because the circuit breaker tripped is alive but NOT healthy — the breaker
/// pauses consumption precisely because downstream is failing — so [#healthy()] is the value a
/// liveness/readiness probe should serve, not `running` alone.
///
/// @param running        the consumer thread is alive (RUNNING or PAUSED state)
/// @param paused         consumption is currently paused (any source)
/// @param pauseSources   names of the active pause sources (`MANUAL`, `BACKPRESSURE`,
///                       `CIRCUIT_BREAKER`); empty when not paused
/// @param circuitBreaker current circuit-breaker state; `CLOSED` when no breaker is configured
/// @param inFlight       records currently in flight (dispatched + buffered batch records)
public record HealthSnapshot(
  boolean running,
  boolean paused,
  Set<String> pauseSources,
  CircuitBreakerState circuitBreaker,
  long inFlight
) {
  public HealthSnapshot {
    pauseSources = Set.copyOf(pauseSources);
  }

  /// `true` when the consumer is running and the circuit breaker is not OPEN. Backpressure or
  /// manual pauses do not make a consumer unhealthy — they are normal flow control — but an
  /// OPEN breaker means downstream is failing and consumption is deliberately halted.
  public boolean healthy() {
    return running && circuitBreaker != CircuitBreakerState.OPEN;
  }
}
