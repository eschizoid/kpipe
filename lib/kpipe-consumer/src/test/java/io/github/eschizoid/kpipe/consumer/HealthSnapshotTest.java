package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HealthSnapshotTest {

  @Test
  void pausedIsTrueExactlyWhenASourceHoldsAPause() {
    assertFalse(new HealthSnapshot(true, Set.of(), CircuitBreakerState.CLOSED).paused());
    assertTrue(new HealthSnapshot(true, Set.of(PauseSource.BACKPRESSURE), CircuitBreakerState.CLOSED).paused());
  }

  @Test
  void healthyNeedsARunningConsumerWithItsBreakerNotOpen() {
    assertTrue(new HealthSnapshot(true, Set.of(PauseSource.MANUAL), CircuitBreakerState.HALF_OPEN).healthy());
    assertFalse(new HealthSnapshot(true, Set.of(PauseSource.CIRCUIT_BREAKER), CircuitBreakerState.OPEN).healthy());
    assertFalse(new HealthSnapshot(false, Set.of(), CircuitBreakerState.CLOSED).healthy());
  }

  @Test
  void pauseSourcesAreAnUnmodifiableCopyInDeclarationOrder() {
    final var sources = new HashSet<>(Set.of(PauseSource.CIRCUIT_BREAKER, PauseSource.MANUAL));
    final var snapshot = new HealthSnapshot(true, sources, CircuitBreakerState.CLOSED);
    sources.add(PauseSource.BACKPRESSURE);

    assertEquals(List.of(PauseSource.MANUAL, PauseSource.CIRCUIT_BREAKER), List.copyOf(snapshot.pauseSources()));
    assertThrows(UnsupportedOperationException.class, () -> snapshot.pauseSources().add(PauseSource.BACKPRESSURE));
  }

  @Test
  void nullComponentsAreRejected() {
    assertThrows(NullPointerException.class, () -> new HealthSnapshot(true, null, CircuitBreakerState.CLOSED));
    assertThrows(NullPointerException.class, () -> new HealthSnapshot(true, Set.of(), null));
  }
}
