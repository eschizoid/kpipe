package org.kpipe.consumer;

import java.time.Duration;
import java.util.Objects;

/// Pure-function decision module for the consumer-side circuit breaker. Mirrors the shape of
/// [BackpressureController]: a record holding thresholds + duration, methods that take the current
/// stats / state and return whether a transition should fire.
///
/// **Why hand-rolled, not Resilience4j.** Resilience4j duplicates infrastructure already in
/// `kpipe-consumer`: a state machine, command-queue serialization onto the consumer thread, a
/// scheduled executor for deferred work, and pause/resume choreography via `manualPause` +
/// `LockSupport.park/unpark`. Adopting it for CB-only would mean running parallel state machines
/// or reducing Resilience4j to a thin wrapper exercising ~10% of its API. The reversible decision
/// is to hand-roll here; if KPipe later grows rate-limit / bulkhead / slow-call detection /
/// jittered retries, revisit and migrate at once.
///
/// **State machine** (driven by [KPipeConsumer], not by this controller):
///
///   * `CLOSED` — outcomes feed [CircuitBreakerStats]; on every failure the consumer asks
///     [#shouldTrip] whether to flip to `OPEN`. The check returns true when both the failure
///     rate has crossed `failureThreshold` and the window has at least `windowSize` samples
///     (no tripping on a single failure with an empty window).
///   * `OPEN` — consumer is paused. A timer asks [#shouldProbe] on each tick; once
///     `openDuration` has elapsed the consumer flips to `HALF_OPEN` and resumes Kafka polling.
///   * `HALF_OPEN` — the next record's outcome decides: success → `CLOSED` (window reset),
///     failure → `OPEN` (timer restarted).
///
/// **Action enum.** The controller exposes a small `Action` enum mainly for symmetry with
/// [BackpressureController]. In practice the consumer drives transitions inline via CAS using
/// the two predicates above; the enum is convenient for tests and explicit state-machine
/// rendering.
///
/// @param failureThreshold the failure rate (0.0..1.0) at or above which the breaker trips
/// @param windowSize       the rolling sample size — must match the stats window
/// @param openDuration     how long the breaker stays in `OPEN` before probing
public record CircuitBreakerController(double failureThreshold, int windowSize, Duration openDuration) {
  /// Actions the consumer can take given the current stats + state.
  public enum Action {
    /// No transition is needed.
    NONE,
    /// `CLOSED → OPEN`: the failure rate crossed the threshold; the breaker should trip.
    TRIP,
    /// `OPEN → HALF_OPEN`: the open duration has elapsed; the breaker should probe.
    PROBE,
  }

  public CircuitBreakerController {
    if (failureThreshold <= 0.0 || failureThreshold > 1.0) {
      throw new IllegalArgumentException("failureThreshold must be in (0.0, 1.0], got %f".formatted(failureThreshold));
    }
    if (windowSize <= 0) throw new IllegalArgumentException("windowSize must be positive, got " + windowSize);
    Objects.requireNonNull(openDuration, "openDuration cannot be null");
    if (openDuration.isNegative() || openDuration.isZero()) {
      throw new IllegalArgumentException("openDuration must be positive, got " + openDuration);
    }
  }

  /// In `CLOSED` state, returns `true` iff the window is full AND the failure rate has crossed
  /// the threshold. Requiring a full window avoids tripping on the first few failures before the
  /// breaker has any signal to work with.
  public boolean shouldTrip(final CircuitBreakerStats stats) {
    Objects.requireNonNull(stats, "stats cannot be null");
    if (stats.totalSamples() < windowSize) return false;
    return stats.failureRate() >= failureThreshold;
  }

  /// In `OPEN` state, returns `true` iff `openDuration` has elapsed since the breaker tripped.
  /// `openedAtNanos` is `System.nanoTime()` from the trip; a `0L` sentinel (never tripped) always
  /// returns `false`.
  public boolean shouldProbe(final long openedAtNanos, final long nowNanos) {
    if (openedAtNanos == 0L) return false;
    return (nowNanos - openedAtNanos) >= openDuration.toNanos();
  }
}
