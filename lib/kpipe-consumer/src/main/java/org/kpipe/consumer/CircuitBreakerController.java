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
///   * `CLOSED` — outcomes feed a rolling window; on every failure the consumer asks
///     [#shouldTrip] whether to flip to `OPEN`. The check returns true when both the failure
///     rate has crossed `failureThreshold` and the window has at least `windowSize` samples
///     (no tripping on a single failure with an empty window).
///   * `OPEN` — consumer is paused. A timer asks [#shouldProbe] on each tick; once
///     `openDuration` has elapsed the consumer flips to `HALF_OPEN` and resumes Kafka polling.
///   * `HALF_OPEN` — the next record's outcome decides: success → `CLOSED` (window reset),
///     failure → `OPEN` (timer restarted).
///
/// @param failureThreshold the failure rate (0.0..1.0) at or above which the breaker trips
/// @param windowSize       the rolling sample size used by the host's outcome window
/// @param openDuration     how long the breaker stays in `OPEN` before probing
public record CircuitBreakerController(double failureThreshold, int windowSize, Duration openDuration) {
  /// Canonical constructor; validates `failureThreshold` is in `(0, 1]`, `windowSize` is positive,
  /// and `openDuration` is non-null and positive.
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
  ///
  /// @param totalSamples the number of slots currently filled in the rolling window
  /// @param failureRate  the proportion of failures in the window, in `[0.0, 1.0]`
  /// @return `true` if the breaker should transition CLOSED → OPEN
  public boolean shouldTrip(final long totalSamples, final double failureRate) {
    if (totalSamples < windowSize) return false;
    return failureRate >= failureThreshold;
  }

  /// In `OPEN` state, returns `true` iff `openDuration` has elapsed since the breaker tripped.
  /// `openedAtNanos` is `System.nanoTime()` from the trip; a `0L` sentinel (never tripped) always
  /// returns `false`.
  ///
  /// @param openedAtNanos `System.nanoTime()` captured at the trip, or `0L` if never tripped
  /// @param nowNanos      the current `System.nanoTime()`
  /// @return `true` if `openDuration` has elapsed and the consumer should transition
  ///     OPEN → HALF_OPEN
  public boolean shouldProbe(final long openedAtNanos, final long nowNanos) {
    if (openedAtNanos == 0L) return false;
    return (nowNanos - openedAtNanos) >= openDuration.toNanos();
  }
}
