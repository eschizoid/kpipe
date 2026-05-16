package org.kpipe.consumer;

/// The three states a [CircuitBreakerController] can be in.
public enum CircuitBreakerState {
  /// Normal operation. Outcomes feed the rolling window; on the first failure that crosses the
  /// controller's threshold the breaker transitions to [#OPEN].
  CLOSED,

  /// Failures crossed the threshold. The consumer is paused; no records are processed until a
  /// scheduled timer flips the state to [#HALF_OPEN].
  OPEN,

  /// The breaker is probing. The consumer is resumed and the next record's outcome decides:
  /// success returns to [#CLOSED] (window reset), failure returns to [#OPEN] (timer restarted).
  HALF_OPEN,
}
