package org.kpipe.consumer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/// Count-based sliding window of success / failure outcomes used by [CircuitBreakerController].
///
/// Each call to [#recordSuccess()] / [#recordFailure()] writes into the next ring-buffer slot,
/// evicting whatever was there before. Total counts are maintained by decrementing the evicted
/// outcome's counter and incrementing the new one's — so [#failureRate] is `O(1)` regardless of
/// window size.
///
/// **Concurrency.** Multiple virtual threads call `recordX` simultaneously under parallel mode.
/// Each thread claims a unique slot via `AtomicLong.getAndIncrement` on `head`; the per-slot
/// `getAndSet` and counter increments are atomic. Slot collisions only occur when `head` wraps —
/// in that case the colliding writes still leave the slot at the latest value AND the counters
/// consistent with the latest set of slots, because each `getAndSet` returns the slot's previous
/// content and the counter delta is computed from that.
///
/// **Reset.** [#reset()] clears all slots and counters in one pass. Called when transitioning out
/// of HALF_OPEN to start the fresh window cleanly.
final class CircuitBreakerStats {

  private static final long EMPTY = 0L;
  private static final long SUCCESS = 1L;
  private static final long FAILURE = 2L;

  private final AtomicLongArray slots;
  private final AtomicLong head = new AtomicLong(0);
  private final AtomicLong successes = new AtomicLong(0);
  private final AtomicLong failures = new AtomicLong(0);

  CircuitBreakerStats(final int windowSize) {
    if (windowSize <= 0) throw new IllegalArgumentException("windowSize must be positive, got " + windowSize);
    this.slots = new AtomicLongArray(windowSize);
  }

  void recordSuccess() {
    record(SUCCESS);
  }

  void recordFailure() {
    record(FAILURE);
  }

  private void record(final long outcome) {
    final var idx = (int) (head.getAndIncrement() % slots.length());
    final var prev = slots.getAndSet(idx, outcome);
    if (prev == SUCCESS) successes.decrementAndGet();
    else if (prev == FAILURE) failures.decrementAndGet();
    if (outcome == SUCCESS) successes.incrementAndGet();
    else failures.incrementAndGet();
  }

  /// Returns the number of slots currently filled with a success or failure outcome.
  long totalSamples() {
    return successes.get() + failures.get();
  }

  /// Returns the proportion of failures in the current window, in `[0.0, 1.0]`. Returns `0.0` when
  /// the window is empty.
  double failureRate() {
    final var total = totalSamples();
    return total == 0 ? 0.0 : (double) failures.get() / total;
  }

  /// Clears every slot and resets both counters to zero. Cheap — `O(windowSize)` but called only
  /// on state transitions, never on the hot path.
  void reset() {
    for (int i = 0; i < slots.length(); i++) slots.set(i, EMPTY);
    head.set(0);
    successes.set(0);
    failures.set(0);
  }

  /// Returns the configured window size. Useful for the controller's "minimum samples before
  /// evaluating" gate.
  int windowSize() {
    return slots.length();
  }
}
