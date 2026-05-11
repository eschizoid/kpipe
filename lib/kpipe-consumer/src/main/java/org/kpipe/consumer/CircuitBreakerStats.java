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
/// **Concurrency invariant.** Each `recordX` claims a unique slot via `head.getAndIncrement`, then
/// atomically swaps the slot. Concurrent writes to the same slot (only possible once `head` wraps
/// past `windowSize`) compose correctly because each `getAndSet` returns the slot's previous
/// content and the counter delta is computed from that — the counters always match the actual
/// slot contents at any quiescent point.
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
    // Snapshot both counters once — re-reading `failures` here after `totalSamples()` would
    // race and could produce a ratio > 1.0 when a failure arrived between the two reads.
    final var f = failures.get();
    final var s = successes.get();
    final var total = f + s;
    return total == 0 ? 0.0 : (double) f / total;
  }

  /// Clears every slot and resets both counters to zero. Called only on state transitions, never
  /// on the hot path.
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
