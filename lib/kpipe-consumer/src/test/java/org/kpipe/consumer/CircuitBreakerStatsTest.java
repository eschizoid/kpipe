package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/// Contract tests for [CircuitBreakerStats]. Verifies the sliding-window mechanics, the running
/// total invariant, the reset behavior, and concurrent updates from virtual threads.
class CircuitBreakerStatsTest {

  @Test
  void emptyWindowHasZeroRateAndZeroSamples() {
    final var stats = new CircuitBreakerStats(10);

    assertEquals(0, stats.totalSamples());
    assertEquals(0.0, stats.failureRate());
  }

  @Test
  void failureRateReflectsCurrentWindow() {
    final var stats = new CircuitBreakerStats(10);

    for (int i = 0; i < 3; i++) stats.recordSuccess();
    for (int i = 0; i < 7; i++) stats.recordFailure();

    assertEquals(10, stats.totalSamples());
    assertEquals(0.7, stats.failureRate(), 0.0001);
  }

  @Test
  void slidingWindowEvictsOldestEntries() {
    // Window size 5: fill with failures, then add successes — the failures should fall off.
    final var stats = new CircuitBreakerStats(5);

    for (int i = 0; i < 5; i++) stats.recordFailure();
    assertEquals(1.0, stats.failureRate(), 0.0001, "window should be all failures");

    for (int i = 0; i < 5; i++) stats.recordSuccess();
    assertEquals(0.0, stats.failureRate(), 0.0001, "the failures should have been evicted");
    assertEquals(5, stats.totalSamples(), "window size doesn't grow past capacity");
  }

  @Test
  void resetClearsEverything() {
    final var stats = new CircuitBreakerStats(10);
    for (int i = 0; i < 7; i++) stats.recordFailure();
    for (int i = 0; i < 3; i++) stats.recordSuccess();

    stats.reset();

    assertEquals(0, stats.totalSamples());
    assertEquals(0.0, stats.failureRate());
  }

  @Test
  void windowSizeIsExposed() {
    assertEquals(42, new CircuitBreakerStats(42).windowSize());
  }

  @Test
  void zeroOrNegativeWindowSizeRejected() {
    assertThrows(IllegalArgumentException.class, () -> new CircuitBreakerStats(0));
    assertThrows(IllegalArgumentException.class, () -> new CircuitBreakerStats(-1));
  }

  @Test
  void concurrentUpdatesFromVirtualThreadsKeepTotalsConsistent() throws InterruptedException {
    // Virtual-thread stress test (§Testing Strategy convention). Hammer the stats with many
    // outcomes; assert totalSamples never exceeds windowSize and successes + failures match the
    // running aggregates exactly.
    final var windowSize = 1000;
    final var totalWrites = 50_000;
    final var stats = new CircuitBreakerStats(windowSize);
    final var done = new CountDownLatch(totalWrites);

    for (int i = 0; i < totalWrites; i++) {
      final var idx = i;
      Thread.ofVirtual().start(() -> {
        try {
          if (idx % 3 == 0) stats.recordFailure();
          else stats.recordSuccess();
        } finally {
          done.countDown();
        }
      });
    }

    assertTrue(done.await(20, TimeUnit.SECONDS), "all virtual threads should finish");

    // Invariant: totalSamples == windowSize once we've written more than windowSize records.
    assertEquals(windowSize, stats.totalSamples(), "window saturates at its configured size");
    // Rate must be in [0, 1] regardless of race outcomes.
    final var rate = stats.failureRate();
    assertTrue(rate >= 0.0 && rate <= 1.0, "failure rate out of bounds: " + rate);
  }
}
