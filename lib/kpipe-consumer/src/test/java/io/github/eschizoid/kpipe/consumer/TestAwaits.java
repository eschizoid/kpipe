package io.github.eschizoid.kpipe.consumer;

import java.time.Duration;
import java.util.function.BooleanSupplier;

/// Poll-until-deadline helpers for flake-prone consumer tests.
///
/// Use these in place of bare `Thread.sleep(N)` followed by an immediate assertion. On a loaded
/// CI runner a fixed sleep budget can be exceeded by GC pauses or virtual-thread scheduling
/// jitter, and the follow-up assertion will then race against the wall clock instead of the
/// actual condition.
///
/// Typical shape:
/// ```java
/// pollUntil(() -> metrics.get("messagesProcessed") == 1L, Duration.ofSeconds(2),
///   "messagesProcessed reaches 1");
/// assertEquals(1L, metrics.get("messagesProcessed"));
/// ```
///
/// The trailing `assertEquals` is the fence: the poll proves the condition has been observed at
/// least once before the deadline; the assertion ratifies the final state.
final class TestAwaits {

  private static final Duration POLL_INTERVAL = Duration.ofMillis(20);

  private TestAwaits() {}

  /// Polls `cond` until it returns true or `timeout` elapses, sleeping briefly between checks.
  ///
  /// Throws [AssertionError] on timeout. Silently passing would let downstream assertions run
  /// against a partial state.
  ///
  /// @param cond predicate observed each poll iteration
  /// @param timeout deadline measured from the call site
  /// @param desc human-readable description of the awaited condition for the timeout message
  /// @throws InterruptedException if the polling thread is interrupted
  static void pollUntil(final BooleanSupplier cond, final Duration timeout, final String desc)
    throws InterruptedException {
    final var deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() >= deadlineNanos) {
        throw new AssertionError("Timed out after " + timeout.toMillis() + "ms waiting for: " + desc);
      }
      Thread.sleep(POLL_INTERVAL.toMillis());
    }
  }
}
