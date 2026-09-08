package io.github.eschizoid.kpipe.consumer;

/// Shared plumbing for the Fray ports: run a set of actions on their own threads and wait for
/// every one to finish.
///
/// Joining matters more here than it does under jcstress, which owns actor lifecycles itself.
/// Fray does not finish an iteration while any thread it started is still live, so a port that
/// leaks a thread wedges exploration on its first schedule rather than failing. Everything the
/// scenarios drive must therefore be joined, and anything holding a scheduler must not be
/// started at all.
final class FrayScenarios {

  private FrayScenarios() {}

  /// Runs each action on its own platform thread and joins them all before returning.
  ///
  /// @param actions the concurrent actions making up one schedule
  static void runConcurrently(final Runnable... actions) {
    final var threads = new Thread[actions.length];
    for (var i = 0; i < actions.length; i++) {
      threads[i] = new Thread(actions[i], "fray-actor-" + i);
    }
    for (final var t : threads) t.start();
    for (final var t : threads) join(t);
  }

  /// Unbounded on purpose. Under a controlled scheduler a real-time deadline measures the
  /// exploration order rather than the code's liveness; Fray reports a schedule that genuinely
  /// cannot finish as a deadlock.
  ///
  /// @param thread the thread to wait for
  private static void join(final Thread thread) {
    try {
      thread.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted joining " + thread.getName(), e);
    }
  }
}
