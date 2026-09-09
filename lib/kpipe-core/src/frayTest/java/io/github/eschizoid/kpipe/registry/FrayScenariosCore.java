package io.github.eschizoid.kpipe.registry;

/// Runs actions on their own threads and joins them.
///
/// Fray does not finish an iteration while any thread it started is still live, so joining is a
/// correctness requirement rather than tidiness — a leaked thread wedges exploration instead of
/// failing.
public final class FrayScenariosCore {

  private FrayScenariosCore() {}

  /// Runs each action on its own thread and joins them all before returning.
  ///
  /// @param actions the concurrent actions making up one schedule
  public static void runConcurrently(final Runnable... actions) {
    final var threads = new Thread[actions.length];
    for (var i = 0; i < actions.length; i++) {
      threads[i] = new Thread(actions[i], "fray-actor-" + i);
    }
    for (final var t : threads) t.start();
    for (final var t : threads) {
      try {
        t.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted joining " + t.getName(), e);
      }
    }
  }
}
