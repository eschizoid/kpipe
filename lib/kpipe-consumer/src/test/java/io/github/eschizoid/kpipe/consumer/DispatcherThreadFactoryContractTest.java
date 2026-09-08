package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/// Pins the daemon-thread contract on the dispatchers' package-private thread-factory seams.
///
/// Both dispatchers interrupt workers that outlast the drain wait, but a task that ignores
/// interruption or is CPU-bound survives that, and only daemon status lets the JVM exit with one
/// still running. A non-daemon factory therefore turns a shutdown into a hang rather than an
/// error, which is the failure mode worth catching at construction — the constructor is the only
/// place able to see the factory at all.
class DispatcherThreadFactoryContractTest {

  @Test
  void keyOrderedRejectsANonDaemonFactory() {
    final var thrown = assertThrows(
      IllegalArgumentException.class,
      () -> new KeyOrderedDispatcher(2, Thread.ofPlatform().factory())
    );
    assertTrue(thrown.getMessage().contains("daemon"), "the message should name the violated contract");
  }

  @Test
  void parallelRejectsANonDaemonFactory() {
    final var thrown = assertThrows(
      IllegalArgumentException.class,
      () -> new ParallelDispatcher((_, _) -> {}, Duration.ofSeconds(1), Thread.ofPlatform().factory())
    );
    assertTrue(thrown.getMessage().contains("daemon"), "the message should name the violated contract");
  }

  @Test
  void daemonFactoriesAndTheProductionDefaultsAreAccepted() {
    assertDoesNotThrow(() -> new KeyOrderedDispatcher(2, Thread.ofPlatform().daemon().factory()).close());
    assertDoesNotThrow(
      () -> new ParallelDispatcher((_, _) -> {}, Duration.ofSeconds(1), Thread.ofPlatform().daemon().factory()).close()
    );
    // The production constructors pass a virtual-thread factory, and virtual threads are always
    // daemon — so the enforcement must not reject the path every consumer actually takes.
    assertDoesNotThrow(() -> new KeyOrderedDispatcher(2).close());
    assertDoesNotThrow(() -> new ParallelDispatcher((_, _) -> {}, Duration.ofSeconds(1)).close());
  }

  /// The probe hands the factory a runnable it must be able to wrap, and asks for exactly one
  /// thread. A factory that runs the runnable immediately is still a legal `ThreadFactory`, and
  /// using one here executes the probe's no-op body — which the production path deliberately
  /// never reaches, because it creates the probe thread and leaves it unstarted.
  ///
  /// The assertion counts threads requested rather than bodies run: the no-op has no observable
  /// effect, so a caller cannot detect it executing. What it does pin is that neither probe
  /// allocates more than the one thread it needs to read `isDaemon()`.
  @Test
  void theProbeSuppliesAUsableRunnable() {
    final var threadsRequested = new java.util.concurrent.atomic.AtomicInteger();
    final java.util.concurrent.ThreadFactory runsImmediately = r -> {
      r.run();
      threadsRequested.incrementAndGet();
      return Thread.ofPlatform().daemon().unstarted(r);
    };

    assertDoesNotThrow(() -> KeyOrderedDispatcher.requireDaemonFactory(runsImmediately));
    assertDoesNotThrow(() -> ParallelDispatcher.requireDaemonFactory(runsImmediately));

    assertEquals(
      2,
      threadsRequested.get(),
      "each probe should ask the factory for exactly one thread"
    );
  }

  /// `ThreadFactory.newThread` is specified to return null when it declines to create a thread,
  /// so the probe must report that as a contract violation rather than dereference it. Without
  /// the guard the caller gets an opaque NullPointerException from inside a constructor.
  @Test
  void aFactoryThatRefusesToCreateAThreadIsRejectedClearly() {
    final java.util.concurrent.ThreadFactory refuses = r -> null;

    final var fromKeyOrdered = assertThrows(
      IllegalArgumentException.class,
      () -> new KeyOrderedDispatcher(2, refuses)
    );
    assertTrue(fromKeyOrdered.getMessage().contains("refused"), "the message should say the factory refused");

    final var fromParallel = assertThrows(
      IllegalArgumentException.class,
      () -> new ParallelDispatcher((_, _) -> {}, Duration.ofSeconds(1), refuses)
    );
    assertTrue(fromParallel.getMessage().contains("refused"), "the message should say the factory refused");
  }

  /// A null factory is a caller mistake worth naming, not an NPE thrown from inside a
  /// constructor several frames below where the argument was passed.
  @Test
  void aNullFactoryIsRejectedClearly() {
    final var fromKeyOrdered = assertThrows(
      IllegalArgumentException.class,
      () -> new KeyOrderedDispatcher(2, null)
    );
    assertTrue(fromKeyOrdered.getMessage().contains("null"), "the message should name the null argument");

    final var fromParallel = assertThrows(
      IllegalArgumentException.class,
      () -> new ParallelDispatcher((_, _) -> {}, Duration.ofSeconds(1), null)
    );
    assertTrue(fromParallel.getMessage().contains("null"), "the message should name the null argument");
  }

  /// `ThreadFactory` is not required to hand back an unstarted thread, and a probe that runs work
  /// would both have side effects and leave the dispatcher not owning the thread's lifecycle.
  @Test
  void aFactoryReturningAStartedThreadIsRejected() {
    final java.util.concurrent.ThreadFactory startsIt = r -> {
      final var t = Thread.ofPlatform().daemon().unstarted(r);
      t.start();
      return t;
    };

    assertThrows(IllegalArgumentException.class, () -> new KeyOrderedDispatcher(2, startsIt));
    assertThrows(
      IllegalArgumentException.class,
      () -> new ParallelDispatcher((_, _) -> {}, Duration.ofSeconds(1), startsIt)
    );
  }
}
