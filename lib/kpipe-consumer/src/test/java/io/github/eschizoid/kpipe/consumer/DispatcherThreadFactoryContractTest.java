package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
}
