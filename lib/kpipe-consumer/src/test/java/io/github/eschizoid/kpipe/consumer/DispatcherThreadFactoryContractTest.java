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

  /// The factory returned by validation applies the same checks to every later thread, not only
  /// to the construction probe. A factory that passes once and then misbehaves would otherwise
  /// surface as an opaque failure at dispatch time.
  @Test
  void everyThreadTheFactoryLaterProducesIsValidated() {
    final var calls = new java.util.concurrent.atomic.AtomicInteger();
    final java.util.concurrent.ThreadFactory goodThenBad = r -> {
      if (calls.incrementAndGet() == 1) {
        return Thread.ofPlatform().daemon().unstarted(r);
      }
      return null;
    };

    final var dispatcher = new KeyOrderedDispatcher(2, goodThenBad);
    assertThrows(IllegalStateException.class, () -> dispatcher.dispatch(record(1L), () -> {}, () -> {}));
  }

  private static org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record(final long offset) {
    return new org.apache.kafka.clients.consumer.ConsumerRecord<>(
      "contract-topic",
      0,
      offset,
      "k".getBytes(java.nio.charset.StandardCharsets.UTF_8),
      "v".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    );
  }

  /// The parallel path must roll back and route through the reject handler when the factory
  /// declines a later request, rather than letting the failure escape `dispatch`.
  ///
  /// `dispatch` increments the in-flight count before submitting and catches only
  /// `RejectedExecutionException`. Any other type escapes with the count stranded, and that count
  /// drives the backpressure watermark and the drain wait — so a consumer would pause and never
  /// resume. An earlier revision of the validating wrapper threw `IllegalStateException` here and
  /// did exactly that, with the key-ordered path covered and this one not.
  @Test
  void aParallelFactoryThatDeclinesLaterDoesNotStrandTheInFlightCount() {
    final var rejections = new java.util.concurrent.atomic.AtomicInteger();
    final var calls = new java.util.concurrent.atomic.AtomicInteger();
    final var dispatcher = new ParallelDispatcher(
      (r, e) -> rejections.incrementAndGet(),
      Duration.ofSeconds(1),
      r -> calls.incrementAndGet() == 1 ? Thread.ofPlatform().daemon().unstarted(r) : null
    );

    dispatcher.dispatch(record(1L), () -> {}, () -> {});

    assertEquals(0L, dispatcher.drainableCount(), "the in-flight count was stranded by the refusal");
    assertEquals(1, rejections.get(), "the reject handler should have been invoked");
    dispatcher.close();
  }
}
