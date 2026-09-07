package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pastalab.fray.junit.plain.FrayInTestLauncher;

/// Permanent guard against a Fray run that reads green without instrumenting anything.
///
/// The concurrency suite is a correctness gate, and two candidate tools have already been
/// caught passing deliberately racy code when they could not instrument it — the worst
/// possible failure mode, because it turns an untested invariant into a green check. Fray
/// carries the same hazard in two places. Its Gradle plugin ships a native JVMTI agent for
/// `linux-x8664`, `windows-x8664` and `macos-aarch64` only; on any other platform it logs a
/// warning, skips the agent, and lets the build proceed. And its JUnit extension answers
/// "Fray is not enabled in this JVM" by marking every `@FrayTest` method **skipped**, which
/// is also green. This class exists so neither can happen quietly.
///
/// **Two independent gates, both required.** Each is blind to a failure the other catches,
/// so a green suite needs both to hold.
///
/// 1. [#frayInstrumentedJdkIsTheRunningJvm()] — the JVM executing these tests must be
///    Fray's jlink-instrumented JDK. Fray stamps `IMPLEMENTOR=Fray` into that image's
///    `release` file, and its own JUnit extension reads exactly that line to decide whether
///    to run or skip a `@FrayTest`. Asserting on it therefore fails the build under
///    precisely the condition that would otherwise skip the suite in silence. The gate
///    reads the running image, so nothing about it is probabilistic.
/// 2. [#frayReportsTheKnownLostUpdate()] — the scheduler must actually find a planted race.
///    A non-atomic increment of a `volatile` field from two threads is a lost update, and
///    Fray treats volatile reads and writes as scheduling points, so a live install parks
///    one thread inside the read-modify-write and reports the violation within a handful of
///    schedules. Nothing reported means the assertion fails.
///
/// **Why this cannot read green on a no-op.** The state being guarded against is "Fray did
/// not instrument", and that state always fails gate 1: with no jlinked image there is no
/// `IMPLEMENTOR=Fray` stamp to find. Gate 2 is deliberately not load-bearing for that case,
/// because a race can in principle be lost by chance on an ordinary JVM and a probe that
/// leans on chance is the thing this suite replaces. Gate 2 covers the narrower case gate 1
/// cannot see: the JDK is Fray's, but the agents are missing or inert, so no schedule is
/// ever controlled. Neither gate can be neutralised by being skipped — both are plain JUnit
/// `@Test` methods, so a disabled Fray cannot quietly disable them the way it disables
/// `@FrayTest`.
@Tag("FrayTest")
class FrayInstrumentationSelfCheckTest {

  /// Marker embedded in the planted violation. Matching on it, rather than on "something was
  /// thrown", keeps an unrelated internal Fray error from being credited as a detection.
  private static final String LOST_UPDATE_MARKER = "kpipe-fray-self-check-lost-update";

  /// The line Fray's jlink step writes into the instrumented image's `release` file, and the
  /// exact string its JUnit extension looks for before agreeing to run a `@FrayTest`.
  private static final String FRAY_IMPLEMENTOR = "IMPLEMENTOR=Fray";

  @Test
  void frayInstrumentedJdkIsTheRunningJvm() throws IOException {
    final var release = Path.of(System.getProperty("java.home"), "release");
    assertTrue(
      Files.isRegularFile(release),
      () -> "No release file at " + release + "; this JVM is not a jlink image, so it cannot be Fray's."
    );
    final var stamped = Files.readAllLines(release)
      .stream()
      .anyMatch(line -> line.contains(FRAY_IMPLEMENTOR));
    assertTrue(
      stamped,
      () ->
        "The JVM running the Fray suite is " +
        System.getProperty("java.home") +
        ", which carries no " +
        FRAY_IMPLEMENTOR +
        " stamp. Fray is not instrumenting, so every @FrayTest in this source set would be " +
        "reported as skipped and the build would read green. The usual cause is an unsupported " +
        "OS/architecture — the agent is published for linux-x8664, windows-x8664 and " +
        "macos-aarch64 only. Run the suite on a supported platform (see docs/adr/running-fray-locally.md)."
    );
  }

  @Test
  void frayReportsTheKnownLostUpdate() {
    final var reported = assertThrows(
      Throwable.class,
      () -> FrayInTestLauncher.INSTANCE.launchFrayTest(FrayInstrumentationSelfCheckTest::race),
      "Fray explored its schedules without reporting the planted lost update. The race is real " +
        "and unguarded, so a working scheduler must find it: scheduling control is not live here."
    );
    assertTrue(
      carriesMarker(reported),
      () ->
        "Fray reported '" +
        reported +
        "' rather than the planted lost update, so the detection cannot be credited to this probe."
    );
  }

  /// Plants a lost update and reports it by throwing. Fray surfaces whatever the test body
  /// throws as the bug it found, so the thrown marker is what the assertion matches on.
  private static void race() {
    final var counter = new RacyCounter();
    final var first = new Thread(counter::increment, "fray-self-check-a");
    final var second = new Thread(counter::increment, "fray-self-check-b");
    first.start();
    second.start();
    join(first);
    join(second);
    final var observed = counter.count();
    if (observed != 2) {
      throw new AssertionError(LOST_UPDATE_MARKER + ": expected 2 increments, observed " + observed);
    }
  }

  private static void join(final Thread thread) {
    try {
      thread.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while joining " + thread.getName(), e);
    }
  }

  /// Walks the cause chain so the marker is still recognised if Fray ever starts wrapping
  /// the throwable it reports.
  private static boolean carriesMarker(final Throwable reported) {
    var current = reported;
    while (current != null) {
      final var message = current.getMessage();
      if (message != null && message.contains(LOST_UPDATE_MARKER)) return true;
      current = current.getCause() == current ? null : current.getCause();
    }
    return false;
  }

  /// Deliberately racy: `count` is `volatile`, so each read and each write is a Fray
  /// scheduling point, while the read-modify-write as a whole is not atomic. Fray can
  /// therefore park one thread between its read and its write and let the other run to
  /// completion, losing an update. The field must be `volatile`: Fray's JUnit configurations
  /// run with memory-op interleaving disabled, under which plain field accesses are not
  /// scheduling points and this race would never be explored.
  private static final class RacyCounter {

    private volatile int count;

    void increment() {
      count = count + 1;
    }

    int count() {
      return count;
    }
  }
}
