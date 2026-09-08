package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/// Fails this module's Fray suite when Fray is not actually instrumenting it.
///
/// The plugin rewires a `Test` task to run on the jlink-instrumented Fray JDK. On an unsupported
/// OS/architecture it skips that quietly and leaves a plain `Test` run, and every `@FrayTest` in
/// the source set is then reported as *skipped* — a green build that verified nothing. Because
/// the plugin configures each task independently, one module instrumenting correctly says nothing
/// about another, so every module carrying Fray tests carries this guard.
///
/// This is a plain `@Test` on purpose: a `@FrayTest` would itself be skipped in exactly the state
/// it is supposed to detect. It checks the running JVM rather than a log line, so it cannot pass
/// on a stale or optimistic message.
///
/// The full behavioural check — plant a known race and require Fray to report it — lives once, in
/// kpipe-consumer. That one proves Fray detects races at all; this one proves this module's task
/// is running on the instrumented JVM.
class FrayInstrumentationGuardTest {

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
}
