package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/// Unit tests for [CommitCoordinator] — the async commit-correlation primitive extracted from
/// KafkaOffsetManager. Pins the register → complete → await round trip, the await-timeout cleanup,
/// and the late-completion race (a `complete` after the awaiter timed out must be a harmless no-op).
class CommitCoordinatorTest {

  @Test
  void awaitReturnsTheCompletionValueAndClearsPending() throws Exception {
    final var coordinator = new CommitCoordinator();
    final var commitId = coordinator.register();
    assertEquals(1, coordinator.pendingCount());

    // Completed before await, so await returns immediately with the value.
    coordinator.complete(commitId, true);
    assertTrue(coordinator.await(commitId, Duration.ofSeconds(1)));
    assertEquals(0, coordinator.pendingCount(), "await drops the correlation");
  }

  @Test
  void awaitPropagatesAFailureCompletion() throws Exception {
    final var coordinator = new CommitCoordinator();
    final var commitId = coordinator.register();
    coordinator.complete(commitId, false);
    assertFalse(coordinator.await(commitId, Duration.ofSeconds(1)));
  }

  @Test
  void completeUnblocksAConcurrentAwaiter() throws Exception {
    final var coordinator = new CommitCoordinator();
    final var commitId = coordinator.register();
    // Complete from another thread while the main thread blocks in await.
    Thread.ofVirtual().start(() -> {
      try {
        Thread.sleep(50);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      coordinator.complete(commitId, true);
    });
    assertTrue(coordinator.await(commitId, Duration.ofSeconds(2)), "await must observe the completion");
    assertEquals(0, coordinator.pendingCount());
  }

  @Test
  void awaitTimesOutToFalseAndClearsPending() throws Exception {
    final var coordinator = new CommitCoordinator();
    final var commitId = coordinator.register();
    // Never completed → await returns false after the timeout and cleans up.
    assertFalse(coordinator.await(commitId, Duration.ofMillis(50)));
    assertEquals(0, coordinator.pendingCount(), "a timed-out commit must not leak its future");
  }

  @Test
  void distinctRegistrationsGetDistinctIds() {
    final var coordinator = new CommitCoordinator();
    final var a = coordinator.register();
    final var b = coordinator.register();
    assertNotEquals(a, b);
    assertEquals(2, coordinator.pendingCount());
  }

  @Test
  void lateCompleteAfterTimeoutIsHarmlessNoOp() throws Exception {
    // The awaiter times out and drops the correlation; a late notifyCommitComplete then arrives. It
    // must not throw and must not resurrect a pending entry.
    final var coordinator = new CommitCoordinator();
    final var commitId = coordinator.register();
    assertFalse(coordinator.await(commitId, Duration.ofMillis(50)));

    assertDoesNotThrow(() -> coordinator.complete(commitId, true));
    assertEquals(0, coordinator.pendingCount());
  }

  @Test
  void completeUnknownIdIsIgnored() {
    final var coordinator = new CommitCoordinator();
    assertDoesNotThrow(() -> coordinator.complete("never-registered", true));
    assertEquals(0, coordinator.pendingCount());
  }
}
