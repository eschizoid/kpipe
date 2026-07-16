package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/// Unit tests for [CommitCoordinator] — the async commit-correlation primitive extracted from
/// KafkaOffsetManager. Pins the register → complete happy path, the forget/timeout cleanup, and the
/// late-completion race (a `complete` after `forget` must be a harmless no-op).
class CommitCoordinatorTest {

  @Test
  void registerThenCompleteResolvesTheFuture() throws Exception {
    final var coordinator = new CommitCoordinator();
    final var pending = coordinator.register();
    assertEquals(1, coordinator.pendingCount());

    coordinator.complete(pending.commitId(), true);

    assertTrue(pending.future().get(1, TimeUnit.SECONDS), "future resolves to the completion value");
    assertEquals(0, coordinator.pendingCount(), "complete() removes the correlation");
  }

  @Test
  void completePropagatesFailureValue() throws Exception {
    final var coordinator = new CommitCoordinator();
    final var pending = coordinator.register();
    coordinator.complete(pending.commitId(), false);
    assertFalse(pending.future().get(1, TimeUnit.SECONDS));
  }

  @Test
  void distinctRegistrationsGetDistinctIds() {
    final var coordinator = new CommitCoordinator();
    final var a = coordinator.register();
    final var b = coordinator.register();
    assertNotEquals(a.commitId(), b.commitId());
    assertEquals(2, coordinator.pendingCount());
  }

  @Test
  void forgetDropsPendingWithoutCompleting() {
    final var coordinator = new CommitCoordinator();
    final var pending = coordinator.register();
    coordinator.forget(pending.commitId());
    assertEquals(0, coordinator.pendingCount());
    assertFalse(pending.future().isDone(), "forget must not complete the future — the waiter times out");
  }

  @Test
  void lateCompleteAfterForgetIsHarmlessNoOp() {
    // The waiter times out and forgets; a late notifyCommitComplete then arrives. It must not throw
    // and must not resurrect a pending entry or complete the forgotten future.
    final var coordinator = new CommitCoordinator();
    final var pending = coordinator.register();
    coordinator.forget(pending.commitId());

    assertDoesNotThrow(() -> coordinator.complete(pending.commitId(), true));
    assertEquals(0, coordinator.pendingCount());
    assertFalse(pending.future().isDone(), "a forgotten future stays uncompleted");
  }

  @Test
  void completeUnknownIdIsIgnored() {
    final var coordinator = new CommitCoordinator();
    assertDoesNotThrow(() -> coordinator.complete("never-registered", true));
    assertEquals(0, coordinator.pendingCount());
  }
}
