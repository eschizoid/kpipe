package io.github.eschizoid.kpipe.consumer;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/// Correlates an asynchronous offset commit with its completion.
///
/// A commit is a round trip: [KafkaOffsetManager] enqueues a `CommitOffsets` command for the
/// consumer thread, then blocks until that thread reports back. This owns the UUID →
/// [CompletableFuture] correlation that bridges the two — [#register] mints a correlation id and
/// its pending future, the consumer thread calls [#complete] with that id when the commit lands,
/// and the waiter calls [#forget] once it returns (or times out).
///
/// Thread-safe: the backing map is a [ConcurrentHashMap], and a late [#complete] racing the
/// waiter's [#forget] is harmless (whichever removes the future first wins; the other is a no-op).
final class CommitCoordinator {

  /// A registered commit awaiting completion: the correlation id to attach to the outbound
  /// `CommitOffsets` command, and the future the caller blocks on.
  record Pending(String commitId, CompletableFuture<Boolean> future) {}

  private final Map<String, CompletableFuture<Boolean>> commitFutures = new ConcurrentHashMap<>();

  /// Registers a new commit and returns its correlation id + pending future. Attach the id to the
  /// outbound command so [#complete] can find the future when the commit lands.
  Pending register() {
    final var commitId = UUID.randomUUID().toString();
    final var future = new CompletableFuture<Boolean>();
    commitFutures.put(commitId, future);
    return new Pending(commitId, future);
  }

  /// Completes the pending commit for `commitId`. No-op if unknown — a late completion after the
  /// waiter already timed out and forgot it. Called from the consumer thread.
  void complete(final String commitId, final boolean success) {
    final var future = commitFutures.remove(commitId);
    if (future != null) future.complete(success);
  }

  /// Drops the correlation for `commitId` (idempotent) — called by the waiter in its finally block
  /// so a timed-out commit doesn't leak its future.
  void forget(final String commitId) {
    commitFutures.remove(commitId);
  }

  /// Number of commits registered but not yet completed or forgotten — surfaced as
  /// [OffsetStatistics#pendingCommits].
  int pendingCount() {
    return commitFutures.size();
  }
}
