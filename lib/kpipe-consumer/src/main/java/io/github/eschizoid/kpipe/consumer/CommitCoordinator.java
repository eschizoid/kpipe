package io.github.eschizoid.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/// Correlates an asynchronous offset commit with its completion, owning the whole round trip.
///
/// A commit spans two threads: [KafkaOffsetManager] [#register]s a commit and enqueues a
/// `CommitOffsets` command carrying the returned id, then [#await]s the result; the consumer thread
/// [#complete]s that id when the commit lands. This owns the `UUID → CompletableFuture` correlation
/// so the future never escapes — callers hold only the id.
///
/// Thread-safe: the backing map is a [ConcurrentHashMap]. [#await] is the sole remover of a
/// correlation; [#complete] only resolves the future in place (it may run before OR after the
/// awaiter reaches [#await], and it must not remove the entry the awaiter still needs to look up).
/// A late [#complete] after the awaiter already timed out and removed the entry is a no-op.
final class CommitCoordinator {

  private static final Logger LOGGER = System.getLogger(CommitCoordinator.class.getName());

  private final Map<String, CompletableFuture<Boolean>> commitFutures = new ConcurrentHashMap<>();

  /// Registers a new commit and returns its correlation id. Attach the id to the outbound
  /// `CommitOffsets` command, then block on it via [#await]; the completing thread calls [#complete]
  /// with the same id.
  String register() {
    final var commitId = UUID.randomUUID().toString();
    commitFutures.put(commitId, new CompletableFuture<>());
    return commitId;
  }

  /// Blocks until the commit for `commitId` completes or `timeout` elapses, then drops the
  /// correlation. Returns the commit's success value, or `false` if it timed out or failed. Must be
  /// called by the thread that registered the id.
  ///
  /// @throws InterruptedException if interrupted while waiting (the correlation is still cleaned up)
  boolean await(final String commitId, final Duration timeout) throws InterruptedException {
    final var future = commitFutures.get(commitId);
    try {
      return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final ExecutionException | TimeoutException e) {
      LOGGER.log(Level.WARNING, "Error waiting for offset commit", e);
      return false;
    } finally {
      commitFutures.remove(commitId);
    }
  }

  /// Completes the pending commit for `commitId`, resolving the future in place WITHOUT removing it
  /// — [#await] owns the single removal, and may not yet have looked the future up. No-op if unknown
  /// (a late completion after the awaiter already timed out and removed it). Called from the consumer
  /// thread.
  void complete(final String commitId, final boolean success) {
    final var future = commitFutures.get(commitId);
    if (future != null) future.complete(success);
  }

  /// Number of commits registered but not yet completed or awaited-out — surfaced as
  /// [OffsetStatistics#pendingCommits].
  int pendingCount() {
    return commitFutures.size();
  }
}
