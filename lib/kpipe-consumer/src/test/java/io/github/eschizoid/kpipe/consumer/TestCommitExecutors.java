package io.github.eschizoid.kpipe.consumer;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/// Test-side [CommitExecutor] factories.
///
/// Production wiring goes through [KPipeConsumerBuilder#getCommitExecutor()]; these mirror the
/// two shapes tests need: an executor bound to an inspectable queue (the test pumps it like the
/// consumer thread would), and an unwired executor for managers whose commit path is never
/// exercised.
final class TestCommitExecutors {

  private TestCommitExecutors() {}

  /// Enqueues each request as a [ConsumerCommand.CommitOffsets] on `queue` — the exact shape
  /// [KPipeConsumerBuilder#getCommitExecutor()] produces — so a test can capture, inspect, and
  /// complete requests as if it were the consumer thread.
  static CommitExecutor toQueue(final Queue<ConsumerCommand> queue) {
    return offsets -> {
      final var outcome = new CompletableFuture<Boolean>();
      queue.offer(new ConsumerCommand.CommitOffsets(offsets, outcome));
      return outcome;
    };
  }

  /// Accepts every request but never completes it — for tests that construct a
  /// [KafkaOffsetManager] without ever driving its commit path. Matches the legacy behavior of
  /// an undrained command queue (an awaiting committer times out and reports `false`).
  static CommitExecutor unwired() {
    return offsets -> new CompletableFuture<>();
  }
}
