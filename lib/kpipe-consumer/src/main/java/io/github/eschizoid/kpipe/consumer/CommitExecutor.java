package io.github.eschizoid.kpipe.consumer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/// Executes an offset commit on the Kafka consumer thread.
///
/// This is the narrow seam between an offset manager (which decides *when* to commit and *what*
/// the commit frontier is) and the consumer loop (which owns the only thread allowed to touch the
/// underlying `KafkaConsumer`). The manager submits a commit request from any thread; the consumer
/// thread executes it and completes the returned future with the `commitSync` outcome.
///
/// The `offsets` supplier is invoked **on the consumer thread, immediately before `commitSync`**,
/// not at submission time. That late binding is load-bearing for rebalance safety: a rebalance
/// callback (which also runs on the consumer thread, inside `poll`) clears revoked partitions from
/// the manager's ledger before any queued commit request executes, so a request submitted before
/// the revoke can never commit a revoked partition — the supplier simply no longer sees it. It
/// also means the executed commit reflects the freshest frontier, never a stale snapshot.
///
/// Contract:
///
/// * The supplier is called at most once per request, on the consumer thread.
/// * An empty map from the supplier completes the future with `true` without calling
///   `commitSync` (nothing to commit is a success, and `commitSync(emptyMap)` is skipped).
/// * The future completes `true` on commit success, `false` on failure (the executor logs the
///   failure at WARNING). It is never completed exceptionally by the built-in implementation.
/// * A request submitted while the consumer is shutting down may never execute; callers must
///   bound their wait (see `KafkaOffsetManager.commitSyncAndWait`).
///
/// The production implementation is [KPipeConsumerBuilder#getCommitExecutor()], which enqueues
/// onto the consumer's command queue; `KPipeConsumer.processCommands()` drains it on the consumer
/// thread.
@FunctionalInterface
public interface CommitExecutor {
  /// Submits a commit request whose offsets are computed on the consumer thread at execution
  /// time.
  ///
  /// @param offsets supplies the offsets to commit, invoked on the consumer thread just before
  ///     `commitSync`
  /// @return a future completing `true` if the commit succeeded (or there was nothing to
  ///     commit), `false` if it failed
  CompletableFuture<Boolean> commit(final Supplier<Map<TopicPartition, OffsetAndMetadata>> offsets);
}
