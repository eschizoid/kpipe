package io.github.eschizoid.kpipe.consumer;

import static java.lang.System.Logger.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/// Manages Kafka consumer offsets for parallel processing scenarios.
///
/// This class provides safe offset commit management by tracking which offsets are being
/// processed and which have been completed, ensuring that only contiguous completed offsets are
/// committed to Kafka. This is particularly useful when records are processed in parallel or in a
/// non-sequential order.
///
/// Key features include:
///
/// * Thread-safe tracking of offsets across multiple partitions
/// * Contiguous commit strategy that prevents data loss during rebalancing
/// * Explicit offset tracking with `trackOffset` and `markOffsetProcessed`
/// * Support for both synchronous and asynchronous commit operations
/// * Automatic periodic offset commits with configurable intervals
/// * Proper rebalance handling with custom `ConsumerRebalanceListener`
/// * Recovery from consumer failures and partition reassignments
/// * Gap detection to ensure only fully processed offset ranges are committed
/// * Diagnostic methods for monitoring offset state and troubleshooting
/// * Virtual thread execution for non-blocking I/O operations
///
/// Example usage:
///
/// ```java
/// // Create offset manager wired to the consumer builder's commit executor
/// final var offsetManager = KafkaOffsetManager.builder(consumer)
///     .withCommitExecutor(consumerBuilder.getCommitExecutor())
///     .build();
/// offsetManager.start();
///
/// // Track and process records
/// offsetManager.trackOffset(record);
/// // Process the record...
/// offsetManager.markOffsetProcessed(record);
///
/// // Commit offsets explicitly
/// final var success = offsetManager.commitSyncAndWait(Duration.ofSeconds(5));
///
/// // Clean up resources when done
/// offsetManager.close();
/// ```
public class KafkaOffsetManager implements OffsetManager {

  private static final System.Logger LOGGER = System.getLogger(KafkaOffsetManager.class.getName());

  private final Consumer<byte[], byte[]> kafkaConsumer;
  private final AtomicReference<OffsetState> state = new AtomicReference<>(OffsetState.CREATED);

  /// The consumer-thread seam for periodic commits. The manager decides *when* to commit; the
  /// executor runs `commitSync` on the Kafka consumer thread and completes the returned future
  /// with the outcome. The offsets supplier ([OffsetLedger#committableOffsets]) is invoked at
  /// execution time on the consumer thread, so a request that was queued before a rebalance
  /// never commits partitions the rebalance revoked — see [CommitExecutor].
  private final CommitExecutor commitExecutor;

  /// Commits submitted through [#commitExecutor] whose outcome has not yet resolved — surfaced
  /// as [OffsetStatistics#pendingCommits].
  private final AtomicInteger pendingCommits = new AtomicInteger();

  /// Per-partition offset bookkeeping — the pending windows, highest-processed marks, the interned
  /// `TopicPartition` cache, and the single commit-frontier rule they feed. This manager owns
  /// lifecycle, scheduling, commit I/O, and rebalance orchestration; the ledger owns everything
  /// keyed by partition (see [OffsetLedger]).
  private final OffsetLedger ledger = new OffsetLedger();

  private final Duration commitInterval;
  private volatile ScheduledExecutorService scheduler;
  private volatile ScheduledFuture<?> scheduledCommitTask;

  /// Creates a new KafkaOffsetManager instance.
  ///
  /// @param consumer The Kafka consumer to manage offsets for
  /// @return A builder to construct the KafkaOffsetManager
  public static Builder builder(final Consumer<byte[], byte[]> consumer) {
    return new Builder(consumer);
  }

  /// Builder class for KafkaOffsetManager.
  public static class Builder {

    private final Consumer<byte[], byte[]> kafkaConsumer;
    private Duration commitInterval = Duration.ofSeconds(30);
    private CommitExecutor commitExecutor;

    private Builder(final Consumer<byte[], byte[]> consumer) {
      this.kafkaConsumer = Objects.requireNonNull(consumer, "Consumer cannot be null");
    }

    /// Sets the executor that runs commits on the Kafka consumer thread. When pairing with
    /// [KPipeConsumer], obtain it from [KPipeConsumerBuilder#getCommitExecutor()].
    ///
    /// @param commitExecutor The commit executor to use
    /// @return This builder instance
    public Builder withCommitExecutor(final CommitExecutor commitExecutor) {
      this.commitExecutor = Objects.requireNonNull(commitExecutor, "Commit executor cannot be null");
      return this;
    }

    /// Sets the commit interval for periodic offset commits.
    ///
    /// @param interval The duration between commits
    /// @return This builder instance
    public Builder withCommitInterval(final Duration interval) {
      this.commitInterval = Objects.requireNonNull(interval, "Commit interval cannot be null");
      if (interval.isNegative() || interval.isZero()) throw new IllegalArgumentException(
        "Commit interval must be positive, got " + interval
      );
      return this;
    }

    /// Builds the KafkaOffsetManager instance.
    ///
    /// @return A new KafkaOffsetManager instance
    public KafkaOffsetManager build() {
      if (commitExecutor == null) throw new IllegalStateException(
        "withCommitExecutor(...) must be called before build()"
      );
      return new KafkaOffsetManager(this);
    }
  }

  private KafkaOffsetManager(final Builder builder) {
    this.kafkaConsumer = builder.kafkaConsumer;
    this.commitInterval = builder.commitInterval;
    this.commitExecutor = builder.commitExecutor;
  }

  /// Starts the KafkaOffsetManager and begins periodic offset commits. This method is idempotent -
  /// calling it multiple times has no effect if the manager is already started.
  ///
  /// @return this instance for method chaining
  /// @throws IllegalStateException if the KafkaOffsetManager is already closed
  @Override
  public KafkaOffsetManager start() {
    if (state.get() == OffsetState.STOPPED) throw new IllegalStateException(
      "Cannot restart a stopped KafkaOffsetManager"
    );

    if (state.compareAndSet(OffsetState.CREATED, OffsetState.RUNNING)) {
      scheduler = Executors.newSingleThreadScheduledExecutor(r ->
        Thread.ofPlatform().daemon().name("offset-commit-scheduler").unstarted(r)
      );

      scheduledCommitTask = scheduler.scheduleAtFixedRate(
        this::commitSafeOffsets,
        commitInterval.toMillis(),
        commitInterval.toMillis(),
        TimeUnit.MILLISECONDS
      );

      LOGGER.log(Level.INFO, "KafkaOffsetManager started with commit interval of %s".formatted(commitInterval));
    }

    return this;
  }

  /// Tracks an offset that is about to be processed using a ConsumerRecord.
  ///
  /// This method extracts the topic, partition, and offset from the consumer record and adds the
  /// offset+1 to the pending offsets. In Kafka's offset model, committing offset N means you've
  /// processed through offset N-1 and expect to receive N next.
  ///
  /// When using this method with [#markOffsetProcessed(ConsumerRecord)], the offset
  /// transformation is handled automatically. This method initializes the next offset to commit
  /// using the raw record offset, which is appropriate for the first record in a partition.
  ///
  /// @param record The consumer record to track
  @Override
  public void trackOffset(final ConsumerRecord<byte[], byte[]> record) {
    if (state.get() == OffsetState.STOPPED) return;
    ledger.track(record.topic(), record.partition(), record.offset());
  }

  /// Marks an offset as successfully processed using a ConsumerRecord.
  ///
  /// This method extracts the topic, partition, and offset from the consumer record, increments
  /// the offset by 1 to match Kafka's "next offset" semantics.
  ///
  /// The +1 adjustment ensures that when this record's offset is committed, Kafka will begin
  /// delivering messages from the next offset after this one.
  ///
  /// @param record The consumer record that was processed
  @Override
  public void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record) {
    if (state.get() == OffsetState.STOPPED) return;
    ledger.markProcessed(record.topic(), record.partition(), record.offset());
  }

  /// Commits offsets that are safe to commit based on the current processing state.
  ///
  /// This method is called periodically to ensure that offsets are committed in a timely manner
  /// without losing any unprocessed messages.
  public void commitSafeOffsets() {
    try {
      commitSyncAndWait(Duration.ofSeconds(60));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Interrupted while committing offsets", e);
    }
  }

  /// Stops the periodic offset commit task but doesn't close resources. This can be used to
  /// temporarily pause offset management.
  ///
  /// @return this instance for method chaining
  @Override
  public KafkaOffsetManager stop() {
    if (!state.compareAndSet(OffsetState.RUNNING, OffsetState.STOPPING)) return this; // Not running, nothing to stop

    try {
      stopScheduler();
    } finally {
      state.set(OffsetState.STOPPED);
      LOGGER.log(Level.INFO, "KafkaOffsetManager stopped");
    }

    return this;
  }

  //  Stops the scheduler if it's running.
  private void stopScheduler() {
    if (scheduledCommitTask != null) {
      scheduledCommitTask.cancel(false);
      scheduledCommitTask = null;
    }

    if (scheduler != null) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) scheduler.shutdownNow();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        scheduler.shutdownNow();
      }
      scheduler = null;
    }
  }

  /// Commits offsets synchronously and waits up to `timeout` for the commit to complete.
  ///
  /// On failure or timeout this returns `false` and leaves the ledger untouched — nothing is
  /// discarded, so the next interval (or a later explicit call) simply retries with the then-
  /// current frontier.
  ///
  /// @param timeout Maximum time to wait for the commit
  /// @return true if the commit was successful
  /// @throws InterruptedException if the thread is interrupted while waiting
  public boolean commitSyncAndWait(final Duration timeout) throws InterruptedException {
    if (state.get() == OffsetState.STOPPED) return true;
    if (ledger.committableOffsets().isEmpty()) return true;

    return performCommit(timeout);
  }

  /// Submits a commit through the [CommitExecutor] and waits for its outcome. The executor
  /// invokes `ledger::committableOffsets` on the consumer thread at execution time, so the
  /// committed frontier is always current (and never includes partitions revoked between
  /// submission and execution).
  private boolean performCommit(final Duration timeout) throws InterruptedException {
    pendingCommits.incrementAndGet();
    try {
      final var outcome = commitExecutor.commit(ledger::committableOffsets);
      return outcome.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final ExecutionException | TimeoutException e) {
      LOGGER.log(Level.WARNING, "Error waiting for offset commit", e);
      return false;
    } finally {
      pendingCommits.decrementAndGet();
    }
  }

  /// Returns a typed snapshot of the current processing state for a partition.
  ///
  /// @param partition The partition to get state for
  /// @return the partition's offset-tracking state
  /// **Test observation point.** No production caller — jcstress and property suites assert on
  /// the commit frontier through this window. Not intended for operational dashboards; wire
  /// [KPipeConsumer#health()] or `getStatistics()` for those.
  public PartitionState getPartitionState(final TopicPartition partition) {
    return ledger.partitionState(partition, state.get());
  }

  /// Returns typed overall statistics about all partitions being managed.
  ///
  /// @return statistics across all partitions
  @Override
  public OffsetStatistics getStatistics() {
    return ledger.statistics(state.get(), pendingCommits.get());
  }

  /// Gets the current state of the KafkaOffsetManager.
  ///
  /// @return The current state
  @Override
  public OffsetState getState() {
    return state.get();
  }

  /// Checks if the KafkaOffsetManager is running.
  ///
  /// @return true if running, false otherwise
  @Override
  public boolean isRunning() {
    return state.get() == OffsetState.RUNNING;
  }

  @Override
  public void close() {
    if (
      !state.compareAndSet(OffsetState.RUNNING, OffsetState.STOPPING) &&
      !state.compareAndSet(OffsetState.CREATED, OffsetState.STOPPING)
    ) return;

    try {
      stopScheduler();
      try {
        final var offsetsToCommit = ledger.committableOffsets();
        if (!offsetsToCommit.isEmpty()) kafkaConsumer.commitSync(offsetsToCommit, Duration.ofSeconds(5));
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error during final offset commit", e);
      }
    } finally {
      cleanup();
      state.set(OffsetState.STOPPED);
    }
  }

  /// Creates a rebalance listener that commits per-partition offsets on revoke and clears
  /// partition state on assign/revoke. The listener is an inline anonymous class that closes
  /// over the manager's own state — no separate type, no parallel constructor.
  ///
  /// A commit request already queued behind a [CommitExecutor] when the revoke fires needs no
  /// pruning here: its offsets supplier reads this ledger on the consumer thread at execution
  /// time, after this callback has already cleared the revoked partitions, so revoked-partition
  /// entries are structurally invisible to it.
  @Override
  public ConsumerRebalanceListener createRebalanceListener() {
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        if (state.get() == OffsetState.STOPPED) return;
        LOGGER.log(Level.INFO, "Partitions revoked: %s".formatted(partitions));

        final var offsetsToCommit = new HashMap<TopicPartition, OffsetAndMetadata>();
        partitions.forEach(partition ->
          ledger.frontier(partition).ifPresent(offset -> offsetsToCommit.put(partition, new OffsetAndMetadata(offset)))
        );
        ledger.removePartitions(partitions);

        if (!offsetsToCommit.isEmpty()) {
          try {
            kafkaConsumer.commitSync(offsetsToCommit);
            LOGGER.log(Level.INFO, "Committed offsets for revoked partitions: %s".formatted(offsetsToCommit));
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Failed to commit offsets during rebalance", e);
          }
        }
      }

      @Override
      public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        if (state.get() == OffsetState.STOPPED) return;
        LOGGER.log(Level.INFO, "Partitions assigned: %s".formatted(partitions));
        ledger.removePartitions(partitions);
      }

      @Override
      public void onPartitionsLost(final Collection<TopicPartition> partitions) {
        onPartitionsRevoked(partitions);
      }
    };
  }

  /// Cleans up resources.
  private void cleanup() {
    ledger.clear();
  }
}
