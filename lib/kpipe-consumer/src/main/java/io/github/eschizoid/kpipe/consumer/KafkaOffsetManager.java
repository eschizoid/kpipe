package io.github.eschizoid.kpipe.consumer;

import static java.lang.System.Logger.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
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
/// // Create offset manager with default settings
/// final var offsetManager = KafkaOffsetManager.builder(consumer).build();
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

  /// Shared command queue between `KPipeConsumer` and this manager.
  ///
  /// **Single-writer invariant.** All writes to this queue happen from the Kafka consumer thread
  /// — `KPipeConsumer.consumerLoop` enqueues `Pause` / `Resume` / `Close` commands, and
  /// `performCommit` enqueues `CommitOffsets`. The rebalance callback `onPartitionsRevoked`
  /// (which calls `drainCommandQueueForRevokedPartitions`) is invoked by `Consumer.poll(...)`
  /// on that same consumer thread, so the poll-then-readd drain in
  /// `drainCommandQueueForRevokedPartitions` is atomic with respect to any writer.
  ///
  /// If a future change introduces an off-thread `CommitOffsets` writer (async commit hook, an
  /// ad-hoc commit from another module, etc.), the drain becomes racy: a new command landing
  /// between the last `poll()` returning `null` and `addAll(remaining)` completing would jump
  /// the queue and bypass revoked-partition filtering — the consumer thread would then commit
  /// against the new owner of a revoked partition. To enforce the invariant we capture the
  /// consumer thread on the first listener callback and assert subsequent callbacks run on the
  /// same thread (see `assertOnConsumerThread`).
  private final Queue<ConsumerCommand> commandQueue;

  private final CommitCoordinator commitCoordinator = new CommitCoordinator();

  /// Per-partition offset bookkeeping — the pending windows, highest-processed marks, the interned
  /// `TopicPartition` cache, and the single commit-frontier rule they feed. This manager owns
  /// lifecycle, scheduling, commit I/O, and rebalance orchestration; the ledger owns everything
  /// keyed by partition (see [OffsetLedger]).
  private final OffsetLedger ledger = new OffsetLedger();

  private final Duration commitInterval;
  private volatile ScheduledExecutorService scheduler;
  private volatile ScheduledFuture<?> scheduledCommitTask;

  /// Captured on the first rebalance callback to enforce the single-writer invariant documented
  /// on `commandQueue`. `volatile` for visibility across the consumer thread and any test
  /// thread that violates the invariant. Only ever written once: from `null` to the first
  /// thread that runs `onPartitionsRevoked` / `onPartitionsAssigned`.
  private volatile Thread consumerThread;

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
    private Queue<ConsumerCommand> commandQueue;

    private Builder(final Consumer<byte[], byte[]> consumer) {
      this.kafkaConsumer = Objects.requireNonNull(consumer, "Consumer cannot be null");
    }

    /// Shared command queue for the KPipeConsumer and KafkaOffsetManager.
    ///
    /// @param commandQueue The command queue to use
    /// @return This builder instance
    public Builder withCommandQueue(final Queue<ConsumerCommand> commandQueue) {
      this.commandQueue = Objects.requireNonNull(commandQueue, "Command queue cannot be null");
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
      if (commandQueue == null) throw new IllegalStateException("withCommandQueue(...) must be called before build()");
      return new KafkaOffsetManager(this);
    }
  }

  private KafkaOffsetManager(final Builder builder) {
    this.kafkaConsumer = builder.kafkaConsumer;
    this.commitInterval = builder.commitInterval;
    this.commandQueue = builder.commandQueue;
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

  /// Notifies the KafkaOffsetManager that a commit operation has completed.
  ///
  /// @param commitId The ID of the commit operation
  /// @param success True if the commit was successful, false otherwise
  @Override
  public void notifyCommitComplete(final String commitId, final boolean success) {
    commitCoordinator.complete(commitId, success);
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
  /// @param timeout Maximum time to wait for the commit
  /// @return true if the commit was successful
  /// @throws InterruptedException if the thread is interrupted while waiting
  public boolean commitSyncAndWait(final Duration timeout) throws InterruptedException {
    if (state.get() == OffsetState.STOPPED) return true;

    final var offsetsToCommit = ledger.committableOffsets();
    if (offsetsToCommit.isEmpty()) return true;

    return performCommit(offsetsToCommit, timeout);
  }

  private boolean performCommit(final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit, final Duration timeout)
    throws InterruptedException {
    if (offsetsToCommit.isEmpty()) return true;

    final var commitId = commitCoordinator.register();
    commandQueue.offer(new ConsumerCommand.CommitOffsets(offsetsToCommit, commitId));
    return commitCoordinator.await(commitId, timeout);
  }

  /// Returns a typed snapshot of the current processing state for a partition.
  ///
  /// @param partition The partition to get state for
  /// @return the partition's offset-tracking state
  public PartitionState getPartitionState(final TopicPartition partition) {
    return ledger.partitionState(partition, state.get());
  }

  /// Returns typed overall statistics about all partitions being managed.
  ///
  /// @return statistics across all partitions
  @Override
  public OffsetStatistics getStatistics() {
    return ledger.statistics(state.get(), commitCoordinator.pendingCount());
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

  /// Creates a rebalance listener that commits per-partition offsets on revoke, clears partition
  /// state on assign/revoke, and prunes the command queue so commands targeting revoked
  /// partitions don't fire against a stale assignment. The listener is an inline anonymous class
  /// that closes over the manager's own state — no separate type, no parallel constructor.
  @Override
  public ConsumerRebalanceListener createRebalanceListener() {
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        assertOnConsumerThread();
        if (state.get() == OffsetState.STOPPED) return;
        LOGGER.log(Level.INFO, "Partitions revoked: %s".formatted(partitions));

        drainCommandQueueForRevokedPartitions(partitions);

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
        assertOnConsumerThread();
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

  /// Captures the consumer thread on the first invocation and asserts subsequent invocations
  /// run on the same thread. Enforces the single-writer invariant on `commandQueue` for
  /// `drainCommandQueueForRevokedPartitions` — see the field Javadoc. `assert` so the check
  /// costs nothing in production (no `-ea`); tests run with assertions enabled and will fail
  /// loudly if a future change starts firing the listener off-thread.
  ///
  /// The first-touch capture is intentional: there is no clean seam for `KPipeConsumer` to
  /// hand us its thread before `Consumer.poll(...)` first dispatches the listener, so we treat
  /// "whoever ran first" as the source of truth and pin every subsequent caller to that thread.
  private void assertOnConsumerThread() {
    final var captured = consumerThread;
    if (captured == null) {
      consumerThread = Thread.currentThread();
      return;
    }
    assert captured ==
    Thread.currentThread() : "rebalance callback ran on %s but was previously bound to %s — commandQueue single-writer invariant violated".formatted(
      Thread.currentThread(),
      captured
    );
  }

  /// Filters offset-commit commands so any references to revoked partitions are dropped before
  /// the partitions reappear under a new owner. Non-commit commands pass through untouched.
  /// Drains the queue by polling, transforms each command, and re-enqueues whatever remains in
  /// order.
  ///
  /// **Single-writer invariant required.** The poll-then-readd sequence is *not* atomic with
  /// respect to a concurrent `commandQueue.offer(...)`. It is safe today because every writer
  /// is the Kafka consumer thread, and `Consumer.poll(...)` dispatches `onPartitionsRevoked`
  /// on that same thread — see the `commandQueue` field Javadoc. Introducing an off-thread
  /// `CommitOffsets` writer would let a new command land between the loop's last `poll()`
  /// returning `null` and `addAll(remaining)` completing, bypassing revoked-partition
  /// filtering. The runtime check sits in `assertOnConsumerThread`.
  private void drainCommandQueueForRevokedPartitions(final Collection<TopicPartition> partitions) {
    if (commandQueue == null || commandQueue.isEmpty()) return;

    LOGGER.log(Level.INFO, "Draining command queue for revoked partitions");

    final var remainingCommands = new ArrayList<ConsumerCommand>();
    for (var cmd = commandQueue.poll(); cmd != null; cmd = commandQueue.poll()) {
      switch (cmd) {
        case ConsumerCommand.CommitOffsets commitCmd when !commitCmd.offsets().isEmpty() -> {
          final var filteredOffsets = commitCmd
            .offsets()
            .entrySet()
            .stream()
            .filter(entry -> !partitions.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
          if (!filteredOffsets.isEmpty()) remainingCommands.add(commitCmd.withOffsets(filteredOffsets));
        }
        default -> remainingCommands.add(cmd);
      }
    }

    commandQueue.addAll(remainingCommands);
    LOGGER.log(Level.INFO, "Command queue drained, %s commands remaining".formatted(remainingCommands.size()));
  }

  /// Cleans up resources.
  private void cleanup() {
    ledger.clear();
  }
}
