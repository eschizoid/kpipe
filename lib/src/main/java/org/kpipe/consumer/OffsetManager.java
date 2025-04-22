package org.kpipe.consumer;

import static java.util.logging.Level.WARNING;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kpipe.consumer.enums.ConsumerCommand;
import org.kpipe.consumer.enums.OffsetState;

/**
 * Manages Kafka consumer offsets for parallel processing scenarios.
 *
 * <p>This class provides safe offset commit management by tracking which offsets are being
 * processed and which have completed processing. It ensures that only offsets that have been fully
 * processed are committed, even when processing happens in a non-sequential order.
 *
 * <p>Key features include:
 *
 * <ul>
 *   <li>Safe tracking of offsets being processed in parallel
 *   <li>Support for non-contiguous offset processing
 *   <li>Thread-safe offset management
 *   <li>Proper offset commit logic to avoid data loss
 *   <li>Automatic cleanup of processed offsets to prevent memory leaks
 *   <li>Periodic offset commits with configurable interval
 *   <li>Efficient execution using virtual threads for I/O operations
 *   <li>Diagnostic utilities for monitoring and troubleshooting
 * </ul>
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class OffsetManager<K, V> implements AutoCloseable {

  private static final Logger LOGGER = Logger.getLogger(OffsetManager.class.getName());
  private static final long DEFAULT_COMMAND_TIMEOUT_MS = 5000;

  private final Consumer<K, V> kafkaConsumer;
  private final ExecutorService executorService;
  private final AtomicReference<OffsetState> state = new AtomicReference<>(OffsetState.CREATED);
  private final Queue<ConsumerCommand> commandQueue;
  private final Map<String, CompletableFuture<Boolean>> commitFutures = new ConcurrentHashMap<>();

  // Tracking data structures
  private final Map<TopicPartition, ConcurrentSkipListSet<Long>> pendingOffsets = new ConcurrentHashMap<>();
  private final Map<TopicPartition, ConcurrentSkipListSet<Long>> processedOffsets = new ConcurrentHashMap<>();
  private final Map<TopicPartition, Long> nextOffsetsToCommit = new ConcurrentHashMap<>();

  // Fields for periodic commits
  private final Duration commitInterval;
  private ScheduledExecutorService scheduler;
  private ScheduledFuture<?> scheduledCommitTask;
  private long lastKnownContiguousOffset = -1L;

  /**
   * Creates a new OffsetManager instance.
   *
   * @param consumer The Kafka consumer to manage offsets for
   * @param <K> Type parameter for the key of the consumer
   * @param <V> Type parameter for the value of the consumer
   * @return A builder to construct the OffsetManager
   */
  public static <K, V> Builder<K, V> builder(final Consumer<K, V> consumer) {
    return new Builder<>(consumer);
  }

  /**
   * Builder class for OffsetManager.
   *
   * @param <K> The type of the key
   * @param <V> The type of the value
   */
  public static class Builder<K, V> {

    private final Consumer<K, V> kafkaConsumer;
    private Duration commitInterval = Duration.ofSeconds(30);
    private Queue<ConsumerCommand> commandQueue;

    private Builder(final Consumer<K, V> consumer) {
      this.kafkaConsumer = Objects.requireNonNull(consumer, "Consumer cannot be null");
    }

    /**
     * Shared command queue for the FunctionalConsumer and OffsetManager.
     *
     * @param commandQueue The command queue to use
     * @return This builder instance
     */
    public Builder<K, V> withCommandQueue(final Queue<ConsumerCommand> commandQueue) {
      this.commandQueue = Objects.requireNonNull(commandQueue, "Command queue cannot be null");
      return this;
    }

    /**
     * Sets the commit interval for periodic offset commits.
     *
     * @param interval The duration between commits
     * @return This builder instance
     */
    public Builder<K, V> withCommitInterval(final Duration interval) {
      this.commitInterval = Objects.requireNonNull(interval, "Commit interval cannot be null");
      if (interval.isNegative() || interval.isZero()) {
        throw new IllegalArgumentException("Commit interval must be positive");
      }
      return this;
    }

    /**
     * Builds the OffsetManager instance.
     *
     * @return A new OffsetManager instance
     */
    public OffsetManager<K, V> build() {
      if (commandQueue == null) {
        throw new IllegalStateException("Command queue must be provided");
      }
      return new OffsetManager<>(this);
    }
  }

  private OffsetManager(final Builder<K, V> builder) {
    this.kafkaConsumer = builder.kafkaConsumer;
    this.commitInterval = builder.commitInterval;
    this.executorService = Executors.newVirtualThreadPerTaskExecutor();
    this.commandQueue = builder.commandQueue;
  }

  /**
   * Starts the OffsetManager and begins periodic offset commits. This method is idempotent -
   * calling it multiple times has no effect if the manager is already started.
   *
   * @return this instance for method chaining
   * @throws IllegalStateException if the OffsetManager is already closed
   */
  public OffsetManager<K, V> start() {
    if (state.get() == OffsetState.STOPPED) {
      throw new IllegalStateException("Cannot restart a stopped OffsetManager");
    }

    if (state.compareAndSet(OffsetState.CREATED, OffsetState.RUNNING)) {
      scheduler =
        Executors.newSingleThreadScheduledExecutor(r ->
          Thread.ofPlatform().daemon().name("offset-commit-scheduler").unstarted(r)
        );

      scheduledCommitTask =
        scheduler.scheduleAtFixedRate(
          this::commitSafeOffsets,
          commitInterval.toMillis(),
          commitInterval.toMillis(),
          TimeUnit.MILLISECONDS
        );

      LOGGER.info("OffsetManager started with commit interval of " + commitInterval);
    }

    return this;
  }

  /**
   * Notifies the OffsetManager that a commit operation has completed.
   *
   * @param commitId The ID of the commit operation
   * @param success True if the commit was successful, false otherwise
   */
  public void notifyCommitComplete(final String commitId, final boolean success) {
    var future = commitFutures.remove(commitId);
    if (future != null) {
      future.complete(success);
    }
  }

  /**
   * Tracks an offset that is about to be processed.
   *
   * @param record The consumer record to track
   */
  public void trackOffset(final ConsumerRecord<K, V> record) {
    trackOffset(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
  }

  /**
   * Tracks an offset that is about to be processed.
   *
   * @param partition The topic partition the offset belongs to
   * @param offset The offset to track
   */
  public void trackOffset(final TopicPartition partition, final long offset) {
    if (state.get() == OffsetState.STOPPED) {
      return;
    }

    pendingOffsets.computeIfAbsent(partition, k -> new ConcurrentSkipListSet<>()).add(offset);

    // Initialize the next offset to commit if not already set
    if (!nextOffsetsToCommit.containsKey(partition)) {
      initializePartitionOffset(partition);
    }
  }

  /** Initialize the offset position for a partition. */
  private void initializePartitionOffset(final TopicPartition partition) {
    enqueueCommand(
      () -> {
        try {
          nextOffsetsToCommit.put(partition, kafkaConsumer.position(partition));
        } catch (final Exception e) {
          LOGGER.log(WARNING, "Failed to get position for partition " + partition, e);
        }
      },
      "initPosition"
    );
  }

  /**
   * Marks an offset as successfully processed.
   *
   * @param record The consumer record that was processed
   */
  public void markOffsetProcessed(final ConsumerRecord<K, V> record) {
    markOffsetProcessed(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
  }

  /**
   * Marks an offset as successfully processed.
   *
   * @param partition The topic partition the offset belongs to
   * @param offset The offset to mark as processed
   */
  public void markOffsetProcessed(final TopicPartition partition, final long offset) {
    if (state.get() == OffsetState.STOPPED) {
      return;
    }

    final var offsets = pendingOffsets.get(partition);
    if (offsets != null) {
      offsets.remove(offset);
      // Add to processed offsets
      processedOffsets.computeIfAbsent(partition, k -> new ConcurrentSkipListSet<>()).add(offset);
      updateCommittableOffset(partition);
    }
  }

  /**
   * Commits offsets that are safe to commit based on the current processing state.
   *
   * <p>This method is called periodically to ensure that offsets are committed in a timely manner
   * without losing any unprocessed messages.
   */
  public void commitSafeOffsets() {
    try {
      commitSyncAndWait(5);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(WARNING, "Interrupted while committing offsets", e);
    }
  }

  /**
   * Stops the periodic offset commit task but doesn't close resources. This can be used to
   * temporarily pause offset management.
   *
   * @return this instance for method chaining
   */
  public OffsetManager<K, V> stop() {
    if (!state.compareAndSet(OffsetState.RUNNING, OffsetState.STOPPING)) {
      return this; // Not running, nothing to stop
    }

    try {
      stopScheduler();
    } finally {
      state.set(OffsetState.STOPPED);
      LOGGER.info("OffsetManager stopped");
    }

    return this;
  }

  /** Stops the scheduler if it's running. */
  private void stopScheduler() {
    if (scheduledCommitTask != null) {
      scheduledCommitTask.cancel(false);
      scheduledCommitTask = null;
    }

    if (scheduler != null) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        scheduler.shutdownNow();
      }
      scheduler = null;
    }
  }

  /**
   * Commits offsets synchronously and waits for the specified timeout.
   *
   * @param timeoutSeconds Time to wait for commit in seconds
   * @return true if commit was successful
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public boolean commitSyncAndWait(final int timeoutSeconds) throws InterruptedException {
    if (state.get() == OffsetState.STOPPED) {
      return true;
    }

    final var offsetsToCommit = prepareOffsetsToCommit();
    if (offsetsToCommit.isEmpty()) {
      return true;
    }

    return performCommitAndCleanup(offsetsToCommit, timeoutSeconds);
  }

  /** Prepares a map of offsets to commit. */
  private Map<TopicPartition, OffsetAndMetadata> prepareOffsetsToCommit() {
    final var offsetsToCommit = new HashMap<TopicPartition, OffsetAndMetadata>();
    nextOffsetsToCommit.forEach((partition, offset) -> offsetsToCommit.put(partition, new OffsetAndMetadata(offset)));
    return offsetsToCommit;
  }

  /** Performs the actual commit operation and cleans up if successful. */
  private boolean performCommitAndCleanup(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit, int timeoutSeconds)
    throws InterruptedException {
    if (offsetsToCommit.isEmpty()) {
      return true;
    }

    // Create unique ID for this commit operation
    final var commitId = UUID.randomUUID().toString();
    final var future = new CompletableFuture<Boolean>();

    // Register for notification when commit completes
    commitFutures.put(commitId, future);

    // Send command to FunctionalConsumer
    commandQueue.offer(ConsumerCommand.COMMIT_OFFSETS.withOffsets(offsetsToCommit).withCommitId(commitId));

    try {
      final var result = future.get(timeoutSeconds, TimeUnit.SECONDS);
      if (result) {
        cleanupCommittedOffsets(offsetsToCommit);
      }
      return result;
    } catch (final ExecutionException | TimeoutException e) {
      LOGGER.log(WARNING, "Error waiting for offset commit", e);
      return false;
    } finally {
      commitFutures.remove(commitId);
    }
  }

  /**
   * Cleans up offsets that have been successfully committed to prevent memory leaks.
   *
   * @param committedOffsets Map of offsets that were successfully committed
   */
  private void cleanupCommittedOffsets(final Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
    committedOffsets.forEach((partition, metadata) -> {
      long committedOffset = metadata.offset();
      final var processed = processedOffsets.get(partition);
      if (processed != null) {
        try {
          // Get subSet view of elements less than committedOffset and clear it
          final var toRemove = processed.headSet(committedOffset);
          toRemove.clear();
        } catch (final UnsupportedOperationException e) {
          // Fall back to standard removeIf if headSet clear isn't supported
          processed.removeIf(offset -> offset < committedOffset);
        }

        if (processed.isEmpty()) {
          processedOffsets.remove(partition);
        }
      }
    });
  }

  /**
   * Updates the committable offset for a partition based on processed offsets. Handles both
   * contiguous and non-contiguous offset tracking.
   *
   * @param partition The partition to update
   */
  public void updateCommittableOffset(final TopicPartition partition) {
    final var currentPosition = nextOffsetsToCommit.get(partition);
    if (currentPosition == null) {
      return;
    }

    final var pending = pendingOffsets.get(partition);
    final var processed = processedOffsets.get(partition);
    if (processed == null || processed.isEmpty()) {
      return;
    }

    // Find the highest contiguous processed offset
    final var highestContiguous = findHighestContiguousOffset(processed);
    final var nextExpected = highestContiguous + 1;

    // If all offsets are processed, commit one past the highest contiguous
    if (pending == null || pending.isEmpty()) {
      nextOffsetsToCommit.put(partition, nextExpected);
      return;
    }

    // If there are still pending offsets, find the lowest pending
    // and commit up to that offset if there are no gaps before it
    updateCommitPointBasedOnPendingOffsets(partition, currentPosition, pending, nextExpected);
  }

  /** Finds the highest contiguous offset in a set of processed offsets. */
  private long findHighestContiguousOffset(final ConcurrentSkipListSet<Long> processed) {
    if (processed.isEmpty()) {
      return -1L;
    }

    final long startPoint = Math.max(lastKnownContiguousOffset, processed.first() - 1);
    long expected = startPoint + 1;

    for (long offset : processed.tailSet(expected)) {
      if (offset != expected) {
        break;
      }
      expected++;
    }

    lastKnownContiguousOffset = expected - 1;
    return lastKnownContiguousOffset;
  }

  /** Updates the commit point based on pending offsets. */
  private void updateCommitPointBasedOnPendingOffsets(
    final TopicPartition partition,
    final long currentPosition,
    final ConcurrentSkipListSet<Long> pending,
    final long nextExpected
  ) {
    final var firstPending = pending.first();
    if (firstPending > currentPosition && firstPending == nextExpected) {
      nextOffsetsToCommit.put(partition, firstPending);
    } else if (nextExpected > currentPosition) {
      nextOffsetsToCommit.put(partition, nextExpected);
    }
  }

  /**
   * Returns a snapshot of the current processing state for a partition.
   *
   * @param partition The partition to get state for
   * @return Map containing state information
   */
  public Map<String, Object> getPartitionState(TopicPartition partition) {
    final var state = new HashMap<String, Object>();

    state.put("nextOffsetToCommit", nextOffsetsToCommit.getOrDefault(partition, -1L));
    state.put("managerState", this.state.get().name());

    if (pendingOffsets.containsKey(partition)) {
      state.put("pendingCount", pendingOffsets.get(partition).size());
      if (!pendingOffsets.get(partition).isEmpty()) {
        state.put("lowestPendingOffset", pendingOffsets.get(partition).first());
        state.put("highestPendingOffset", pendingOffsets.get(partition).last());
      }
    } else {
      state.put("pendingCount", 0);
    }

    if (processedOffsets.containsKey(partition)) {
      state.put("processedCount", processedOffsets.get(partition).size());
      if (!processedOffsets.get(partition).isEmpty()) {
        state.put("lowestProcessedOffset", processedOffsets.get(partition).first());
        state.put("highestProcessedOffset", processedOffsets.get(partition).last());
      }
    } else {
      state.put("processedCount", 0);
    }

    return state;
  }

  /**
   * Returns overall statistics about all partitions being managed.
   *
   * @return Map containing statistics for all partitions
   */
  public Map<String, Object> getStatistics() {
    final var stats = new HashMap<String, Object>();

    stats.put("partitionCount", nextOffsetsToCommit.size());
    stats.put("totalPendingOffsets", pendingOffsets.values().stream().mapToInt(SortedSet::size).sum());
    stats.put("totalProcessedOffsets", processedOffsets.values().stream().mapToInt(SortedSet::size).sum());
    stats.put("managerState", state.get().name());

    return stats;
  }

  /**
   * Enqueues a command to the executor service with the configured timeout.
   *
   * @param command The command to execute
   * @param name The name of the command for logging
   */
  private void enqueueCommand(final Runnable command, final String name) {
    if (state.get() == OffsetState.STOPPED) {
      return;
    }

    try {
      final var future = executorService.submit(command);
      try {
        future.get(OffsetManager.DEFAULT_COMMAND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.log(WARNING, "Command %s was interrupted".formatted(name), e);
      } catch (final ExecutionException e) {
        LOGGER.log(WARNING, "Command %s failed with exception".formatted(name), e);
      } catch (final TimeoutException e) {
        LOGGER.log(
          WARNING,
          "Command %s timed out after %dms".formatted(name, OffsetManager.DEFAULT_COMMAND_TIMEOUT_MS),
          e
        );
      }
    } catch (final RejectedExecutionException e) {
      if (state.get() != OffsetState.STOPPED) {
        LOGGER.log(WARNING, "Command " + name + " was rejected", e);
      }
    }
  }

  /**
   * Gets the current state of the OffsetManager.
   *
   * @return The current state
   */
  public OffsetState getState() {
    return state.get();
  }

  /**
   * Checks if the OffsetManager is running.
   *
   * @return true if running, false otherwise
   */
  public boolean isRunning() {
    return state.get() == OffsetState.RUNNING;
  }

  @Override
  public void close() {
    if (
      !state.compareAndSet(OffsetState.RUNNING, OffsetState.STOPPING) &&
      !state.compareAndSet(OffsetState.CREATED, OffsetState.STOPPING)
    ) {
      return; // Already stopping or stopped
    }

    try {
      // Stop the scheduler first
      stopScheduler();

      // Final commit - direct without using executor
      performFinalCommit();
    } finally {
      cleanup();
      state.set(OffsetState.STOPPED);
    }
  }

  /** Performs a final commit before shutting down. */
  private void performFinalCommit() {
    try {
      final var offsetsToCommit = new HashMap<TopicPartition, OffsetAndMetadata>();
      nextOffsetsToCommit.forEach((partition, offset) -> offsetsToCommit.put(partition, new OffsetAndMetadata(offset)));

      if (!offsetsToCommit.isEmpty()) {
        final var commitId = UUID.randomUUID().toString();
        final var future = new CompletableFuture<Boolean>();
        commitFutures.put(commitId, future);

        commandQueue.offer(ConsumerCommand.COMMIT_OFFSETS.withOffsets(offsetsToCommit).withCommitId(commitId));

        try {
          future.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException e) {
          LOGGER.log(WARNING, "Final commit did not complete in time", e);
        }
      }
    } catch (final Exception e) {
      LOGGER.log(WARNING, "Error during final offset commit", e);
    }
  }

  /** Cleans up resources. */
  private void cleanup() {
    pendingOffsets.clear();
    processedOffsets.clear();
    nextOffsetsToCommit.clear();

    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(WARNING, "Interrupted while waiting for executor shutdown", e);
      executorService.shutdownNow();
    }
  }
}
