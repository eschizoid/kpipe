package org.kpipe.consumer;

import static java.util.logging.Level.WARNING;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kpipe.consumer.enums.ConsumerCommand;
import org.kpipe.consumer.enums.OffsetState;

/**
 * Manages Kafka consumer offsets for parallel processing scenarios.
 *
 * <p>This class provides safe offset commit management by tracking which offsets are being
 * processed and which have been completed, ensuring that only contiguous completed offsets are
 * committed to Kafka. This is particularly useful when records are processed in parallel or in a
 * non-sequential order.
 *
 * <p>Key features include:
 *
 * <ul>
 *   <li>Thread-safe tracking of offsets across multiple partitions
 *   <li>Contiguous commit strategy that prevents data loss during rebalancing
 *   <li>Explicit offset tracking with {@code trackOffset} and {@code markOffsetProcessed}
 *   <li>Support for both synchronous and asynchronous commit operations
 *   <li>Automatic periodic offset commits with configurable intervals
 *   <li>Proper rebalance handling with custom {@code ConsumerRebalanceListener}
 *   <li>Recovery from consumer failures and partition reassignments
 *   <li>Gap detection to ensure only fully processed offset ranges are committed
 *   <li>Diagnostic methods for monitoring offset state and troubleshooting
 *   <li>Virtual thread execution for non-blocking I/O operations
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create offset manager with default settings
 * var offsetManager = OffsetManager.builder(consumer).build();
 * offsetManager.start();
 *
 * // Track and process records
 * offsetManager.trackOffset(record);
 * // Process the record...
 * offsetManager.markOffsetProcessed(record);
 *
 * // Commit offsets explicitly
 * boolean success = offsetManager.commitSyncAndWait(5000); // 5 second timeout
 *
 * // Clean up resources when done
 * offsetManager.close();
 * }</pre>
 *
 * @param <K> The type of the record key
 * @param <V> The type of the record value
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
  private final Map<TopicPartition, Long> nextOffsetsToCommit = new ConcurrentHashMap<>();
  private final Map<TopicPartition, Long> highestProcessedOffsets = new ConcurrentHashMap<>();

  // Fields for periodic commits
  private final Duration commitInterval;
  private ScheduledExecutorService scheduler;
  private ScheduledFuture<?> scheduledCommitTask;

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
    final var partition = new TopicPartition(record.topic(), record.partition());
    final long offset = record.offset();

    if (state.get() == OffsetState.STOPPED) {
      return;
    }

    // Add to pending offsets (we use offset+1 as the next offset)
    pendingOffsets.computeIfAbsent(partition, k -> new ConcurrentSkipListSet<>()).add(offset + 1);

    // Initialize the next offset to commit if not already set
    // We should use the actual offset here, not offset+1
    nextOffsetsToCommit.computeIfAbsent(partition, k -> offset);
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

    // Add to pending offsets (we expect offset to already be the "next" offset)
    pendingOffsets.computeIfAbsent(partition, k -> new ConcurrentSkipListSet<>()).add(offset);

    // Initialize the next offset to commit if not already set
    // Use offset-1 to be consistent with the ConsumerRecord version
    nextOffsetsToCommit.computeIfAbsent(partition, k -> offset - 1);
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

    // Track highest processed offset for the partition
    highestProcessedOffsets.compute(partition, (k, v) -> v == null ? offset : Math.max(v, offset));

    final var offsets = pendingOffsets.get(partition);
    if (offsets != null) {
      offsets.remove(offset);
      if (offsets.isEmpty()) {
        pendingOffsets.remove(partition);
      }
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
      commitSyncAndWait(60);
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
   * @param timeoutSeconds Time to wait for a commit in seconds
   * @return true if the commit was successful
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

  private boolean performCommitAndCleanup(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit, int timeoutSeconds)
    throws InterruptedException {
    if (offsetsToCommit.isEmpty()) {
      return true;
    }

    final var commitId = UUID.randomUUID().toString();
    final var future = new CompletableFuture<Boolean>();
    commitFutures.put(commitId, future);

    commandQueue.offer(ConsumerCommand.COMMIT_OFFSETS.withOffsets(offsetsToCommit).withCommitId(commitId));

    try {
      final var result = future.get(timeoutSeconds, TimeUnit.SECONDS);
      if (result) {
        // Clean up committed offsets
        offsetsToCommit.forEach((partition, metadata) -> {
          long committedOffset = metadata.offset();
          final var pending = pendingOffsets.get(partition);

          // Remove all pending offsets less than committed offset
          if (pending != null) {
            pending.removeIf(offset -> offset < committedOffset);
            if (pending.isEmpty()) {
              pendingOffsets.remove(partition);
            }
          }
        });

        // Update the next offsets to commit based on highest processed and pending offsets
        highestProcessedOffsets.forEach((partition, highestProcessed) -> {
          final var pending = pendingOffsets.get(partition);
          if (pending != null && !pending.isEmpty()) {
            // Use exactly the lowest pending offset
            nextOffsetsToCommit.put(partition, pending.first());
          } else {
            // Use the highest processed offset directly (not +1) since this happens after
            // commit
            nextOffsetsToCommit.put(partition, highestProcessed);
          }
        });
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
    if (pending == null || pending.isEmpty()) {
      // All offsets are processed - use highest processed offset
      final var highestProcessed = highestProcessedOffsets.get(partition);
      if (highestProcessed != null) {
        nextOffsetsToCommit.put(partition, highestProcessed + 1);
      }
      return;
    }

    // Find the lowest pending offset
    final var lowestPending = pending.first();

    // Check if we can move the committable offset forward
    if (lowestPending > currentPosition) {
      // We can commit up to the lowest pending offset
      nextOffsetsToCommit.put(partition, lowestPending);
    }
    // Otherwise, maintain the current position as we have pending offsets before it
  }

  /**
   * Returns a snapshot of the current processing state for a partition.
   *
   * @param partition The partition to get state for
   * @return Map containing state information
   */
  public Map<String, Object> getPartitionState(final TopicPartition partition) {
    final var state = new HashMap<String, Object>();

    state.put("nextOffsetToCommit", nextOffsetsToCommit.getOrDefault(partition, -1L));
    state.put("highestProcessedOffset", highestProcessedOffsets.getOrDefault(partition, -1L));
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
    stats.put("totalProcessedPartitions", highestProcessedOffsets.size());
    stats.put("managerState", state.get().name());

    if (!highestProcessedOffsets.isEmpty()) {
      stats.put("highestProcessedOffsetsByPartition", new HashMap<>(highestProcessedOffsets));
      stats.put(
        "averageHighestProcessedOffset",
        highestProcessedOffsets.values().stream().mapToLong(Long::longValue).average().orElse(0)
      );
    }

    stats.put("pendingCommits", commitFutures.size());

    return stats;
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
        } catch (final ExecutionException | TimeoutException e) {
          LOGGER.log(WARNING, "Final commit did not complete in time", e);
        }
      }
    } catch (final Exception e) {
      LOGGER.log(WARNING, "Error during final offset commit", e);
    }
  }

  /**
   * Creates a rebalance listener for the Kafka consumer.
   *
   * @return A ConsumerRebalanceListener instance
   */
  public ConsumerRebalanceListener createRebalanceListener() {
    return new RebalanceListener(state, pendingOffsets, nextOffsetsToCommit, highestProcessedOffsets, kafkaConsumer);
  }

  /** Cleans up resources. */
  private void cleanup() {
    pendingOffsets.clear();
    nextOffsetsToCommit.clear();
    highestProcessedOffsets.clear();

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
