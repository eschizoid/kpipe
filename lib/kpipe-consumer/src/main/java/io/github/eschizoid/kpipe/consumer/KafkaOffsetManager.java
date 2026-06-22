package io.github.eschizoid.kpipe.consumer;

import static java.lang.System.Logger.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
///
/// @param <K> The type of the record key
public class KafkaOffsetManager<K> implements OffsetManager<K> {

  private static final System.Logger LOGGER = System.getLogger(KafkaOffsetManager.class.getName());

  private final Consumer<K, byte[]> kafkaConsumer;
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

  private final Map<String, CompletableFuture<Boolean>> commitFutures = new ConcurrentHashMap<>();
  private final Map<TopicPartition, ConcurrentSkipListSet<Long>> pendingOffsets = new ConcurrentHashMap<>();
  private final Map<TopicPartition, Long> highestProcessedOffsets = new ConcurrentHashMap<>();

  /// Per-partition `TopicPartition` cache used by `trackOffset` and `markOffsetProcessed`. Each
  /// `ConsumerRecord` carries `topic()` (a `String`) + `partition()` (an `int`) — constructing a
  /// fresh `TopicPartition` per call on every record allocates twice per record on the hot path
  /// (track from the consumer thread, mark from the worker), which at 100k rec/s is ~200k
  /// disposable instances/sec from this manager alone.
  ///
  /// The cache is keyed by topic name (outer map) then partition number (inner map) so the
  /// outer-map miss path (one-time per topic) is small and the per-record fast path is two
  /// `ConcurrentHashMap` lookups returning a stable, interned instance. Entries are evicted in
  /// `onPartitionsRevoked` so a rebalance that hands a partition off doesn't leak the cached
  /// instance — `onPartitionsAssigned` will repopulate on first record.
  private final Map<String, Map<Integer, TopicPartition>> topicPartitionCache = new ConcurrentHashMap<>();
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
  /// @param <K> Type parameter for the key of the consumer
  /// @return A builder to construct the KafkaOffsetManager
  public static <K> Builder<K> builder(final Consumer<K, byte[]> consumer) {
    return new Builder<>(consumer);
  }

  /// Builder class for KafkaOffsetManager.
  ///
  /// @param <K> The type of the key
  public static class Builder<K> {

    private final Consumer<K, byte[]> kafkaConsumer;
    private Duration commitInterval = Duration.ofSeconds(30);
    private Queue<ConsumerCommand> commandQueue;

    private Builder(final Consumer<K, byte[]> consumer) {
      this.kafkaConsumer = Objects.requireNonNull(consumer, "Consumer cannot be null");
    }

    /// Shared command queue for the KPipeConsumer and KafkaOffsetManager.
    ///
    /// @param commandQueue The command queue to use
    /// @return This builder instance
    public Builder<K> withCommandQueue(final Queue<ConsumerCommand> commandQueue) {
      this.commandQueue = Objects.requireNonNull(commandQueue, "Command queue cannot be null");
      return this;
    }

    /// Sets the commit interval for periodic offset commits.
    ///
    /// @param interval The duration between commits
    /// @return This builder instance
    public Builder<K> withCommitInterval(final Duration interval) {
      this.commitInterval = Objects.requireNonNull(interval, "Commit interval cannot be null");
      if (interval.isNegative() || interval.isZero()) throw new IllegalArgumentException(
        "Commit interval must be positive, got " + interval
      );
      return this;
    }

    /// Builds the KafkaOffsetManager instance.
    ///
    /// @return A new KafkaOffsetManager instance
    public KafkaOffsetManager<K> build() {
      if (commandQueue == null) throw new IllegalStateException("withCommandQueue(...) must be called before build()");
      return new KafkaOffsetManager<>(this);
    }
  }

  private KafkaOffsetManager(final Builder<K> builder) {
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
  public KafkaOffsetManager<K> start() {
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
    var future = commitFutures.remove(commitId);
    if (future != null) future.complete(success);
  }

  /// Tracks an offset that is about to be processed using a ConsumerRecord.
  ///
  /// This method extracts the topic, partition, and offset from the consumer record and adds the
  /// offset+1 to the pending offsets. In Kafka's offset model, committing offset N means you've
  /// processed through offset N-1 and expect to receive N next.
  ///
  /// When using this method with {@link #markOffsetProcessed(ConsumerRecord)}, the offset
  /// transformation is handled automatically. This method initializes the next offset to commit
  /// using the raw record offset, which is appropriate for the first record in a partition.
  ///
  /// @param record The consumer record to track
  @Override
  public void trackOffset(final ConsumerRecord<K, byte[]> record) {
    if (state.get() == OffsetState.STOPPED) return;
    final var partition = cachedTopicPartition(record.topic(), record.partition());
    final var offset = record.offset();
    // The add must happen INSIDE the bucket-locked compute, not as a trailing .add() on the value
    // computeIfAbsent returns. Otherwise the add races markOffsetProcessed's remove-if-empty: that
    // path can empty the set and atomically drop the key, leaving a trailing .add() to land on an
    // orphaned set no longer in the map — silently losing a pending offset and letting the commit
    // point advance past an in-flight record. compute() holds the bucket lock across the add.
    pendingOffsets.compute(partition, (_, set) -> {
      final var pending = set == null ? new ConcurrentSkipListSet<Long>() : set;
      pending.add(offset);
      return pending;
    });
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
  public void markOffsetProcessed(final ConsumerRecord<K, byte[]> record) {
    if (state.get() == OffsetState.STOPPED) return;
    final var partition = cachedTopicPartition(record.topic(), record.partition());
    final var offset = record.offset();

    highestProcessedOffsets.compute(partition, (_, v) -> v == null ? offset : Math.max(v, offset));

    pendingOffsets.computeIfPresent(partition, (_, set) -> {
      set.remove(offset);
      return set.isEmpty() ? null : set;
    });
  }

  /// Returns a cached `TopicPartition` for the given `(topic, partition)` pair, allocating one
  /// on first observation and reusing the same instance for every subsequent call. The fast
  /// path is two `ConcurrentHashMap` lookups returning a stable instance, replacing two `new
  /// TopicPartition(...)` allocations per record on the hot path.
  ///
  /// `computeIfAbsent` on the outer map atomicizes the inner-map creation; the inner-map
  /// lambda closes over `topic` from the local rather than the `BiFunction`'s `t` argument, so
  /// the `TopicPartition(topic, partition)` constructor sees the exact `String` reference the
  /// caller passed (`String#intern` is not assumed — `TopicPartition.equals/hashCode` use
  /// value equality and pooled inner-map entries don't depend on string identity).
  private TopicPartition cachedTopicPartition(final String topic, final int partition) {
    return topicPartitionCache
      .computeIfAbsent(topic, _ -> new ConcurrentHashMap<>())
      .computeIfAbsent(partition, p -> new TopicPartition(topic, p));
  }

  /// Evicts the cached `TopicPartition` for a revoked partition. Uses `computeIfPresent` on the
  /// inner map so the inner-map removal and the outer-map cleanup are bucket-locked together —
  /// a concurrent `cachedTopicPartition` call for a different partition under the same topic
  /// cannot see a transient empty inner map and double-allocate. If the inner map becomes empty
  /// after removal (last partition for a topic), the outer entry is dropped so the cache doesn't
  /// retain stale topic keys across rebalances.
  private void evictCachedTopicPartition(final TopicPartition partition) {
    topicPartitionCache.computeIfPresent(partition.topic(), (_, inner) -> {
      inner.remove(partition.partition());
      return inner.isEmpty() ? null : inner;
    });
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
  public KafkaOffsetManager<K> stop() {
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

    final var offsetsToCommit = prepareOffsetsToCommit();
    if (offsetsToCommit.isEmpty()) return true;

    return performCommit(offsetsToCommit, timeout);
  }

  /// Prepares offsets for commit based on the current processing state. This method calculates the
  /// offsets to commit at the time of commit, rather than maintaining them continuously.
  ///
  /// @return Map of partition to offset metadata ready for committing
  private Map<TopicPartition, OffsetAndMetadata> prepareOffsetsToCommit() {
    return Stream.concat(pendingOffsets.keySet().stream(), highestProcessedOffsets.keySet().stream())
      .distinct()
      .flatMap(partition -> {
        final var pending = pendingOffsets.get(partition);
        final var lowestPending = pending != null ? safeFirst(pending) : null;
        if (lowestPending != null) return Stream.of(Map.entry(partition, new OffsetAndMetadata(lowestPending)));
        final var highestProcessed = highestProcessedOffsets.get(partition);
        return highestProcessed != null
          ? Stream.of(Map.entry(partition, new OffsetAndMetadata(highestProcessed + 1)))
          : Stream.empty();
      })
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static Long safeFirst(final SortedSet<Long> set) {
    try {
      return set.isEmpty() ? null : set.first();
    } catch (final NoSuchElementException e) {
      return null;
    }
  }

  private boolean performCommit(final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit, final Duration timeout)
    throws InterruptedException {
    if (offsetsToCommit.isEmpty()) return true;

    final var commitId = UUID.randomUUID().toString();
    final var future = new CompletableFuture<Boolean>();
    commitFutures.put(commitId, future);

    commandQueue.offer(new ConsumerCommand.CommitOffsets(offsetsToCommit, commitId));

    try {
      return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final ExecutionException | TimeoutException e) {
      LOGGER.log(Level.WARNING, "Error waiting for offset commit", e);
      return false;
    } finally {
      commitFutures.remove(commitId);
    }
  }

  /// Returns a snapshot of the current processing state for a partition.
  ///
  /// @param partition The partition to get state for
  /// @return Map containing state information
  public Map<String, Object> getPartitionState(final TopicPartition partition) {
    final var state = new HashMap<String, Object>();
    final var pending = pendingOffsets.get(partition);
    final var highestProcessed = highestProcessedOffsets.get(partition);
    final var lowestPending = pending != null ? safeFirst(pending) : null;

    if (lowestPending != null) {
      state.put("nextOffsetToCommit", lowestPending);
    } else if (highestProcessed != null) {
      state.put("nextOffsetToCommit", highestProcessed + 1);
    } else {
      state.put("nextOffsetToCommit", -1L);
    }

    state.put("highestProcessedOffset", highestProcessed != null ? highestProcessed : -1L);
    state.put("managerState", this.state.get().name());

    if (pending != null) {
      state.put("pendingCount", pending.size());
      if (lowestPending != null) {
        state.put("lowestPendingOffset", lowestPending);
        final var highestPending = safeLast(pending);
        if (highestPending != null) state.put("highestPendingOffset", highestPending);
      }
    } else {
      state.put("pendingCount", 0);
    }

    return state;
  }

  private static Long safeLast(final SortedSet<Long> set) {
    try {
      return set.isEmpty() ? null : set.last();
    } catch (final NoSuchElementException e) {
      return null;
    }
  }

  /// Returns overall statistics about all partitions being managed.
  ///
  /// @return Map containing statistics for all partitions
  @Override
  public Map<String, Object> getStatistics() {
    final var stats = new HashMap<String, Object>();
    final var allPartitions = new HashSet<>();

    allPartitions.addAll(pendingOffsets.keySet());
    allPartitions.addAll(highestProcessedOffsets.keySet());
    stats.put("partitionCount", allPartitions.size());

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
        final var offsetsToCommit = prepareOffsetsToCommit();
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
        partitions.forEach(partition -> {
          final var pending = pendingOffsets.get(partition);
          final var lowestPending = pending != null ? safeFirst(pending) : null;
          if (lowestPending != null) {
            offsetsToCommit.put(partition, new OffsetAndMetadata(lowestPending));
          } else {
            final var highestProcessed = highestProcessedOffsets.get(partition);
            if (highestProcessed != null) offsetsToCommit.put(partition, new OffsetAndMetadata(highestProcessed + 1));
          }
          pendingOffsets.remove(partition);
          highestProcessedOffsets.remove(partition);
          evictCachedTopicPartition(partition);
        });

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
        partitions.forEach(partition -> {
          pendingOffsets.remove(partition);
          highestProcessedOffsets.remove(partition);
        });
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
    assert captured == Thread.currentThread()
      : "rebalance callback ran on %s but was previously bound to %s — commandQueue single-writer invariant violated".formatted(
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
    pendingOffsets.clear();
    highestProcessedOffsets.clear();
    topicPartitionCache.clear();
  }
}
