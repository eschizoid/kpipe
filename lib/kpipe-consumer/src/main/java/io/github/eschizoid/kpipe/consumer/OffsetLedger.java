package io.github.eschizoid.kpipe.consumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/// Per-partition offset bookkeeping behind [KafkaOffsetManager]: the pending-offset windows, the
/// highest-processed marks, the interned [TopicPartition] cache, and the single **commit-frontier
/// rule** they all feed.
///
/// The frontier rule is the honest core of at-least-once-with-parallelism: commit the lowest
/// still-pending offset if any (you must not advance past an in-flight record), otherwise
/// highest-processed + 1 (Kafka's next-offset semantics), otherwise nothing. It lived inline in
/// three places (commit prep, per-partition reporting, rebalance revoke) before this type pulled it
/// into one function ([#frontierOf]).
///
/// **Ownership boundary.** The ledger owns everything keyed by partition. It knows nothing about the
/// consumer lifecycle (start/stop/close), the commit I/O (command queue, [CommitCoordinator]), or
/// rebalance orchestration — those stay on [KafkaOffsetManager], which injects the two fields it
/// owns ([OffsetState], pending-commit count) into the reporting projections. The `commandQueue`
/// single-writer invariant never crosses this seam: the ledger touches no queue.
///
/// **Concurrency.** The maps are [ConcurrentHashMap]; every read-modify-write on a per-partition
/// value stays inside a bucket-locked `compute` / `computeIfPresent` so it is atomic against the
/// remove-if-empty path. [PendingOffsetSet] is monitor-synchronized, so the diagnostic reads
/// ([#frontier], [#statistics], [#partitionState]) that run outside the bucket lock stay safe.
final class OffsetLedger {

  /// Per-partition pending offsets: a sorted primitive-`long` window ([PendingOffsetSet]) rather
  /// than a `ConcurrentSkipListSet<Long>`. The skiplist cost a `Long` box plus node/marker churn on
  /// every track/mark; the primitive structure allocates nothing in steady state.
  private final Map<TopicPartition, PendingOffsetSet> pendingOffsets = new ConcurrentHashMap<>();
  private final Map<TopicPartition, Long> highestProcessedOffsets = new ConcurrentHashMap<>();

  /// Per-partition [TopicPartition] cache keyed by topic name (outer map) then partition number
  /// (inner map). Each [org.apache.kafka.clients.consumer.ConsumerRecord] carries `topic()` (a
  /// `String`) + `partition()` (an `int`); constructing a fresh `TopicPartition` per call on every
  /// record allocates twice per record on the hot path (track from the consumer thread, mark from
  /// the worker), which at 100k rec/s is ~200k disposable instances/sec. The cache returns a stable
  /// interned instance instead; entries are evicted in [#removePartitions] on rebalance.
  private final Map<String, Map<Integer, TopicPartition>> topicPartitionCache = new ConcurrentHashMap<>();

  /// Tracks an offset that is about to be processed.
  ///
  /// The `add` must happen INSIDE the bucket-locked `compute`, not as a trailing `.add()` on the
  /// value `computeIfAbsent` returns. Otherwise the add races [#markProcessed]'s remove-if-empty:
  /// that path can empty the set and atomically drop the key, leaving a trailing `.add()` to land on
  /// an orphaned set no longer in the map — silently losing a pending offset and letting the commit
  /// point advance past an in-flight record. `compute()` holds the bucket lock across the add.
  ///
  /// @param topic     the record's topic
  /// @param partition the record's partition
  /// @param offset    the record's offset
  void track(final String topic, final int partition, final long offset) {
    final var tp = cachedTopicPartition(topic, partition);
    pendingOffsets.compute(tp, (_, set) -> {
      final var pending = set == null ? new PendingOffsetSet() : set;
      pending.add(offset);
      return pending;
    });
  }

  /// Marks an offset as successfully processed: bumps the partition's highest-processed mark and
  /// drops the offset from the pending window (removing the key when the window empties).
  ///
  /// @param topic     the record's topic
  /// @param partition the record's partition
  /// @param offset    the record's offset
  void markProcessed(final String topic, final int partition, final long offset) {
    final var tp = cachedTopicPartition(topic, partition);
    highestProcessedOffsets.compute(tp, (_, v) -> v == null ? offset : Math.max(v, offset));
    pendingOffsets.computeIfPresent(tp, (_, set) -> {
      set.remove(offset);
      return set.isEmpty() ? null : set;
    });
  }

  /// The commit-frontier rule for one partition. Lowest still-pending offset if any (can't commit
  /// past an in-flight record); else highest-processed + 1 (Kafka's next-offset semantics); else
  /// empty (nothing tracked for this partition).
  ///
  /// @param partition the partition to compute the frontier for
  /// @return the offset that would next be committed, or empty if nothing is tracked
  OptionalLong frontier(final TopicPartition partition) {
    final var pending = pendingOffsets.get(partition);
    final var lowestPending = pending != null ? pending.firstOrNull() : null;
    return frontierOf(lowestPending, highestProcessedOffsets.get(partition));
  }

  /// Frontiers for every tracked partition, ready to hand to `commitSync`. Computed at call time
  /// (not maintained continuously) from the current pending/processed state.
  ///
  /// @return partition → next-offset-to-commit for every partition with a frontier
  Map<TopicPartition, OffsetAndMetadata> committableOffsets() {
    return trackedPartitions()
      .stream()
      .flatMap(partition -> {
        final var frontier = frontier(partition);
        return frontier.isPresent()
          ? Stream.of(Map.entry(partition, new OffsetAndMetadata(frontier.getAsLong())))
          : Stream.empty();
      })
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /// Drops all tracked state for the given partitions — pending window, highest-processed mark, and
  /// interned [TopicPartition] cache entry. Used on rebalance revoke (after the caller has read the
  /// frontiers it wants to commit) and assign (to clear any stale state from a prior ownership).
  /// Evicting a cache entry that isn't present is a no-op.
  ///
  /// @param partitions the partitions to forget
  void removePartitions(final Collection<TopicPartition> partitions) {
    partitions.forEach(partition -> {
      pendingOffsets.remove(partition);
      highestProcessedOffsets.remove(partition);
      evictCachedTopicPartition(partition);
    });
  }

  /// Overall statistics across all tracked partitions. The offset-derived fields are computed here;
  /// the caller injects the two fields the ledger doesn't own — the manager's lifecycle state and
  /// the pending-commit count.
  ///
  /// @param lifecycleState the offset manager's lifecycle state
  /// @param pendingCommits commits issued but not yet acknowledged
  /// @return a typed snapshot across all partitions
  OffsetStatistics statistics(final OffsetState lifecycleState, final int pendingCommits) {
    final var totalPending = pendingOffsets.values().stream().mapToInt(PendingOffsetSet::size).sum();

    final Map<TopicPartition, Long> byPartition;
    final double average;
    if (!highestProcessedOffsets.isEmpty()) {
      byPartition = new HashMap<>(highestProcessedOffsets);
      average = highestProcessedOffsets.values().stream().mapToLong(Long::longValue).average().orElse(0);
    } else {
      byPartition = Map.of();
      average = 0.0;
    }

    return new OffsetStatistics(
      trackedPartitions().size(),
      totalPending,
      highestProcessedOffsets.size(),
      lifecycleState,
      byPartition,
      average,
      pendingCommits
    );
  }

  /// A typed snapshot of one partition's offset-tracking state. Reads the pending window once so the
  /// frontier and the pending-window fields come from a single consistent snapshot.
  ///
  /// @param partition     the partition to snapshot
  /// @param lifecycleState the offset manager's lifecycle state
  /// @return the partition's offset-tracking state
  PartitionState partitionState(final TopicPartition partition, final OffsetState lifecycleState) {
    final var pending = pendingOffsets.get(partition);
    final var highestProcessed = highestProcessedOffsets.get(partition);
    final var lowestPending = pending != null ? pending.firstOrNull() : null;

    final var nextOffsetToCommit = frontierOf(lowestPending, highestProcessed).orElse(-1L);

    var lowestPendingOffset = OptionalLong.empty();
    var highestPendingOffset = OptionalLong.empty();
    if (pending != null && lowestPending != null) {
      lowestPendingOffset = OptionalLong.of(lowestPending);
      final var highestPending = pending.lastOrNull();
      if (highestPending != null) highestPendingOffset = OptionalLong.of(highestPending);
    }

    return new PartitionState(
      nextOffsetToCommit,
      highestProcessed != null ? highestProcessed : -1L,
      lifecycleState,
      pending != null ? pending.size() : 0,
      lowestPendingOffset,
      highestPendingOffset
    );
  }

  /// Drops all tracked state. Called from [KafkaOffsetManager#close].
  void clear() {
    pendingOffsets.clear();
    highestProcessedOffsets.clear();
    topicPartitionCache.clear();
  }

  /// The commit-frontier rule as a pure function of a partition's two facts, so the single-read
  /// callers ([#frontier], [#partitionState]) share one definition of the rule without either
  /// re-reading the maps.
  ///
  /// @param lowestPending   the lowest in-flight offset, or `null` if none pending
  /// @param highestProcessed the highest processed offset, or `null` if none processed
  /// @return the offset that would next be committed, or empty if neither is present
  private static OptionalLong frontierOf(final Long lowestPending, final Long highestProcessed) {
    if (lowestPending != null) return OptionalLong.of(lowestPending);
    return highestProcessed != null ? OptionalLong.of(highestProcessed + 1) : OptionalLong.empty();
  }

  /// Distinct partitions with either pending or processed state.
  private Set<TopicPartition> trackedPartitions() {
    final var all = new HashSet<TopicPartition>();
    all.addAll(pendingOffsets.keySet());
    all.addAll(highestProcessedOffsets.keySet());
    return all;
  }

  /// Returns a cached [TopicPartition] for the given `(topic, partition)` pair, allocating one on
  /// first observation and reusing it thereafter. `computeIfAbsent` on the outer map atomicizes the
  /// inner-map creation; the inner-map lambda closes over `topic` from the local so the
  /// `TopicPartition(topic, partition)` constructor sees the exact `String` the caller passed.
  ///
  /// Package-private (not `private`) so same-package tests can assert the interning/eviction
  /// contract by identity (`assertSame` / `assertNotSame`) without reaching in by reflection.
  TopicPartition cachedTopicPartition(final String topic, final int partition) {
    return topicPartitionCache
      .computeIfAbsent(topic, _ -> new ConcurrentHashMap<>())
      .computeIfAbsent(partition, p -> new TopicPartition(topic, p));
  }

  /// Evicts the cached [TopicPartition] for a revoked partition. `computeIfPresent` bucket-locks the
  /// inner-map removal and the outer-map cleanup together, so a concurrent [#cachedTopicPartition]
  /// for a different partition under the same topic can't see a transient empty inner map and
  /// double-allocate. Drops the outer entry when the last partition for a topic leaves.
  private void evictCachedTopicPartition(final TopicPartition partition) {
    topicPartitionCache.computeIfPresent(partition.topic(), (_, inner) -> {
      inner.remove(partition.partition());
      return inner.isEmpty() ? null : inner;
    });
  }
}
