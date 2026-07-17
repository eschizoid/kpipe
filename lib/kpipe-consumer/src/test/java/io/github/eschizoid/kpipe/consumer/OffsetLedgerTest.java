package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.OptionalLong;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// Unit tests for [OffsetLedger] — the per-partition offset bookkeeping extracted from
/// KafkaOffsetManager. Pins the commit-frontier rule (especially gap-hold, the honest core of
/// at-least-once-with-parallelism), the reporting projections, and partition removal. Pure
/// in-memory, no Kafka.
class OffsetLedgerTest {

  private static final TopicPartition P0 = new TopicPartition("topic", 0);
  private static final TopicPartition P1 = new TopicPartition("topic", 1);

  @Test
  void frontierIsEmptyForAnUntrackedPartition() {
    assertEquals(OptionalLong.empty(), new OffsetLedger().frontier(P0));
  }

  @Test
  void frontierIsTheLowestPendingOffsetWhileInFlight() {
    final var ledger = new OffsetLedger();
    ledger.track("topic", 0, 100L);
    ledger.track("topic", 0, 101L);
    ledger.track("topic", 0, 102L);
    assertEquals(OptionalLong.of(100L), ledger.frontier(P0));
  }

  @Test
  void frontierHoldsAtTheGapWhenAHigherOffsetCompletesFirst() {
    final var ledger = new OffsetLedger();
    ledger.track("topic", 0, 100L);
    ledger.track("topic", 0, 101L);
    ledger.track("topic", 0, 102L);

    // 102 finishes out of order — the frontier must NOT jump past the still-pending 100/101.
    ledger.markProcessed("topic", 0, 102L);
    assertEquals(OptionalLong.of(100L), ledger.frontier(P0), "commit point must not advance past an in-flight record");
  }

  @Test
  void frontierAdvancesAsContiguousOffsetsComplete() {
    final var ledger = new OffsetLedger();
    ledger.track("topic", 0, 100L);
    ledger.track("topic", 0, 101L);
    ledger.track("topic", 0, 102L);
    ledger.markProcessed("topic", 0, 102L);

    ledger.markProcessed("topic", 0, 100L);
    assertEquals(OptionalLong.of(101L), ledger.frontier(P0));

    ledger.markProcessed("topic", 0, 101L);
    // All pending drained; frontier is now highest-processed (102) + 1.
    assertEquals(OptionalLong.of(103L), ledger.frontier(P0));
  }

  @Test
  void frontierIsHighestProcessedPlusOneWhenNothingPending() {
    final var ledger = new OffsetLedger();
    ledger.track("topic", 0, 100L);
    ledger.markProcessed("topic", 0, 100L);
    assertEquals(OptionalLong.of(101L), ledger.frontier(P0));
  }

  @Test
  void committableOffsetsSpansEveryTrackedPartition() {
    final var ledger = new OffsetLedger();
    ledger.track("topic", 0, 100L); // still pending -> frontier 100
    ledger.track("topic", 1, 200L);
    ledger.markProcessed("topic", 1, 200L); // drained -> frontier 201

    final var committable = ledger.committableOffsets();
    assertEquals(2, committable.size());
    assertEquals(100L, committable.get(P0).offset());
    assertEquals(201L, committable.get(P1).offset());
  }

  @Test
  void removePartitionsForgetsAllState() {
    final var ledger = new OffsetLedger();
    ledger.track("topic", 0, 100L);
    ledger.track("topic", 1, 200L);

    ledger.removePartitions(List.of(P0));

    assertEquals(OptionalLong.empty(), ledger.frontier(P0), "removed partition is forgotten");
    assertEquals(OptionalLong.of(200L), ledger.frontier(P1), "sibling partition is untouched");
    assertFalse(ledger.committableOffsets().containsKey(P0));
  }

  @Test
  void statisticsAggregateOffsetStateAndInjectLifecycleFields() {
    final var ledger = new OffsetLedger();
    ledger.track("topic", 0, 100L); // pending
    ledger.track("topic", 1, 200L);
    ledger.markProcessed("topic", 1, 200L); // processed, drained

    final var stats = ledger.statistics(OffsetState.RUNNING, 3);
    assertEquals(2, stats.partitionCount());
    assertEquals(1, stats.totalPendingOffsets(), "only P0 is still pending");
    assertEquals(1, stats.totalProcessedPartitions(), "only P1 has a processed offset");
    assertEquals(OffsetState.RUNNING, stats.managerState(), "lifecycle state is injected by the manager");
    assertEquals(3, stats.pendingCommits(), "pending-commit count is injected by the manager");
    assertEquals(200L, stats.highestProcessedOffsetsByPartition().get(P1));
  }

  @Test
  void partitionStateSnapshotReportsFrontierAndPendingWindow() {
    final var ledger = new OffsetLedger();
    ledger.track("topic", 0, 100L);
    ledger.track("topic", 0, 102L);
    ledger.markProcessed("topic", 0, 102L); // gap: 100 pending, 102 processed

    final var snapshot = ledger.partitionState(P0, OffsetState.RUNNING);
    assertEquals(100L, snapshot.nextOffsetToCommit(), "frontier holds at the pending 100");
    assertEquals(102L, snapshot.highestProcessedOffset());
    assertEquals(OffsetState.RUNNING, snapshot.managerState());
    assertEquals(1, snapshot.pendingCount());
    assertTrue(snapshot.lowestPendingOffset().isPresent());
    assertEquals(100L, snapshot.lowestPendingOffset().getAsLong());
    assertEquals(100L, snapshot.highestPendingOffset().getAsLong());
  }

  @Test
  void partitionStateForUntrackedPartitionUsesSentinels() {
    final var snapshot = new OffsetLedger().partitionState(P0, OffsetState.CREATED);
    assertEquals(-1L, snapshot.nextOffsetToCommit());
    assertEquals(-1L, snapshot.highestProcessedOffset());
    assertEquals(0, snapshot.pendingCount());
    assertFalse(snapshot.lowestPendingOffset().isPresent());
  }

  // The interned TopicPartition cache exists to keep the track/mark hot path off the per-record
  // `new TopicPartition(...)` allocation. Observed by identity through the package-private
  // `cachedTopicPartition` accessor — no reflection.

  @Test
  void cachedTopicPartitionInternsTheSameInstance() {
    final var ledger = new OffsetLedger();
    final var first = ledger.cachedTopicPartition("topic", 0);
    assertSame(first, ledger.cachedTopicPartition("topic", 0), "the same (topic, partition) reuses the cached instance");
  }

  @Test
  void removePartitionsEvictsTheCachedInstance() {
    final var ledger = new OffsetLedger();
    final var original = ledger.cachedTopicPartition("topic", 0);

    ledger.removePartitions(List.of(P0));

    final var reAllocated = ledger.cachedTopicPartition("topic", 0);
    assertNotSame(original, reAllocated, "after eviction a fresh instance is allocated, not the evicted one");
    assertEquals(original, reAllocated, "value-equality still holds (same topic + partition number)");
  }

  @Test
  void evictingOnePartitionPreservesSiblingsUnderTheSameTopic() {
    final var ledger = new OffsetLedger();
    final var p0 = ledger.cachedTopicPartition("topic", 0);
    final var p1 = ledger.cachedTopicPartition("topic", 1);

    ledger.removePartitions(List.of(P0));

    assertSame(p1, ledger.cachedTopicPartition("topic", 1), "sibling partition 1 stays cached as the same instance");
    assertNotSame(p0, ledger.cachedTopicPartition("topic", 0), "partition 0 was evicted, so it re-allocates");
  }
}
