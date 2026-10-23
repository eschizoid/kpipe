package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;
import net.jqwik.api.constraints.IntRange;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;

/// Property-based coverage for the offset-lifecycle ordering invariants — lowest-pending commits,
/// no commit-ahead, and no-loss across interleaved track/mark sequences, and the revoke and
/// late-mark rules across sequences that also move partitions in and out of this member's
/// ownership.
///
/// Where `OffsetInvariantPropertyTest` is the single-partition smoke proving the toolchain, this
/// suite exercises the combinatorial ordering space that property testing is built for: random
/// subsets of a tracked range marked in random order, fully shuffled mark orders, and several
/// partitions tracked/marked simultaneously with independent commit points.
///
/// All properties here are single-threaded — they constrain ordering, not concurrency. The
/// concurrency model-checking layer is a sibling concern. Every assertion reads the manager's
/// observable per-partition state through `getPartitionState(partition)` rather than reaching into
/// internal fields, so the tests stay valid against any implementation that honors the contract.
///
/// The expected commit point is reconstructed independently from the spec's definition: lowest
/// still-pending offset when anything is pending, else `highestMarked + 1`, else `-1`.
class OffsetOrderingPropertyTest {

  private static final String TOPIC = "ordering-prop-topic";

  /// Builds a manager wired to a `MockConsumer` with a fresh command queue, started and ready.
  private static KafkaOffsetManager newManager() {
    final var consumer = new MockConsumer<byte[], byte[]>("earliest");
    final var manager = KafkaOffsetManager.builder(consumer).withCommitExecutor(TestCommitExecutors.unwired()).build();
    manager.start();
    return manager;
  }

  private static ConsumerRecord<byte[], byte[]> record(final int partition, final long offset) {
    return new ConsumerRecord<>(TOPIC, partition, offset, "k".getBytes(UTF_8), "v".getBytes());
  }

  private static long commitPoint(final KafkaOffsetManager manager, final TopicPartition tp) {
    return (long) manager.getPartitionState(tp).nextOffsetToCommit();
  }

  private static long highestProcessed(final KafkaOffsetManager manager, final TopicPartition tp) {
    return (long) manager.getPartitionState(tp).highestProcessedOffset();
  }

  /// The commit point the spec mandates for a partition given its pending set and highest marked
  /// offset: lowest pending if anything is pending, else `highestMarked + 1`, else `-1`.
  private static long expectedCommitPoint(final Set<Long> pending, final long highestMarked) {
    if (!pending.isEmpty()) return pending.stream().mapToLong(Long::longValue).min().orElseThrow();
    if (highestMarked >= 0) return highestMarked + 1;
    return -1L;
  }

  // ---------------------------------------------------------------------------------------------
  // No commit past a gap — random subset of a contiguous range marked in random order.
  // ---------------------------------------------------------------------------------------------

  /// A tracked offset range `[0, size)` plus the subset of those offsets that get marked, in a
  /// shuffled order. Seeded so jqwik shrinking and replay stay deterministic.
  private record GapScenario(int size, List<Long> markOrder) {}

  @Provide
  Arbitrary<GapScenario> gapScenarios() {
    final Arbitrary<Integer> sizes = Arbitraries.integers().between(1, 30);
    return sizes.flatMap(size -> {
      final Arbitrary<Long> seeds = Arbitraries.longs();
      return seeds.map(seed -> {
        final var all = new ArrayList<Long>(size);
        for (var i = 0; i < size; i++) all.add((long) i);
        final var rnd = new Random(seed);
        // Pick a random subset to mark (each offset independently kept), then shuffle it.
        final var subset = all
          .stream()
          .filter(o -> rnd.nextBoolean())
          .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(subset, rnd);
        return new GapScenario(size, subset);
      });
    });
  }

  /// Track a contiguous range, then mark a random subset in random order. After every mark the
  /// commit point equals the lowest still-unmarked tracked offset — it never jumps across the gap
  /// left by an unmarked offset, even when higher offsets have been marked.
  @Property(tries = 300)
  void commitPointStaysAtLowestUnmarkedAcrossGaps(@ForAll("gapScenarios") final GapScenario scenario) {
    final var manager = newManager();
    final var tp = new TopicPartition(TOPIC, 0);
    try {
      final Set<Long> pending = new TreeSet<>();
      for (var offset = 0L; offset < scenario.size(); offset++) {
        manager.trackOffset(record(0, offset));
        pending.add(offset);
      }

      // Before any mark the commit point is the lowest tracked offset (0).
      assertEquals(0L, commitPoint(manager, tp), "commit point with full range pending must be offset 0");

      var highestMarked = -1L;
      for (final var offset : scenario.markOrder()) {
        manager.markOffsetProcessed(record(0, offset));
        pending.remove(offset);
        highestMarked = Math.max(highestMarked, offset);

        final var expected = expectedCommitPoint(pending, highestMarked);
        assertEquals(
          expected,
          commitPoint(manager, tp),
          "after marking %d (order %s) commit point must be lowest unmarked / highestMarked+1".formatted(
            offset,
            scenario.markOrder()
          )
        );

        // Restated as an inequality: the commit point can never pass any still-pending offset.
        for (final var stillPending : pending) {
          assertTrue(
            commitPoint(manager, tp) <= stillPending,
            "commit point %d passed still-pending offset %d".formatted(commitPoint(manager, tp), stillPending)
          );
        }
      }
    } finally {
      manager.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Out-of-order marking — full shuffle of a contiguous range => contiguous-from-start prefix.
  // ---------------------------------------------------------------------------------------------

  /// A contiguous range size plus a fully shuffled permutation of every offset in it.
  private record ShuffleScenario(int size, List<Long> markOrder) {}

  @Provide
  Arbitrary<ShuffleScenario> shuffleScenarios() {
    final Arbitrary<Integer> sizes = Arbitraries.integers().between(1, 40);
    return sizes.flatMap(size -> {
      final Arbitrary<Long> seeds = Arbitraries.longs();
      return seeds.map(seed -> {
        final var all = new ArrayList<Long>(size);
        for (var i = 0; i < size; i++) all.add((long) i);
        Collections.shuffle(all, new Random(seed));
        return new ShuffleScenario(size, all);
      });
    });
  }

  /// Out-of-order marking: track `[0, size)` then mark ALL of them in a fully shuffled order. The
  /// running commit point always equals the highest contiguous-from-start marked offset (the
  /// length of the unbroken `0,1,2,...` prefix of marked offsets). Once the whole range is marked
  /// the final commit point is `size` (i.e. `max + 1`).
  @Property(tries = 300)
  void commitPointTracksContiguousPrefixUnderFullShuffle(@ForAll("shuffleScenarios") final ShuffleScenario scenario) {
    final var manager = newManager();
    final var tp = new TopicPartition(TOPIC, 0);
    try {
      for (var offset = 0L; offset < scenario.size(); offset++) manager.trackOffset(record(0, offset));

      final Set<Long> marked = new TreeSet<>();
      for (final var offset : scenario.markOrder()) {
        manager.markOffsetProcessed(record(0, offset));
        marked.add(offset);

        // Highest contiguous-from-zero marked offset: the length of the unbroken 0..k prefix.
        var contiguous = 0L;
        while (marked.contains(contiguous)) contiguous++;

        // While the prefix hasn't consumed the whole range, offset `contiguous` is still pending,
        // so the commit point must sit exactly there. Once everything is marked it is size.
        final var expected = contiguous; // pending-lowest when partial, == size when complete
        assertEquals(
          expected,
          commitPoint(manager, tp),
          "commit point must equal contiguous-from-start prefix length; order=%s".formatted(scenario.markOrder())
        );
      }

      assertEquals(scenario.size(), commitPoint(manager, tp), "fully-marked range commits max+1 == size");
      assertEquals(scenario.size() - 1, highestProcessed(manager, tp), "highestProcessed must be the top offset");
    } finally {
      manager.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // No commit-ahead and no loss across multiple partitions — independent, correct commit points;
  // nothing committable before it is marked.
  // ---------------------------------------------------------------------------------------------

  /// One generated step: track-or-mark a given offset on a given partition.
  private record MultiOp(int partition, boolean track, long offset) {}

  @Provide
  Arbitrary<List<MultiOp>> multiPartitionSequences() {
    final var partitions = Arbitraries.integers().between(0, 3);
    final var offsets = Arbitraries.longs().between(0L, 15L);
    final var kinds = Arbitraries.of(true, false);
    final Arbitrary<MultiOp> ops = Combinators.combine(partitions, kinds, offsets).as((p, track, off) ->
      new MultiOp(p, track, off)
    );
    return ops.list().ofMinSize(1).ofMaxSize(80);
  }

  /// No commit-ahead and no loss across several partitions interleaved in one sequence. Each
  /// partition keeps its own model (pending set + highest marked); after every op every touched
  /// partition's observable commit point must equal its independently-computed expected value.
  /// This proves commit points don't bleed between partitions and that an offset is never
  /// committable (counted toward the commit point) before its record is marked terminal.
  @Property(tries = 300)
  void perPartitionCommitPointsAreIndependentAndNeverAhead(@ForAll("multiPartitionSequences") final List<MultiOp> ops) {
    final var manager = newManager();
    try {
      // Per-partition model state.
      final var pendingByPartition = new HashMap<Integer, Set<Long>>();
      final var highestMarkedByPartition = new HashMap<Integer, Long>();
      final var touched = new LinkedHashSet<Integer>();

      for (final var op : ops) {
        final var pending = pendingByPartition.computeIfAbsent(op.partition(), _ -> new TreeSet<>());
        highestMarkedByPartition.putIfAbsent(op.partition(), -1L);
        touched.add(op.partition());

        if (op.track()) {
          manager.trackOffset(record(op.partition(), op.offset()));
          pending.add(op.offset());
        } else {
          // An unmarked offset is part of pending; marking is what makes it terminal/eligible.
          // Marking an offset that is not pending changes nothing.
          manager.markOffsetProcessed(record(op.partition(), op.offset()));
          if (pending.remove(op.offset())) highestMarkedByPartition.merge(op.partition(), op.offset(), Math::max);
        }

        // Every partition we have ever touched must still report its own correct commit point —
        // an op on partition A must not move partition B's commit point.
        for (final var p : touched) {
          final var tp = new TopicPartition(TOPIC, p);
          final var expected = expectedCommitPoint(pendingByPartition.get(p), highestMarkedByPartition.get(p));
          assertEquals(
            expected,
            commitPoint(manager, tp),
            "partition %d commit point diverged from independent model after op %s".formatted(p, op)
          );

          // No loss: never commit past the lowest still-unprocessed offset on this partition.
          final var partitionPending = pendingByPartition.get(p);
          if (!partitionPending.isEmpty()) {
            final var lowest = partitionPending.stream().mapToLong(Long::longValue).min().orElseThrow();
            assertTrue(
              commitPoint(manager, tp) <= lowest,
              "partition %d commit point %d skipped unprocessed offset %d".formatted(
                p,
                commitPoint(manager, tp),
                lowest
              )
            );
          }
        }
      }
    } finally {
      manager.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Terminal-before-eligible, sharpened: a tracked-but-unmarked offset can never be the thing that
  // advances the commit point. Track a range, mark a strict subset, assert the commit point is
  // never greater than the lowest UNmarked offset (it is held by the unmarked record).
  // ---------------------------------------------------------------------------------------------

  /// Across a tracked range with a randomly-chosen strict subset marked, the commit point is
  /// always held at or below the lowest tracked-but-unmarked offset. No unmarked offset is ever
  /// treated as committed — `highestProcessed + 1` becomes the commit point only when nothing is
  /// pending.
  @Property(tries = 300)
  void unmarkedOffsetIsNeverCounted(@ForAll @IntRange(min = 1, max = 30) final int size, @ForAll final long seed) {
    final var manager = newManager();
    final var tp = new TopicPartition(TOPIC, 0);
    try {
      for (var offset = 0L; offset < size; offset++) manager.trackOffset(record(0, offset));

      final var rnd = new Random(seed);
      final Set<Long> unmarked = new TreeSet<>();
      final var order = new ArrayList<Long>();
      for (var offset = 0L; offset < size; offset++) order.add(offset);
      Collections.shuffle(order, rnd);

      // Mark each offset with probability ~1/2; keep at least the rest unmarked so the line holds.
      for (final var offset : order) {
        if (rnd.nextBoolean()) {
          manager.markOffsetProcessed(record(0, offset));
        } else {
          unmarked.add(offset);
        }

        final var cp = commitPoint(manager, tp);
        if (!unmarked.isEmpty()) {
          final var lowestUnmarked = unmarked.stream().mapToLong(Long::longValue).min().orElseThrow();
          assertTrue(
            cp <= lowestUnmarked,
            "commit point %d counted an unmarked offset (lowest unmarked %d)".formatted(cp, lowestUnmarked)
          );
        }
      }
    } finally {
      manager.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Rebalance: ownership, the revoke commit, and records that finish after their partition left.
  // ---------------------------------------------------------------------------------------------

  private enum RebalanceKind {
    TRACK,
    MARK,
    REVOKE,
    LOST,
    ASSIGN,
  }

  /// One generated step. `offset` is ignored by the ownership steps.
  private record RebalanceOp(RebalanceKind kind, int partition, long offset) {}

  @Provide
  Arbitrary<List<RebalanceOp>> rebalanceSequences() {
    // Tracks and marks dominate and the offset range is small, so marks often hit offsets that
    // were pending when their partition was revoked, the case a late mark has to get right.
    final var kinds = Arbitraries.frequencyOf(
      Tuple.of(4, Arbitraries.just(RebalanceKind.TRACK)),
      Tuple.of(4, Arbitraries.just(RebalanceKind.MARK)),
      Tuple.of(1, Arbitraries.just(RebalanceKind.REVOKE)),
      Tuple.of(1, Arbitraries.just(RebalanceKind.LOST)),
      Tuple.of(2, Arbitraries.just(RebalanceKind.ASSIGN))
    );
    final var partitions = Arbitraries.integers().between(0, 2);
    final var offsets = Arbitraries.longs().between(0L, 5L);
    final Arbitrary<RebalanceOp> ops = Combinators.combine(kinds, partitions, offsets).as(RebalanceOp::new);
    return ops.list().ofMinSize(20).ofMaxSize(100);
  }

  /// Tracks, marks, revokes, losses and assignments interleaved across partitions, checked against
  /// an independent model after every step.
  ///
  /// The model follows the consumer's rules. Only an owned partition gets new records tracked,
  /// because a consumer only polls what it is assigned. A mark can arrive at any time, including
  /// after its partition was revoked, because a worker finishes a record on its own schedule; it
  /// counts only for an offset that is still pending. A revoke or a loss commits the partition's
  /// frontier and forgets the partition, and an assignment starts it from scratch.
  ///
  /// Three things must hold. A revoke commits exactly the model's frontier. Every partition's
  /// commit point matches the model. And a partition this member does not own has no commit point
  /// at all, however many of its records finish late, and never appears in what a commit would
  /// send, so no later commit can reach a partition another member owns. A loss is not required to
  /// commit: Kafka advises against committing for lost partitions, so only its clearing is checked.
  @Property(tries = 500)
  void rebalancesNeverLeaveACommitPointOnAPartitionThisMemberDoesNotOwn(
    @ForAll("rebalanceSequences") final List<RebalanceOp> ops
  ) throws InterruptedException {
    final var lastCommit = new HashMap<TopicPartition, Long>();
    final var consumer = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void commitSync(
        final java.util.Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets
      ) {
        super.commitSync(offsets);
        offsets.forEach((tp, meta) -> lastCommit.put(tp, meta.offset()));
      }
    };
    // Captures what each commit request would send, and completes it at once.
    final var requested = new HashMap<TopicPartition, Long>();
    final CommitExecutor capturing = offsets -> {
      requested.clear();
      offsets.get().forEach((tp, meta) -> requested.put(tp, meta.offset()));
      return CompletableFuture.completedFuture(true);
    };
    final var manager = KafkaOffsetManager.builder(consumer).withCommitExecutor(capturing).build();
    manager.start();
    final var listener = manager.createRebalanceListener();
    try {
      final var owned = new TreeSet<Integer>(List.of(0, 1, 2));
      final var pendingByPartition = new HashMap<Integer, Set<Long>>();
      final var highestMarkedByPartition = new HashMap<Integer, Long>();
      for (var p = 0; p < 3; p++) {
        pendingByPartition.put(p, new TreeSet<>());
        highestMarkedByPartition.put(p, -1L);
      }

      for (final var op : ops) {
        final var p = op.partition();
        final var tp = new TopicPartition(TOPIC, p);
        final var pending = pendingByPartition.get(p);
        switch (op.kind()) {
          case TRACK -> {
            if (!owned.contains(p)) continue;
            manager.trackOffset(record(p, op.offset()));
            pending.add(op.offset());
          }
          case MARK -> {
            manager.markOffsetProcessed(record(p, op.offset()));
            if (pending.remove(op.offset())) highestMarkedByPartition.merge(p, op.offset(), Math::max);
          }
          case REVOKE, LOST -> {
            if (!owned.contains(p)) continue;
            final var frontier = expectedCommitPoint(pending, highestMarkedByPartition.get(p));
            lastCommit.remove(tp);
            if (op.kind() == RebalanceKind.REVOKE) listener.onPartitionsRevoked(List.of(tp));
            else listener.onPartitionsLost(List.of(tp));
            if (op.kind() == RebalanceKind.REVOKE) {
              if (frontier >= 0) {
                assertEquals(
                  frontier,
                  lastCommit.get(tp),
                  "revoke of partition %d must commit its frontier".formatted(p)
                );
              } else {
                assertFalse(
                  lastCommit.containsKey(tp),
                  "revoke of an untouched partition %d must commit nothing".formatted(p)
                );
              }
            }
            owned.remove(p);
            pending.clear();
            highestMarkedByPartition.put(p, -1L);
          }
          case ASSIGN -> {
            if (owned.contains(p)) continue;
            listener.onPartitionsAssigned(List.of(tp));
            owned.add(p);
          }
        }

        requested.clear();
        assertTrue(manager.commitSyncAndWait(java.time.Duration.ofSeconds(1)), "a captured commit completes at once");
        for (final var entry : requested.entrySet()) {
          final var partition = entry.getKey().partition();
          assertTrue(
            owned.contains(partition),
            "a commit after %s would send partition %d, which is not owned".formatted(op, partition)
          );
          assertEquals(
            expectedCommitPoint(pendingByPartition.get(partition), highestMarkedByPartition.get(partition)),
            entry.getValue(),
            "a commit after %s would send the wrong offset for partition %d".formatted(op, partition)
          );
        }

        for (var partition = 0; partition < 3; partition++) {
          final var partitionTp = new TopicPartition(TOPIC, partition);
          final var expected = expectedCommitPoint(
            pendingByPartition.get(partition),
            highestMarkedByPartition.get(partition)
          );
          assertEquals(
            expected,
            commitPoint(manager, partitionTp),
            "partition %d commit point diverged from the model after %s".formatted(partition, op)
          );
          if (!owned.contains(partition)) {
            assertEquals(
              -1L,
              commitPoint(manager, partitionTp),
              "partition %d is not owned but has a commit point after %s".formatted(partition, op)
            );
          }
        }
      }
    } finally {
      manager.close();
    }
  }
}
