package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

/// Property-based smoke test for the offset-lifecycle invariants — the commit point only ever
/// advances over contiguous completed offsets, never past a still-pending gap.
///
/// This is a SETUP/SMOKE test proving the jqwik toolchain runs against `KafkaOffsetManager`. It is
/// deliberately scoped to a single partition and asserts the lowest-pending invariant: the
/// commit point never advances past a gap. A later fleet adds exhaustive coverage.
///
/// The generated input is a randomized sequence of track/mark operations over a small offset space.
/// After every operation the test reconstructs the expected pending set and asserts the manager's
/// observable commit point (`nextOffsetToCommit`) never passes the lowest still-pending offset.
class OffsetInvariantPropertyTest {

  private static final String TOPIC = "prop-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  /// A single generated operation: track or mark a given offset.
  private record Op(boolean track, long offset) {}

  @Provide
  Arbitrary<List<Op>> operationSequences() {
    final var offsets = Arbitraries.longs().between(0L, 20L);
    final var kinds = Arbitraries.of(true, false);
    final Arbitrary<Op> ops = Combinators.combine(kinds, offsets).as(Op::new);
    return ops.list().ofMinSize(1).ofMaxSize(40);
  }

  @Property(tries = 200)
  void commitPointNeverPassesAGap(@ForAll("operationSequences") final List<Op> ops) {
    final var consumer = new MockConsumer<String, byte[]>(OffsetResetStrategy.EARLIEST);
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();
    final var manager = KafkaOffsetManager.builder(consumer).withCommandQueue(commandQueue).build();
    manager.start();

    try {
      // Model state: offsets tracked-but-not-yet-marked (the pending set) and the highest marked.
      final Set<Long> pending = new LinkedHashSet<>();
      var highestMarked = -1L;

      for (final var op : ops) {
        final var record = new ConsumerRecord<>(TOPIC, 0, op.offset(), "k", "v".getBytes());

        if (op.track()) {
          manager.trackOffset(record);
          pending.add(op.offset());
        } else {
          manager.markOffsetProcessed(record);
          pending.remove(op.offset());
          highestMarked = Math.max(highestMarked, op.offset());
        }

        final var commitPoint = (long) manager.getPartitionState(PARTITION).get("nextOffsetToCommit");

        if (!pending.isEmpty()) {
          final var lowestPending = pending.stream().mapToLong(Long::longValue).min().orElseThrow();
          // The commit point must never pass the lowest still-pending offset.
          assertTrue(
            commitPoint <= lowestPending,
            "commit point %d must not pass lowest pending offset %d".formatted(commitPoint, lowestPending)
          );
        } else if (highestMarked >= 0) {
          // Pending set empty and something was marked: commit point is highest-marked + 1.
          assertEquals(highestMarked + 1, commitPoint, "commit point with empty pending set must be highestMarked + 1");
        }
      }
    } finally {
      manager.close();
    }
  }
}
