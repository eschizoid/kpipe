package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// Fray ports of the offset-manager read-path races.
///
/// The manager is never started in these scenarios. `start()` schedules a periodic commit task
/// on a `ScheduledThreadPoolExecutor` that lives until the manager is closed, and Fray does not
/// finish an iteration while any thread is still live, so a started manager wedges exploration
/// on the first schedule. Tracking and marking only skip their work once the manager reaches
/// `STOPPED`, so an unstarted manager in `CREATED` exercises the full read/write race.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class OffsetManagerFrayTest {

  private static final String TOPIC = "fray-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  /// A diagnostic read races the removal that empties the pending window.
  ///
  /// Three answers are all correct, which is the point: the reader takes the structure monitor
  /// but sees whatever the writer has committed so far. It can see 100 still pending; it can see
  /// the window drained and compute the frontier as highest-processed + 1; or it can catch the
  /// torn moment where the window already looks empty but the highest-processed write is not yet
  /// visible, giving the `-1` "nothing known for this partition" sentinel.
  ///
  /// What must never happen is a throw. The guarded `firstOrNull` exists because the natural
  /// `isEmpty()`-then-`first()` shape is a check-then-act that can throw `NoSuchElementException`
  /// when the last element leaves between the two calls.
  @FrayTest(iterations = 500)
  void frontierReadNeverThrowsWhileTheWindowDrains() {
    final var manager = newManager();
    manager.trackOffset(record(100L));
    final var observed = new AtomicLong(Long.MIN_VALUE);

    FrayScenarios.runConcurrently(
      () -> manager.markOffsetProcessed(record(100L)),
      () -> observed.set(manager.getPartitionState(PARTITION).nextOffsetToCommit())
    );

    final var seen = observed.get();
    assertTrue(
      seen == 100L || seen == 101L || seen == -1L,
      () ->
        "frontier read observed " +
        seen +
        ", which is neither the pending offset, the post-drain commit point, nor the " +
        "nothing-tracked sentinel"
    );
  }

  /// Two workers retire offsets on both sides of a permanent gap. Offset 100 is tracked and never
  /// marked, so it is the lowest pending offset for the whole scenario; the workers track and
  /// retire 101 and 102 around it. The commit point must stay pinned at 100 under every schedule.
  ///
  /// This is the at-least-once guarantee stated as an invariant: committing 102 because it
  /// finished would silently discard offset 100 on the next restart, since Kafka resumes from the
  /// committed position and never revisits earlier offsets.
  @FrayTest(iterations = 500)
  void commitPointHoldsAtTheGapWhenTheSuccessorRetires() {
    assertGapHolds(101L, 102L);
  }

  /// The same invariant with both retiring offsets above the gap's immediate successor, so the
  /// pending window has a hole at 101 as well. The frontier rule is "lowest still-pending offset",
  /// not "highest contiguous run", and this pins that distinction.
  @FrayTest(iterations = 500)
  void commitPointHoldsAtTheGapWhenNonAdjacentOffsetsRetire() {
    assertGapHolds(102L, 103L);
  }

  /// A partition revoke races a worker still tracking and retiring offsets on that partition.
  ///
  /// Three end states are legitimate, depending on where the revoke lands: the worker's track
  /// survives and pins the commit point at 100; the revoke clears everything and leaves the
  /// nothing-tracked sentinel; or the revoke clears the pending window after the worker's mark,
  /// leaving the retired offset's successor. The forbidden shape is a commit point that advanced
  /// past an offset still counted as pending — that combination means a record in flight during a
  /// rebalance was committed away.
  @FrayTest(iterations = 500)
  void revokeNeverCommitsPastAStillPendingOffset() {
    final var manager = newManager();
    final var rebalanceListener = manager.createRebalanceListener();
    manager.trackOffset(record(100L));

    FrayScenarios.runConcurrently(
      () -> rebalanceListener.onPartitionsRevoked(List.of(PARTITION)),
      () -> {
        manager.trackOffset(record(100L));
        manager.markOffsetProcessed(record(101L));
      }
    );

    final var state = manager.getPartitionState(PARTITION);
    final var seen = state.nextOffsetToCommit();
    final var pending = state.pendingCount() > 0;
    assertTrue(
      (seen == 100L && pending) || (seen == -1L && !pending) || (seen == 102L && !pending),
      () ->
        "revoke left commit point " +
        seen +
        " with pending=" +
        pending +
        "; a commit point past a still-pending offset would drop an in-flight record"
    );
  }

  /// Tracks offset 100 and leaves it pending, then has two workers track and retire `first` and
  /// `second` concurrently. The commit frontier must remain at 100 throughout.
  ///
  /// @param first  offset the first worker tracks and retires
  /// @param second offset the second worker tracks and retires
  private static void assertGapHolds(final long first, final long second) {
    final var manager = newManager();
    // Tracked and never marked: the permanent gap that pins the commit point.
    manager.trackOffset(record(100L));

    FrayScenarios.runConcurrently(
      () -> {
        manager.trackOffset(record(first));
        manager.markOffsetProcessed(record(first));
      },
      () -> {
        manager.trackOffset(record(second));
        manager.markOffsetProcessed(record(second));
      }
    );

    assertEquals(
      100L,
      manager.getPartitionState(PARTITION).nextOffsetToCommit(),
      "the commit point advanced past offset 100, which is still pending"
    );
  }

  private static KafkaOffsetManager newManager() {
    final var consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
    return KafkaOffsetManager.builder(consumer)
      .withCommitExecutor(offsets -> CompletableFuture.completedFuture(null))
      .build();
  }

  private static ConsumerRecord<byte[], byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, "k".getBytes(UTF_8), "v".getBytes(UTF_8));
  }
}
