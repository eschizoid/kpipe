package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// The remove-if-empty race.
///
/// **The race.** Per-partition pending offsets live in a `ConcurrentHashMap`. Retiring the last
/// pending offset empties its set and drops the map key. If tracking a fresh offset adds to that
/// set outside the same bucket lock, the add can land on a set that was concurrently emptied and
/// unlinked — orphaned — silently losing the offset and letting the commit point advance past a
/// record that is still in flight. Production hits this exact pairing: `trackOffset` runs on the
/// poll thread while `markOffsetProcessed` runs on worker virtual threads for the same partition.
///
/// **Falsifiable in one edit.** Replacing the atomic `computeIfPresent` remove-if-empty with a
/// separate `if (isEmpty()) remove(key)` reopens the window; a run that still passes after that
/// edit is not exploring.
///
/// Nothing on this path waits by sleeping and the scenario leaves no thread running when the body
/// returns, so every schedule terminates.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class RemoveIfEmptyFrayTest {

  private static final String TOPIC = "fray-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
  private static final long RETIRED_OFFSET = 100L;
  private static final long TRACKED_OFFSET = 200L;

  @FrayTest(iterations = 500)
  void freshlyTrackedOffsetSurvivesTheConcurrentRetire() {
    final var manager = newManager();
    // Offset 100 is the only pending offset, so retiring it empties the set and drops the key.
    manager.trackOffset(record(RETIRED_OFFSET));

    final var retire = new Thread(() -> manager.markOffsetProcessed(record(RETIRED_OFFSET)), "fray-retire");
    final var track = new Thread(() -> manager.trackOffset(record(TRACKED_OFFSET)), "fray-track");
    retire.start();
    track.start();
    join(retire);
    join(track);

    final var state = manager.getPartitionState(PARTITION);
    assertEquals(
      TRACKED_OFFSET,
      state.nextOffsetToCommit(),
      "offset 200 was lost to the concurrent remove-if-empty, so the commit point advanced past a " +
        "record that never finished"
    );
    assertEquals(1, state.pendingCount(), "the partition should hold exactly the one still-pending offset");
  }

  /// Deliberately never started. `start()` schedules a periodic commit task on a
  /// `ScheduledThreadPoolExecutor` that runs until the manager is closed, and Fray does not finish
  /// an iteration while any thread is still live — a started manager wedges exploration on the
  /// first schedule. Neither `trackOffset` nor `markOffsetProcessed` needs the scheduler: both
  /// mutate the ledger directly and only skip their work once the manager reaches `STOPPED`, so an
  /// unstarted manager in `CREATED` exercises the full race. The commit executor is likewise a
  /// completed future rather than a pending one, so nothing can block on it.
  private static KafkaOffsetManager newManager() {
    final var consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
    return KafkaOffsetManager.builder(consumer)
      .withCommitExecutor(offsets -> CompletableFuture.completedFuture(null))
      .build();
  }

  private static ConsumerRecord<byte[], byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, "k".getBytes(UTF_8), "v".getBytes(UTF_8));
  }

  /// Unbounded on purpose. Both threads run to completion without waiting on each other, so a
  /// real-time deadline here would measure Fray's exploration order rather than the code's
  /// liveness; a schedule that genuinely fails to terminate is reported by Fray as a deadlock.
  private static void join(final Thread thread) {
    try {
      thread.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted joining " + thread.getName(), e);
    }
  }
}
