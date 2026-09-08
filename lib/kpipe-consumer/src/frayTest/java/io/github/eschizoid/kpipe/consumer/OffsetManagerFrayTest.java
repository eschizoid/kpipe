package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
