package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies that {@link ConsumerCommand} is thread-safe when used concurrently from
/// multiple virtual threads, as happens during parallel record processing.
@DisplayName("ConsumerCommand concurrency")
class ConsumerCommandConcurrencyTest {

  @Test
  @DisplayName("concurrent TrackOffset commands preserve per-record identity")
  void concurrentTrackOffsetCommandsPreservePerRecordIdentity() throws Exception {
    final var queue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var threadCount = 100;
    final var latch = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      final var record = new ConsumerRecord<>("topic", 0, (long) i, "key-" + i, "value-" + i);
      Thread.ofVirtual().start(() -> {
        queue.offer(new ConsumerCommand.TrackOffset(record));
        latch.countDown();
      });
    }

    assertTrue(latch.await(5, TimeUnit.SECONDS), "All threads should complete");
    assertEquals(threadCount, queue.size(), "Every command should be in the queue");

    // Each command must reference its own distinct record, no cross-contamination
    final var seenOffsets = new java.util.concurrent.ConcurrentSkipListSet<Long>();
    for (final var cmd : queue) {
      final var trackCmd = assertInstanceOf(ConsumerCommand.TrackOffset.class, cmd);
      assertTrue(
        seenOffsets.add(trackCmd.record().offset()),
        "Offset %d appeared more than once - indicates a thread-safety issue".formatted(trackCmd.record().offset())
      );
    }

    assertEquals(threadCount, seenOffsets.size(), "All offsets should be unique");
  }

  @Test
  @DisplayName("concurrent MarkOffsetProcessed commands preserve per-record identity")
  void concurrentMarkOffsetProcessedCommandsPreservePerRecordIdentity() throws Exception {
    final var queue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var threadCount = 100;
    final var latch = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      final var record = new ConsumerRecord<>("topic", 0, (long) i, "key-" + i, "value-" + i);
      Thread.ofVirtual().start(() -> {
        queue.offer(new ConsumerCommand.MarkOffsetProcessed(record));
        latch.countDown();
      });
    }

    assertTrue(latch.await(5, TimeUnit.SECONDS), "All threads should complete");
    assertEquals(threadCount, queue.size());

    final var seenOffsets = new java.util.concurrent.ConcurrentSkipListSet<Long>();
    for (final var cmd : queue) {
      final var mopCmd = assertInstanceOf(ConsumerCommand.MarkOffsetProcessed.class, cmd);
      assertTrue(
        seenOffsets.add(mopCmd.record().offset()),
        "Offset %d appeared more than once - indicates a thread-safety issue".formatted(mopCmd.record().offset())
      );
    }

    assertEquals(threadCount, seenOffsets.size());
  }

  @Test
  @DisplayName("immutable command records cannot be mutated after creation")
  void immutableCommandRecordsCannotBeMutatedAfterCreation() {
    final var record1 = new ConsumerRecord<>("topic", 0, 1L, "key-1", "value-1");
    final var record2 = new ConsumerRecord<>("topic", 0, 2L, "key-2", "value-2");

    final var cmd1 = new ConsumerCommand.TrackOffset(record1);
    final var cmd2 = new ConsumerCommand.TrackOffset(record2);

    // Each command is its own instance with its own record, no shared mutable state
    assertNotSame(cmd1, cmd2);
    assertEquals(1L, cmd1.record().offset());
    assertEquals(2L, cmd2.record().offset());

    // Creating cmd2 did not affect cmd1's record
    assertEquals(1L, cmd1.record().offset(), "cmd1 should still reference record1 after cmd2 was created");
  }
}
