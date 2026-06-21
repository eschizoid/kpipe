package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// Deterministic concurrency-stress tests for `KafkaOffsetManager`.
///
/// These hammer real virtual-thread interleavings against the offset-lifecycle invariants spelled
/// out in `OFFSET-INVARIANTS.md` (I1–I5). They are the runtime layer that exercises the same
/// state machine a concurrency model-checker would have explored, scaled up to thousands of ops so
/// genuine races surface rather than staying theoretical.
///
/// Conventions:
///
/// * Worker threads are real virtual threads (`Thread.ofVirtual()`), not `CompletableFuture`, so
///   they exercise actual VT scheduling behaviour.
/// * A `CountDownLatch` start-gate releases every worker at once to maximize true overlap, and a
///   second latch joins them; thrown errors are collected into a thread-safe list and asserted
///   empty.
/// * Invariant assertions read the manager's own observable state through
///   `getPartitionState(partition)` — the single value (`nextOffsetToCommit`) every invariant
///   constrains — both DURING the storm (a concurrent probe thread) and after it drains.
///
/// The commit point per the spec: if any offset is still pending → the lowest pending offset; else
/// if anything was processed → `highestProcessedOffset + 1`; else `-1`.
class OffsetConcurrencyStressTest {

  private static final String TOPIC = "stress-topic";

  private Consumer<String, byte[]> newMockConsumer() {
    @SuppressWarnings("unchecked")
    final Consumer<String, byte[]> consumer = mock(Consumer.class);
    return consumer;
  }

  private KafkaOffsetManager<String> newManager(final Consumer<String, byte[]> consumer) {
    return KafkaOffsetManager.builder(consumer).withCommandQueue(new LinkedBlockingQueue<>()).build();
  }

  private static ConsumerRecord<String, byte[]> record(final int partition, final long offset) {
    return new ConsumerRecord<>(TOPIC, partition, offset, "k", "v".getBytes());
  }

  private static long commitPoint(final KafkaOffsetManager<String> manager, final TopicPartition partition) {
    return (long) manager.getPartitionState(partition).get("nextOffsetToCommit");
  }

  /// I1 under parallel marking: N virtual threads each own a disjoint contiguous slice of offsets
  /// on the SAME partition. Each thread tracks its whole slice, then marks them in a deterministic
  /// but per-thread-shuffled order, so at every instant there are gaps spread across the partition.
  ///
  /// The load-bearing assertion is the concurrent probe: a separate thread continuously reads the
  /// commit point and asserts it never sits above the lowest still-pending offset the probe can
  /// observe. Because reads of `pendingOffsets` and `highestProcessedOffsets` aren't a single
  /// atomic snapshot, the probe asserts the weaker-but-sound bound that the commit point never
  /// exceeds `lowestPendingOffset` when one is reported — i.e. the commit point can never jump a
  /// reported gap. After the storm drains, the commit point must be exactly `total` (every offset
  /// `0..total-1` marked → `highestProcessed + 1`).
  @Test
  void i1_commitPointNeverPassesAGapUnderParallelMarking() throws Exception {
    final var consumer = newMockConsumer();
    final var manager = newManager(consumer);
    manager.start();

    final var partition = new TopicPartition(TOPIC, 0);
    final int threadCount = 24;
    final int offsetsPerThread = 400;
    final int total = threadCount * offsetsPerThread;

    final var startGate = new CountDownLatch(1);
    final var doneLatch = new CountDownLatch(threadCount);
    final var errors = Collections.synchronizedList(new ArrayList<Throwable>());
    final var stopProbe = new AtomicBoolean(false);

    // Concurrent probe: while the storm runs, the commit point must never pass a reported gap.
    final var probe = Thread.ofVirtual().start(() -> {
      while (!stopProbe.get()) {
        try {
          final var s = manager.getPartitionState(partition);
          final var next = (long) s.get("nextOffsetToCommit");
          final var lowestPending = s.get("lowestPendingOffset");
          if (lowestPending != null) {
            // Commit point can never exceed the lowest pending offset — it cannot jump the gap.
            if (next > (long) lowestPending) {
              errors.add(
                new AssertionError(
                  "commit point %d passed pending gap at %d".formatted(next, (long) lowestPending)
                )
              );
            }
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }
    });

    final var workers = new ArrayList<Thread>();
    for (int t = 0; t < threadCount; t++) {
      final int threadId = t;
      workers.add(
        Thread.ofVirtual().start(() -> {
          try {
            startGate.await();
            final var base = threadId * offsetsPerThread;
            // Track the whole slice first so gaps genuinely exist across the partition.
            for (int i = 0; i < offsetsPerThread; i++) manager.trackOffset(record(0, base + i));
            // Mark in a thread-specific rotated order — interleaves out-of-order completion.
            final int rotate = (threadId * 37) % offsetsPerThread;
            for (int j = 0; j < offsetsPerThread; j++) {
              final int i = (j + rotate) % offsetsPerThread;
              manager.markOffsetProcessed(record(0, base + i));
            }
          } catch (final Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        })
      );
    }

    startGate.countDown();
    assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "all workers should finish");
    stopProbe.set(true);
    probe.join(TimeUnit.SECONDS.toMillis(5));

    assertTrue(errors.isEmpty(), "no invariant violation under parallel marking: " + errors);

    // After drain: everything marked, no pending → commit point is exactly total.
    final var finalState = manager.getPartitionState(partition);
    assertEquals(0, (int) finalState.get("pendingCount"), "no offset left pending");
    assertEquals((long) total, commitPoint(manager, partition), "commit point is exactly highestProcessed+1");
    assertEquals(total - 1L, (long) finalState.get("highestProcessedOffset"));

    manager.close();
  }

  /// I1 explicit gap-hold: with a single low offset (100) deliberately left pending while many
  /// threads track+mark a dense block ABOVE it (101..N), the commit point must stay pinned at 100
  /// throughout — it can never advance to 101+ while 100 holds the line. Once 100 is finally
  /// marked, the commit point jumps straight to the contiguous high-water mark.
  @Test
  void i1_lowPendingOffsetPinsCommitPointAcrossConcurrentHighMarks() throws Exception {
    final var consumer = newMockConsumer();
    final var manager = newManager(consumer);
    manager.start();

    final var partition = new TopicPartition(TOPIC, 0);
    final long held = 100L;
    final int highThreads = 16;
    final int highPerThread = 500;
    final int highCount = highThreads * highPerThread; // offsets 101 .. 100+highCount

    manager.trackOffset(record(0, held)); // 100 stays pending the whole time

    final var startGate = new CountDownLatch(1);
    final var doneLatch = new CountDownLatch(highThreads);
    final var errors = Collections.synchronizedList(new ArrayList<Throwable>());
    final var stopProbe = new AtomicBoolean(false);

    final var probe = Thread.ofVirtual().start(() -> {
      while (!stopProbe.get()) {
        final var next = commitPoint(manager, partition);
        if (next != held) {
          errors.add(new AssertionError("commit point moved off held offset 100 to " + next));
        }
      }
    });

    for (int t = 0; t < highThreads; t++) {
      final int threadId = t;
      Thread.ofVirtual().start(() -> {
        try {
          startGate.await();
          final long base = held + 1 + (long) threadId * highPerThread;
          for (int i = 0; i < highPerThread; i++) {
            final var rec = record(0, base + i);
            manager.trackOffset(rec);
            manager.markOffsetProcessed(rec);
          }
        } catch (final Throwable e) {
          errors.add(e);
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startGate.countDown();
    assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "high-offset workers should finish");
    stopProbe.set(true);
    probe.join(TimeUnit.SECONDS.toMillis(5));

    assertTrue(errors.isEmpty(), "commit point must stay pinned at 100 while it is pending: " + errors);
    assertEquals(held, commitPoint(manager, partition), "still pinned at 100 before it is marked");

    // Release the held offset — commit point jumps over the now-contiguous run.
    manager.markOffsetProcessed(record(0, held));
    assertEquals(held + 1 + highCount, commitPoint(manager, partition), "commit point jumps to contiguous high-water");

    manager.close();
  }

  /// I5 revoke-during-processing: one dedicated "consumer thread" repeatedly fires the rebalance
  /// listener's `onPartitionsRevoked` for partition 0 while many worker virtual threads keep
  /// calling `trackOffset` / `markOffsetProcessed` for that same partition.
  ///
  /// The manager pins its rebalance listener to whichever thread first invokes it and asserts every
  /// later callback runs on that same thread (the command-queue single-writer invariant). So EVERY
  /// `onPartitionsRevoked` call — the storm loop AND the final quiescent revoke — must run on the
  /// one dedicated thread; the main test thread only ever reads `getPartitionState` (unguarded) and
  /// does `trackOffset` / `markOffsetProcessed` (not thread-pinned). The dedicated thread runs its
  /// final revoke as its last action before exiting so the cleared-state assertions never touch the
  /// listener from another thread.
  ///
  /// Assertions: no exception escapes any thread; a revoke followed by no further marks leaves a
  /// cleared partition (commit point `-1`); and at no point does the partition report a commit point
  /// that skips a still-pending offset (revive-after-revoke must still obey lowest-pending).
  @Test
  void i5_revokeDuringProcessingIsCleanAndNeverCorrupts() throws Exception {
    final var consumer = newMockConsumer();
    final var manager = newManager(consumer);
    manager.start();

    final var partition = new TopicPartition(TOPIC, 0);
    final var listener = manager.createRebalanceListener();
    final var revokedList = List.of(partition);

    final int workerCount = 16;
    final var startGate = new CountDownLatch(1);
    final var doneLatch = new CountDownLatch(workerCount);
    final var errors = Collections.synchronizedList(new ArrayList<Throwable>());
    final var offsetSeq = new AtomicLong(0);
    final var stop = new AtomicBoolean(false);
    final var revokeCount = new AtomicLong(0);
    final var finalRevokeDone = new CountDownLatch(1);

    // Dedicated listener thread: owns ALL listener callbacks (single-writer invariant) plus the
    // concurrent commit-point probe that asserts no skipped-gap corruption. It keeps firing revokes
    // until signalled to stop, then runs ONE final quiescent revoke as its last action so the
    // cleared-state assertions never touch the listener from another thread.
    final var listenerThread = Thread.ofVirtual().start(() -> {
      try {
        startGate.await();
        while (!stop.get()) {
          // Revoke partition 0 — commits commit point, clears state.
          listener.onPartitionsRevoked(revokedList);
          revokeCount.incrementAndGet();
          // Immediately probe: any pending offset must still pin the commit point at or below it.
          final var s = manager.getPartitionState(partition);
          final var next = (long) s.get("nextOffsetToCommit");
          final var lowestPending = s.get("lowestPendingOffset");
          if (lowestPending != null && next > (long) lowestPending) {
            errors.add(
              new AssertionError("post-revoke commit point %d skipped pending %d".formatted(next, (long) lowestPending))
            );
          }
          Thread.yield();
        }
        // Final revoke after all marks stopped, still on this same thread → state fully cleared.
        listener.onPartitionsRevoked(revokedList);
      } catch (final Throwable e) {
        errors.add(e);
      } finally {
        finalRevokeDone.countDown();
      }
    });

    final var workers = new ArrayList<Thread>();
    for (int t = 0; t < workerCount; t++) {
      workers.add(
        Thread.ofVirtual().start(() -> {
          try {
            startGate.await();
            for (int i = 0; i < 2000; i++) {
              final var off = offsetSeq.getAndIncrement();
              final var rec = record(0, off);
              // mark-for-revoked-partition is the I5 case — must be a clean no-op, never throw.
              manager.trackOffset(rec);
              manager.markOffsetProcessed(rec);
            }
          } catch (final Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        })
      );
    }

    startGate.countDown();
    assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "workers should finish under revoke churn");
    stop.set(true);
    assertTrue(finalRevokeDone.await(5, TimeUnit.SECONDS), "listener thread should run its final revoke");
    listenerThread.join(TimeUnit.SECONDS.toMillis(5));

    assertTrue(errors.isEmpty(), "revoke-during-processing must be clean and never corrupt: " + errors);
    assertTrue(revokeCount.get() > 0, "revoke should have fired at least once");

    // After the dedicated thread's final quiescent revoke: state fully cleared, commit point -1.
    final var cleared = manager.getPartitionState(partition);
    assertEquals(-1L, (long) cleared.get("nextOffsetToCommit"), "revoked partition reports cleared commit point");
    assertEquals(0, (int) cleared.get("pendingCount"), "revoked partition has no pending offsets");

    // I5 explicit: a mark for the already-revoked/cleared partition is a clean no-op (no throw), and
    // the lazily re-created entry still obeys lowest-pending. trackOffset / markOffsetProcessed are
    // not thread-pinned, so running them from the test thread is legitimate.
    final var lateTrack = record(0, 5L);
    final var lateGap = record(0, 7L);
    assertDoesNotThrow(() -> manager.trackOffset(lateTrack));
    assertDoesNotThrow(() -> manager.trackOffset(lateGap));
    assertDoesNotThrow(() -> manager.markOffsetProcessed(lateGap)); // mark 7 while 5 pending
    assertEquals(5L, commitPoint(manager, partition), "re-created entry still pins commit at lowest pending (5)");

    manager.close();
  }

  /// Multi-partition parallelism: many virtual threads track+mark dense slices across MANY
  /// partitions concurrently. Each partition's worker set owns a disjoint contiguous offset range,
  /// and a concurrent probe sweeps every partition asserting no commit point ever passes a reported
  /// gap. After drain, each partition independently reports the exact contiguous commit point —
  /// proving per-partition state never bleeds across partitions.
  @Test
  void multiPartition_independenceHoldsUnderParallelLoad() throws Exception {
    final var consumer = newMockConsumer();
    final var manager = newManager(consumer);
    manager.start();

    final int partitionCount = 8;
    final int threadsPerPartition = 6;
    final int offsetsPerThread = 300;
    final int perPartitionTotal = threadsPerPartition * offsetsPerThread;
    final int totalThreads = partitionCount * threadsPerPartition;

    final var partitions = new ArrayList<TopicPartition>();
    for (int p = 0; p < partitionCount; p++) partitions.add(new TopicPartition(TOPIC, p));

    final var startGate = new CountDownLatch(1);
    final var doneLatch = new CountDownLatch(totalThreads);
    final var errors = Collections.synchronizedList(new ArrayList<Throwable>());
    final var stopProbe = new AtomicBoolean(false);

    final var probe = Thread.ofVirtual().start(() -> {
      while (!stopProbe.get()) {
        for (final var partition : partitions) {
          try {
            final var s = manager.getPartitionState(partition);
            final var next = (long) s.get("nextOffsetToCommit");
            final var lowestPending = s.get("lowestPendingOffset");
            if (lowestPending != null && next > (long) lowestPending) {
              errors.add(
                new AssertionError(
                  "partition %s commit point %d passed gap %d".formatted(partition, next, (long) lowestPending)
                )
              );
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }
      }
    });

    for (int p = 0; p < partitionCount; p++) {
      final int part = p;
      for (int t = 0; t < threadsPerPartition; t++) {
        final int threadId = t;
        Thread.ofVirtual().start(() -> {
          try {
            startGate.await();
            final int base = threadId * offsetsPerThread;
            for (int i = 0; i < offsetsPerThread; i++) manager.trackOffset(record(part, base + i));
            final int rotate = (threadId * 53) % offsetsPerThread;
            for (int j = 0; j < offsetsPerThread; j++) {
              final int i = (j + rotate) % offsetsPerThread;
              manager.markOffsetProcessed(record(part, base + i));
            }
          } catch (final Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }
    }

    startGate.countDown();
    assertTrue(doneLatch.await(45, TimeUnit.SECONDS), "all multi-partition workers should finish");
    stopProbe.set(true);
    probe.join(TimeUnit.SECONDS.toMillis(5));

    assertTrue(errors.isEmpty(), "no cross-partition corruption or gap-skip: " + errors);

    // Each partition independently reaches the exact contiguous commit point.
    for (final var partition : partitions) {
      final var s = manager.getPartitionState(partition);
      assertEquals(0, (int) s.get("pendingCount"), "partition " + partition + " fully drained");
      assertEquals(
        (long) perPartitionTotal,
        (long) s.get("nextOffsetToCommit"),
        "partition " + partition + " commits exactly highestProcessed+1"
      );
      assertEquals(perPartitionTotal - 1L, (long) s.get("highestProcessedOffset"), "partition " + partition);
    }

    manager.close();
  }
}
