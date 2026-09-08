package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// Fray ports of the dispatcher races that do not saturate the key cap.
///
/// Neither scenario reaches `reserveCapacity`'s saturation stall, so both explore to completion:
/// the handoff test runs a single key against the default 10,000-key cap, and the drainable-count
/// test uses [ParallelDispatcher], which has no per-key map and no stall loop at all.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class DispatcherFrayTest {

  private static final String TOPIC = "fray-topic";
  private static final byte[] SHARED_KEY = "shared-key".getBytes(UTF_8);

  /// Two records for the same key are dispatched concurrently. Each key has one serial queue
  /// drained by one worker, and the worker exits when its queue empties — so the dangerous
  /// schedule is the second dispatch arriving exactly as the first worker decides it is done.
  /// Losing that handoff drops the record silently: no error, no retry, just a task that never
  /// runs.
  /// `abortThreadExecutionAfterMainExit` is required, not cosmetic. At main exit Fray waits for
  /// every registered thread to complete, excluding only ForkJoinWorkerThreads belonging to its
  /// own tracked pool. Virtual-thread carriers are ForkJoinWorkerThreads in the JDK's
  /// VirtualThread scheduler pool, which is a different pool, so Fray waits for threads that
  /// park for work and never complete — the iteration then never ends and the run reports
  /// `Iterations: 0` until the job is killed. The flag lets Fray abort those stragglers once the
  /// test body has returned.
  @FrayTest(iterations = 500, abortThreadExecutionAfterMainExit = true)
  void sameKeyHandoffNeverLosesATask() {
    final var dispatcher = new KeyOrderedDispatcher(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var tasksRun = new AtomicInteger();
    final var done = new CountDownLatch(2);

    FrayScenarios.runConcurrently(
      () -> dispatcher.dispatch(record(SHARED_KEY, 0L), tasksRun::incrementAndGet, done::countDown),
      () -> dispatcher.dispatch(record(SHARED_KEY, 1L), tasksRun::incrementAndGet, done::countDown)
    );
    await(done);
    dispatcher.close();

    assertEquals(2, tasksRun.get(), "a same-key task was lost or ran more than once");
  }

  /// A normal record and a throwing record settle concurrently. `drainableCount` is what the
  /// in-flight backpressure watermark reads, so the accounting has to hold on both paths: a
  /// decrement that runs without a matching increment drives the count negative, and an
  /// increment whose decrement is skipped on the throwing path leaves the consumer permanently
  /// believing work is outstanding.
  /// `abortThreadExecutionAfterMainExit` is required, not cosmetic. At main exit Fray waits for
  /// every registered thread to complete, excluding only ForkJoinWorkerThreads belonging to its
  /// own tracked pool. Virtual-thread carriers are ForkJoinWorkerThreads in the JDK's
  /// VirtualThread scheduler pool, which is a different pool, so Fray waits for threads that
  /// park for work and never complete — the iteration then never ends and the run reports
  /// `Iterations: 0` until the job is killed. The flag lets Fray abort those stragglers once the
  /// test body has returned.
  @FrayTest(iterations = 500, abortThreadExecutionAfterMainExit = true)
  void drainableCountBalancesAcrossNormalAndThrowingRecords() {
    final var dispatcher = new ParallelDispatcher((_, _) -> {}, Duration.ofSeconds(5));
    final var normalDone = new CountDownLatch(1);
    final var throwDone = new CountDownLatch(1);
    final var afterNormal = new AtomicLong();
    final var afterThrow = new AtomicLong();

    FrayScenarios.runConcurrently(
      () -> {
        dispatcher.dispatch(record(SHARED_KEY, 1L), () -> {}, normalDone::countDown);
        await(normalDone);
        afterNormal.set(dispatcher.drainableCount());
      },
      () -> {
        dispatcher.dispatch(
          record(SHARED_KEY, 2L),
          () -> {
            throw new RuntimeException("boom");
          },
          throwDone::countDown
        );
        await(throwDone);
        afterThrow.set(dispatcher.drainableCount());
      }
    );
    await(normalDone);
    await(throwDone);
    final var finalCount = dispatcher.drainableCount();
    dispatcher.close();

    // A snapshot taken after one record's own decrement may still see its sibling in flight, so
    // 0 and 1 are both legal here; what must never appear is a negative count.
    assertTrue(afterNormal.get() >= 0, () -> "negative drainable count after the normal record: " + afterNormal.get());
    assertTrue(afterThrow.get() >= 0, () -> "negative drainable count after the throwing record: " + afterThrow.get());
    assertEquals(0L, finalCount, "the drainable count did not settle to zero once both records completed");
  }

  /// Unbounded on purpose: under a controlled scheduler a real-time deadline measures the
  /// exploration order rather than the code's liveness, and Fray reports a schedule that cannot
  /// finish as a deadlock.
  private static void await(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted awaiting dispatch completion", e);
    }
  }

  private static ConsumerRecord<byte[], byte[]> record(final byte[] key, final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, key, ("v-" + offset).getBytes(UTF_8));
  }
}
