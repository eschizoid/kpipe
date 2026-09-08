package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// Fray port of the eviction-tombstone and evict-race stress tests.
///
/// **The window.** [KeyOrderedDispatcher] evicts an empty, idle key queue to make room for a new
/// key. A dispatcher that read that queue out of the map a moment earlier still holds a live
/// reference to it. Eviction marks the queue `dead` under its monitor, atomically with the map
/// removal, and the dispatcher re-checks `dead` under the same monitor before enqueuing — the
/// retry that closes the window. Losing that check does not lose the record: the dispatcher
/// enqueues into the orphaned queue and starts a worker on it, so the task still runs. What breaks
/// is per-key serialization. The orphan is unreachable through the map, so the *next* record for
/// that key allocates a second queue and a second worker, and two workers then process the same
/// key at once.
///
/// **Why a permanently idle queue is part of the setup.** `reserveCapacity` waits by sleeping when
/// the cap is saturated and nothing is evictable, and Fray models a sleep as a yield, so a thread
/// in that loop stays runnable forever and exploration never terminates. Key B is seeded, drained,
/// and then never dispatched to again, so an idle queue is always available and `evictOneIdle`
/// always succeeds on its first attempt. Eviction still races the dispatcher exactly as before —
/// the loop that cannot be explored is simply never entered. Cap 2 with at most two evictions
/// needed keeps that guarantee: A can be evicted for C, and B for A's reallocation.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class KeyOrderedEvictTombstoneFrayTest {

  private static final String TOPIC = "fray-topic";
  private static final byte[] KEY_A = "key-a".getBytes(UTF_8);
  private static final byte[] KEY_B = "key-b".getBytes(UTF_8);
  private static final byte[] KEY_C = "key-c".getBytes(UTF_8);

  /// One thread dispatches two records for key A back to back, mirroring production where a single
  /// consumer thread dispatches, while another forces an eviction by introducing key C. Every task
  /// must run exactly once, and key A's two tasks must never overlap.
  /// `abortThreadExecutionAfterMainExit` is required, not cosmetic. At main exit Fray waits for
  /// every registered thread to complete, excluding only ForkJoinWorkerThreads belonging to its
  /// own tracked pool. Virtual-thread carriers are ForkJoinWorkerThreads in the JDK's
  /// VirtualThread scheduler pool, which is a different pool, so Fray waits for threads that
  /// park for work and never complete — the iteration then never ends and the run reports
  /// `Iterations: 0` until the job is killed. The flag lets Fray abort those stragglers once the
  /// test body has returned.
  @FrayTest(iterations = 500, abortThreadExecutionAfterMainExit = true)
  void evictionNeverBreaksPerKeySerialization() {
    final var dispatcher = new KeyOrderedDispatcher(2);
    seedAndDrain(dispatcher, KEY_A, 0L);
    seedAndDrain(dispatcher, KEY_B, 1L);

    final var tasksRun = new AtomicInteger();
    final var concurrentOnA = new AtomicInteger();
    final var maxConcurrentOnA = new AtomicInteger();
    final var done = new CountDownLatch(3);
    final Runnable keyATask = () -> {
      final var live = concurrentOnA.incrementAndGet();
      maxConcurrentOnA.accumulateAndGet(live, Math::max);
      tasksRun.incrementAndGet();
      concurrentOnA.decrementAndGet();
    };

    FrayScenarios.runConcurrently(
      () -> {
        dispatcher.dispatch(record(KEY_A, 10L), keyATask, done::countDown);
        dispatcher.dispatch(record(KEY_A, 11L), keyATask, done::countDown);
      },
      () -> dispatcher.dispatch(record(KEY_C, 12L), tasksRun::incrementAndGet, done::countDown)
    );
    await(done);
    dispatcher.close();

    assertEquals(3, tasksRun.get(), "a task was lost or ran more than once across the eviction");
    assertEquals(
      1,
      maxConcurrentOnA.get(),
      "two workers processed key A at the same time: a dispatcher enqueued into an evicted queue "
        + "and the next record for that key allocated a second queue and worker"
    );
  }

  /// Dispatches one record for the key and waits for it to finish, leaving the queue present in
  /// the map but empty and idle — the exact state eviction looks for.
  private static void seedAndDrain(final KeyOrderedDispatcher dispatcher, final byte[] key, final long offset) {
    final var seeded = new CountDownLatch(1);
    dispatcher.dispatch(record(key, offset), () -> {}, seeded::countDown);
    await(seeded);
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
