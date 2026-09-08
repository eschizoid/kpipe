package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
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
/// **Why a permanently idle queue is part of the setup.** Key B is seeded, drained and never
/// dispatched to again, so an idle queue is always available and `evictOneIdle` succeeds on its
/// first attempt without entering `reserveCapacity`'s stall loop. Eviction still races the
/// dispatcher exactly as before. Which idle queue the scan picks is NOT assumed: `evictOneIdle`
/// walks a `ConcurrentHashMap` keySet, whose iteration order is unspecified, so a schedule may
/// evict either key. That is why [#theEvictionWindowWasActuallyReached()] asserts the retry path
/// was entered across the run rather than trusting any single schedule to reach it.
///
/// This is a scheduling-space choice, not a workaround for a Fray limitation. `@FrayTest`
/// defaults `sleepAsYield` to false, so a thread in that loop is modelled as blocked and released
/// once nothing else is runnable; the loop terminates under exploration either way. Avoiding it
/// keeps the schedules spent on the eviction window rather than on the stall.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class KeyOrderedEvictTombstoneFrayTest {

  private static final String TOPIC = "fray-topic";
  private static final byte[] KEY_A = "key-a".getBytes(UTF_8);
  private static final byte[] KEY_B = "key-b".getBytes(UTF_8);
  private static final byte[] KEY_C = "key-c".getBytes(UTF_8);

  /// Total retries against a dead tombstone, summed across every schedule, so the class can
  /// assert the interesting region was entered at least once. This is the raw retry count rather
  /// than a per-schedule hit count — one schedule can retry more than once — which is the more
  /// useful number when diagnosing how often exploration reaches the window.
  ///
  /// Whether a given schedule enters that path is invisible from the public surface, because the
  /// record processes correctly either way. Without this counter the test passes identically when
  /// no schedule ever exercises what it exists to cover.
  ///
  /// Kept in a system property rather than a static field. `@FrayTest` defaults
  /// `resetClassLoaderPerIteration` to true, and Fray's loader is child-first, so this class is
  /// redefined every iteration and any static it holds is a fresh zero — while `@AfterAll` runs
  /// on the application-loaded copy and would read a counter no iteration ever touched. `System`
  /// is a JDK class shared by every loader, so a property survives both.
  private static final String HITS_PROPERTY = "kpipe.fray.tombstoneHits";

  /// One thread dispatches two records for key A back to back, mirroring production where a single
  /// consumer thread dispatches, while another forces an eviction by introducing key C. Every task
  /// must run exactly once, and key A's two tasks must never overlap.
  @FrayTest(iterations = 500)
  void evictionNeverBreaksPerKeySerialization() {
    final var dispatcher = new KeyOrderedDispatcher(2, Thread.ofPlatform().daemon().factory());
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

    recordTombstoneHits(dispatcher.tombstoneRetries.get());
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

  /// Fails when no schedule reached the dead-tombstone retry path, so "the suite ran but never
  /// explored the interesting region" is a red build rather than a silent pass.
  @AfterAll
  static void theEvictionWindowWasActuallyReached() {
    final var hits = Long.parseLong(System.getProperty(HITS_PROPERTY, "0"));
    System.clearProperty(HITS_PROPERTY);
    assertTrue(
      hits > 0,
      "no schedule reached the dead-tombstone retry path, so this run proved nothing about it. "
        + "Either the key cap leaves a spare idle queue so eviction never has to touch key A, or "
        + "the scenario no longer sets up the {A: empty, idle} precondition eviction needs, or "
        + "the suite ran un-instrumented and every schedule was skipped."
    );
  }

  /// Adds this schedule's retry count to the cross-loader total.
  ///
  /// @param hits retries observed by one scenario
  private static void recordTombstoneHits(final long hits) {
    final var total = Long.parseLong(System.getProperty(HITS_PROPERTY, "0")) + hits;
    System.setProperty(HITS_PROPERTY, Long.toString(total));
  }
}
