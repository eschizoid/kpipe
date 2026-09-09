package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// Eviction racing a dispatcher that holds a stale queue reference.
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
/// **Cap of one, which is what makes key A the eviction target.** With a spare idle key, the
/// eviction only lands elsewhere when key A is *busy*: `evictOneIdle` finds A non-evictable once
/// the first dispatch has enqueued, falls through to the spare, and the new key is admitted
/// without ever condemning A. The retry path is then unreachable for that schedule. At cap one
/// there is no fallback — the dispatch stalls until A drains and must then evict A.
///
/// That makes the eviction *target* certain, not the window itself: the retry still requires the
/// key-A dispatcher to be holding a stale reference across the gap between its map lookup and its
/// monitor entry. Hence the run-wide assertion counts hits rather than expecting one per schedule.
///
/// Entering `reserveCapacity`'s stall loop is safe here. `@FrayTest` defaults `sleepAsYield` to
/// false, so the sleeper blocks rather than staying runnable, and a non-idle queue always has an
/// active worker — the stalling dispatcher is therefore always waiting on a runnable thread.

@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class KeyOrderedEvictTombstoneFrayTest {

  private static final String TOPIC = "fray-topic";
  private static final byte[] KEY_A = "key-a".getBytes(UTF_8);
  private static final byte[] KEY_B = "key-b".getBytes(UTF_8);

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
  /// consumer thread dispatches, while another forces an eviction by introducing key B. Every task
  /// must run exactly once, key A's two tasks must never overlap, and they must run in the
/// order they were dispatched.
  @FrayTest(iterations = 500)
  void evictionNeverBreaksPerKeySerialization() {
    final var dispatcher = new KeyOrderedDispatcher(1, Thread.ofPlatform().daemon().factory());
    seedAndDrain(dispatcher, KEY_A, 0L);

    final var tasksRun = new AtomicInteger();
    final var concurrentOnA = new AtomicInteger();
    final var maxConcurrentOnA = new AtomicInteger();
    final var done = new CountDownLatch(3);
    final var keyAOrder = Collections.synchronizedList(new ArrayList<Long>());
    final java.util.function.LongFunction<Runnable> keyATask = offset -> () -> {
      final var live = concurrentOnA.incrementAndGet();
      maxConcurrentOnA.accumulateAndGet(live, Math::max);
      keyAOrder.add(offset);
      tasksRun.incrementAndGet();
      concurrentOnA.decrementAndGet();
    };

    FrayScenarios.runConcurrently(
      () -> {
        dispatcher.dispatch(record(KEY_A, 10L), keyATask.apply(10L), done::countDown);
        dispatcher.dispatch(record(KEY_A, 11L), keyATask.apply(11L), done::countDown);
      },
      () -> dispatcher.dispatch(record(KEY_B, 12L), tasksRun::incrementAndGet, done::countDown)
    );
    await(done);
    dispatcher.close();

    recordTombstoneHits(dispatcher.tombstoneRetries.get());
    assertEquals(3, tasksRun.get(), "a task was lost or ran more than once across the eviction");
    assertEquals(
      List.of(10L, 11L),
      List.copyOf(keyAOrder),
      "key A's records ran out of dispatch order. Non-overlap alone is not the KEY_ORDERED "
        + "guarantee — records sharing a key must also run in the order they were dispatched, and "
        + "an eviction between the two dispatches is exactly where that can be lost."
    );
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
  /// explored the interesting region" is a red build rather than a silent pass. A setup that
  /// leaves any spare idle queue makes the window unreachable while every other assertion still
  /// holds, which is the shape this guards against.
  ///
  /// The count is printed on every run, not only on failure. Whether exploration reaches the
  /// window is a property of Fray's scheduler and this scenario, and a rate drifting toward zero
  /// is the early warning that the gate is about to stop meaning anything — visible in the log
  /// before it ever turns red.
  @AfterAll
  static void theEvictionWindowWasActuallyReached() {
    final var hits = Long.parseLong(System.getProperty(HITS_PROPERTY, "0"));
    System.clearProperty(HITS_PROPERTY);
    System.out.printf("eviction-tombstone retries observed across the run: %d%n", hits);
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
