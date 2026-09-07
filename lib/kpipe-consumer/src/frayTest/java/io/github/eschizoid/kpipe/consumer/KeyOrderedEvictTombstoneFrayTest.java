package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.pastalab.fray.junit.plain.FrayInTestLauncher;

/// Fray port of the jcstress eviction-tombstone stress test.
///
/// **The window.** [KeyOrderedDispatcher] evicts an empty, idle key queue to make room for a
/// new key. A dispatcher that read that queue out of the map a moment earlier still holds a
/// live reference to it. Eviction marks the queue `dead` under its monitor, atomically with
/// the map removal, and the dispatcher re-checks `dead` under the same monitor before
/// enqueuing — the retry that closes the window. Losing that check does not lose the record:
/// the dispatcher enqueues into the orphaned queue and starts a worker on it, so the task
/// still runs. What breaks is per-key serialization. The orphan is no longer reachable
/// through the map, so the *next* record for that key allocates a second queue and a second
/// worker, and two workers then process the same key at once.
///
/// **What this test drives.** Cap of one key. Key A is pre-seeded and drained, leaving the
/// map in the exact state eviction wants: `{A: empty, idle}`. Then one thread dispatches two
/// records for key A back to back — mirroring production, where a single consumer thread
/// dispatches — while a second thread dispatches one record for key B, forcing A's queue to
/// be evicted. The interesting schedule slips B's eviction between the first thread's map
/// lookup and its monitor entry.
///
/// **How the window is confirmed.** Whether a schedule reached the retry path is invisible
/// from the public surface, because the record processes correctly either way. The
/// dispatcher keeps a package-private `tombstoneRetries` counter for exactly this purpose;
/// the scenario reads it directly after each schedule (same package, classpath compilation,
/// no reflection). Under jcstress the window was reached in 78 of 23,427 runs — 0.33%, found
/// by luck. [#evictVsRedispatchPreservesPerKeySerialization()] asserts the count of schedules
/// that reached it is non-zero and prints the hit rate, so "the suite ran but never explored
/// the interesting region" is a failure rather than a silent pass.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class KeyOrderedEvictTombstoneFrayTest {

  /// Drives the scenario across Fray's whole schedule space and fails on the first schedule
  /// that breaks a per-key invariant. Fray reports whatever the body throws, so a violated
  /// assertion inside a schedule surfaces here as the thrown `AssertionError`.
  ///
  /// The hit counters live outside the explored lambda so they accumulate across schedules —
  /// Fray's plain launcher runs the body directly, without reloading its classes per
  /// iteration, so ordinary fields survive the whole exploration.
  @Test
  void evictVsRedispatchPreservesPerKeySerialization() {
    final var schedules = new AtomicLong();
    final var windowHits = new AtomicLong();
    FrayInTestLauncher.INSTANCE.launchFrayTest(() -> {
      schedules.incrementAndGet();
      final var scenario = new Scenario();
      scenario.run();
      if (scenario.windowReached()) windowHits.incrementAndGet();
    });
    System.out.printf(
      "Fray reached the eviction-tombstone window in %d of %d schedules (%.2f%%)%n",
      windowHits.get(),
      schedules.get(),
      schedules.get() == 0 ? 0.0 : (100.0 * windowHits.get()) / schedules.get()
    );
    assertTrue(
      windowHits.get() > 0,
      () ->
        "No schedule out of " +
        schedules.get() +
        " reached the dead-tombstone retry path, so this run proved nothing about it. Either the " +
        "scheduler is not exploring (check the instrumentation self-check) or the scenario no " +
        "longer sets up the {A: empty, idle} precondition eviction needs."
    );
  }

  /// The same scenario in the annotation-driven shape a full migration would use, kept so the
  /// pilot exercises both entry points. Unlike the launcher-driven test above, a `@FrayTest`
  /// is reported as *skipped* when Fray is not enabled — which is why the instrumentation
  /// self-check, not this method, is what stands between a no-op and a green build.
  @FrayTest(iterations = 300, sleepAsYield = true)
  void evictVsRedispatchUnderTheFrayTestAnnotation() {
    new Scenario().run();
  }

  /// One schedule's worth of state and assertions. A fresh instance per schedule keeps
  /// iterations independent.
  private static final class Scenario {

    private static final String TOPIC = "fray-topic";
    private static final byte[] KEY_A = "key-a".getBytes(UTF_8);
    private static final byte[] KEY_B = "key-b".getBytes(UTF_8);
    private static final long SEED_OFFSET = 0L;
    private static final long FIRST_A_OFFSET = 1L;
    private static final long B_OFFSET = 2L;
    private static final long SECOND_A_OFFSET = 3L;
    private static final long DRAIN_TIMEOUT_SECONDS = 10L;

    private final KeyOrderedDispatcher dispatcher = new KeyOrderedDispatcher(1);
    private final AtomicInteger tasksRun = new AtomicInteger();
    private final AtomicInteger liveKeyATasks = new AtomicInteger();
    private final AtomicInteger peakLiveKeyATasks = new AtomicInteger();
    private final List<Long> keyACompletions = Collections.synchronizedList(new ArrayList<>());
    private final CountDownLatch allDone = new CountDownLatch(3);

    void run() {
      seedKeyA();
      final var keyBDispatcher = new Thread(this::dispatchKeyB, "fray-dispatch-b");
      keyBDispatcher.start();
      dispatcher.dispatch(record(KEY_A, FIRST_A_OFFSET), () -> keyATask(FIRST_A_OFFSET), allDone::countDown);
      dispatcher.dispatch(record(KEY_A, SECOND_A_OFFSET), () -> keyATask(SECOND_A_OFFSET), allDone::countDown);
      join(keyBDispatcher);
      awaitDrain();
      dispatcher.close();
      verify();
    }

    boolean windowReached() {
      return dispatcher.tombstoneRetries.get() > 0;
    }

    /// Dispatches one record for key A and waits for it to complete, so the map holds a
    /// single empty queue for A. That is the precondition eviction needs: the queue is
    /// reclaimable the instant its worker exits, which is what lets key B's dispatch kill it
    /// out from under a dispatcher that already holds the reference.
    private void seedKeyA() {
      final var seeded = new CountDownLatch(1);
      dispatcher.dispatch(record(KEY_A, SEED_OFFSET), () -> {}, seeded::countDown);
      await(seeded, "pre-seed of key A");
    }

    private void dispatchKeyB() {
      dispatcher.dispatch(record(KEY_B, B_OFFSET), tasksRun::incrementAndGet, allDone::countDown);
    }

    /// Body of both key-A records. Tracks how many key-A tasks are inside it at once and the
    /// order in which they leave it — the two facts per-key serialization is made of. The
    /// short pause widens the observation window so an overlapping partner is seen even when
    /// the per-key workers run outside the scheduler's control.
    private void keyATask(final long offset) {
      final var live = liveKeyATasks.incrementAndGet();
      peakLiveKeyATasks.accumulateAndGet(live, Math::max);
      pause();
      keyACompletions.add(offset);
      liveKeyATasks.decrementAndGet();
      tasksRun.incrementAndGet();
    }

    private void verify() {
      if (tasksRun.get() != 3) {
        throw new AssertionError("Expected 3 tasks to run exactly once, observed " + tasksRun.get());
      }
      if (peakLiveKeyATasks.get() > 1) {
        throw new AssertionError(
          "Per-key serialization broken: " +
            peakLiveKeyATasks.get() +
            " tasks for key A ran concurrently. A dispatcher enqueued into an evicted queue and " +
            "started a second worker for the key."
        );
      }
      final var completions = List.copyOf(keyACompletions);
      if (!List.of(FIRST_A_OFFSET, SECOND_A_OFFSET).equals(completions)) {
        throw new AssertionError(
          "Per-key ordering broken: key A completed in offset order " +
            completions +
            " but was dispatched in order [1, 3] by a single thread."
        );
      }
    }

    private void awaitDrain() {
      try {
        if (!allDone.await(DRAIN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          throw new AssertionError(
            "A dispatched task never completed: " + allDone.getCount() + " of 3 callbacks outstanding."
          );
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted while draining the dispatcher", e);
      }
    }

    private static void await(final CountDownLatch latch, final String what) {
      try {
        if (!latch.await(DRAIN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          throw new AssertionError(what + " did not complete");
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted during " + what, e);
      }
    }

    private static void join(final Thread thread) {
      try {
        thread.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted while joining " + thread.getName(), e);
      }
    }

    private static void pause() {
      try {
        Thread.sleep(1);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    private static ConsumerRecord<byte[], byte[]> record(final byte[] key, final long offset) {
      return new ConsumerRecord<>(TOPIC, 0, offset, key.clone(), new byte[0]);
    }
  }
}
