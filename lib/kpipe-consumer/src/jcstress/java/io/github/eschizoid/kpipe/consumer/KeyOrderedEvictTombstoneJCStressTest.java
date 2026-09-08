package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/// Sharpened variant of [KeyOrderedEvictRaceJCStressTest] that maximizes the hit rate of the
/// dead-tombstone window and PROVES the window is reached, rather than inferring it.
///
/// Why the 3-actor test rarely reaches the window: for a dispatcher to observe `dead`, the
/// whole sequence {key's task runs, worker drains and exits, evictor removes the queue} must
/// land inside another dispatcher's nanosecond-scale gap between `queues.get(key)` and
/// `synchronized (queue)` — but that sequence includes a virtual-thread worker being
/// scheduled, running, and exiting, which is microseconds-to-milliseconds. Most
/// interleavings serialize trivially.
///
/// This test removes the worker from the measurement window entirely: the constructor
/// pre-seeds key A and awaits its completion, so at actor time the map is exactly
/// `{A: empty, idle}` at cap 1. Actor A's very first `queues.get(A)` returns a reference
/// eviction can kill; actor B's dispatch evicts A immediately (no worker-exit timing
/// involved). The race collapses to two short critical sections colliding directly.
///
/// The second result cell reports whether the dispatcher's tombstone-retry counter moved —
/// jcstress's outcome histogram then shows, per run, how many interleavings actually entered
/// the retry path (`"2, 1"`) versus serialized trivially (`"2, 0"`). Both are correct
/// behavior; what is FORBIDDEN is any lost or duplicated task, window hit or not.
@JCStressTest
@Outcome(id = "2, 0", expect = Expect.ACCEPTABLE, desc = "Both tasks ran once; tombstone window not entered.")
@Outcome(
  id = "2, 1",
  expect = Expect.ACCEPTABLE_INTERESTING,
  desc = "Both tasks ran once via the tombstone retry path — the window this test exists to reach."
)
@Outcome(id = "-1, .*", expect = Expect.FORBIDDEN, desc = "Drain did not complete within the arbiter deadline.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "A task was lost or ran more than once.")
@State
public class KeyOrderedEvictTombstoneJCStressTest {

  private static final String TOPIC = "jcstress-topic";
  private static final String KEY_A = "key-a";
  private static final String KEY_B = "key-b";

  private final KeyOrderedDispatcher dispatcher = new KeyOrderedDispatcher(1);
  private final AtomicInteger tasksRun = new AtomicInteger(0);
  private final CountDownLatch done = new CountDownLatch(2);

  public KeyOrderedEvictTombstoneJCStressTest() {
    // Pre-seed: dispatch A and wait for it to fully complete, so the actors start from the
    // deterministic state {A: empty, idle} at cap — the exact precondition for B's dispatch
    // to evict A's queue while actor A may hold a stale reference to it.
    final var seeded = new CountDownLatch(1);
    dispatcher.dispatch(record(KEY_A, 0L), () -> {}, seeded::countDown);
    try {
      if (!seeded.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("pre-seed of key A did not complete");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while pre-seeding key A", e);
    }
  }

  @Actor
  public void dispatcherA() {
    dispatcher.dispatch(record(KEY_A, 1L), tasksRun::incrementAndGet, done::countDown);
  }

  @Actor
  public void dispatcherB() {
    dispatcher.dispatch(record(KEY_B, 2L), tasksRun::incrementAndGet, done::countDown);
  }

  @Arbiter
  public void observe(final II_Result r) {
    // Await both tasks' onComplete — the happens-before edge that both task bodies fully ran
    // before tasksRun is read. A lost task leaves the latch above zero → timeout → -1.
    try {
      if (!done.await(5, TimeUnit.SECONDS)) {
        r.r1 = -1;
        return;
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      r.r1 = -1;
      return;
    }
    r.r1 = tasksRun.get();
    r.r2 = dispatcher.tombstoneRetries.get() > 0 ? 1 : 0;
    dispatcher.close();
  }

  private static ConsumerRecord<byte[], byte[]> record(final String key, final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, key.getBytes(UTF_8), new byte[0]);
  }
}
