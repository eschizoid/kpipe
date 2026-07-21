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
import org.openjdk.jcstress.infra.results.I_Result;

/// Concurrency-stress check for the dispatcher's evict-vs-enqueue race: a task must never be
/// lost or double-run when its key's queue is concurrently evicted and re-allocated.
///
/// The race. The dispatcher keeps a concurrent map of per-key queues, each guarded by its own
/// monitor. Eviction (triggered by a dispatch for a new key while the map is at its cap)
/// removes an empty + idle queue via `computeIfPresent`, marking it `dead` inside the queue's
/// monitor atomically with the removal. A dispatcher for the evicted key may have looked the
/// queue up just before removal; it must observe the tombstone under the monitor and retry
/// against the live map, allocating a fresh queue. The forbidden failures are an enqueue into
/// a dead queue (the task is stranded — no worker will ever drain an unmapped queue) and a
/// double-run via a stale-plus-fresh queue pair.
///
/// Scenario. The dispatcher's cap is 1, so every dispatch for a new key first forces the
/// previous key's queue out (or stalls until that queue is empty and idle). Three actors:
/// two dispatch key A, one dispatches key B. Interleavings reached include: B's eviction of
/// A's drained queue racing A's second dispatch (the tombstone window), and B stalling while
/// A's queue is still active. Each task increments a shared counter once; the dispatcher's
/// per-task `onComplete` counts down a latch the arbiter awaits, establishing a
/// happens-before edge that all three tasks fully ran before the counter is read. The only
/// acceptable outcome is 3. A stranded task leaves the latch above zero; the await times out
/// and the run reports the forbidden -1.
@JCStressTest
@Outcome(id = "3", expect = Expect.ACCEPTABLE, desc = "All three tasks ran exactly once.")
@Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Drain did not complete within the arbiter deadline.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "A task was lost or ran more than once.")
@State
public class KeyOrderedEvictRaceJCStressTest {

  private static final String TOPIC = "jcstress-topic";
  private static final String KEY_A = "key-a";
  private static final String KEY_B = "key-b";

  /// Cap of 1: every new-key dispatch must evict the other key's queue or stall until it can.
  private final KeyOrderedDispatcher dispatcher = new KeyOrderedDispatcher(1);

  private final AtomicInteger tasksRun = new AtomicInteger(0);
  private final CountDownLatch done = new CountDownLatch(3);

  @Actor
  public void dispatcherA1() {
    dispatcher.dispatch(record(KEY_A, 0L), tasksRun::incrementAndGet, done::countDown);
  }

  @Actor
  public void dispatcherA2() {
    dispatcher.dispatch(record(KEY_A, 1L), tasksRun::incrementAndGet, done::countDown);
  }

  @Actor
  public void dispatcherB() {
    dispatcher.dispatch(record(KEY_B, 2L), tasksRun::incrementAndGet, done::countDown);
  }

  @Arbiter
  public void observe(final I_Result r) {
    // Await all three tasks' onComplete (fired per task by the dispatcher after task.run()).
    // The await is the happens-before edge guaranteeing all task bodies fully ran before
    // tasksRun is read. A stranded task (enqueued into an evicted, unmapped queue) leaves the
    // latch above zero; the await times out and the run reports -1.
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
    dispatcher.close();
  }

  private static ConsumerRecord<byte[], byte[]> record(final String key, final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, key.getBytes(UTF_8), new byte[0]);
  }
}
