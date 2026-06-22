package io.github.eschizoid.kpipe.consumer;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

/// Concurrency-stress check that two same-key dispatches never lose or duplicate a task.
///
/// jcstress runs the two actors below against fresh state under every interleaving its
/// scheduler can produce, then evaluates the arbiter once both actors have finished. This
/// reaches the interleaving where one dispatch enqueues a task into a per-key queue at the
/// exact moment the queue's worker is deciding whether to keep draining or to exit. A
/// single-threaded sequence test cannot exercise that window.
///
/// The race. The dispatcher keeps one FIFO queue per key plus a `workerActive` flag. A
/// dispatch enqueues its task under the lock, then starts a worker only if none is active.
/// The worker drains under the same lock: it polls the queue and, when it finds the queue
/// empty, clears `workerActive` and exits. The create-or-append in dispatch and the
/// drain-and-exit in the worker both run fully under the lock, so a task that is enqueued
/// while a worker is alive must be observed by that worker before it can exit, and a task
/// enqueued after a worker exits must start a fresh worker. Either way every enqueued task
/// runs exactly once. The forbidden failures are a lost enqueue (a task that never runs
/// because it landed on a queue whose worker exited without seeing it) and a double-run (the
/// same task picked up twice).
///
/// Scenario. Both actors dispatch a record for the same key. Each dispatched task increments
/// a shared counter once. The arbiter waits for the dispatcher's pending count to fall to
/// zero (both workers drained) and reports the counter. The only acceptable outcome is 2:
/// both tasks ran exactly once. Zero or one means a lost enqueue; three or more means a
/// task ran more than once.
@JCStressTest
@Outcome(id = "2", expect = Expect.ACCEPTABLE, desc = "Both same-key tasks ran exactly once.")
@Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Drain did not complete within the arbiter deadline.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "A same-key task was lost or ran more than once.")
@State
public class KeyOrderedLruJCStressTest {

  private static final String TOPIC = "jcstress-topic";
  private static final String KEY = "shared-key";

  private final KeyOrderedDispatcher<String> dispatcher =
    new KeyOrderedDispatcher<>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
  private final AtomicInteger tasksRun = new AtomicInteger(0);

  @Actor
  public void dispatcherA() {
    dispatcher.dispatch(record(0L), tasksRun::incrementAndGet, () -> {});
  }

  @Actor
  public void dispatcherB() {
    dispatcher.dispatch(record(1L), tasksRun::incrementAndGet, () -> {});
  }

  @Arbiter
  public void observe(final I_Result r) {
    // Workers run tasks on virtual threads outside the dispatch lock, so the count may still
    // be settling when the arbiter starts. Spin on the pending count with a bounded deadline;
    // report -1 if the drain never completes so a hang surfaces as a forbidden outcome rather
    // than a wedged run.
    final var deadline = System.nanoTime() + 5_000_000_000L;
    while (dispatcher.pendingCount() > 0) {
      if (System.nanoTime() > deadline) {
        r.r1 = -1;
        return;
      }
      Thread.onSpinWait();
    }
    r.r1 = tasksRun.get();
  }

  private static ConsumerRecord<String, byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, KEY, "v".getBytes());
  }
}
