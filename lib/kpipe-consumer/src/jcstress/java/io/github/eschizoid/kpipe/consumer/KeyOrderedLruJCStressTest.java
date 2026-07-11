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
/// a shared counter once; the dispatcher's per-task `onComplete` then counts down a latch the
/// arbiter awaits. Awaiting the latch establishes a happens-before edge that BOTH tasks have
/// fully run before the counter is read — closing the early-exit window a `pendingCount()` spin
/// had (the count can momentarily read zero between an enqueue and the task body executing). The
/// only acceptable outcome is 2: both tasks ran exactly once. A lost enqueue means a task never
/// runs, the latch never reaches zero, the await times out, and the run reports the forbidden -1.
@JCStressTest
@Outcome(id = "2", expect = Expect.ACCEPTABLE, desc = "Both same-key tasks ran exactly once.")
@Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "Drain did not complete within the arbiter deadline.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "A same-key task was lost or ran more than once.")
@State
public class KeyOrderedLruJCStressTest {

  private static final String TOPIC = "jcstress-topic";
  private static final String KEY = "shared-key";

  private final KeyOrderedDispatcher dispatcher = new KeyOrderedDispatcher(
    KeyOrderedDispatcher.DEFAULT_MAX_KEYS
  );
  private final AtomicInteger tasksRun = new AtomicInteger(0);
  private final CountDownLatch done = new CountDownLatch(2);

  @Actor
  public void dispatcherA() {
    dispatcher.dispatch(record(0L), tasksRun::incrementAndGet, done::countDown);
  }

  @Actor
  public void dispatcherB() {
    dispatcher.dispatch(record(1L), tasksRun::incrementAndGet, done::countDown);
  }

  @Arbiter
  public void observe(final I_Result r) {
    // Await both tasks' onComplete (fired per task by the dispatcher after task.run()). The await
    // is the happens-before edge guaranteeing both task bodies fully ran before tasksRun is read,
    // so a momentary pendingCount==0 between enqueue and execution can't make the read fire early.
    // A lost enqueue leaves the latch above zero; the await times out and the run reports -1.
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
  }

  private static ConsumerRecord<byte[], byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, KEY.getBytes(UTF_8), "v".getBytes());
  }
}
