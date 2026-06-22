package io.github.eschizoid.kpipe.consumer;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.JJJ_Result;

/// Concurrency-stress check for the in-flight counter the real `ParallelDispatcher` owns.
///
/// The dispatcher increments its `AtomicLong` before submitting each record to a
/// virtual-thread executor; the submitted task's finally block decrements it and then fires
/// the `onComplete` callback. That count is the single source of truth for in-flight
/// backpressure, so a lost increment, a lost decrement, or a double-decrement on the throwing
/// record's path would either latch backpressure on forever or let the count underflow below
/// zero and stop engaging. The property: concurrent dispatch (increment) racing completion
/// (decrement, including the throw path's finally) never drives the count negative and always
/// settles to exactly zero once both records finish, with no increment or decrement lost.
///
/// This @State drives the production `ParallelDispatcher` directly: it is package-private and
/// this test shares its package, so no hand-rolled replica is needed. Completion runs on a
/// virtual thread asynchronously, so each actor makes its decrement observable by awaiting a
/// latch the dispatcher fires from `onComplete`, which runs after the finally-block decrement.
/// Awaiting that latch establishes a happens-before edge: by the time an actor returns, its
/// own record's decrement is visible. One actor dispatches a record whose task returns
/// normally; the other dispatches a record whose task throws, exercising the throw-path
/// finally. Each actor reads `pendingCount()` after its own completion and records it; the
/// arbiter reads the final settled count once both actors finish.
///
/// jcstress runs the two actors against fresh state under every interleaving its scheduler
/// produces. r1/r2 carry each actor's post-completion observation (never negative); r3 carries
/// the final settled count (exactly zero). Any negative observation, or a final count other
/// than zero, means an increment or decrement was lost, double-counted, or torn.
@JCStressTest
@Outcome(
  id = "0, 0, 0",
  expect = Expect.ACCEPTABLE,
  desc = "Both records settled before the other's observation; count drained to zero."
)
@Outcome(
  id = "1, 0, 0",
  expect = Expect.ACCEPTABLE,
  desc = "First actor still saw the other record in flight; count drained to zero."
)
@Outcome(
  id = "0, 1, 0",
  expect = Expect.ACCEPTABLE,
  desc = "Second actor still saw the other record in flight; count drained to zero."
)
@Outcome(
  id = "1, 1, 0",
  expect = Expect.ACCEPTABLE,
  desc = "Each actor saw the other record still in flight; count drained to zero."
)
@Outcome(
  id = ".*",
  expect = Expect.FORBIDDEN,
  desc = "A negative observation or non-zero final count: an increment or decrement was lost."
)
@State
public class DispatcherPendingCountJCStressTest {

  private static final String TOPIC = "jcstress-topic";

  private final ParallelDispatcher<String> dispatcher =
    new ParallelDispatcher<>((_, _) -> {}, Duration.ofSeconds(5));
  private final CountDownLatch normalDone = new CountDownLatch(1);
  private final CountDownLatch throwDone = new CountDownLatch(1);

  @Actor
  public void normalRecord(final JJJ_Result r) {
    dispatcher.dispatch(record(1L), () -> {}, normalDone::countDown);
    r.r1 = awaitThenObserve(normalDone);
  }

  @Actor
  public void throwingRecord(final JJJ_Result r) {
    dispatcher.dispatch(
      record(2L),
      () -> {
        throw new RuntimeException("boom");
      },
      throwDone::countDown
    );
    r.r2 = awaitThenObserve(throwDone);
  }

  @Arbiter
  public void observe(final JJJ_Result r) {
    awaitQuietly(normalDone);
    awaitQuietly(throwDone);
    r.r3 = dispatcher.pendingCount();
  }

  /// Blocks until this record's completion fires, then snapshots the live count. A snapshot
  /// taken after the awaited record's own decrement can still see the sibling record in flight
  /// (legal: 0 or 1), but it must never be negative — a negative read means a decrement ran
  /// without a matching increment.
  private long awaitThenObserve(final CountDownLatch done) {
    awaitQuietly(done);
    return dispatcher.pendingCount();
  }

  private static void awaitQuietly(final CountDownLatch done) {
    try {
      done.await(10, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static ConsumerRecord<String, byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, "k", new byte[0]);
  }
}
