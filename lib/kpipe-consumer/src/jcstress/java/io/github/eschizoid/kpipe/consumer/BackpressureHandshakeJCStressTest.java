package io.github.eschizoid.kpipe.consumer;

import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/// Concurrency-stress check for the backpressure pause/park handshake: when the consumer thread
/// publishes the BACKPRESSURE pause bit while worker threads are draining the in-flight count,
/// at least one side must observe the other, or the consumer thread parks forever with nothing
/// left in flight (the wedge that froze the `ParallelProcessingBenchmark.kpipe` arm).
///
/// The two sides of the production handshake, reduced to their StoreLoad essence:
///
///   * **Consumer thread** (the PAUSE arm of `ConsumerHealthController.tickBackpressure`):
///     `requestPause(BACKPRESSURE)` publishes the bit, then the lost-wakeup guard re-reads the
///     in-flight metric. Seeing the drain (`r1 = 1`) means it releases the pause immediately
///     instead of parking.
///   * **Worker thread** (`ParallelDispatcher`'s task finally + `afterRecordComplete`): decrement
///     the in-flight count, then read the pause bit via `isHeldBy(BACKPRESSURE)`. Seeing the bit
///     (`r2 = 1`) means it unparks the consumer thread.
///
/// This drives the real `ConsumerHealthController` pause mask — `requestPause` / `isHeldBy` are
/// the exact methods on both sides in production — paired with a plain `AtomicLong` for the
/// in-flight count, which is precisely what `ParallelDispatcher` owns. `tickBackpressure` itself
/// is not driven because its PAUSE arm logs a WARNING per invocation, which would swamp a
/// jcstress campaign; the guard's post-publish metric read is represented by the actor's
/// `inFlight.get()`.
///
/// Both accesses on each side are volatile (`AtomicInteger` mask, `AtomicLong` count), so the
/// synchronization order forbids the `0, 0` outcome: if the consumer's count read missed the
/// worker's decrement, the consumer's earlier bit publication precedes that decrement in the
/// total order, and the worker's later bit read must observe it. `0, 0` — both sides blind —
/// is the parked-forever deadlock and must never occur.
@JCStressTest
@Outcome(id = "1, 0", expect = Expect.ACCEPTABLE, desc = "Consumer saw the drain and releases immediately; no park.")
@Outcome(id = "0, 1", expect = Expect.ACCEPTABLE, desc = "Worker saw the pause bit and unparks the consumer.")
@Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "Both sides observed each other; release AND unpark.")
@Outcome(id = "0, 0", expect = Expect.FORBIDDEN, desc = "Both sides blind: consumer parks forever with 0 in flight.")
@State
public class BackpressureHandshakeJCStressTest {

  private static final ConsumerHealthController.Hook NOOP_HOOK = new NoopHook();

  private final ConsumerHealthController health = new ConsumerHealthController(null, null, null, NOOP_HOOK);
  private final AtomicLong inFlight = new AtomicLong(1);

  /// Consumer-thread side: publish the pause bit, then run the lost-wakeup guard's re-read of
  /// the in-flight metric.
  @Actor
  public void consumer(final II_Result r) {
    health.requestPause(ConsumerHealthController.Source.BACKPRESSURE);
    r.r1 = inFlight.get() == 0 ? 1 : 0;
  }

  /// Worker side: complete the last in-flight record, then observe the pause bit the way
  /// `afterRecordComplete` does to decide whether to unpark.
  @Actor
  public void worker(final II_Result r) {
    inFlight.decrementAndGet();
    r.r2 = health.isHeldBy(ConsumerHealthController.Source.BACKPRESSURE) ? 1 : 0;
  }

  /// Side-effect-free hook: the stress test exercises only the pause mask, never the pause /
  /// resume choreography, so every callback is a no-op.
  private static final class NoopHook implements ConsumerHealthController.Hook {

    @Override
    public void onPause() {}

    @Override
    public void onResume() {}

    @Override
    public void onBackpressurePause() {}

    @Override
    public void onBackpressureTimeMs(final long ms) {}

    @Override
    public void onCircuitBreakerTrip() {}

    @Override
    public void onCircuitBreakerStateChange(final CircuitBreakerState state) {}

    @Override
    public void onCircuitBreakerTimeOpenMs(final long ms) {}
  }
}
