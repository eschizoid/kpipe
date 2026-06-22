package io.github.eschizoid.kpipe.consumer;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.JD_Result;

/// Concurrency-stress check for the count-based rolling window that backs the consumer-side
/// circuit breaker's failure rate.
///
/// The breaker records one success/failure outcome per processed record into a fixed-size window
/// that keeps running `successes` and `failures` counters so the failure rate is `O(1)`. In a
/// parallel-processing consumer those outcomes are recorded from many virtual-thread workers at
/// once, so the per-record bookkeeping — claim a slot, swap the slot, adjust the evicted and the
/// new counters — runs fully concurrently. The property under test: two concurrent records (one
/// success, one failure) must both land, leaving a coherent snapshot. The total sample count must
/// equal 2 (neither record's counter increment was lost) and the failure rate must be exactly 0.5
/// (one failure out of two), which also means failures never exceed samples.
///
/// This drives the REAL window — `ConsumerHealthController.SlidingWindow`, made package-private for
/// exactly this — not a copy, so a future change to its slot-claim / swap / counter-adjust logic is
/// covered here rather than silently diverging from a replica. The two actors record one success
/// and one failure concurrently; the arbiter reads the running total and the derived failure rate.
///
/// jcstress runs the actors against fresh state under every interleaving its scheduler can
/// produce, then evaluates the arbiter once both have finished. r1 carries the total sample count
/// and r2 carries the failure rate. The only acceptable snapshot is `2` samples at rate `0.5`; any
/// lost update (a sample count below 2) or torn rate exposes a non-atomic counter update.
@JCStressTest
@Outcome(
  id = "2, 0.5",
  expect = Expect.ACCEPTABLE,
  desc = "Both outcomes recorded: 2 samples, one of them a failure (rate 0.5)."
)
@Outcome(
  id = ".*",
  expect = Expect.FORBIDDEN,
  desc = "A lost update or torn counter: sample count off 2 or failure rate not 0.5."
)
@State
public class CircuitBreakerWindowJCStressTest {

  private final ConsumerHealthController.SlidingWindow window = new ConsumerHealthController.SlidingWindow(2);

  @Actor
  public void recordSuccess() {
    window.record(true);
  }

  @Actor
  public void recordFailure() {
    window.record(false);
  }

  @Arbiter
  public void observe(final JD_Result r) {
    r.r1 = window.totalSamples();
    r.r2 = window.failureRate();
  }
}
