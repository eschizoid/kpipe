package io.github.eschizoid.kpipe.consumer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
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
/// The real window lives as a `private static final` nested class inside the package-private
/// `ConsumerHealthController`, and the host controller exposes neither its sample count nor its
/// failure rate — only the resulting breaker state. The window therefore cannot be constructed or
/// read back from a jcstress @State through the production surface. This @State replicates the
/// exact slot-claim / swap / counter-adjust logic of that window on a fresh window of size 2, the
/// same replicate-the-real-logic approach the state-transition CAS stress test uses. The two
/// actors record one success and one failure concurrently; the arbiter then reads the running
/// total and the derived failure rate.
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

  private final RollingWindow window = new RollingWindow(2);

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

  /// Count-based rolling window of success/failure outcomes, replicating the production window the
  /// circuit breaker uses. Each `record` claims a unique slot via `head.getAndIncrement`, then
  /// atomically swaps the slot, keeping the `successes` / `failures` counters in sync by
  /// decrementing the evicted outcome and incrementing the new one so the failure rate is `O(1)`.
  private static final class RollingWindow {

    private static final long SUCCESS = 1L;
    private static final long FAILURE = 2L;

    private final AtomicLongArray slots;
    private final AtomicLong head = new AtomicLong(0);
    private final AtomicLong successes = new AtomicLong(0);
    private final AtomicLong failures = new AtomicLong(0);

    RollingWindow(final int windowSize) {
      this.slots = new AtomicLongArray(windowSize);
    }

    void record(final boolean success) {
      final long outcome = success ? SUCCESS : FAILURE;
      final var idx = (int) (head.getAndIncrement() % slots.length());
      final var prev = slots.getAndSet(idx, outcome);
      if (prev == SUCCESS) successes.decrementAndGet();
      else if (prev == FAILURE) failures.decrementAndGet();
      if (outcome == SUCCESS) successes.incrementAndGet();
      else failures.incrementAndGet();
    }

    long totalSamples() {
      return successes.get() + failures.get();
    }

    double failureRate() {
      // Snapshot both counters once — re-reading `failures` after `totalSamples` could produce a
      // ratio above 1.0 if a failure arrived between the two reads.
      final var f = failures.get();
      final var s = successes.get();
      final var total = f + s;
      return total == 0 ? 0.0 : (double) f / total;
    }
  }
}
