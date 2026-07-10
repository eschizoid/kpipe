package io.github.eschizoid.kpipe.consumer;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.JJJ_Result;

/// Concurrency-stress check for `PendingOffsetSet` under two concurrent out-of-order `add`s —
/// one prepending below the current lowest offset, one landing mid-window.
///
/// What this proves — and what it cannot. `add` is `synchronized` on the instance monitor, so
/// the two actors are fully mutually exclusive: jcstress can only realize the two SERIAL
/// orderings (prepend-then-mid-insert, mid-insert-then-prepend), never an interleaving inside
/// the `System.arraycopy` shifts or the `head`/`tail` adjustments. The properties this test pins
/// are therefore:
///
/// * **Order-insensitive serial correctness.** The two inserts exercise different shift
///   machinery (prepend into head slack vs mid-window shift), and both orderings must converge
///   on the same sorted final state `{50, 100, 150, 200}`.
/// * **Monitor discipline as a regression guard.** If a future change dropped or weakened
///   `synchronized`, the adds WOULD interleave, and one insert overwriting the other's shift, a
///   duplicated element, or an unsorted window would grade as forbidden via a wrong first, last,
///   or size. The monitor's happens-before is also what makes both inserts visible to the
///   arbiter.
///
/// The genuinely concurrent lock-set interaction in production (monitor-only readers vs
/// bucket-lock-plus-monitor writers) is exercised end-to-end by `SafeFirstJCStressTest`.
///
/// Scenario. The set starts holding `{100, 200}`. One actor adds 50 (prepend path: below the
/// current lowest), the other adds 150 (mid-window insert between 100 and 200). Under either
/// ordering both inserts must take effect: the set ends as `{50, 100, 150, 200}` — first 50,
/// last 200, size 4.
@JCStressTest
@Outcome(id = "50, 200, 4", expect = Expect.ACCEPTABLE, desc = "Both concurrent inserts survived; window sorted.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "An insert was lost, duplicated, or the window corrupted.")
@State
public class PendingOffsetSetConcurrentAddJCStressTest {

  private final PendingOffsetSet set = new PendingOffsetSet();

  public PendingOffsetSetConcurrentAddJCStressTest() {
    set.add(100L);
    set.add(200L);
  }

  @Actor
  public void prepender() {
    set.add(50L);
  }

  @Actor
  public void midInserter() {
    set.add(150L);
  }

  @Arbiter
  public void observe(final JJJ_Result r) {
    final var first = set.firstOrNull();
    final var last = set.lastOrNull();
    r.r1 = first == null ? -1L : first;
    r.r2 = last == null ? -1L : last;
    r.r3 = set.size();
  }
}
