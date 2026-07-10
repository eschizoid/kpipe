package io.github.eschizoid.kpipe.consumer;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.JJ_Result;

/// Concurrency-stress check for `PendingOffsetSet` — the sorted primitive-`long` structure that
/// replaced `ConcurrentSkipListSet<Long>` as the per-partition pending set — racing an `add`
/// against the `remove` that empties the window.
///
/// This is the structure-level analogue of `RemoveIfEmptyJCStressTest`: in production, `add` runs
/// on the poll thread (`trackOffset`) while `remove` runs on worker virtual threads
/// (`markOffsetProcessed`) for the same partition. Both mutate the shared head/tail window and its
/// backing array. If the monitor discipline were broken — say, a torn head/tail update or a shift
/// racing a reset-to-zero — the add could be lost, land unsorted, or corrupt the window bounds.
///
/// Scenario. The set starts holding exactly `{100}`. One actor removes 100 (draining the window,
/// which resets `head`/`tail` to 0); the other adds 200. Whatever the interleaving, both
/// operations must take effect: the set must end holding exactly `{200}`, so `firstOrNull` is 200
/// and `size` is 1. Any other observation means a mutation was lost or the window corrupted.
@JCStressTest
@Outcome(id = "200, 1", expect = Expect.ACCEPTABLE, desc = "Added offset 200 survived the concurrent drain of 100.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "A mutation was lost or the window was corrupted.")
@State
public class PendingOffsetSetAddRemoveJCStressTest {

  private final PendingOffsetSet set = new PendingOffsetSet();

  public PendingOffsetSetAddRemoveJCStressTest() {
    set.add(100L);
  }

  @Actor
  public void remover() {
    set.remove(100L);
  }

  @Actor
  public void adder() {
    set.add(200L);
  }

  @Arbiter
  public void observe(final JJ_Result r) {
    final var first = set.firstOrNull();
    r.r1 = first == null ? -1L : first;
    r.r2 = set.size();
  }
}
