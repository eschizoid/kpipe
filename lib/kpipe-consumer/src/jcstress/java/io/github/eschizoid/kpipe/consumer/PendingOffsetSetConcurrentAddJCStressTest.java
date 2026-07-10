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
/// Both insert paths shift array elements with `System.arraycopy` and adjust `head`/`tail`. If the
/// two adds could interleave inside the window mutation (broken monitor discipline), one insert
/// could overwrite the other's shift, duplicate an element, or leave the window unsorted — all of
/// which would surface as a wrong size, first, or last.
///
/// Scenario. The set starts holding `{100, 200}`. One actor adds 50 (prepend path: below the
/// current lowest), the other adds 150 (mid-window insert between 100 and 200). Both must
/// survive: the set ends as `{50, 100, 150, 200}` — first 50, last 200, size 4 — under every
/// interleaving.
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
