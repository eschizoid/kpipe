package io.github.eschizoid.kpipe.consumer;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.J_Result;

/// Concurrency-stress check that `PendingOffsetSet.firstOrNull()` is safe against a concurrent
/// drain — the structure-level guarantee that replaced the old `safeFirst` exception guard.
///
/// On the previous `ConcurrentSkipListSet<Long>`, `if (!set.isEmpty()) set.first()` was a
/// check-then-act trap: a concurrent remover could drain the set between the two calls and
/// `first()` threw `NoSuchElementException`. `firstOrNull` performs the emptiness check and the
/// read under one monitor acquisition, so the trap is structurally impossible — but only as long
/// as every mutation path honors the same monitor. This test races the reader against the remove
/// that empties the window (the reset of `head`/`tail` to 0) to keep that pinned.
///
/// Scenario. The set starts holding exactly `{100}`. The remover drains it; the reader snapshots
/// `firstOrNull`. The reader must observe either 100 (read before the remove) or null, encoded as
/// -1 (read after). jcstress reports a thrown actor as a hard error, so an escaped exception —
/// the old skiplist failure mode — fails the run before outcome grading. A stale or torn value
/// (e.g. a garbage `head` index reading past the window) would grade as forbidden.
@JCStressTest
@Outcome(id = "100", expect = Expect.ACCEPTABLE, desc = "Reader saw offset 100 still pending.")
@Outcome(id = "-1", expect = Expect.ACCEPTABLE, desc = "Reader saw the window already drained.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "Torn read of the window bounds, or a throw.")
@State
public class PendingOffsetSetFirstReadJCStressTest {

  private final PendingOffsetSet set = new PendingOffsetSet();

  public PendingOffsetSetFirstReadJCStressTest() {
    set.add(100L);
  }

  @Actor
  public void remover() {
    set.remove(100L);
  }

  @Actor
  public void reader(final J_Result r) {
    final var first = set.firstOrNull();
    r.r1 = first == null ? -1L : first;
  }
}
