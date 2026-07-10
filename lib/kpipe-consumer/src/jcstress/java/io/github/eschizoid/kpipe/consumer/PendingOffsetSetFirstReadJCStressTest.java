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
/// read under one monitor acquisition, so the trap is structurally impossible — as long as every
/// access path honors the same monitor.
///
/// What this proves — and what it cannot. Both `remove` and `firstOrNull` are `synchronized` on
/// the instance monitor, so the reader and the remover are fully mutually exclusive: jcstress
/// can only realize the two SERIAL orderings (read-then-remove → 100, remove-then-read → -1),
/// never a read landing mid-drain. The properties this test pins are therefore:
///
/// * **No-throw under either ordering.** jcstress reports a thrown actor as a hard error, so an
///   escaped exception (the old skiplist failure mode) fails the run before outcome grading.
/// * **Monitor discipline as a regression guard.** If a future change let `firstOrNull` read
///   without the monitor, the read COULD land mid-drain, and a stale or torn value (e.g. a
///   garbage `head` index reading past the window) would grade as forbidden. The monitor's
///   happens-before is also what makes the drain visible to the post-remove read.
///
/// The genuinely concurrent cross-lock-set interleaving in production — the commit scheduler
/// reading through `firstOrNull` (monitor only) while a writer holds the map bucket lock plus
/// the monitor — is exercised end-to-end through the manager by `SafeFirstJCStressTest`.
///
/// Scenario. The set starts holding exactly `{100}`. The remover drains it; the reader snapshots
/// `firstOrNull`. The reader must observe either 100 (read serialized before the remove) or
/// null, encoded as -1 (read serialized after).
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
