package io.github.eschizoid.kpipe.consumer;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.JJ_Result;

/// Concurrency-stress check for `PendingOffsetSet` — the sorted primitive-`long` structure that
/// replaced `ConcurrentSkipListSet<Long>` as the per-partition pending set — running an `add`
/// concurrently with the `remove` that empties the window.
///
/// What this proves — and what it cannot. Every `PendingOffsetSet` method is `synchronized` on
/// the instance monitor, so the two mutator actors are fully mutually exclusive: jcstress can
/// only realize the two SERIAL orderings (remove-then-add, add-then-remove), never an
/// interleaving inside a window mutation. The properties this test pins are therefore:
///
/// * **Order-insensitive serial correctness.** Both orderings must converge on the same final
///   state `{200}` — remove-then-add drains the window and repopulates it; add-then-remove takes
///   the head-removal path out of `{100, 200}`.
/// * **Monitor discipline as a regression guard.** If a future change dropped or weakened
///   `synchronized` (plain fields, per-method locks), the actors WOULD interleave, and a lost
///   add, torn `head`/`tail` update, or shift racing the reset-to-zero would grade as forbidden.
///   The monitor's happens-before is also what makes both mutations visible to the arbiter.
///
/// The genuinely concurrent lock-set interaction in production — manager reads taking only the
/// structure monitor while writers hold the map bucket lock plus the monitor — is exercised
/// end-to-end by `SafeFirstJCStressTest`; the map-eviction race is `RemoveIfEmptyJCStressTest`.
///
/// Scenario. The set starts holding exactly `{100}`. One actor removes 100 (draining the window,
/// which resets `head`/`tail` to 0); the other adds 200. Under either ordering both operations
/// must take effect: the set ends holding exactly `{200}`, so `firstOrNull` is 200 and `size`
/// is 1.
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
