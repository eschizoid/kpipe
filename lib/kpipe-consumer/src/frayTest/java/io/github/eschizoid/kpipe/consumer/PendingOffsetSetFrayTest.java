package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// Fray ports of the three [PendingOffsetSet] stress tests — the sorted primitive-`long` window
/// that replaced `ConcurrentSkipListSet<Long>` as the per-partition pending set.
///
/// **What these can and cannot show.** Every mutator on the structure is `synchronized` on the
/// instance monitor, so the mutating actors are mutually exclusive and no schedule interleaves
/// two window mutations. What the ports pin is that the reachable serial orderings all converge
/// on the same state, and that the monitor discipline is still in place: if a change dropped or
/// weakened `synchronized`, the actors would interleave and a lost insert or a torn head/tail
/// update would surface as a failing schedule.
///
/// The reader case is the one with genuine concurrency — reads take the monitor but observe
/// whatever the writer has committed so far, so more than one answer is correct and the port
/// asserts membership in the acceptable set rather than one value.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class PendingOffsetSetFrayTest {

  /// Draining the window races repopulating it. Either order must leave exactly `{200}`:
  /// remove-then-add drains and repopulates, add-then-remove takes the head-removal path out
  /// of `{100, 200}`.
  @FrayTest(iterations = 300)
  void addSurvivesTheConcurrentDrain() {
    final var set = new PendingOffsetSet();
    set.add(100L);

    FrayScenarios.runConcurrently(() -> set.remove(100L), () -> set.add(200L));

    assertEquals(200L, set.firstOrNull(), "the added offset should be the only one left pending");
    assertEquals(1, set.size(), "exactly one offset should remain in the window");
  }

  /// Two inserts land in different regions of the window at once — one below the current head,
  /// one between the existing entries. Both must survive and the window must stay sorted.
  @FrayTest(iterations = 300)
  void concurrentInsertsBothSurvive() {
    final var set = new PendingOffsetSet();
    set.add(100L);
    set.add(200L);

    FrayScenarios.runConcurrently(() -> set.add(50L), () -> set.add(150L));

    assertEquals(50L, set.firstOrNull(), "the prepended offset should become the window head");
    assertEquals(200L, set.lastOrNull(), "the window tail should be unchanged");
    assertEquals(4, set.size(), "no insert should have been lost or duplicated");
  }

  /// A read races the removal that empties the window. Both answers are correct — the reader
  /// either sees 100 still pending or sees the window already drained. What must never happen
  /// is a throw or a torn value: `firstOrNull` is the guarded replacement for the
  /// `isEmpty()`-then-`first()` check-then-act that could throw under contention.
  @FrayTest(iterations = 300)
  void readerNeverSeesATornWindow() {
    final var set = new PendingOffsetSet();
    set.add(100L);
    final var observed = new AtomicLong(Long.MIN_VALUE);

    FrayScenarios.runConcurrently(
      () -> set.remove(100L),
      () -> {
        final var first = set.firstOrNull();
        observed.set(first == null ? -1L : first);
      }
    );

    final var seen = observed.get();
    assertTrue(
      seen == 100L || seen == -1L,
      () -> "reader observed " + seen + ", which is neither the pending offset nor an empty window"
    );
  }
}
