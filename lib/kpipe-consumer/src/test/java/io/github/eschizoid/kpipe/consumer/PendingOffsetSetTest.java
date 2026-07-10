package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.TreeSet;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;

/// Unit + property coverage for `PendingOffsetSet`, the sorted primitive-`long` window that
/// replaced `ConcurrentSkipListSet<Long>` as the per-partition pending-offset structure.
///
/// The deterministic tests pin the shift/compaction/growth edge cases of the head/tail window;
/// the jqwik property drives randomized add/remove sequences against a `TreeSet<Long>` oracle and
/// asserts every observable (`add`/`remove` return values, `size`, `isEmpty`, `firstOrNull`,
/// `lastOrNull`) matches the reference set after every operation.
class PendingOffsetSetTest {

  @Test
  void emptySetObservables() {
    final var set = new PendingOffsetSet();
    assertTrue(set.isEmpty());
    assertEquals(0, set.size());
    assertNull(set.firstOrNull());
    assertNull(set.lastOrNull());
    assertFalse(set.remove(42L));
  }

  @Test
  void inlineModeSpillsToArrayOnSecondDistinctOffset() {
    final var set = new PendingOffsetSet();
    // Inline mode: one element, no array.
    assertTrue(set.add(100L));
    assertEquals(100L, set.firstOrNull());
    assertEquals(100L, set.lastOrNull());
    assertFalse(set.add(100L));
    // Spill with the new value below the inline one — order must hold either way.
    assertTrue(set.add(50L));
    assertEquals(2, set.size());
    assertEquals(50L, set.firstOrNull());
    assertEquals(100L, set.lastOrNull());
    // Inline remove path: drain a fresh inline set completely.
    final var inline = new PendingOffsetSet();
    inline.add(7L);
    assertFalse(inline.remove(8L));
    assertTrue(inline.remove(7L));
    assertTrue(inline.isEmpty());
    assertFalse(inline.remove(7L));
  }

  @Test
  void addDuplicateReturnsFalseAndDoesNotGrow() {
    final var set = new PendingOffsetSet();
    assertTrue(set.add(100L));
    assertFalse(set.add(100L));
    assertEquals(1, set.size());
  }

  @Test
  void monotonicAppendThenInOrderRemoval() {
    final var set = new PendingOffsetSet();
    for (var i = 0L; i < 100L; i++) assertTrue(set.add(i));
    assertEquals(100, set.size());
    assertEquals(0L, set.firstOrNull());
    assertEquals(99L, set.lastOrNull());
    for (var i = 0L; i < 100L; i++) {
      assertTrue(set.remove(i));
      assertEquals(i == 99L ? null : i + 1, set.firstOrNull());
    }
    assertTrue(set.isEmpty());
  }

  @Test
  void prependBelowCurrentLowest() {
    final var set = new PendingOffsetSet();
    // Fill forward so head has no slack, then insert below the lowest — exercises the
    // shift-right prepend path (head == 0) and, once full, growth from the prepend path.
    for (var i = 10L; i < 20L; i++) set.add(i);
    for (var i = 9L; i >= 0L; i--) assertTrue(set.add(i));
    assertEquals(20, set.size());
    assertEquals(0L, set.firstOrNull());
    assertEquals(19L, set.lastOrNull());
  }

  @Test
  void middleInsertsBothShiftDirections() {
    final var set = new PendingOffsetSet();
    // Evens first, then odds: every odd insert lands mid-window and picks the cheaper shift.
    for (var i = 0L; i < 64L; i += 2) set.add(i);
    for (var i = 1L; i < 64L; i += 2) assertTrue(set.add(i));
    assertEquals(64, set.size());
    for (var i = 0L; i < 64L; i++) {
      assertEquals(i, set.firstOrNull());
      assertTrue(set.remove(i));
    }
    assertTrue(set.isEmpty());
  }

  @Test
  void middleRemovalsBothShiftDirections() {
    final var set = new PendingOffsetSet();
    for (var i = 0L; i < 32L; i++) set.add(i);
    // Remove just above the head (cheap left shift) and just below the tail (cheap right shift).
    assertTrue(set.remove(1L));
    assertTrue(set.remove(30L));
    // And dead-center removals from both halves.
    assertTrue(set.remove(15L));
    assertTrue(set.remove(16L));
    assertEquals(28, set.size());
    assertEquals(0L, set.firstOrNull());
    assertEquals(31L, set.lastOrNull());
    assertFalse(set.remove(15L));
  }

  @Test
  void headSlackIsCompactedInsteadOfGrowing() {
    final var set = new PendingOffsetSet();
    // Rolling window: append one, remove the lowest — head advances forever. Compaction must
    // reuse the freed head slack instead of growing without bound.
    set.add(0L);
    for (var i = 1L; i < 10_000L; i++) {
      assertTrue(set.add(i));
      assertTrue(set.remove(i - 1));
      assertEquals(1, set.size());
      assertEquals(i, set.firstOrNull());
    }
  }

  @Test
  void removeOutsideWindowReturnsFalse() {
    final var set = new PendingOffsetSet();
    set.add(10L);
    set.add(20L);
    assertFalse(set.remove(5L)); // below the window
    assertFalse(set.remove(25L)); // above the window
    assertFalse(set.remove(15L)); // inside the window but absent
    assertEquals(2, set.size());
  }

  @Test
  void negativeOffsetsAreSupported() {
    // The bench warm-up path tracks offset -1; the structure must not assume non-negative values.
    final var set = new PendingOffsetSet();
    assertTrue(set.add(-1L));
    assertTrue(set.add(-5L));
    assertTrue(set.add(3L));
    assertEquals(-5L, set.firstOrNull());
    assertEquals(3L, set.lastOrNull());
    assertTrue(set.remove(-5L));
    assertEquals(-1L, set.firstOrNull());
  }

  /// A single generated operation: add or remove a given offset.
  private record Op(boolean add, long offset) {}

  @Provide
  Arbitrary<List<Op>> operationSequences() {
    // A small offset space forces duplicate adds, absent removes, window re-use, and both shift
    // directions; long sequences push the structure through growth and compaction.
    final var offsets = Arbitraries.longs().between(0L, 48L);
    final var kinds = Arbitraries.of(true, false);
    final Arbitrary<Op> ops = Combinators.combine(kinds, offsets).as(Op::new);
    return ops.list().ofMinSize(1).ofMaxSize(300);
  }

  @Property(tries = 500)
  void behavesExactlyLikeASortedLongSet(@ForAll("operationSequences") final List<Op> ops) {
    final var set = new PendingOffsetSet();
    final var oracle = new TreeSet<Long>();

    for (final var op : ops) {
      if (op.add()) {
        assertEquals(oracle.add(op.offset()), set.add(op.offset()), "add(%d) return value".formatted(op.offset()));
      } else {
        assertEquals(
          oracle.remove(op.offset()),
          set.remove(op.offset()),
          "remove(%d) return value".formatted(op.offset())
        );
      }
      assertEquals(oracle.size(), set.size(), "size");
      assertEquals(oracle.isEmpty(), set.isEmpty(), "isEmpty");
      assertEquals(oracle.isEmpty() ? null : oracle.first(), set.firstOrNull(), "firstOrNull");
      assertEquals(oracle.isEmpty() ? null : oracle.last(), set.lastOrNull(), "lastOrNull");
    }
  }
}
