package io.github.eschizoid.kpipe.consumer;

/// A thread-safe sorted set of primitive `long` offsets — the per-partition pending-offset
/// structure behind `KafkaOffsetManager`'s lowest-pending-offset commit invariant.
///
/// Replaces the previous `ConcurrentSkipListSet<Long>`: on the track/mark hot path the skiplist
/// cost one `Long` box plus one skiplist node per add, another box per remove, and deletion-marker
/// churn on unlink — roughly 400 B per track+mark pair measured by `OffsetManagerBoxingBenchmark`.
/// This structure keeps the live offsets in a sorted `long[]` window and allocates nothing in
/// steady state; mutations shift elements with `System.arraycopy`.
///
/// ### Layout
///
/// The structure has two modes:
///
/// * **Inline mode** (`offsets == null`) — zero or one element, held in the `single` field with
///   no array at all. The owning map evicts a set the moment it empties and recreates it on the
///   next track, so in low-in-flight regimes (sequential processing, a keeping-up consumer) a
///   fresh instance is born, holds one offset, and dies on every record — inline mode keeps that
///   churn to one small object instead of object + backing array.
/// * **Array mode** — from the second concurrent offset onward, live elements occupy
///   `offsets[head, tail)` in strictly ascending order with no duplicates. A spilled set never
///   returns to inline mode; once it drains it is evicted and replaced wholesale.
///
/// In array mode, slack is kept on **both** sides of the window so each fast path is O(1):
///
/// * **Append** (`add` of a new highest offset) — the common track order, since Kafka delivers
///   per-partition offsets monotonically — writes at `tail`.
/// * **Head removal** (`remove` of the lowest pending offset) — the common completion order —
///   just advances `head`.
///
/// Out-of-order operations binary-search the window and shift the cheaper side. When an append
/// hits the array end, the window is compacted to index 0 if head slack exists, else the array
/// doubles — amortized O(1). The array never shrinks while non-empty; a partition whose pending
/// count spiked retains its high-water capacity until the set empties and is evicted from the
/// per-partition map (or the partition is revoked), which drops the whole structure.
///
/// ### Concurrency
///
/// Every method is `synchronized` on the instance monitor. Writers (`trackOffset` on the consumer
/// thread, `markOffsetProcessed` on worker virtual threads) already serialize through the owning
/// `ConcurrentHashMap` bucket lock; the monitor additionally makes the read paths (`firstOrNull`,
/// `lastOrNull`, `size`, `isEmpty` — invoked by the commit scheduler and diagnostics without the
/// bucket lock) safe against a mid-shift torn read. Contention is per-partition and the critical
/// sections are tiny, so the monitor is uncontended in practice.
///
/// Unlike `ConcurrentSkipListSet.first()`, `firstOrNull` cannot throw on a concurrently drained
/// set — emptiness check and read happen under the same monitor — so the check-then-act trap the
/// old `safeFirst` wrapper guarded against is structurally impossible here.
final class PendingOffsetSet {

  private static final int SPILL_CAPACITY = 8;

  /// Sorted ascending; live region is `[head, tail)`. `null` while in inline mode.
  private long[] offsets;

  /// The sole element while in inline mode (`offsets == null` and `hasSingle`).
  private long single;

  /// Whether `single` holds an element (inline mode only).
  private boolean hasSingle;

  /// Inclusive start of the live region (array mode only).
  private int head;

  /// Exclusive end of the live region (array mode only).
  private int tail;

  /// Adds `value` to the set.
  ///
  /// @param value the offset to add
  /// @return `true` if the set changed, `false` if `value` was already present
  synchronized boolean add(final long value) {
    if (offsets == null) {
      if (!hasSingle) {
        single = value;
        hasSingle = true;
        return true;
      }
      if (value == single) return false;
      // Second distinct offset: spill from inline mode to array mode.
      offsets = new long[SPILL_CAPACITY];
      offsets[0] = Math.min(single, value);
      offsets[1] = Math.max(single, value);
      head = 0;
      tail = 2;
      hasSingle = false;
      return true;
    }
    if (head == tail) {
      head = 0;
      tail = 1;
      offsets[0] = value;
      return true;
    }
    // Append fast path: new highest offset (the monotonic per-partition track order).
    if (value > offsets[tail - 1]) {
      if (tail == offsets.length) compactOrGrow();
      offsets[tail++] = value;
      return true;
    }
    // Prepend fast path: new lowest offset.
    if (value < offsets[head]) {
      if (head > 0) {
        offsets[--head] = value;
        return true;
      }
      if (tail == offsets.length) compactOrGrow(); // head == 0, so this grows
      System.arraycopy(offsets, 0, offsets, 1, tail);
      offsets[0] = value;
      tail++;
      return true;
    }
    final var found = search(value);
    if (found >= 0) return false; // already present — set semantics
    var insertion = -found - 1;
    final var leftLength = insertion - head;
    final var rightLength = tail - insertion;
    if (head > 0 && leftLength <= rightLength) {
      System.arraycopy(offsets, head, offsets, head - 1, leftLength);
      head--;
      offsets[insertion - 1] = value;
    } else {
      if (tail == offsets.length) insertion -= compactOrGrow();
      System.arraycopy(offsets, insertion, offsets, insertion + 1, tail - insertion);
      offsets[insertion] = value;
      tail++;
    }
    return true;
  }

  /// Removes `value` from the set.
  ///
  /// @param value the offset to remove
  /// @return `true` if the set changed, `false` if `value` was not present
  synchronized boolean remove(final long value) {
    if (offsets == null) {
      if (hasSingle && value == single) {
        hasSingle = false;
        return true;
      }
      return false;
    }
    if (head == tail) return false;
    // Head fast path: removing the lowest pending offset (the common completion order).
    if (value == offsets[head]) {
      head++;
      if (head == tail) head = tail = 0;
      return true;
    }
    if (value < offsets[head] || value > offsets[tail - 1]) return false;
    if (value == offsets[tail - 1]) {
      tail--;
      return true;
    }
    final var found = search(value);
    if (found < 0) return false;
    final var leftLength = found - head;
    final var rightLength = tail - found - 1;
    if (leftLength <= rightLength) {
      System.arraycopy(offsets, head, offsets, head + 1, leftLength);
      head++;
    } else {
      System.arraycopy(offsets, found + 1, offsets, found, rightLength);
      tail--;
    }
    return true;
  }

  /// @return `true` if the set holds no offsets
  synchronized boolean isEmpty() {
    return offsets == null ? !hasSingle : head == tail;
  }

  /// @return the number of offsets in the set
  synchronized int size() {
    if (offsets == null) return hasSingle ? 1 : 0;
    return tail - head;
  }

  /// Returns the lowest offset, or `null` when empty. Boxed `Long` on purpose: this is only
  /// called on the commit-preparation and diagnostics paths (periodic, not per-record), and the
  /// null-for-empty shape matches what the old `safeFirst(SortedSet)` helper returned. Unlike
  /// `SortedSet.first()` this can never throw — check and read share the monitor.
  ///
  /// @return the lowest pending offset, or `null` if the set is empty
  synchronized Long firstOrNull() {
    if (offsets == null) return hasSingle ? single : null;
    return head == tail ? null : offsets[head];
  }

  /// Returns the highest offset, or `null` when empty. Same boxing rationale as `firstOrNull`.
  ///
  /// @return the highest pending offset, or `null` if the set is empty
  synchronized Long lastOrNull() {
    if (offsets == null) return hasSingle ? single : null;
    return head == tail ? null : offsets[tail - 1];
  }

  /// Makes room for one more element when `tail` has hit the array end: compacts the live
  /// region to index 0 when head slack exists (amortized against the O(1) head removals that
  /// created the slack), otherwise doubles the array.
  ///
  /// @return how far the live region moved left, so callers can adjust a pending insertion index
  private int compactOrGrow() {
    final var shift = head;
    final var length = tail - head;
    if (shift > 0) {
      System.arraycopy(offsets, head, offsets, 0, length);
    } else {
      final var grown = new long[offsets.length * 2];
      System.arraycopy(offsets, 0, grown, 0, length);
      offsets = grown;
    }
    head = 0;
    tail = length;
    return shift;
  }

  /// Binary search over the live region `[head, tail)`.
  ///
  /// @return the index of `value` if present, else `-(insertionPoint) - 1`
  private int search(final long value) {
    var low = head;
    var high = tail - 1;
    while (low <= high) {
      final var mid = (low + high) >>> 1;
      final var candidate = offsets[mid];
      if (candidate < value) {
        low = mid + 1;
      } else if (candidate > value) {
        high = mid - 1;
      } else {
        return mid;
      }
    }
    return -(low + 1);
  }
}
