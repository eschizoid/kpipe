package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// Property-based coverage for [KeyOrderedDispatcher]'s per-key serial-ordering guarantee. Where
/// `KeyOrderedDispatcherCorrectnessTest` pins specific high-contention scenarios, this suite
/// generates randomized dispatch streams — arbitrary key cardinality, arbitrary per-key record
/// counts, arbitrary cap sizes including caps far below the live key count — and asserts the two
/// invariants hold for EVERY generated stream:
///
///   - per-key arrival order equals submission order (offsets strictly increasing within a key);
///   - no two records sharing a key are ever inside the body concurrently (overlap detection).
///
/// The generated stream is dispatched from a single producer thread (the dispatcher's own
/// per-key virtual-thread workers supply the concurrency); each property runs many randomized
/// trials, so the cap-vs-cardinality eviction + stall paths get broad combinatorial exposure.
class KeyOrderedDispatcherPropertyTest {

  private static final class KeyTracker {

    final AtomicBoolean inside = new AtomicBoolean(false);
    final List<Long> observed = new CopyOnWriteArrayList<>();
    final ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
  }

  /// A single dispatch instruction: which key, which offset (offsets per key are dense 0..n-1,
  /// assigned in submission order so "submission order" == "ascending offset" within a key).
  private record Instruction(String key, long offset) {}

  @Property(tries = 60)
  void perKeyOrderingHoldsForRandomStreams(
    @ForAll("dispatchStreams") final List<Instruction> stream,
    @ForAll @IntRange(min = 1, max = 64) final int cap
  ) throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher(cap);
    try {
      final var trackers = new ConcurrentHashMap<String, KeyTracker>();
      final var submittedOrder = new HashMap<String, List<Long>>();
      for (final var ins : stream) {
        trackers.computeIfAbsent(ins.key(), k -> new KeyTracker());
        submittedOrder.computeIfAbsent(ins.key(), k -> new ArrayList<>()).add(ins.offset());
      }

      final var processed = new CountDownLatch(stream.size());
      for (final var ins : stream) {
        final var tracker = trackers.get(ins.key());
        final var rec = recordWithKey(ins.key(), ins.offset());
        dispatcher.dispatch(
          rec,
          () -> {
            guardedBody(tracker, rec.offset());
            processed.countDown();
          },
          () -> {}
        );
      }

      assertTrue(processed.await(30, TimeUnit.SECONDS), "all generated records must process within 30s");

      for (final var entry : trackers.entrySet()) {
        final var key = entry.getKey();
        final var tracker = entry.getValue();
        assertTrue(tracker.violations.isEmpty(), () -> "key " + key + " violations: " + tracker.violations);
        assertEquals(
          submittedOrder.get(key),
          tracker.observed,
          () -> "key " + key + " arrival order != submission order: " + tracker.observed
        );
      }
    } finally {
      dispatcher.close();
    }
  }

  @Property(tries = 40)
  void nullKeysCollapseToOneSerialQueueAcrossRandomMixes(
    @ForAll @IntRange(min = 1, max = 50) final int nullCount,
    @ForAll @IntRange(min = 0, max = 30) final int keyedCount,
    @ForAll @IntRange(min = 1, max = 16) final int cap
  ) throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher(cap);
    try {
      final var nullTracker = new KeyTracker();
      final var keyedTracker = new KeyTracker();
      final var processed = new CountDownLatch(nullCount + keyedCount);

      // Interleave null-keyed and keyed dispatches in submission order. Null-keyed offsets and
      // keyed offsets are each dense ascending within their own stream.
      var nullOffset = 0L;
      var keyedOffset = 0L;
      final var total = nullCount + keyedCount;
      for (int i = 0; i < total; i++) {
        // Deterministic interleave: even slots null, odd slots keyed, until one stream is spent.
        final var emitNull = (i % 2 == 0 && nullOffset < nullCount) || keyedOffset >= keyedCount;
        if (emitNull && nullOffset < nullCount) {
          final var off = nullOffset++;
          final var rec = recordWithKey(null, off);
          dispatcher.dispatch(
            rec,
            () -> {
              guardedBody(nullTracker, rec.offset());
              processed.countDown();
            },
            () -> {}
          );
        } else if (keyedOffset < keyedCount) {
          final var off = keyedOffset++;
          final var rec = recordWithKey("k", off);
          dispatcher.dispatch(
            rec,
            () -> {
              guardedBody(keyedTracker, rec.offset());
              processed.countDown();
            },
            () -> {}
          );
        }
      }

      assertTrue(processed.await(30, TimeUnit.SECONDS), "mixed null/keyed stream must complete within 30s");
      assertTrue(nullTracker.violations.isEmpty(), () -> "null-key violations: " + nullTracker.violations);
      assertEquals(dense(nullCount), nullTracker.observed, "null-key stream out of order");
      assertTrue(keyedTracker.violations.isEmpty(), () -> "keyed violations: " + keyedTracker.violations);
      assertEquals(dense(keyedCount), keyedTracker.observed, "keyed stream out of order");
    } finally {
      dispatcher.close();
    }
  }

  /// Generates a list of dispatch instructions: a random number of keys, each with a random
  /// number of records, offsets dense ascending within each key, round-robin interleaved so keys
  /// mix in submission order (but each key's relative order is preserved).
  @Provide
  Arbitrary<List<Instruction>> dispatchStreams() {
    final Arbitrary<Integer> keyCount = Arbitraries.integers().between(1, 40);
    return keyCount.flatMap(keys -> {
      final var perKeyCounts = Arbitraries.integers().between(1, 30).list().ofSize(keys);
      return perKeyCounts.map(counts -> buildInterleaved(counts));
    });
  }

  /// Builds an interleaved instruction list from per-key record counts. Within each key, offsets
  /// are dense ascending (submission order). Keys are interleaved round-robin so the stream is a
  /// realistic mix rather than key-by-key blocks — but each key's relative order is intact, which
  /// is what the dispatcher must preserve at the processing end.
  private static List<Instruction> buildInterleaved(final List<Integer> counts) {
    final var nextOffset = new long[counts.size()];
    final var remaining = new int[counts.size()];
    var total = 0;
    for (int k = 0; k < counts.size(); k++) {
      remaining[k] = counts.get(k);
      total += counts.get(k);
    }
    final var out = new ArrayList<Instruction>(total);
    var produced = 0;
    while (produced < total) {
      for (int k = 0; k < counts.size(); k++) {
        if (remaining[k] > 0) {
          out.add(new Instruction("key-" + k, nextOffset[k]++));
          remaining[k]--;
          produced++;
        }
      }
    }
    return out;
  }

  private static void guardedBody(final KeyTracker tracker, final long offset) {
    if (!tracker.inside.compareAndSet(false, true)) {
      tracker.violations.add("concurrent overlap at offset " + offset);
      return;
    }
    try {
      tracker.observed.add(offset);
      Thread.onSpinWait();
    } finally {
      if (!tracker.inside.compareAndSet(true, false)) {
        tracker.violations.add("inside flag already cleared at offset " + offset);
      }
    }
  }

  private static List<Long> dense(final int n) {
    final var out = new ArrayList<Long>(n);
    for (long i = 0; i < n; i++) out.add(i);
    return out;
  }

  private static ConsumerRecord<byte[], byte[]> recordWithKey(final String key, final long offset) {
    return new ConsumerRecord<>("test-topic", 0, offset, key.getBytes(UTF_8), new byte[0]);
  }
}
