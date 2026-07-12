package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/// Concurrency correctness harness for [KeyOrderedDispatcher] — the per-key serial-ordering
/// guarantee documented in the dispatcher doctrine. Where `KeyOrderedDispatcherTest` covers
/// single-threaded behaviour and lifecycle, this suite drives the dispatcher from MANY producer
/// threads concurrently and checks the two properties that make KEY_ORDERED honest:
///
///   1. records sharing a key are never processed concurrently (no overlap);
///   2. records sharing a key are processed in submission (offset) order.
///
/// It also stresses the LRU-eviction and stall-at-cap backpressure paths under saturation churn,
/// and the null-key sentinel's serialization + non-starvation. Detection is active, not passive:
/// each per-key task flips an `AtomicBoolean` on entry and clears it on exit, and any observed
/// overlap or out-of-order arrival is recorded as a violation the assertions fail on.
///
/// Workers are real virtual threads (the dispatcher spawns `Thread.ofVirtual()` per key); the
/// test's own concurrent producers also use `Thread.ofVirtual()` with a [CyclicBarrier] /
/// [CountDownLatch] so every producer races into `dispatch()` at the same instant — maximising
/// the interleaving pressure on the single [java.util.concurrent.locks.ReentrantLock].
class KeyOrderedDispatcherCorrectnessTest {

  private static ConsumerRecord<byte[], byte[]> recordWithKey(final String key, final long offset) {
    return new ConsumerRecord<byte[], byte[]>(
      "test-topic",
      0,
      offset,
      key == null ? null : key.getBytes(UTF_8),
      new byte[0]
    );
  }

  /// Per-key bookkeeping shared across all worker threads. `inside` detects concurrent overlap;
  /// `observed` records the arrival order so we can assert it equals submission order.
  private static final class KeyTracker {

    final AtomicBoolean inside = new AtomicBoolean(false);
    final List<Long> observed = new CopyOnWriteArrayList<>();
    final ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
  }

  /// Runs the per-record body: assert no other worker is inside this key, record the offset,
  /// hold the key "occupied" for a beat to widen any overlap window, then exit. Any overlap or
  /// reentry shows up as a violation string rather than an exception (exceptions on a worker VT
  /// would just be logged + swallowed by the dispatcher's drain catch, hiding the failure).
  private static void runGuardedBody(final KeyTracker tracker, final long offset) {
    if (!tracker.inside.compareAndSet(false, true)) {
      tracker.violations.add("concurrent overlap detected at offset " + offset);
      return;
    }
    try {
      tracker.observed.add(offset);
      // Brief occupancy to widen the overlap window — if the lock/worker handshake ever let a
      // second worker into the same key, holding the flag set makes the race likelier to show.
      Thread.onSpinWait();
    } finally {
      if (!tracker.inside.compareAndSet(true, false)) {
        tracker.violations.add("inside flag was already cleared at offset " + offset);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Property 1 — per-key serial ordering under heavy concurrent dispatch.
  // ---------------------------------------------------------------------------------------------

  @Test
  void perKeySerialOrderingUnderConcurrentDispatch() throws InterruptedException {
    final var keys = 64;
    final var recordsPerKey = 200;
    final var dispatcher = new KeyOrderedDispatcher(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);

    final var trackers = new ConcurrentHashMap<String, KeyTracker>();
    for (int k = 0; k < keys; k++) trackers.put("key-" + k, new KeyTracker());

    final var totalRecords = keys * recordsPerKey;
    final var processed = new CountDownLatch(totalRecords);

    // One producer thread PER KEY. Each submits that key's records in offset order. The producers
    // all start together so the dispatcher's lock sees maximal cross-key contention, but within a
    // key the submission order is strictly increasing offsets — which is exactly what arrival
    // order must equal.
    final var barrier = new CyclicBarrier(keys);
    final var producers = new ArrayList<Thread>(keys);
    for (int k = 0; k < keys; k++) {
      final var key = "key-" + k;
      final var tracker = trackers.get(key);
      producers.add(
        Thread.ofVirtual().start(() -> {
          awaitBarrier(barrier);
          for (long offset = 0; offset < recordsPerKey; offset++) {
            final var rec = recordWithKey(key, offset);
            dispatcher.dispatch(
              rec,
              () -> {
                runGuardedBody(tracker, rec.offset());
                processed.countDown();
              },
              () -> {}
            );
          }
        })
      );
    }

    assertTrue(processed.await(30, TimeUnit.SECONDS), "all records must be processed within 30s");
    for (final var p : producers) p.join(Duration.ofSeconds(5));

    for (final var entry : trackers.entrySet()) {
      final var key = entry.getKey();
      final var tracker = entry.getValue();
      assertTrue(tracker.violations.isEmpty(), () -> "key " + key + " violations: " + tracker.violations);
      assertEquals(recordsPerKey, tracker.observed.size(), () -> "key " + key + " lost records");
      assertEquals(
        expectedOrder(recordsPerKey),
        tracker.observed,
        () -> "key " + key + " processed out of submission order: " + tracker.observed
      );
    }
    dispatcher.close();
  }

  // ---------------------------------------------------------------------------------------------
  // Property 2 — LRU eviction safety: many more distinct keys than the cap, no loss, no reorder.
  // ---------------------------------------------------------------------------------------------

  @Test
  void evictionChurnNeverDropsOrReordersRecords() throws InterruptedException {
    // Cap is far smaller than the live key count, so the dispatcher must continuously evict empty
    // queues to make room. A non-empty queue must never be evicted (would lose/reorder its
    // records). We feed each key a short burst so queues drain fast and become evictable, while
    // new keys keep arriving — sustained eviction pressure.
    final var cap = 8;
    final var keys = 500;
    final var recordsPerKey = 6;
    final var dispatcher = new KeyOrderedDispatcher(cap);

    final var trackers = new ConcurrentHashMap<String, KeyTracker>();
    for (int k = 0; k < keys; k++) trackers.put("key-" + k, new KeyTracker());

    final var totalRecords = keys * recordsPerKey;
    final var processed = new CountDownLatch(totalRecords);

    // Multiple producer threads, each owning a disjoint slice of keys. Concurrency here drives
    // eviction + allocation racing against worker drain + queue removal.
    final var producerCount = 8;
    final var barrier = new CyclicBarrier(producerCount);
    final var producers = new ArrayList<Thread>(producerCount);
    for (int t = 0; t < producerCount; t++) {
      final var slice = t;
      producers.add(
        Thread.ofVirtual().start(() -> {
          awaitBarrier(barrier);
          for (int k = slice; k < keys; k += producerCount) {
            final var key = "key-" + k;
            final var tracker = trackers.get(key);
            for (long offset = 0; offset < recordsPerKey; offset++) {
              final var rec = recordWithKey(key, offset);
              dispatcher.dispatch(
                rec,
                () -> {
                  runGuardedBody(tracker, rec.offset());
                  processed.countDown();
                },
                () -> {}
              );
            }
          }
        })
      );
    }

    assertTrue(processed.await(60, TimeUnit.SECONDS), "all records must survive eviction churn within 60s");
    for (final var p : producers) p.join(Duration.ofSeconds(5));

    for (final var entry : trackers.entrySet()) {
      final var key = entry.getKey();
      final var tracker = entry.getValue();
      assertTrue(tracker.violations.isEmpty(), () -> "key " + key + " violations: " + tracker.violations);
      assertEquals(
        recordsPerKey,
        tracker.observed.size(),
        () -> "key " + key + " lost records under eviction churn: " + tracker.observed
      );
      assertEquals(
        expectedOrder(recordsPerKey),
        tracker.observed,
        () -> "key " + key + " reordered under eviction churn: " + tracker.observed
      );
    }
    dispatcher.close();
  }

  // ---------------------------------------------------------------------------------------------
  // Property 3 — stall-hold-at-cap: when all queues at cap are non-empty, dispatch HOLDS, then
  // recovers as queues drain. No loss, no corruption under saturation.
  // ---------------------------------------------------------------------------------------------

  @Test
  void saturationHoldDoesNotLoseOrCorruptRecords() throws InterruptedException {
    // Tiny cap, every key gated by a barrier so all `cap` queues stay non-empty simultaneously —
    // forcing later dispatches into the saturation yield-loop. Once the gate opens, every queue
    // drains and the held dispatches must all eventually land, in order, with no loss.
    final var cap = 4;
    final var keys = 40;
    final var recordsPerKey = 10;
    final var dispatcher = new KeyOrderedDispatcher(cap);

    final var trackers = new ConcurrentHashMap<String, KeyTracker>();
    for (int k = 0; k < keys; k++) trackers.put("key-" + k, new KeyTracker());

    // The first `cap` keys block on this gate so their queues stay occupied, saturating the cap
    // while the remaining keys' dispatches pile up against the stall path.
    final var gate = new CountDownLatch(1);
    final var gatedStarted = new CountDownLatch(cap);

    final var totalRecords = keys * recordsPerKey;
    final var processed = new CountDownLatch(totalRecords);

    final var producers = new ArrayList<Thread>(keys);
    for (int k = 0; k < keys; k++) {
      final var key = "key-" + k;
      final var tracker = trackers.get(key);
      final var gated = k < cap;
      producers.add(
        Thread.ofVirtual().start(() -> {
          for (long offset = 0; offset < recordsPerKey; offset++) {
            final var rec = recordWithKey(key, offset);
            final var first = offset == 0;
            dispatcher.dispatch(
              rec,
              () -> {
                if (gated && first) {
                  gatedStarted.countDown();
                  awaitGate(gate);
                }
                runGuardedBody(tracker, rec.offset());
                processed.countDown();
              },
              () -> {}
            );
          }
        })
      );
    }

    // Confirm the cap is genuinely saturated (all `cap` gated workers are parked) before opening.
    assertTrue(gatedStarted.await(10, TimeUnit.SECONDS), "all cap-many gated workers must be parked (saturated)");
    // Give the non-gated producers a moment to pile into the stall path.
    Thread.sleep(100);
    gate.countDown();

    assertTrue(processed.await(60, TimeUnit.SECONDS), "all records must land after saturation releases");
    for (final var p : producers) p.join(Duration.ofSeconds(5));

    for (final var entry : trackers.entrySet()) {
      final var key = entry.getKey();
      final var tracker = entry.getValue();
      assertTrue(tracker.violations.isEmpty(), () -> "key " + key + " violations: " + tracker.violations);
      assertEquals(
        recordsPerKey,
        tracker.observed.size(),
        () -> "key " + key + " lost records under saturation hold: " + tracker.observed
      );
      assertEquals(
        expectedOrder(recordsPerKey),
        tracker.observed,
        () -> "key " + key + " reordered under saturation hold: " + tracker.observed
      );
    }
    // pendingCount is decremented in the worker's finally, which can run just after the per-record
    // latch the producers awaited counts down — so poll for the drain rather than reading it the
    // instant the latch releases (an immediate read flaked on slow CI runners).
    final var drainDeadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (dispatcher.pendingCount() != 0 && System.nanoTime() < drainDeadline) {
      Thread.onSpinWait();
    }
    assertEquals(0, dispatcher.pendingCount(), "no records may remain pending after saturation drains");
    dispatcher.close();
  }

  // ---------------------------------------------------------------------------------------------
  // Property 4 — null-key sentinel: all null-keyed records share ONE serial queue (ordered, never
  // overlapping) and don't starve or cross-contaminate keyed traffic.
  // ---------------------------------------------------------------------------------------------

  @Test
  void nullKeyTrafficSerializesAndCoexistsWithKeyedTraffic() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);

    final var nullTracker = new KeyTracker();
    final var keyedKeys = 16;
    final var recordsPerStream = 150;
    final var keyedTrackers = new ConcurrentHashMap<String, KeyTracker>();
    for (int k = 0; k < keyedKeys; k++) keyedTrackers.put("key-" + k, new KeyTracker());

    final var totalRecords = recordsPerStream + keyedKeys * recordsPerStream;
    final var processed = new CountDownLatch(totalRecords);
    final var barrier = new CyclicBarrier(keyedKeys + 1);

    final var producers = new ArrayList<Thread>();
    // Null-key producer.
    producers.add(
      Thread.ofVirtual().start(() -> {
        awaitBarrier(barrier);
        for (long offset = 0; offset < recordsPerStream; offset++) {
          final var rec = recordWithKey(null, offset);
          dispatcher.dispatch(
            rec,
            () -> {
              runGuardedBody(nullTracker, rec.offset());
              processed.countDown();
            },
            () -> {}
          );
        }
      })
    );
    // Keyed producers running concurrently with the null stream.
    for (int k = 0; k < keyedKeys; k++) {
      final var key = "key-" + k;
      final var tracker = keyedTrackers.get(key);
      producers.add(
        Thread.ofVirtual().start(() -> {
          awaitBarrier(barrier);
          for (long offset = 0; offset < recordsPerStream; offset++) {
            final var rec = recordWithKey(key, offset);
            dispatcher.dispatch(
              rec,
              () -> {
                runGuardedBody(tracker, rec.offset());
                processed.countDown();
              },
              () -> {}
            );
          }
        })
      );
    }

    assertTrue(processed.await(30, TimeUnit.SECONDS), "null + keyed traffic must all complete within 30s");
    for (final var p : producers) p.join(Duration.ofSeconds(5));

    // Null-key stream: serialized + ordered + complete (no starvation).
    assertTrue(nullTracker.violations.isEmpty(), () -> "null-key overlap/violations: " + nullTracker.violations);
    assertEquals(recordsPerStream, nullTracker.observed.size(), "null-key stream must not starve or lose records");
    assertEquals(expectedOrder(recordsPerStream), nullTracker.observed, "null-key stream must be in order");

    // Keyed streams unaffected by null-key coexistence.
    for (final var entry : keyedTrackers.entrySet()) {
      final var key = entry.getKey();
      final var tracker = entry.getValue();
      assertTrue(tracker.violations.isEmpty(), () -> "key " + key + " violations: " + tracker.violations);
      assertEquals(recordsPerStream, tracker.observed.size(), () -> "key " + key + " lost records");
      assertEquals(expectedOrder(recordsPerStream), tracker.observed, () -> "key " + key + " out of order");
    }
    dispatcher.close();
  }

  // ---------------------------------------------------------------------------------------------
  // Combined: eviction churn + concurrent overlap detection with the dispatcher's own
  // diagnostic snapshot, to ensure topKeyQueueDepths under contention never throws or reports a
  // negative depth (it walks the live map under lock).
  // ---------------------------------------------------------------------------------------------

  @Test
  void topKeyQueueDepthsSnapshotIsStableUnderConcurrentChurn() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher(32);
    final var keys = 200;
    final var recordsPerKey = 8;
    final var totalRecords = keys * recordsPerKey;
    final var processed = new CountDownLatch(totalRecords);
    final var snapshotErrors = new ConcurrentLinkedQueue<String>();
    final var stopSnapshots = new AtomicBoolean(false);

    // Background snapshotter hammering the diagnostic path while dispatch + drain churn the map.
    final var snapshotter = Thread.ofVirtual().start(() -> {
      while (!stopSnapshots.get()) {
        try {
          final List<Map.Entry<byte[], Integer>> snap = dispatcher.topKeyQueueDepths(5);
          for (final var e : snap) {
            if (e.getValue() < 0) snapshotErrors.add("negative depth: " + e.getValue());
          }
        } catch (final RuntimeException ex) {
          snapshotErrors.add("snapshot threw: " + ex);
        }
        Thread.onSpinWait();
      }
    });

    final var producerCount = 8;
    final var barrier = new CyclicBarrier(producerCount);
    final var producers = new ArrayList<Thread>(producerCount);
    for (int t = 0; t < producerCount; t++) {
      final var slice = t;
      producers.add(
        Thread.ofVirtual().start(() -> {
          awaitBarrier(barrier);
          for (int k = slice; k < keys; k += producerCount) {
            final var key = "key-" + k;
            for (long offset = 0; offset < recordsPerKey; offset++) {
              dispatcher.dispatch(recordWithKey(key, offset), processed::countDown, () -> {});
            }
          }
        })
      );
    }

    assertTrue(processed.await(60, TimeUnit.SECONDS), "all records processed under snapshot churn");
    stopSnapshots.set(true);
    snapshotter.join(Duration.ofSeconds(5));
    for (final var p : producers) p.join(Duration.ofSeconds(5));

    assertTrue(snapshotErrors.isEmpty(), () -> "diagnostic snapshot errors under churn: " + snapshotErrors);
    // pendingCount is decremented in the worker's finally, which can run just after the per-record
    // latch the producers awaited counts down — so poll for the drain rather than reading it the
    // instant the latch releases (an immediate read flaked on slow CI runners).
    final var drainDeadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (dispatcher.pendingCount() != 0 && System.nanoTime() < drainDeadline) {
      Thread.onSpinWait();
    }
    assertEquals(0, dispatcher.pendingCount(), "pending must drain to 0");
    dispatcher.close();
  }

  @Test
  void concurrentDispatchAndCloseNeverLeavesPendingNegativeOrStuck() throws InterruptedException {
    // Race a close() against in-flight dispatch. pendingCount() must never go negative (a
    // double-decrement bug) and must settle. We don't assert all records ran — close() is allowed
    // to abandon stragglers — only that the counter is non-negative throughout and ends >= 0.
    final var dispatcher = new KeyOrderedDispatcher(16);
    final var keys = 50;
    final var recordsPerKey = 20;
    final var negativeSeen = new AtomicBoolean(false);
    final var stopWatch = new AtomicBoolean(false);

    final var watcher = Thread.ofVirtual().start(() -> {
      while (!stopWatch.get()) {
        if (dispatcher.pendingCount() < 0) negativeSeen.set(true);
        Thread.onSpinWait();
      }
    });

    final var producerCount = 6;
    final var barrier = new CyclicBarrier(producerCount);
    final var producers = new ArrayList<Thread>(producerCount);
    final var dispatched = new AtomicInteger();
    for (int t = 0; t < producerCount; t++) {
      final var slice = t;
      producers.add(
        Thread.ofVirtual().start(() -> {
          awaitBarrier(barrier);
          for (int k = slice; k < keys; k += producerCount) {
            final var key = "key-" + k;
            for (long offset = 0; offset < recordsPerKey; offset++) {
              dispatcher.dispatch(recordWithKey(key, offset), dispatched::incrementAndGet, () -> {});
            }
          }
        })
      );
    }

    // Close concurrently with dispatch, slightly delayed so some records are in flight.
    Thread.sleep(20);
    dispatcher.close();

    for (final var p : producers) p.join(Duration.ofSeconds(10));
    stopWatch.set(true);
    watcher.join(Duration.ofSeconds(5));

    assertFalse(negativeSeen.get(), "pendingCount() must never go negative under dispatch/close race");
    assertTrue(dispatcher.pendingCount() >= 0, "pendingCount() must end non-negative");
  }

  private static List<Long> expectedOrder(final int n) {
    final var expected = new ArrayList<Long>(n);
    for (long i = 0; i < n; i++) expected.add(i);
    return expected;
  }

  private static void awaitBarrier(final CyclicBarrier barrier) {
    try {
      barrier.await(10, TimeUnit.SECONDS);
    } catch (final Exception e) {
      throw new RuntimeException("barrier wait failed", e);
    }
  }

  private static void awaitGate(final CountDownLatch gate) {
    try {
      gate.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
