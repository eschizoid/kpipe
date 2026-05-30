package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/// Unit tests for [KeyOrderedDispatcher]. Drives the dispatcher directly with synthetic
/// records and a `processTask` that records the per-key call order; no Kafka, no consumer.
class KeyOrderedDispatcherTest {

  private static ConsumerRecord<String, byte[]> recordWithKey(final String key, final long offset) {
    return new ConsumerRecord<>("test-topic", 0, offset, key, new byte[0]);
  }

  @Test
  void recordsWithSameKeyProcessInOrder() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var observed = new ArrayList<Long>();
    final var latch = new CountDownLatch(5);

    for (long offset = 0; offset < 5; offset++) {
      final var record = recordWithKey("user-1", offset);
      dispatcher.dispatch(
        record,
        () -> {
          synchronized (observed) {
            observed.add(record.offset());
          }
          latch.countDown();
        },
        () -> {}
      );
    }

    assertTrue(latch.await(5, TimeUnit.SECONDS), "all records should finish within 5s");
    synchronized (observed) {
      assertEquals(List.of(0L, 1L, 2L, 3L, 4L), observed, "same-key records must process in offset order");
    }
    dispatcher.close();
  }

  @Test
  void recordsWithDifferentKeysProcessInParallel() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var startedConcurrently = new CountDownLatch(2);
    final var allowFinish = new CountDownLatch(1);
    final var finished = new CountDownLatch(2);

    // Two different keys: if processing is truly parallel, both `startedConcurrently` latches
    // should count down before either task is allowed to finish.
    for (final var key : List.of("a", "b")) {
      dispatcher.dispatch(
        recordWithKey(key, 0L),
        () -> {
          startedConcurrently.countDown();
          try {
            allowFinish.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          finished.countDown();
        },
        () -> {}
      );
    }

    assertTrue(startedConcurrently.await(2, TimeUnit.SECONDS), "different-key records must run concurrently");
    allowFinish.countDown();
    assertTrue(finished.await(2, TimeUnit.SECONDS), "both keys finish after permit released");
    dispatcher.close();
  }

  @Test
  void nullKeysSerializeThroughSentinelQueue() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var observed = new ArrayList<Long>();
    final var latch = new CountDownLatch(3);

    for (long offset = 10; offset < 13; offset++) {
      final var record = recordWithKey(null, offset);
      dispatcher.dispatch(
        record,
        () -> {
          synchronized (observed) {
            observed.add(record.offset());
          }
          latch.countDown();
        },
        () -> {}
      );
    }

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    synchronized (observed) {
      assertEquals(List.of(10L, 11L, 12L), observed, "null-key records share one serial queue");
    }
    dispatcher.close();
  }

  @Test
  void pendingCountReflectsBufferedAndInFlight() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var allowFinish = new CountDownLatch(1);
    final var firstStarted = new CountDownLatch(1);

    // Dispatch 3 records on same key — first runs immediately, other 2 wait in the queue.
    for (int i = 0; i < 3; i++) {
      final int idx = i;
      dispatcher.dispatch(
        recordWithKey("k", idx),
        () -> {
          if (idx == 0) firstStarted.countDown();
          try {
            allowFinish.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        },
        () -> {}
      );
    }

    assertTrue(firstStarted.await(2, TimeUnit.SECONDS));
    assertEquals(3, dispatcher.pendingCount(), "all 3 records pending (1 in-flight + 2 queued)");

    allowFinish.countDown();
    // Wait for everything to drain
    final var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (dispatcher.pendingCount() > 0 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(0, dispatcher.pendingCount());
    dispatcher.close();
  }

  @Test
  void onCompleteFiresAfterEveryRecord() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var completed = new AtomicInteger();
    final var processed = new AtomicInteger();
    final var latch = new CountDownLatch(10);

    for (int i = 0; i < 10; i++) {
      dispatcher.dispatch(
        recordWithKey("key-" + (i % 3), i),
        () -> {
          processed.incrementAndGet();
          latch.countDown();
        },
        completed::incrementAndGet
      );
    }

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    // onComplete is called from the worker after the wrapped task; allow a brief settle.
    final var deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (completed.get() < 10 && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertEquals(10, processed.get());
    assertEquals(10, completed.get(), "onComplete fires once per record");
    dispatcher.close();
  }

  @Test
  void processTaskThatThrowsDoesNotBreakWorkerLiveness() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var processedAfterThrow = new CountDownLatch(1);

    // First task throws; second task must still execute.
    dispatcher.dispatch(
      recordWithKey("k", 0),
      () -> {
        throw new RuntimeException("intentional");
      },
      () -> {}
    );
    dispatcher.dispatch(recordWithKey("k", 1), processedAfterThrow::countDown, () -> {});

    assertTrue(processedAfterThrow.await(2, TimeUnit.SECONDS), "worker must keep draining after a task throws");
    dispatcher.close();
  }

  @Test
  void evictionFreesLruEmptyQueueUnderCap() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher<String>(2);
    final var latches = new HashMap<String, CountDownLatch>();
    for (final var k : List.of("a", "b")) {
      final var done = new CountDownLatch(1);
      latches.put(k, done);
      dispatcher.dispatch(recordWithKey(k, 0), done::countDown, () -> {});
    }
    // Drain both keys.
    for (final var done : latches.values()) assertTrue(done.await(2, TimeUnit.SECONDS));

    // Both queues should be empty and evictable. Dispatch on a third key — should succeed
    // without blocking, evicting the LRU one ("a").
    final var thirdDone = new CountDownLatch(1);
    dispatcher.dispatch(recordWithKey("c", 0), thirdDone::countDown, () -> {});
    assertTrue(thirdDone.await(2, TimeUnit.SECONDS), "third key should dispatch by evicting an empty queue");
    dispatcher.close();
  }

  @Test
  void rejectsNonPositiveMaxKeys() {
    assertThrows(IllegalArgumentException.class, () -> new KeyOrderedDispatcher<String>(0));
    assertThrows(IllegalArgumentException.class, () -> new KeyOrderedDispatcher<String>(-1));
  }

  @Test
  void closeReturnsCleanlyWhenIdle() {
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    // No work dispatched, close should be a no-op (or near-no-op).
    dispatcher.close();
    // Calling close again should also be safe.
    dispatcher.close();
  }

  @Test
  void onCompleteExceptionsAreSwallowed() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var nextProcessed = new CountDownLatch(1);

    // First record's onComplete throws — must not break the worker.
    dispatcher.dispatch(
      recordWithKey("k", 0),
      () -> {},
      () -> {
        throw new RuntimeException("onComplete blew up");
      }
    );
    dispatcher.dispatch(recordWithKey("k", 1), nextProcessed::countDown, () -> {});

    assertTrue(nextProcessed.await(2, TimeUnit.SECONDS), "worker must keep draining after onComplete throws");
    dispatcher.close();
  }

  @Test
  void manyKeysWithDistinctOrderingPreserveTheirIndividualOrder() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var keys = 50;
    final var recordsPerKey = 20;
    // ConcurrentHashMap + CopyOnWriteArrayList because workers from many keys read+write
    // these structures concurrently. Per-key ordering is the dispatcher's job; per-key
    // observation visibility across worker threads is the test's job.
    final var perKeyObserved = new ConcurrentHashMap<String, List<Long>>();
    for (int k = 0; k < keys; k++) perKeyObserved.put("key-" + k, new CopyOnWriteArrayList<>());

    final var latch = new CountDownLatch(keys * recordsPerKey);

    for (int k = 0; k < keys; k++) {
      final var key = "key-" + k;
      for (long offset = 0; offset < recordsPerKey; offset++) {
        final var record = recordWithKey(key, offset);
        dispatcher.dispatch(
          record,
          () -> {
            perKeyObserved.get(key).add(record.offset());
            latch.countDown();
          },
          () -> {}
        );
      }
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS), "all records should finish within 10s");
    for (final var entry : perKeyObserved.entrySet()) {
      final var observed = entry.getValue();
      assertEquals(recordsPerKey, observed.size(), "all records per key should be observed");
      // Each per-key list should be strictly ordered by offset.
      for (int i = 1; i < observed.size(); i++) {
        assertTrue(observed.get(i) > observed.get(i - 1), () ->
          "key %s out of order: %s".formatted(entry.getKey(), entry.getValue())
        );
      }
    }
    dispatcher.close();
  }

  @Test
  void topKeyQueueDepthsReturnsDeepestFirstAndCapsAtN() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var allowFinish = new CountDownLatch(1);
    final var firstStarted = new CountDownLatch(3);

    // 3 keys, varied queue depths: a→5, b→2, c→8. Workers block on `allowFinish` so the queues
    // stay populated for the snapshot.
    final var depths = new HashMap<String, Integer>();
    depths.put("a", 5);
    depths.put("b", 2);
    depths.put("c", 8);
    for (final var entry : depths.entrySet()) {
      final var key = entry.getKey();
      for (int i = 0; i < entry.getValue(); i++) {
        final int idx = i;
        dispatcher.dispatch(
          recordWithKey(key, idx),
          () -> {
            if (idx == 0) firstStarted.countDown();
            try {
              allowFinish.await();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          },
          () -> {}
        );
      }
    }
    assertTrue(firstStarted.await(2, TimeUnit.SECONDS), "first task on each key should start");

    // queue depth = total enqueued minus the one being actively processed (poll removed it).
    // So expected depths are a=4, b=1, c=7.
    final var top2 = dispatcher.topKeyQueueDepths(2);
    assertEquals(2, top2.size(), "top-2 must contain exactly 2 entries");
    assertEquals("c", top2.get(0).getKey(), "deepest queue first");
    assertEquals(7, top2.get(0).getValue());
    assertEquals("a", top2.get(1).getKey(), "second-deepest queue second");
    assertEquals(4, top2.get(1).getValue());

    final var topAll = dispatcher.topKeyQueueDepths(100);
    assertEquals(3, topAll.size(), "asking for more than exists returns all");

    allowFinish.countDown();
    dispatcher.close();
  }

  @Test
  void topKeyQueueDepthsRejectsNonPositive() {
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    assertThrows(IllegalArgumentException.class, () -> dispatcher.topKeyQueueDepths(0));
    assertThrows(IllegalArgumentException.class, () -> dispatcher.topKeyQueueDepths(-1));
    dispatcher.close();
  }

  @Test
  void dispatchStallUnderSaturationEventuallyRecoversWhenAQueueDrains() throws InterruptedException {
    // Cap=2. Hold both keys' workers blocked, then attempt to dispatch a third key — that call
    // must stall in allocateNewQueue. Releasing one of the blocked workers must let the third
    // dispatch complete.
    final var dispatcher = new KeyOrderedDispatcher<String>(2);
    final var workerA = new CountDownLatch(1);
    final var workerB = new CountDownLatch(1);
    final var workerAStarted = new CountDownLatch(1);
    final var workerBStarted = new CountDownLatch(1);

    dispatcher.dispatch(
      recordWithKey("a", 0),
      () -> {
        workerAStarted.countDown();
        try {
          workerA.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      },
      () -> {}
    );
    dispatcher.dispatch(
      recordWithKey("b", 0),
      () -> {
        workerBStarted.countDown();
        try {
          workerB.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      },
      () -> {}
    );
    assertTrue(workerAStarted.await(2, TimeUnit.SECONDS));
    assertTrue(workerBStarted.await(2, TimeUnit.SECONDS));

    // Spawn the stalled dispatch on a separate thread so we can observe its eventual completion.
    final var thirdCompleted = new CountDownLatch(1);
    Thread.ofVirtual().start(() -> dispatcher.dispatch(recordWithKey("c", 0), thirdCompleted::countDown, () -> {}));

    // The third dispatch must NOT complete while both workers are still blocked.
    assertTrue(
      !thirdCompleted.await(200, TimeUnit.MILLISECONDS),
      "third dispatch must stall while all queues at cap are non-empty"
    );

    // Releasing one worker drains its queue, makes it evictable, and lets the stalled dispatch
    // proceed.
    workerA.countDown();
    assertTrue(thirdCompleted.await(2, TimeUnit.SECONDS), "stalled dispatch must recover after a queue drains");

    workerB.countDown();
    dispatcher.close();
  }

  @Test
  void byteArrayKeysWithSameContentSerializeThroughOneQueue() throws InterruptedException {
    // Regression test for the Object-equality footgun on byte[] keys. Kafka's
    // ByteArrayDeserializer hands us a fresh byte[] per record, so two records with the same
    // logical key arrive as different array instances. Without content-based normalization
    // (ByteBuffer.wrap), the LRU map would treat them as distinct keys and the records would
    // run concurrently — silently breaking the KEY_ORDERED contract.
    final var dispatcher = new KeyOrderedDispatcher<byte[]>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var keyContent = "user-A".getBytes();
    final var observed = new CopyOnWriteArrayList<long[]>();
    final var latch = new CountDownLatch(4);

    for (int i = 0; i < 4; i++) {
      // Each record gets a *new* byte[] instance with identical content. Reference equality
      // would treat these as four distinct keys; content equality (after normalization)
      // collapses them to one.
      final byte[] freshKey = keyContent.clone();
      final long offset = i;
      dispatcher.dispatch(
        new ConsumerRecord<>("test-topic", 0, offset, freshKey, new byte[0]),
        () -> {
          final long startNs = System.nanoTime();
          try {
            Thread.sleep(20);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          observed.add(new long[] { offset, startNs, System.nanoTime() });
          latch.countDown();
        },
        () -> {}
      );
    }

    assertTrue(latch.await(5, TimeUnit.SECONDS), "all 4 records must finish");

    // All records share a logical key → must NOT overlap in time.
    final var sorted = new ArrayList<>(observed);
    sorted.sort(Comparator.comparingLong(o -> o[1]));
    for (int i = 1; i < sorted.size(); i++) {
      assertTrue(
        sorted.get(i)[1] >= sorted.get(i - 1)[2],
        "byte[] keys with same content must serialize — overlap detected"
      );
    }
    dispatcher.close();
  }

  @Test
  void topKeyQueueDepthsReturnsNullForTheNullKeyedQueue() throws InterruptedException {
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var allowFinish = new CountDownLatch(1);
    final var firstStarted = new CountDownLatch(2);

    // One null-key record + one regular-key record, both blocked so the queues stay populated
    // for the snapshot.
    dispatcher.dispatch(
      recordWithKey(null, 0),
      () -> {
        firstStarted.countDown();
        try {
          allowFinish.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      },
      () -> {}
    );
    dispatcher.dispatch(
      recordWithKey("normal", 0),
      () -> {
        firstStarted.countDown();
        try {
          allowFinish.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      },
      () -> {}
    );
    assertTrue(firstStarted.await(2, TimeUnit.SECONDS));

    final var snapshot = dispatcher.topKeyQueueDepths(10);
    assertEquals(2, snapshot.size());
    final var hasNullKey = snapshot.stream().anyMatch(e -> e.getKey() == null);
    final var hasNormalKey = snapshot.stream().anyMatch(e -> "normal".equals(e.getKey()));
    assertTrue(hasNullKey, "null-keyed queue must appear with key == null, not an opaque sentinel");
    assertTrue(hasNormalKey, "regular key must appear unchanged in snapshot");

    allowFinish.countDown();
    dispatcher.close();
  }

  @Test
  void byteArrayKeyMutationAfterDispatchDoesNotCorruptQueue() throws InterruptedException {
    // Defensive-copy guarantee: if the caller retains and mutates the byte[] key after
    // dispatch, the dispatcher's internal LRU key identity must NOT change (otherwise the
    // map entry becomes unreachable and the queue leaks). normalizeKey clones the bytes
    // before wrapping, so the wrapper holds a snapshot the caller can't alter.
    final var dispatcher = new KeyOrderedDispatcher<byte[]>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var keyContent = "user-A".getBytes();
    final var allowFinish = new CountDownLatch(1);
    final var firstStarted = new CountDownLatch(1);
    final var processedOffsets = new CopyOnWriteArrayList<Long>();

    // First record locks the worker so we can observe map state mid-dispatch.
    dispatcher.dispatch(
      new ConsumerRecord<>("test-topic", 0, 0L, keyContent, new byte[0]),
      () -> {
        firstStarted.countDown();
        try {
          allowFinish.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        processedOffsets.add(0L);
      },
      () -> {}
    );
    assertTrue(firstStarted.await(2, TimeUnit.SECONDS));

    // Mutate the original key bytes after dispatch — would corrupt a non-defensive wrapper.
    Arrays.fill(keyContent, (byte) 0);

    // A new record with the original content (different array instance) must still route to
    // the same queue — proving the dispatcher's internal key was a defensive copy.
    dispatcher.dispatch(
      new ConsumerRecord<>("test-topic", 0, 1L, "user-A".getBytes(), new byte[0]),
      () -> processedOffsets.add(1L),
      () -> {}
    );

    allowFinish.countDown();
    // Wait for both to process.
    final var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (processedOffsets.size() < 2 && System.nanoTime() < deadline) Thread.sleep(10);

    assertEquals(List.of(0L, 1L), processedOffsets, "both records must serialize through the same queue, in order");
    dispatcher.close();
  }

  @Test
  void dispatchAfterSignalShutdownStillProcessesNormally() throws InterruptedException {
    // signalShutdown() must only break the saturation yield-loop — it must NOT cause a
    // normal dispatch to skip its work. Skipping would orphan the offset that
    // KPipeConsumer.processRecords already tracked on the offset manager, leaving pending
    // offsets that no worker ever marks processed.
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var processed = new CountDownLatch(1);

    dispatcher.signalShutdown();
    dispatcher.dispatch(recordWithKey("k", 0), processed::countDown, () -> {});

    assertTrue(
      processed.await(2, TimeUnit.SECONDS),
      "dispatch after signalShutdown (no saturation) must still process normally"
    );
    dispatcher.close();
  }

  @Test
  void atCapDuringShutdownStillEvictsIdleQueueRatherThanAbandoning() throws InterruptedException {
    // During shutdown, a new-key record that hits the cap should still be processed if an
    // empty+idle queue can be evicted to make room — abandoning it would orphan its already
    // tracked offset and force avoidable reprocessing. We only give up when stalling (no
    // evictable queue) is the only option.
    final var dispatcher = new KeyOrderedDispatcher<String>(2);
    final var keyADone = new CountDownLatch(1);
    final var keyBStarted = new CountDownLatch(1);
    final var allowBFinish = new CountDownLatch(1);

    // Key "a": finishes immediately → its queue becomes empty + idle (evictable).
    dispatcher.dispatch(recordWithKey("a", 0), keyADone::countDown, () -> {});
    // Key "b": blocks → its queue stays non-empty + active (not evictable). Now at cap=2.
    dispatcher.dispatch(
      recordWithKey("b", 0),
      () -> {
        keyBStarted.countDown();
        try {
          allowBFinish.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      },
      () -> {}
    );
    assertTrue(keyADone.await(2, TimeUnit.SECONDS), "key a must finish (queue becomes evictable)");
    assertTrue(keyBStarted.await(2, TimeUnit.SECONDS), "key b must be in-flight (queue stays at cap)");

    // Shutdown signalled while at cap — but "a" is evictable, so "c" must still process.
    dispatcher.signalShutdown();
    final var keyCProcessed = new CountDownLatch(1);
    dispatcher.dispatch(recordWithKey("c", 0), keyCProcessed::countDown, () -> {});

    assertTrue(
      keyCProcessed.await(2, TimeUnit.SECONDS),
      "new-key record must be processed during shutdown when an idle queue is evictable"
    );

    allowBFinish.countDown();
    dispatcher.close();
  }

  @Test
  void closeInterruptsStuckWorkersAfterDrainTimeout() throws InterruptedException {
    // Matches ParallelDispatcher's `executor.shutdownNow()` semantics: workers stuck in
    // user code (here a long `Thread.sleep`) get interrupted when close() runs past its
    // drain timeout. The interrupt-aware sleep throws InterruptedException → the worker
    // exits without completing the sleep.
    final var dispatcher = new KeyOrderedDispatcher<String>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var started = new CountDownLatch(1);
    final var wasInterrupted = new AtomicBoolean(false);
    final var taskExited = new CountDownLatch(1);

    dispatcher.dispatch(
      recordWithKey("stuck", 0),
      () -> {
        started.countDown();
        try {
          Thread.sleep(Duration.ofMinutes(10).toMillis()); // longer than any reasonable test
        } catch (final InterruptedException ie) {
          wasInterrupted.set(true);
          Thread.currentThread().interrupt();
        } finally {
          taskExited.countDown();
        }
      },
      () -> {}
    );
    assertTrue(started.await(2, TimeUnit.SECONDS), "worker must start");

    // close() runs its 5s drain wait (pending stays > 0 because worker is stuck), then
    // interrupts the active worker(s). The interrupt unblocks Thread.sleep.
    dispatcher.close();
    assertTrue(taskExited.await(2, TimeUnit.SECONDS), "worker should exit after interrupt");
    assertTrue(wasInterrupted.get(), "worker must observe InterruptedException from close()");
  }

  @Test
  void signalShutdownUnblocksStalledDispatchBeforeClose() throws InterruptedException {
    // signalShutdown() must be enough on its own — no close() needed — to release a consumer
    // thread spinning in the saturation yield-loop. This is what KPipeConsumer.close() relies
    // on to call signalShutdown() BEFORE waitForInFlightDrain (so drain doesn't wait
    // indefinitely on a stuck pending counter).
    final var dispatcher = new KeyOrderedDispatcher<String>(2);
    final var workerA = new CountDownLatch(1);
    final var workerB = new CountDownLatch(1);
    final var workerAStarted = new CountDownLatch(1);
    final var workerBStarted = new CountDownLatch(1);

    dispatcher.dispatch(
      recordWithKey("a", 0),
      () -> {
        workerAStarted.countDown();
        try {
          workerA.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      },
      () -> {}
    );
    dispatcher.dispatch(
      recordWithKey("b", 0),
      () -> {
        workerBStarted.countDown();
        try {
          workerB.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      },
      () -> {}
    );
    assertTrue(workerAStarted.await(2, TimeUnit.SECONDS));
    assertTrue(workerBStarted.await(2, TimeUnit.SECONDS));

    // Third key dispatches on a separate thread → will stall in allocateNewQueue's yield-loop.
    final var thirdCompleted = new CountDownLatch(1);
    final var pendingBeforeSignal = new AtomicInteger();
    Thread.ofVirtual().start(() -> {
      dispatcher.dispatch(recordWithKey("c", 0), () -> {}, () -> {});
      pendingBeforeSignal.set((int) dispatcher.pendingCount());
      thirdCompleted.countDown();
    });

    // Confirm it's actually stuck (workers still blocked, no queues drained).
    assertTrue(
      !thirdCompleted.await(200, TimeUnit.MILLISECONDS),
      "third dispatch must stall while all queues at cap are non-empty"
    );

    // signalShutdown — should unblock the stalled dispatch WITHOUT calling close() first.
    dispatcher.signalShutdown();
    assertTrue(thirdCompleted.await(2, TimeUnit.SECONDS), "signalShutdown alone must release the stalled dispatch");
    // The abandoned record should NOT leave pending stuck high — dispatch must roll it back.
    assertEquals(
      2,
      pendingBeforeSignal.get(),
      "only the two blocked records should be pending; the stalled one rolls back"
    );

    workerA.countDown();
    workerB.countDown();
    dispatcher.close();
  }

  @Test
  void topKeyQueueDepthsReturnsByteArrayCopyNotInternalBuffer() throws InterruptedException {
    // Diagnostic snapshot must return a defensive byte[] copy for byte[]-keyed queues so a
    // caller can't mutate the internal ByteBuffer (position/limit/backing array) and corrupt
    // the dispatcher's LRU.
    final var dispatcher = new KeyOrderedDispatcher<byte[]>(KeyOrderedDispatcher.DEFAULT_MAX_KEYS);
    final var allowFinish = new CountDownLatch(1);
    final var started = new CountDownLatch(1);
    final var originalKey = "user-A".getBytes();

    dispatcher.dispatch(
      new ConsumerRecord<>("test-topic", 0, 0L, originalKey, new byte[0]),
      () -> {
        started.countDown();
        try {
          allowFinish.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      },
      () -> {}
    );
    assertTrue(started.await(2, TimeUnit.SECONDS));

    final var snapshot = dispatcher.topKeyQueueDepths(1);
    assertEquals(1, snapshot.size());
    final var key = snapshot.get(0).getKey();
    assertTrue(key instanceof byte[], () -> "byte[] keys must be returned as byte[], got " + key.getClass());
    final var keyBytes = (byte[]) key;
    assertArrayEquals(originalKey, keyBytes, "snapshot must reflect the original key bytes");

    // Mutate the returned array — the dispatcher's internal map key must be unaffected.
    Arrays.fill(keyBytes, (byte) 0);
    final var snapshot2 = dispatcher.topKeyQueueDepths(1);
    final var keyBytes2 = (byte[]) snapshot2.get(0).getKey();
    assertArrayEquals(originalKey, keyBytes2, "internal map key must survive caller mutation of the previous snapshot");

    allowFinish.countDown();
    dispatcher.close();
  }
}
