package io.github.eschizoid.kpipe.schemaregistry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CachedSchemaResolverTest {

  /// Counts every lookup-by-id and serves a deterministic JSON string per id so tests can prove
  /// the cache short-circuited the delegate.
  private static final class CountingResolver implements SchemaResolver {

    final AtomicInteger calls = new AtomicInteger();
    final ConcurrentHashMap<Integer, String> seenIds = new ConcurrentHashMap<>();

    @Override
    public String lookupById(final int schemaId) {
      calls.incrementAndGet();
      seenIds.put(schemaId, "");
      return "{\"type\":\"record\",\"name\":\"S" + schemaId + "\",\"fields\":[]}";
    }
  }

  @Test
  void firstLookupHitsTheDelegate() {
    final var counter = new CountingResolver();
    final var cached = new CachedSchemaResolver(counter);

    final var json = cached.lookupById(42);

    assertEquals("{\"type\":\"record\",\"name\":\"S42\",\"fields\":[]}", json);
    assertEquals(1, counter.calls.get());
    assertEquals(1, cached.missCount());
    assertEquals(0, cached.hitCount());
  }

  @Test
  void repeatedLookupOfSameIdServesFromCache() {
    final var counter = new CountingResolver();
    final var cached = new CachedSchemaResolver(counter);

    final var first = cached.lookupById(42);
    final var second = cached.lookupById(42);
    final var third = cached.lookupById(42);

    assertSame(first, second, "cached value must be the same instance on hit");
    assertSame(second, third);
    assertEquals(1, counter.calls.get(), "delegate must only be called on the first miss");
    assertEquals(1, cached.missCount());
    assertEquals(2, cached.hitCount());
  }

  @Test
  void differentIdsHitTheDelegateIndependently() {
    final var counter = new CountingResolver();
    final var cached = new CachedSchemaResolver(counter);

    cached.lookupById(1);
    cached.lookupById(2);
    cached.lookupById(3);
    cached.lookupById(2); // hit
    cached.lookupById(1); // hit

    assertEquals(3, counter.calls.get());
    assertEquals(3, cached.missCount());
    assertEquals(2, cached.hitCount());
    assertEquals(3, cached.size());
  }

  @Test
  void concurrentMissesOnSameIdResultInExactlyOneDelegateCall() throws InterruptedException {
    final var counter = new CountingResolver();
    final var cached = new CachedSchemaResolver(counter);
    final var threads = 32;
    final var start = new CountDownLatch(1);
    final var done = new CountDownLatch(threads);

    for (int i = 0; i < threads; i++) {
      Thread.ofVirtual().start(() -> {
        try {
          start.await();
          cached.lookupById(7);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
    }

    start.countDown();
    done.await();

    assertEquals(1, counter.calls.get(), "computeIfAbsent must atomicize the load");
  }

  @Test
  void invalidateAllDropsEverything() {
    final var counter = new CountingResolver();
    final var cached = new CachedSchemaResolver(counter);
    cached.lookupById(1);
    cached.lookupById(2);
    assertEquals(2, cached.size());

    cached.invalidateAll();

    assertEquals(0, cached.size());
    cached.lookupById(1);
    assertEquals(3, counter.calls.get(), "invalidateAll forces a re-fetch");
  }

  @Test
  void rejectsNullDelegate() {
    assertThrows(NullPointerException.class, () -> new CachedSchemaResolver(null));
  }

  @Test
  void failedLookupIsNotCachedAndIsRetriedOnNextCall() {
    /// First call's delegate throws; second call's delegate succeeds.
    /// If the cache poisoned itself with a sentinel on failure, the second call would either
    /// throw again (sentinel re-raised) or skip the delegate entirely (sentinel served as a
    /// silent success). Either way the topic would be pinned into a permanent decode-error
    /// state, violating the §19 "cache forever in-process" invariant which only applies AFTER a
    /// successful lookup.
    final var calls = new AtomicInteger();
    final var shouldFail = new AtomicReference<>(Boolean.TRUE);
    final SchemaResolver flaky = id -> {
      calls.incrementAndGet();
      if (shouldFail.get()) {
        throw new RuntimeException("transient SR failure for id=" + id);
      }
      return "{\"type\":\"record\",\"name\":\"S" + id + "\",\"fields\":[]}";
    };
    final var cached = new CachedSchemaResolver(flaky);

    final var thrown = assertThrows(RuntimeException.class, () -> cached.lookupById(99));
    assertTrue(thrown.getMessage().contains("transient SR failure"), "delegate exception must propagate verbatim");
    assertEquals(1, calls.get(), "delegate must be invoked exactly once on the failing call");
    assertEquals(0, cached.size(), "failed lookup must not leave any entry in the cache");

    shouldFail.set(Boolean.FALSE);
    final var schema = cached.lookupById(99);

    assertEquals("{\"type\":\"record\",\"name\":\"S99\",\"fields\":[]}", schema);
    assertEquals(2, calls.get(), "delegate must be re-invoked on retry — failure was not cached");
    assertEquals(1, cached.size(), "successful retry must populate the cache");

    cached.lookupById(99);
    assertEquals(2, calls.get(), "after success, subsequent lookups serve from cache");
    assertEquals(1, cached.hitCount());
  }

  @Test
  void concurrentMissOnSameIdCollapsesToOneDelegateCall() throws InterruptedException {
    /// Two virtual threads race a miss on the same id; the delegate is slow enough that the
    /// second thread blocks inside `computeIfAbsent` waiting for the first. The §19 contract is
    /// that `computeIfAbsent` collapses concurrent misses to a single delegate call — anything
    /// else would amplify HTTP load to the registry under deserializer fan-in.
    final var delegateCalls = new AtomicInteger();
    final var inDelegate = new CountDownLatch(1);
    final var releaseDelegate = new CountDownLatch(1);
    final SchemaResolver slow = id -> {
      delegateCalls.incrementAndGet();
      inDelegate.countDown();
      try {
        releaseDelegate.await(5, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      return "{\"type\":\"record\",\"name\":\"S" + id + "\",\"fields\":[]}";
    };
    final var cached = new CachedSchemaResolver(slow);
    final var result1 = new AtomicReference<String>();
    final var result2 = new AtomicReference<String>();
    final var done = new CountDownLatch(2);

    final var t1 = Thread.ofVirtual().start(() -> {
      try {
        result1.set(cached.lookupById(123));
      } finally {
        done.countDown();
      }
    });
    /// Wait for t1 to be inside the delegate, then start t2 — guarantees t2 hits the
    /// `computeIfAbsent` bucket while t1 still holds it.
    assertTrue(inDelegate.await(5, TimeUnit.SECONDS), "first thread should enter delegate");
    final var t2 = Thread.ofVirtual().start(() -> {
      try {
        result2.set(cached.lookupById(123));
      } finally {
        done.countDown();
      }
    });

    releaseDelegate.countDown();
    assertTrue(done.await(5, TimeUnit.SECONDS), "both threads should complete");
    t1.join();
    t2.join();

    assertEquals(1, delegateCalls.get(), "concurrent misses on same id must collapse to one delegate call");
    assertNotNull(result1.get());
    assertEquals(result1.get(), result2.get(), "both threads must observe the same schema string");
    assertEquals(1, cached.size());
  }

  @Test
  void concurrentFailureOnSameIdDoesNotPoisonCache() throws InterruptedException {
    /// N virtual threads concurrently race a miss on the same id where the delegate throws.
    /// After all failures settle the delegate is flipped to success and one more call is made.
    /// If a regression ever wrote a sentinel into the map BEFORE the delegate exception unwound
    /// (e.g. a future "negative-cache" optimization), the recovery call would either throw or
    /// skip the delegate. The success on the recovery call proves the cache stayed clean
    /// through the failure storm.
    final var threads = 16;
    final var delegateCalls = new AtomicInteger();
    final var shouldFail = new AtomicReference<>(Boolean.TRUE);
    final SchemaResolver flaky = id -> {
      delegateCalls.incrementAndGet();
      if (shouldFail.get()) {
        throw new RuntimeException("storm failure for id=" + id);
      }
      return "{\"type\":\"record\",\"name\":\"S" + id + "\",\"fields\":[]}";
    };
    final var cached = new CachedSchemaResolver(flaky);
    final var start = new CountDownLatch(1);
    final var done = new CountDownLatch(threads);
    final var failuresObserved = new AtomicInteger();

    for (int i = 0; i < threads; i++) {
      Thread.ofVirtual().start(() -> {
        try {
          start.await();
          try {
            cached.lookupById(7);
          } catch (final RuntimeException expected) {
            failuresObserved.incrementAndGet();
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
    }

    start.countDown();
    assertTrue(done.await(10, TimeUnit.SECONDS), "all racing threads should complete");

    assertEquals(threads, failuresObserved.get(), "every racing thread must observe the failure (no silent sentinel)");
    assertEquals(0, cached.size(), "failure storm must not have written any sentinel into the cache");

    shouldFail.set(Boolean.FALSE);
    final var recovered = cached.lookupById(7);

    assertEquals("{\"type\":\"record\",\"name\":\"S7\",\"fields\":[]}", recovered);
    assertEquals(1, cached.size(), "successful recovery must populate the cache normally");
    /// One more lookup proves the success is now cached — no further delegate call.
    final var callsAfterRecovery = delegateCalls.get();
    cached.lookupById(7);
    assertEquals(callsAfterRecovery, delegateCalls.get(), "post-recovery lookups must hit the cache");
  }

  @Test
  void closePropagatesToCloseableDelegate() throws Exception {
    final var closed = new AtomicInteger();
    final class ClosableResolver implements SchemaResolver, AutoCloseable {

      @Override
      public String lookupById(final int schemaId) {
        return "{}";
      }

      @Override
      public void close() {
        closed.incrementAndGet();
      }
    }
    final var cached = new CachedSchemaResolver(new ClosableResolver());
    cached.close();
    assertEquals(1, closed.get());
  }

  @Test
  void closeIsNoopForNonCloseableDelegate() {
    final var cached = new CachedSchemaResolver(_ -> "{}");
    cached.close(); // must not throw
  }
}
