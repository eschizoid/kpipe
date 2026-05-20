package org.kpipe.schemaregistry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.kpipe.registry.SchemaResolver;

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
    final var cached = new CachedSchemaResolver(id -> "{}");
    cached.close(); // must not throw
  }
}
