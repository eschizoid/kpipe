package io.github.eschizoid.kpipe.schemaregistry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/// Pins caching behaviour past the cardinality the no-eviction design assumes.
///
/// The warning emitted at that point goes through `System.Logger` and is not asserted here —
/// intercepting it would need a custom `LoggerFinder` installed process-wide. What these tests
/// do pin is that crossing the threshold changes nothing else: entries are still retained, the
/// delegate is still called exactly once per distinct id, and the hit and miss counters still
/// add up. A warning path that quietly broke caching would fail here.
class CachedSchemaResolverSizeWarningTest {

  @Test
  void everyDistinctIdIsCachedAndLoadedExactlyOnce() {
    final var loads = new AtomicInteger();
    final SchemaResolver counting = id -> {
      loads.incrementAndGet();
      return "{\"type\":\"string\",\"id\":" + id + "}";
    };

    try (final var resolver = new CachedSchemaResolver(counting)) {
      for (var id = 0; id < 1_200; id++) {
        resolver.lookupById(id);
      }
      // A second pass must not reach the delegate: the warning path must not disturb caching.
      for (var id = 0; id < 1_200; id++) {
        resolver.lookupById(id);
      }

      assertEquals(1_200, loads.get(), "each distinct id should load exactly once");
      assertEquals(1_200, resolver.size(), "every entry should be retained; this cache does not evict");
      assertEquals(1_200, resolver.hitCount(), "the second pass should be served entirely from cache");
      assertTrue(resolver.missCount() == 1_200, "the first pass should account for every miss");
    }
  }

  @Test
  void stayingBelowTheThresholdChangesNothing() {
    final var loads = new AtomicInteger();
    try (final var resolver = new CachedSchemaResolver(id -> {
      loads.incrementAndGet();
      return "s" + id;
    })) {
      for (var id = 0; id < 50; id++) {
        resolver.lookupById(id);
      }
      assertEquals(50, resolver.size());
      assertEquals(50, loads.get());
    }
  }
}
