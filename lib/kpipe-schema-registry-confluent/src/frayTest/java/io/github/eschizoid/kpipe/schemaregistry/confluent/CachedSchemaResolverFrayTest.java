package io.github.eschizoid.kpipe.schemaregistry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// The schema-cache stampede race.
///
/// Two callers miss on the same schema id at once. `computeIfAbsent` has to collapse them into a
/// single underlying load: schema ids are immutable in the registry, so caching by id needs no TTL
/// or invalidation, but a miss that escapes the atomic load turns every cold key into as many
/// registry round-trips as there are concurrent callers — the classic stampede, against an HTTP
/// dependency on the record path.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class CachedSchemaResolverFrayTest {

  private static final int SCHEMA_ID = 42;
  private static final String SCHEMA_JSON = "{\"type\":\"string\"}";

  @FrayTest(iterations = 500)
  void concurrentMissesCollapseToOneLoad() {
    final var loadCount = new AtomicInteger();
    // Counting fake: every underlying load bumps the counter and returns a DISTINCT instance.
    // A compile-time constant would be interned and handed back identically on every call, which
    // would make the reference check below pass however many loads ran.
    final SchemaResolver counting = id -> {
      loadCount.incrementAndGet();
      return new String(SCHEMA_JSON.toCharArray());
    };
    final var resolver = new CachedSchemaResolver(counting);
    final var first = new AtomicReference<String>();
    final var second = new AtomicReference<String>();

    FrayScenariosSchemaRegistry.runConcurrently(
      () -> first.set(resolver.lookupById(SCHEMA_ID)),
      () -> second.set(resolver.lookupById(SCHEMA_ID))
    );

    assertEquals(1, loadCount.get(), "concurrent misses did not collapse into a single registry load");
    assertEquals(SCHEMA_JSON, first.get(), "first caller resolved the wrong schema");
    assertSame(first.get(), second.get(), "callers resolved different instances for the same immutable schema id");
  }
}
