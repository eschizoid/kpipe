package io.github.eschizoid.kpipe.schemaregistry.confluent;

import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/// In-process LRU-free cache wrapping any [SchemaResolver].
///
/// **Why no TTL or LRU eviction in v1.** Confluent Schema Registry schema IDs are immutable —
/// ID 42 today means the same schema tomorrow and three years from now. Schemas only get added,
/// never reassigned. That makes the cache trivially correct:
///
/// - Cache hit: always returns the right schema for that ID.
/// - Cache miss: one HTTP call to the underlying resolver, then cached forever.
///
/// Cardinality grows on the order of schema-version registrations — typically tens per topic
/// over the lifetime of a production system, even with active evolution. Unbounded cache size
/// is not a real concern at that rate. If a user does need a bound, they can wrap this
/// resolver themselves.
///
/// That bound is an assumption about producer behaviour, not something this class enforces, and
/// a producer registering a schema per deploy — or a consumer spanning many topics with
/// independent lineages — breaks it. The failure mode is a slow leak in a long-running consumer,
/// which is among the hardest to attribute, so [#lookupById] logs once at WARNING when the cache
/// passes [#UNEXPECTED_SIZE]. Bounding is deliberately not attempted: eviction would mean a
/// synchronous registry round-trip on the record path for a schema about to be used again, and
/// the warning is what would tell us the assumption had failed.
///
/// **Thread-safety.** Backed by a [ConcurrentHashMap] so the hot read path is lock-free per
/// bucket. `computeIfAbsent` makes the load+store atomic — if two threads miss on the same ID
/// concurrently, only one HTTP call goes out and the other waits for it to populate.
///
/// **Observability.** [#hitCount] and [#missCount] expose cumulative counters so the user can
/// wire these into their metrics layer (see `kpipe-metrics-otel`'s
/// `PipelineMetricsObserver.bindSchemaRegistryCache(...)`).
///
/// ```java
/// final var raw = new ConfluentSchemaResolver("<http://schema-registry:8081>");
/// final var cached = new CachedSchemaResolver(raw);
/// final var format = AvroFormat.withRegistry(cached);  // per-record auto-lookup
/// ```
public final class CachedSchemaResolver implements SchemaResolver, AutoCloseable {

  private static final Logger LOGGER = System.getLogger(CachedSchemaResolver.class.getName());

  /// Distinct schema IDs beyond which the cardinality assumption looks wrong. Set well above the
  /// tens a healthy topic reaches over its lifetime, so crossing it means producer behaviour has
  /// changed rather than that the cache is merely warm.
  private static final int UNEXPECTED_SIZE = 1_000;

  private final SchemaResolver delegate;
  private final ConcurrentHashMap<Integer, String> cache = new ConcurrentHashMap<>();
  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();
  private final AtomicBoolean sizeWarningEmitted = new AtomicBoolean();

  /// Wraps `delegate` with an unbounded by-ID cache.
  ///
  /// @param delegate the resolver to call on cache miss (must be non-null)
  public CachedSchemaResolver(final SchemaResolver delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate cannot be null");
  }

  @Override
  public String lookupById(final int schemaId) {
    final var hit = cache.get(schemaId);
    if (hit != null) {
      hits.incrementAndGet();
      return hit;
    }
    final var resolved = cache.computeIfAbsent(schemaId, id -> {
      misses.incrementAndGet();
      return delegate.lookupById(id);
    });
    warnOnceIfCacheLooksUnbounded();
    return resolved;
  }

  /// Logs once when the cache exceeds the size its no-eviction design assumes.
  ///
  /// Guarded by a compare-and-set so sustained growth produces one line rather than one per
  /// record. Schema IDs are immutable, so nothing here is a correctness problem — the entries
  /// stay valid — but the memory is held for the life of the process.
  private void warnOnceIfCacheLooksUnbounded() {
    if (cache.size() <= UNEXPECTED_SIZE || !sizeWarningEmitted.compareAndSet(false, true)) {
      return;
    }
    LOGGER.log(
      Level.WARNING,
      "Schema cache holds more than {0} distinct IDs. This cache never evicts, which assumes a "
        + "topic registers tens of schemas over its lifetime; a producer registering per deploy "
        + "breaks that and the entries are held for the life of the process. Wrap this resolver "
        + "with your own bounded cache if the count keeps climbing. Logged once.",
      UNEXPECTED_SIZE
    );
  }

  /// Returns the number of cache hits since this resolver was constructed.
  ///
  /// @return cumulative hit count
  public long hitCount() {
    return hits.get();
  }

  /// Returns the number of cache misses (and thus underlying-resolver lookups) since
  /// construction.
  ///
  /// @return cumulative miss count
  public long missCount() {
    return misses.get();
  }

  /// Returns the number of distinct schema IDs currently cached.
  ///
  /// @return current cache size
  public int size() {
    return cache.size();
  }

  /// Closes the delegate resolver when it is [AutoCloseable]; a no-op otherwise. The cache
  /// itself holds no closeable resources.
  ///
  /// @throws RuntimeException if the delegate's close fails — the failure is logged at WARNING
  ///     and then rethrown wrapped, so callers still observe it
  @Override
  public void close() {
    if (delegate instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Failed to close delegate resolver", e);
        throw new RuntimeException("Failed to close delegate resolver", e);
      }
    }
  }
}
