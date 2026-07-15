package io.github.eschizoid.kpipe.metrics.otel;

import io.github.eschizoid.kpipe.registry.Result;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/// OpenTelemetry-backed observer for pipeline outcomes. Hand to
/// `Stream.peekResult(observer)` and each [Result] flowing through the pipeline increments one
/// of three counters: `kpipe.pipeline.passed`, `kpipe.pipeline.filtered`, `kpipe.pipeline.failed`.
///
/// Why this is an observer, not a handler. The observer is invoked **after** the pipeline has
/// already classified the record. It cannot suppress a filter, retry a failure, or reroute a
/// pass. The downstream consumer flow (DLQ, retry, sink) runs regardless. If the observer
/// itself throws, the exception is caught and swallowed by the wrapping pipeline
/// (`DefaultSink`'s observer-wrap layer) — observer bugs cannot crash the consumer.
///
/// Example wiring (single-format facade):
///
/// ```java
/// final var observer = new PipelineMetricsObserver(openTelemetry, "orders");
/// try (var handle = KPipe.json("orders", props)
///     .pipe(enrich)
///     .peekResult(observer)
///     .toCustom(sink)
///     .start()) {
///   handle.awaitShutdown();
/// }
/// ```
///
/// Optional: bind Confluent Schema Registry cache metrics via
/// [#bindSchemaRegistryCache(LongSupplier, LongSupplier, LongSupplier)]. The observer uses
/// `LongSupplier` rather than a concrete `CachedSchemaResolver` type so this module stays
/// independent of `kpipe-schema-registry-confluent`.
///
/// Lifecycle. The pipeline-outcome counters are synchronous and need no teardown. The optional
/// Schema-Registry cache instruments are **asynchronous** — the OTel SDK strongly retains their
/// callbacks until the instrument is closed. The observer is [AutoCloseable]: call [#close()]
/// when discarding it so those callbacks stop firing. Re-binding closes the previous cache
/// registration first, so binding twice never leaks the earlier callbacks.
public final class PipelineMetricsObserver implements Consumer<Result<?>>, AutoCloseable {

  private static final AttributeKey<String> PIPELINE_KEY = AttributeKey.stringKey("pipeline");

  private final LongCounter passed;
  private final LongCounter filtered;
  private final LongCounter failed;
  private final Attributes baseAttributes;
  private final Meter meter;

  // Async Schema-Registry cache instruments, registered lazily by bindSchemaRegistryCache and held
  // so close() (and a re-bind) can unregister them. buildWithCallback hands the SDK a callback it
  // strongly retains until the instrument is closed; overwriting these fields without closing would
  // leak the old callbacks (they keep firing against stale suppliers → duplicate/stale series).
  private ObservableLongCounter srCacheHits;
  private ObservableLongCounter srCacheMisses;
  private ObservableLongGauge srCacheSize;

  /// Creates an observer that reports pipeline-outcome counters under the given OTel meter.
  ///
  /// @param openTelemetry the OTel entry point (must be non-null)
  /// @param pipelineName  optional pipeline label attached to every metric as `pipeline=...`;
  ///                      pass null to omit the label entirely
  public PipelineMetricsObserver(final OpenTelemetry openTelemetry, final String pipelineName) {
    Objects.requireNonNull(openTelemetry, "openTelemetry cannot be null");
    this.meter = openTelemetry.getMeter("io.github.eschizoid.kpipe.pipeline");
    this.baseAttributes = pipelineName == null ? Attributes.empty() : Attributes.of(PIPELINE_KEY, pipelineName);
    this.passed = meter
      .counterBuilder("kpipe.pipeline.passed")
      .setDescription("Records that flowed through the pipeline without filtering or failure")
      .setUnit("{record}")
      .build();
    this.filtered = meter
      .counterBuilder("kpipe.pipeline.filtered")
      .setDescription("Records intentionally dropped by an operator returning null")
      .setUnit("{record}")
      .build();
    this.failed = meter
      .counterBuilder("kpipe.pipeline.failed")
      .setDescription("Records whose pipeline raised an exception (captured as Result.Failed)")
      .setUnit("{record}")
      .build();
  }

  /// Pattern-matches the [Result] and increments the matching counter. Exceptions are not
  /// expected from `LongCounter.add(...)` itself; if a downstream exporter throws, it
  /// propagates and the `DefaultSink` observer-wrap layer catches it.
  @Override
  public void accept(final Result<?> result) {
    if (result == null) return;
    switch (result) {
      case Result.Passed<?> _ -> passed.add(1, baseAttributes);
      case Result.Filtered<?> _ -> filtered.add(1, baseAttributes);
      case Result.Failed<?> _ -> failed.add(1, baseAttributes);
    }
  }

  /// Registers instruments that report the current state of a Confluent Schema Registry cache
  /// (`CachedSchemaResolver` in `kpipe-schema-registry-confluent`). Wire the suppliers to the
  /// resolver's `hitCount`, `missCount`, and `size` methods.
  ///
  /// Metrics emitted:
  ///
  ///   * `kpipe.schema_registry.cache.hits`   — async **counter**: cumulative hits since start.
  ///   * `kpipe.schema_registry.cache.misses` — async **counter**: cumulative misses (each is an
  ///     HTTP call).
  ///   * `kpipe.schema_registry.cache.size`   — gauge: current distinct-id cardinality.
  ///
  /// Hits and misses are monotonic cumulative totals, so they are modeled as (observable) counters,
  /// not gauges — a downstream backend can then compute rates over them. Size is a level, so it
  /// stays a gauge.
  ///
  /// Binding is one-shot per registration: calling this again closes the previous three instruments
  /// before registering the new ones, so a re-bind swaps suppliers cleanly instead of leaking the
  /// earlier callbacks. Returns `this` for fluent chaining.
  ///
  /// @param hits   supplier of cumulative cache hits (must be non-null)
  /// @param misses supplier of cumulative cache misses (must be non-null)
  /// @param size   supplier of current cache size (must be non-null)
  /// @return this observer
  public PipelineMetricsObserver bindSchemaRegistryCache(
    final LongSupplier hits,
    final LongSupplier misses,
    final LongSupplier size
  ) {
    Objects.requireNonNull(hits, "hits supplier cannot be null");
    Objects.requireNonNull(misses, "misses supplier cannot be null");
    Objects.requireNonNull(size, "size supplier cannot be null");
    closeSchemaRegistryInstruments();
    this.srCacheHits = meter
      .counterBuilder("kpipe.schema_registry.cache.hits")
      .setDescription("Cumulative cache hits for the Confluent Schema Registry resolver")
      .setUnit("{lookup}")
      .buildWithCallback(m -> m.record(hits.getAsLong(), baseAttributes));
    this.srCacheMisses = meter
      .counterBuilder("kpipe.schema_registry.cache.misses")
      .setDescription("Cumulative cache misses (each one resulted in a registry HTTP call)")
      .setUnit("{lookup}")
      .buildWithCallback(m -> m.record(misses.getAsLong(), baseAttributes));
    this.srCacheSize = meter
      .gaugeBuilder("kpipe.schema_registry.cache.size")
      .ofLongs()
      .setDescription("Current number of distinct schema ids cached")
      .setUnit("{schema}")
      .buildWithCallback(m -> m.record(size.getAsLong(), baseAttributes));
    return this;
  }

  /// Unregisters the Schema-Registry cache instruments if bound. Idempotent — safe to call when
  /// nothing was bound, and safe to call more than once. The pipeline-outcome counters are
  /// synchronous and require no teardown, so this only tears down the async cache callbacks.
  @Override
  public void close() {
    closeSchemaRegistryInstruments();
  }

  private void closeSchemaRegistryInstruments() {
    if (srCacheHits != null) {
      srCacheHits.close();
      srCacheHits = null;
    }
    if (srCacheMisses != null) {
      srCacheMisses.close();
      srCacheMisses = null;
    }
    if (srCacheSize != null) {
      srCacheSize.close();
      srCacheSize = null;
    }
  }
}
