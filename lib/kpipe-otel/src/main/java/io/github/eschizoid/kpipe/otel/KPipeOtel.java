package io.github.eschizoid.kpipe.otel;

import io.github.eschizoid.kpipe.consumer.MetricsExporter;
import io.github.eschizoid.kpipe.tracing.Tracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.BatchCallback;
import java.util.Objects;
import java.util.function.LongSupplier;

/// KPipe on OpenTelemetry: a tracer, a metrics exporter, and the export of a Schema Registry cache,
/// all reporting under the instrumentation scope [#SCOPE].
///
/// ```java
/// final var metrics = KPipeOtel.metrics(openTelemetry); // one per OpenTelemetry, shared
///
/// KPipe.json("orders", props)
///   .withTracer(KPipeOtel.tracer(openTelemetry))
///   .withMetrics(metrics)
///   .toKafka("orders-enriched")
///   .start();
/// ```
///
/// This module depends only on `opentelemetry-api`. Bring an SDK, such as
/// `io.opentelemetry:opentelemetry-sdk`, and an exporter.
public final class KPipeOtel {

  /// The instrumentation scope every span and instrument reports under.
  public static final String SCOPE = "io.github.eschizoid.kpipe";

  private KPipeOtel() {}

  /// A tracer that gives each consumed record a `CONSUMER` span named `process <topic>`, a child of
  /// the trace in its headers, with the OpenTelemetry messaging attributes. Each failed attempt
  /// adds a `kpipe.attempt.failed` event; a record that fails for good records its exception, sets
  /// `error.type` and marks the span as an error. Every record written on the record's behalf
  /// carries the span's context.
  ///
  /// Context travels in the headers through `openTelemetry`'s text-map propagator, so configure
  /// one, such as W3C trace context. With none, nothing propagates, and this method logs a
  /// warning.
  ///
  /// @param openTelemetry where spans go and which propagator carries their context
  /// @return the tracer, for `withTracer`
  public static Tracer tracer(final OpenTelemetry openTelemetry) {
    return new OtelTracer(openTelemetry);
  }

  /// An exporter of consumers' metrics: record, DLQ, retry, lag, backpressure, circuit-breaker and
  /// offset-commit counts, and the processing time. One exporter serves any number of consumers and
  /// stays registered with the SDK, so create one per `OpenTelemetry` and share it.
  ///
  /// @param openTelemetry where to register the instruments
  /// @return the exporter, for `withMetrics`
  public static MetricsExporter metrics(final OpenTelemetry openTelemetry) {
    return OtelMetrics.of(openTelemetry);
  }

  /// Exports a Schema Registry cache, such as `CachedSchemaResolver` in
  /// `kpipe-schema-registry-confluent`, as `kpipe.schema_registry.cache.hits` and `.misses`
  /// (counters, `{lookup}`) and `.size` (a gauge, `{schema}`), with no attributes. Call it once
  /// per cache: each call reports the cache, so two calls over one cache double its counts.
  ///
  /// ```java
  /// final var registration = KPipeOtel.schemaRegistryCache(
  ///   openTelemetry, resolver::hitCount, resolver::missCount, () -> (long) resolver.size());
  /// ```
  ///
  /// @param openTelemetry where to register the instruments
  /// @param hits cumulative cache hits
  /// @param misses cumulative cache misses, each one a registry request
  /// @param size schemas currently cached
  /// @return the registration; close it to stop exporting the cache
  public static BatchCallback schemaRegistryCache(
    final OpenTelemetry openTelemetry,
    final LongSupplier hits,
    final LongSupplier misses,
    final LongSupplier size
  ) {
    return OtelMetrics.exportSchemaRegistryCache(
      Objects.requireNonNull(openTelemetry, "openTelemetry"),
      hits,
      misses,
      size
    );
  }
}
