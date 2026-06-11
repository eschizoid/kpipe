package io.github.eschizoid.kpipe.metrics.otel;

import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.registry.Result;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/// Asserts that [PipelineMetricsObserver] increments the right `kpipe.pipeline.*` counter for
/// each `Result<T>` variant and that the optional Schema Registry cache gauges expose the
/// configured suppliers. Built against `InMemoryMetricReader`, not `OpenTelemetry.noop()`, so the
/// assertions are on captured metric data rather than just on the absence of exceptions.
class PipelineMetricsObserverTest {

  private static final AttributeKey<String> PIPELINE_KEY = AttributeKey.stringKey("pipeline");

  private record Fixture(PipelineMetricsObserver observer, InMemoryMetricReader reader) {
    Collection<MetricData> collect() {
      return reader.collectAllMetrics();
    }
  }

  private static Fixture fixture(final String pipelineName) {
    final var reader = InMemoryMetricReader.create();
    final var sdk = OpenTelemetrySdk.builder()
      .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(reader).build())
      .build();
    return new Fixture(new PipelineMetricsObserver(sdk, pipelineName), reader);
  }

  private static long valueOf(final Collection<MetricData> metrics, final String name) {
    return metrics
      .stream()
      .filter(m -> m.getName().equals(name))
      .findFirst()
      .orElseThrow(() ->
        new AssertionError(
          "metric not emitted: " + name + " (saw: " + metrics.stream().map(MetricData::getName).sorted().toList() + ")"
        )
      )
      .getLongSumData()
      .getPoints()
      .iterator()
      .next()
      .getValue();
  }

  @Test
  void rejectsNullOpenTelemetry() {
    assertThrows(NullPointerException.class, () -> new PipelineMetricsObserver(null, "test"));
  }

  @Test
  void passedResultIncrementsPassedCounter() {
    final var f = fixture("test");
    f.observer().accept(Result.passed("value"));
    f.observer().accept(Result.passed("value"));
    final var metrics = f.collect();
    assertEquals(2L, valueOf(metrics, "kpipe.pipeline.passed"));
  }

  @Test
  void filteredResultIncrementsFilteredCounter() {
    final var f = fixture("test");
    f.observer().accept(Result.filtered());
    assertEquals(1L, valueOf(f.collect(), "kpipe.pipeline.filtered"));
  }

  @Test
  void failedResultIncrementsFailedCounter() {
    final var f = fixture("test");
    f.observer().accept(Result.failed(new RuntimeException("boom")));
    assertEquals(1L, valueOf(f.collect(), "kpipe.pipeline.failed"));
  }

  @Test
  void countersAreIndependent() {
    // Mixed sequence: one Passed, two Filtered, three Failed. Each counter must reflect only
    // its own variant — catches the regression where the switch arms are crossed.
    final var f = fixture("test");
    f.observer().accept(Result.passed("value"));
    f.observer().accept(Result.filtered());
    f.observer().accept(Result.filtered());
    f.observer().accept(Result.failed(new RuntimeException("a")));
    f.observer().accept(Result.failed(new RuntimeException("b")));
    f.observer().accept(Result.failed(new RuntimeException("c")));
    final var metrics = f.collect();
    assertEquals(1L, valueOf(metrics, "kpipe.pipeline.passed"));
    assertEquals(2L, valueOf(metrics, "kpipe.pipeline.filtered"));
    assertEquals(3L, valueOf(metrics, "kpipe.pipeline.failed"));
  }

  @Test
  void nullResultIsIgnored() {
    final var f = fixture("test");
    f.observer().accept(null);
    // No metric points expected — collecting may yield empty data, but should not throw.
    assertDoesNotThrow(f::collect);
  }

  @Test
  void pipelineAttributeIsAttached() {
    final var f = fixture("my-pipeline");
    f.observer().accept(Result.passed("value"));
    final var point = f
      .collect()
      .stream()
      .filter(m -> m.getName().equals("kpipe.pipeline.passed"))
      .findFirst()
      .orElseThrow()
      .getLongSumData()
      .getPoints()
      .iterator()
      .next();
    assertEquals("my-pipeline", point.getAttributes().get(PIPELINE_KEY));
  }

  @Test
  void nullPipelineNameOmitsAttribute() {
    final var f = fixture(null);
    f.observer().accept(Result.passed("value"));
    final var point = f
      .collect()
      .stream()
      .filter(m -> m.getName().equals("kpipe.pipeline.passed"))
      .findFirst()
      .orElseThrow()
      .getLongSumData()
      .getPoints()
      .iterator()
      .next();
    assertNull(
      point.getAttributes().get(PIPELINE_KEY),
      "null pipelineName must omit the attribute entirely, not write the literal 'null'"
    );
  }

  @Test
  void bindSchemaRegistryCacheRegistersThreeGaugesAndReadsFromSuppliers() {
    final var f = fixture("sr-test");
    final var hits = new AtomicLong(7);
    final var misses = new AtomicLong(3);
    final var size = new AtomicLong(10);
    final var returned = f.observer().bindSchemaRegistryCache(hits::get, misses::get, size::get);
    assertSame(f.observer(), returned, "bindSchemaRegistryCache must return `this` for fluent chaining");

    final var metrics = f.collect();
    assertEquals(7L, gaugeValue(metrics, "kpipe.schema_registry.cache.hits"));
    assertEquals(3L, gaugeValue(metrics, "kpipe.schema_registry.cache.misses"));
    assertEquals(10L, gaugeValue(metrics, "kpipe.schema_registry.cache.size"));

    // Suppliers are read on each collection — bumping the values must surface on the next read.
    hits.set(15);
    misses.set(4);
    size.set(11);
    final var second = f.collect();
    assertEquals(15L, gaugeValue(second, "kpipe.schema_registry.cache.hits"));
    assertEquals(4L, gaugeValue(second, "kpipe.schema_registry.cache.misses"));
    assertEquals(11L, gaugeValue(second, "kpipe.schema_registry.cache.size"));
  }

  @Test
  void bindSchemaRegistryCacheRejectsNullSuppliers() {
    final var f = fixture("test");
    assertThrows(NullPointerException.class, () -> f.observer().bindSchemaRegistryCache(null, () -> 0L, () -> 0L));
    assertThrows(NullPointerException.class, () -> f.observer().bindSchemaRegistryCache(() -> 0L, null, () -> 0L));
    assertThrows(NullPointerException.class, () -> f.observer().bindSchemaRegistryCache(() -> 0L, () -> 0L, null));
  }

  private static long gaugeValue(final Collection<MetricData> metrics, final String name) {
    return metrics
      .stream()
      .filter(m -> m.getName().equals(name))
      .findFirst()
      .orElseThrow(() -> new AssertionError("gauge not registered: " + name))
      .getLongGaugeData()
      .getPoints()
      .iterator()
      .next()
      .getValue();
  }
}
