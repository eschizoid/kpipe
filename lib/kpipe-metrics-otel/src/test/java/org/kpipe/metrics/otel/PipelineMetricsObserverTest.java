package org.kpipe.metrics.otel;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;
import org.kpipe.registry.Result;

class PipelineMetricsObserverTest {

  @Test
  void buildsAgainstNoopOpenTelemetry() {
    final var observer = new PipelineMetricsObserver(OpenTelemetry.noop(), "test-pipeline");
    assertNotNull(observer);
  }

  @Test
  void acceptsNullPipelineName() {
    assertDoesNotThrow(() -> new PipelineMetricsObserver(OpenTelemetry.noop(), null));
  }

  @Test
  void rejectsNullOpenTelemetry() {
    assertThrows(NullPointerException.class, () -> new PipelineMetricsObserver(null, "test"));
  }

  @Test
  void acceptsPassedResultWithoutThrowing() {
    final var observer = new PipelineMetricsObserver(OpenTelemetry.noop(), "test");
    assertDoesNotThrow(() -> observer.accept(Result.passed("hello")));
  }

  @Test
  void acceptsFilteredResultWithoutThrowing() {
    final var observer = new PipelineMetricsObserver(OpenTelemetry.noop(), "test");
    assertDoesNotThrow(() -> observer.accept(Result.filtered()));
  }

  @Test
  void acceptsFailedResultWithoutThrowing() {
    final var observer = new PipelineMetricsObserver(OpenTelemetry.noop(), "test");
    assertDoesNotThrow(() -> observer.accept(Result.failed(new RuntimeException("nope"))));
  }

  @Test
  void acceptsNullResultSilently() {
    final var observer = new PipelineMetricsObserver(OpenTelemetry.noop(), "test");
    assertDoesNotThrow(() -> observer.accept(null));
  }

  @Test
  void bindSchemaRegistryCacheReturnsSameInstanceForFluentChaining() {
    final var observer = new PipelineMetricsObserver(OpenTelemetry.noop(), "test");
    final var chained = observer.bindSchemaRegistryCache(() -> 10L, () -> 2L, () -> 7L);
    assertSame(observer, chained);
  }

  @Test
  void bindSchemaRegistryCacheRejectsNullSuppliers() {
    final var observer = new PipelineMetricsObserver(OpenTelemetry.noop(), "test");
    assertThrows(NullPointerException.class, () -> observer.bindSchemaRegistryCache(null, () -> 0L, () -> 0L));
    assertThrows(NullPointerException.class, () -> observer.bindSchemaRegistryCache(() -> 0L, null, () -> 0L));
    assertThrows(NullPointerException.class, () -> observer.bindSchemaRegistryCache(() -> 0L, () -> 0L, null));
  }

  @Test
  void allThreeVariantsCanBeRecordedFromOneObserver() {
    final var observer = new PipelineMetricsObserver(OpenTelemetry.noop(), "mixed");
    observer.accept(Result.passed("a"));
    observer.accept(Result.filtered());
    observer.accept(Result.failed(new RuntimeException()));
    observer.accept(Result.passed("b"));
    // The OTel noop provider doesn't expose the recorded values, so we just verify the
    // observer doesn't reject any variant or maintain bogus internal state.
  }
}
