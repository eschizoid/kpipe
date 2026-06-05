package io.github.eschizoid.kpipe.metrics.otel;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.github.eschizoid.kpipe.metrics.ConsumerMetrics;
import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;

/// Smoke tests for [OtelConsumerMetrics] using the no-op OTel provider — verifies the
/// implementation wires up correctly without requiring an SDK.
class OtelConsumerMetricsTest {

  @Test
  void shouldImplementConsumerMetricsInterface() {
    final ConsumerMetrics metrics = new OtelConsumerMetrics(OpenTelemetry.noop());
    assertNotNull(metrics);
  }

  @Test
  void shouldAcceptInFlightSupplier() {
    final var metrics = new OtelConsumerMetrics(OpenTelemetry.noop(), () -> 42L);
    assertNotNull(metrics);
  }

  @Test
  void shouldAcceptPipelineLabel() {
    final var metrics = new OtelConsumerMetrics(OpenTelemetry.noop(), "my-pipeline");
    assertNotNull(metrics);
  }

  @Test
  void shouldRecordEventsWithoutThrowing() {
    final var metrics = new OtelConsumerMetrics(OpenTelemetry.noop(), () -> 0L, "test");
    assertDoesNotThrow(() -> metrics.recordMessageReceived());
    assertDoesNotThrow(() -> metrics.recordMessageProcessed());
    assertDoesNotThrow(() -> metrics.recordProcessingError());
    assertDoesNotThrow(() -> metrics.recordProcessingDuration(42L));
    assertDoesNotThrow(metrics::recordBackpressurePause);
    assertDoesNotThrow(() -> metrics.recordBackpressureTime(150L));
  }

  @Test
  void shouldRecordTopicScopedEventsWithoutThrowing() {
    final var metrics = new OtelConsumerMetrics(OpenTelemetry.noop(), () -> 0L, "test");
    assertDoesNotThrow(() -> metrics.recordMessageReceived("events-a"));
    assertDoesNotThrow(() -> metrics.recordMessageProcessed("events-a"));
    assertDoesNotThrow(() -> metrics.recordProcessingError("events-a"));
    assertDoesNotThrow(() -> metrics.recordProcessingDuration("events-a", 42L));
  }

  @Test
  void shouldSupportConcurrentRecordingFromVirtualThreads() throws InterruptedException {
    final var metrics = new OtelConsumerMetrics(OpenTelemetry.noop(), () -> 0L, "concurrent");
    final var threads = new Thread[50];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = Thread.ofVirtual().start(() -> {
        metrics.recordMessageReceived();
        metrics.recordMessageProcessed();
        metrics.recordProcessingError();
        metrics.recordProcessingDuration(10L);
        metrics.recordBackpressurePause();
        metrics.recordBackpressureTime(5L);
      });
    }
    for (final var t : threads) t.join();
  }
}
