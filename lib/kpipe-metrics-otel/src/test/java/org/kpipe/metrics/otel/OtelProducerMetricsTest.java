package org.kpipe.metrics.otel;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;
import org.kpipe.metrics.ProducerMetrics;

/// Smoke tests for [OtelProducerMetrics] using the no-op OTel provider.
class OtelProducerMetricsTest {

  @Test
  void shouldImplementProducerMetricsInterface() {
    final ProducerMetrics metrics = new OtelProducerMetrics(OpenTelemetry.noop());
    assertNotNull(metrics);
  }

  @Test
  void shouldRecordEventsWithoutThrowing() {
    final var metrics = new OtelProducerMetrics(OpenTelemetry.noop());
    assertDoesNotThrow(metrics::recordMessageSent);
    assertDoesNotThrow(metrics::recordMessageFailed);
    assertDoesNotThrow(metrics::recordDlqSent);
  }

  @Test
  void shouldSupportConcurrentRecordingFromVirtualThreads() throws InterruptedException {
    final var metrics = new OtelProducerMetrics(OpenTelemetry.noop());
    final var threads = new Thread[50];
    for (var i = 0; i < threads.length; i++) {
      threads[i] = Thread.ofVirtual().start(() -> {
        metrics.recordMessageSent();
        metrics.recordMessageFailed();
        metrics.recordDlqSent();
      });
    }
    for (final var t : threads) t.join();
  }
}
