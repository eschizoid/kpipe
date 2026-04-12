package org.kpipe.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;

class ProducerMetricsTest {

  @Test
  void shouldCreateWithOpenTelemetry() {
    assertNotNull(new ProducerMetrics(OpenTelemetry.noop()));
  }

  @Test
  void shouldCreateNoop() {
    assertNotNull(ProducerMetrics.noop());
  }

  @Test
  void shouldRecordMessageSentWithoutThrowing() {
    final var metrics = ProducerMetrics.noop();
    assertDoesNotThrow(metrics::recordMessageSent);
  }

  @Test
  void shouldRecordMessageFailedWithoutThrowing() {
    final var metrics = ProducerMetrics.noop();
    assertDoesNotThrow(metrics::recordMessageFailed);
  }

  @Test
  void shouldRecordDlqSentWithoutThrowing() {
    final var metrics = ProducerMetrics.noop();
    assertDoesNotThrow(metrics::recordDlqSent);
  }

  @Test
  void shouldSupportConcurrentRecordingFromVirtualThreads() throws InterruptedException {
    final var metrics = ProducerMetrics.noop();
    final var threads = new Thread[50];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = Thread.ofVirtual().start(() -> {
        metrics.recordMessageSent();
        metrics.recordMessageFailed();
        metrics.recordDlqSent();
      });
    }
    for (final var t : threads) t.join();
  }
}
