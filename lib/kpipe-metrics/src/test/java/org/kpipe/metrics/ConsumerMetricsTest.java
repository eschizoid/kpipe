package org.kpipe.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;

class ConsumerMetricsTest {

  @Test
  void shouldCreateWithOpenTelemetry() {
    assertNotNull(new ConsumerMetrics(OpenTelemetry.noop()));
  }

  @Test
  void shouldCreateNoop() {
    assertNotNull(ConsumerMetrics.noop());
  }

  @Test
  void shouldRecordMessageReceivedWithoutThrowing() {
    final var metrics = ConsumerMetrics.noop();
    assertDoesNotThrow(metrics::recordMessageReceived);
  }

  @Test
  void shouldRecordMessageProcessedWithoutThrowing() {
    final var metrics = ConsumerMetrics.noop();
    assertDoesNotThrow(metrics::recordMessageProcessed);
  }

  @Test
  void shouldRecordProcessingErrorWithoutThrowing() {
    final var metrics = ConsumerMetrics.noop();
    assertDoesNotThrow(metrics::recordProcessingError);
  }

  @Test
  void shouldRecordProcessingDurationWithoutThrowing() {
    final var metrics = ConsumerMetrics.noop();
    assertDoesNotThrow(() -> metrics.recordProcessingDuration(42L));
  }

  @Test
  void shouldSupportConcurrentRecordingFromVirtualThreads() throws InterruptedException {
    final var metrics = ConsumerMetrics.noop();
    final var threads = new Thread[50];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = Thread.ofVirtual().start(() -> {
        metrics.recordMessageReceived();
        metrics.recordMessageProcessed();
        metrics.recordProcessingError();
        metrics.recordProcessingDuration(10L);
      });
    }
    for (final var t : threads) t.join();
  }
}
