package org.kpipe.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class ConsumerMetricsTest {

  @Test
  void shouldReturnNoopSingleton() {
    assertNotNull(ConsumerMetrics.noop());
    assertSame(ConsumerMetrics.noop(), ConsumerMetrics.noop(), "noop() should return a singleton");
  }

  @Test
  void shouldReturnNoopWithInFlightSupplierSingleton() {
    final var metrics = ConsumerMetrics.noop(() -> 42L);
    assertNotNull(metrics);
    // The inFlightSupplier is ignored by the no-op impl — the call returns the same singleton.
    assertSame(ConsumerMetrics.noop(), metrics);
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
  void shouldRecordBackpressurePauseWithoutThrowing() {
    final var metrics = ConsumerMetrics.noop();
    assertDoesNotThrow(metrics::recordBackpressurePause);
  }

  @Test
  void shouldRecordBackpressureTimeWithoutThrowing() {
    final var metrics = ConsumerMetrics.noop();
    assertDoesNotThrow(() -> metrics.recordBackpressureTime(150L));
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
        metrics.recordBackpressurePause();
        metrics.recordBackpressureTime(5L);
      });
    }
    for (final var t : threads) t.join();
  }
}
