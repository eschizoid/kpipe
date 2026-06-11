package io.github.eschizoid.kpipe.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class ProducerMetricsTest {

  @Test
  void shouldReturnNoopSingleton() {
    final var first = ProducerMetrics.noop();
    final var second = ProducerMetrics.noop();
    assertNotNull(first);
    assertSame(first, second, "noop() should return a singleton");
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
