package io.github.eschizoid.kpipe.metrics;

/// Metrics interface for KPipe producers — captures send/failure/DLQ counts.
///
/// `kpipe-metrics` provides only the interface and a no-op default implementation so the
/// library can run without any telemetry dependency. To wire OpenTelemetry-backed metrics,
/// add the `kpipe-metrics-otel` module and pass an `OtelProducerMetrics` instance to the
/// producer builder.
///
/// Example — no-op (default, zero overhead):
///
/// ```java
/// KPipeProducer.<byte[], byte[]>builder()
///   .withProperties(props)
///   .withMetrics(ProducerMetrics.noop())
///   .build();
/// ```
///
/// Example — OpenTelemetry-backed (requires `kpipe-metrics-otel`):
///
/// ```java
/// KPipeProducer.<byte[], byte[]>builder()
///   .withProperties(props)
///   .withMetrics(new OtelProducerMetrics(openTelemetry))
///   .build();
/// ```
public interface ProducerMetrics {
  /// Returns a no-op instance. Zero allocation overhead.
  ///
  /// @return a no-op ProducerMetrics instance
  static ProducerMetrics noop() {
    return NoopProducerMetrics.INSTANCE;
  }

  /// Records that a message was successfully sent.
  void recordMessageSent();

  /// Records that a message failed to send.
  void recordMessageFailed();

  /// Records that a message was sent to the dead-letter queue.
  void recordDlqSent();
}
