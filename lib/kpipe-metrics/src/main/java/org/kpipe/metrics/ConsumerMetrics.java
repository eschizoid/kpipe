package org.kpipe.metrics;

/// Metrics interface for KPipe consumers — captures message counts, processing duration,
/// in-flight gauge, and backpressure events.
///
/// `kpipe-metrics` provides only the interface and a no-op default implementation so the
/// library can run without any telemetry dependency. To wire OpenTelemetry-backed metrics,
/// add the `kpipe-metrics-otel` module and pass an `OtelConsumerMetrics` instance to the
/// consumer builder.
///
/// Example — no-op (default, zero overhead):
///
/// ```java
/// KPipeConsumer.<byte[]>builder()
///   .withMetrics(ConsumerMetrics.noop())
///   .build();
/// ```
///
/// Example — OpenTelemetry-backed (requires `kpipe-metrics-otel`):
///
/// ```java
/// KPipeConsumer.<byte[]>builder()
///   .withMetrics(new OtelConsumerMetrics(openTelemetry, "my-pipeline"))
///   .build();
/// ```
public interface ConsumerMetrics {
  /// Returns a no-op instance with no in-flight tracking. Zero allocation overhead.
  ///
  /// @return a no-op ConsumerMetrics instance
  static ConsumerMetrics noop() {
    return NoopConsumerMetrics.INSTANCE;
  }

  /// Records that a message was received from Kafka.
  void recordMessageReceived();

  /// Records that a message was successfully processed.
  void recordMessageProcessed();

  /// Records that a message failed processing.
  void recordProcessingError();

  /// Records the time taken to process a single message.
  ///
  /// @param millis processing duration in milliseconds
  void recordProcessingDuration(long millis);

  /// Records that backpressure paused the consumer.
  void recordBackpressurePause();

  /// Records time the consumer spent paused due to backpressure.
  ///
  /// @param millis pause duration in milliseconds
  void recordBackpressureTime(long millis);

  /// Records that a message was received from `topic`. Implementations that care about per-topic
  /// breakdown should override; the default delegates to [#recordMessageReceived()] and ignores
  /// the topic.
  ///
  /// @param topic the Kafka topic the record arrived from
  default void recordMessageReceived(final String topic) {
    recordMessageReceived();
  }

  /// Records that a message from `topic` was successfully processed.
  ///
  /// @param topic the Kafka topic the record arrived from
  default void recordMessageProcessed(final String topic) {
    recordMessageProcessed();
  }

  /// Records that a message from `topic` failed processing.
  ///
  /// @param topic the Kafka topic the record arrived from
  default void recordProcessingError(final String topic) {
    recordProcessingError();
  }

  /// Records the time taken to process a single message from `topic`.
  ///
  /// @param topic the Kafka topic the record arrived from
  /// @param millis processing duration in milliseconds
  default void recordProcessingDuration(final String topic, final long millis) {
    recordProcessingDuration(millis);
  }
}
