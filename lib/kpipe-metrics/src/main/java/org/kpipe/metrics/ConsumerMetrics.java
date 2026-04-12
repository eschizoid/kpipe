package org.kpipe.metrics;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;

/// OTel instrument holder for KPipe consumer metrics.
///
/// Usage — wire via the consumer builder:
///
/// ```java
/// KPipeConsumer.builder()
///   .withProperties(props)
///   .withTopic(topic)
///   .withOpenTelemetry(openTelemetry)
///   .build();
/// ```
///
/// When no {@link OpenTelemetry} instance is provided, {@link OpenTelemetry#noop()} is used
/// automatically — zero allocation overhead, no SDK required.
public final class ConsumerMetrics {

  private final LongCounter messagesReceived;
  private final LongCounter messagesProcessed;
  private final LongCounter processingErrors;
  private final DoubleHistogram processingDuration;

  public ConsumerMetrics(final OpenTelemetry openTelemetry) {
    final var meter = openTelemetry.getMeter("org.kpipe.consumer");
    messagesReceived = meter
      .counterBuilder("kpipe.consumer.messages.received")
      .setDescription("Number of messages received from Kafka")
      .setUnit("{message}")
      .build();
    messagesProcessed = meter
      .counterBuilder("kpipe.consumer.messages.processed")
      .setDescription("Number of messages successfully processed")
      .setUnit("{message}")
      .build();
    processingErrors = meter
      .counterBuilder("kpipe.consumer.messages.errors")
      .setDescription("Number of messages that failed processing")
      .setUnit("{message}")
      .build();
    processingDuration = meter
      .histogramBuilder("kpipe.consumer.processing.duration")
      .setDescription("Time taken to process a single message")
      .setUnit("ms")
      .build();
  }

  /// Creates a no-op instance — use when OTel is not configured.
  public static ConsumerMetrics noop() {
    return new ConsumerMetrics(OpenTelemetry.noop());
  }

  public void recordMessageReceived() {
    messagesReceived.add(1);
  }

  public void recordMessageProcessed() {
    messagesProcessed.add(1);
  }

  public void recordProcessingError() {
    processingErrors.add(1);
  }

  public void recordProcessingDuration(final long millis) {
    processingDuration.record(millis);
  }
}
