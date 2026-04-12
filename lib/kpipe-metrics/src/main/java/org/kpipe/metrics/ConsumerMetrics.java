package org.kpipe.metrics;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.util.function.LongSupplier;

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
  private final LongCounter backpressurePauses;
  private final LongCounter backpressureTime;
  @SuppressWarnings("unused")
  private final ObservableLongGauge inFlight;

  /// Creates a fully-instrumented instance with an in-flight gauge backed by the given supplier.
  ///
  /// @param openTelemetry the OTel entry point
  /// @param inFlightSupplier supplies the current number of in-flight messages
  public ConsumerMetrics(final OpenTelemetry openTelemetry, final LongSupplier inFlightSupplier) {
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
    backpressurePauses = meter
      .counterBuilder("kpipe.consumer.backpressure.pauses")
      .setDescription("Number of times backpressure paused the consumer")
      .setUnit("{pause}")
      .build();
    backpressureTime = meter
      .counterBuilder("kpipe.consumer.backpressure.time")
      .setDescription("Total time spent paused due to backpressure")
      .setUnit("ms")
      .build();
    inFlight = meter
      .gaugeBuilder("kpipe.consumer.messages.inflight")
      .setDescription("Current number of messages being processed")
      .setUnit("{message}")
      .ofLongs()
      .buildWithCallback(obs -> obs.record(inFlightSupplier.getAsLong()));
  }

  /// Creates an instance without an in-flight gauge.
  ///
  /// @param openTelemetry the OTel entry point
  public ConsumerMetrics(final OpenTelemetry openTelemetry) {
    this(openTelemetry, () -> 0L);
  }

  /// Creates a no-op instance — use when OTel is not configured.
  ///
  /// @param inFlightSupplier supplies the current number of in-flight messages
  /// @return a no-op ConsumerMetrics wired to the in-flight count
  public static ConsumerMetrics noop(final LongSupplier inFlightSupplier) {
    return new ConsumerMetrics(OpenTelemetry.noop(), inFlightSupplier);
  }

  /// Creates a no-op instance with no in-flight tracking.
  public static ConsumerMetrics noop() {
    return noop(() -> 0L);
  }

  /// Records that a message was received from Kafka.
  public void recordMessageReceived() {
    messagesReceived.add(1);
  }

  /// Records that a message was successfully processed.
  public void recordMessageProcessed() {
    messagesProcessed.add(1);
  }

  /// Records that a message failed processing.
  public void recordProcessingError() {
    processingErrors.add(1);
  }

  /// Records the time taken to process a single message.
  ///
  /// @param millis processing duration in milliseconds
  public void recordProcessingDuration(final long millis) {
    processingDuration.record(millis);
  }

  /// Records that backpressure paused the consumer.
  public void recordBackpressurePause() {
    backpressurePauses.add(1);
  }

  /// Records time the consumer spent paused due to backpressure.
  ///
  /// @param millis pause duration in milliseconds
  public void recordBackpressureTime(final long millis) {
    backpressureTime.add(millis);
  }
}
