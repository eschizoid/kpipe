package org.kpipe.metrics.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.util.function.LongSupplier;
import org.kpipe.metrics.ConsumerMetrics;

/// OpenTelemetry-backed [ConsumerMetrics] implementation.
///
/// Wire via the consumer builder:
///
/// ```java
/// KPipeConsumer.<byte[]>builder()
///   .withProperties(props)
///   .withMetrics(new OtelConsumerMetrics(openTelemetry, "my-pipeline"))
///   .build();
/// ```
///
/// When OpenTelemetry isn't configured, prefer [ConsumerMetrics#noop] from `kpipe-metrics`
/// to avoid this module's classpath dependency entirely.
public final class OtelConsumerMetrics implements ConsumerMetrics {

  private static final AttributeKey<String> PIPELINE_KEY = AttributeKey.stringKey("pipeline");

  private final LongCounter messagesReceived;
  private final LongCounter messagesProcessed;
  private final LongCounter processingErrors;
  private final DoubleHistogram processingDuration;
  private final LongCounter backpressurePauses;
  private final LongCounter backpressureTime;

  @SuppressWarnings("unused")
  private final ObservableLongGauge inFlight;

  private final Attributes attributes;

  /// Creates a fully-instrumented instance with an in-flight gauge backed by the given supplier.
  ///
  /// @param openTelemetry the OTel entry point
  /// @param inFlightSupplier supplies the current number of in-flight messages
  /// @param pipelineName optional pipeline name attached as a `pipeline` attribute on all metrics;
  ///                    pass null to omit
  public OtelConsumerMetrics(
    final OpenTelemetry openTelemetry,
    final LongSupplier inFlightSupplier,
    final String pipelineName
  ) {
    final var meter = openTelemetry.getMeter("org.kpipe.consumer");
    attributes = pipelineName == null ? Attributes.empty() : Attributes.of(PIPELINE_KEY, pipelineName);
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
      .buildWithCallback(obs -> obs.record(inFlightSupplier.getAsLong(), attributes));
  }

  /// Creates a fully-instrumented instance without a pipeline label.
  ///
  /// @param openTelemetry the OTel entry point
  /// @param inFlightSupplier supplies the current number of in-flight messages
  public OtelConsumerMetrics(final OpenTelemetry openTelemetry, final LongSupplier inFlightSupplier) {
    this(openTelemetry, inFlightSupplier, null);
  }

  /// Creates an instance without an in-flight gauge.
  ///
  /// @param openTelemetry the OTel entry point
  public OtelConsumerMetrics(final OpenTelemetry openTelemetry) {
    this(openTelemetry, () -> 0L, null);
  }

  /// Creates an instance without an in-flight gauge, labeled with a pipeline name.
  ///
  /// @param openTelemetry the OTel entry point
  /// @param pipelineName pipeline name attached as a `pipeline` attribute on all metrics
  public OtelConsumerMetrics(final OpenTelemetry openTelemetry, final String pipelineName) {
    this(openTelemetry, () -> 0L, pipelineName);
  }

  @Override
  public void recordMessageReceived() {
    messagesReceived.add(1, attributes);
  }

  @Override
  public void recordMessageProcessed() {
    messagesProcessed.add(1, attributes);
  }

  @Override
  public void recordProcessingError() {
    processingErrors.add(1, attributes);
  }

  @Override
  public void recordProcessingDuration(final long millis) {
    processingDuration.record(millis, attributes);
  }

  @Override
  public void recordBackpressurePause() {
    backpressurePauses.add(1, attributes);
  }

  @Override
  public void recordBackpressureTime(final long millis) {
    backpressureTime.add(millis, attributes);
  }
}
