package io.github.eschizoid.kpipe.metrics.otel;

import io.github.eschizoid.kpipe.metrics.ProducerMetrics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;

/// OpenTelemetry-backed [ProducerMetrics] implementation.
///
/// Wire via the producer builder:
///
/// ```java
/// KPipeProducer.<byte[], byte[]>builder()
///   .withProperties(props)
///   .withMetrics(new OtelProducerMetrics(openTelemetry))
///   .build();
/// ```
///
/// When OpenTelemetry isn't configured, prefer [ProducerMetrics#noop] from `kpipe-metrics`
/// to avoid this module's classpath dependency entirely.
public final class OtelProducerMetrics implements ProducerMetrics {

  private final LongCounter messagesSent;
  private final LongCounter messagesFailed;
  private final LongCounter dlqSent;
  private final LongCounter dlqFailed;

  /// Creates a fully-instrumented instance.
  ///
  /// @param openTelemetry the OTel entry point
  public OtelProducerMetrics(final OpenTelemetry openTelemetry) {
    final var meter = openTelemetry.getMeter("io.github.eschizoid.kpipe.producer");
    messagesSent = meter
      .counterBuilder("kpipe.producer.messages.sent")
      .setDescription("Number of messages successfully sent")
      .setUnit("{message}")
      .build();
    messagesFailed = meter
      .counterBuilder("kpipe.producer.messages.failed")
      .setDescription("Number of messages that failed to send")
      .setUnit("{message}")
      .build();
    dlqSent = meter
      .counterBuilder("kpipe.producer.dlq.sent")
      .setDescription("Number of messages sent to the dead-letter queue")
      .setUnit("{message}")
      .build();
    dlqFailed = meter
      .counterBuilder("kpipe.producer.dlq.failed")
      .setDescription("Number of dead-letter sends that failed (consumer holds the offset for reprocessing)")
      .setUnit("{message}")
      .build();
  }

  @Override
  public void recordMessageSent() {
    messagesSent.add(1);
  }

  @Override
  public void recordMessageFailed() {
    messagesFailed.add(1);
  }

  @Override
  public void recordDlqSent() {
    dlqSent.add(1);
  }

  @Override
  public void recordDlqFailed() {
    dlqFailed.add(1);
  }
}
