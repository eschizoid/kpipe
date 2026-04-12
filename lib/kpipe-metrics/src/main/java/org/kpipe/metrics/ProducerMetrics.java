package org.kpipe.metrics;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;

/// OTel instrument holder for KPipe producer metrics.
///
/// Usage — wire via the producer builder:
///
/// ```java
/// KPipeProducer.builder()
///   .withProperties(props)
///   .withOpenTelemetry(openTelemetry)
///   .build();
/// ```
///
/// Or construct directly for testing:
///
/// ```java
/// final var metrics = new ProducerMetrics(openTelemetry);
/// ```
///
/// When no {@link OpenTelemetry} instance is provided, {@link OpenTelemetry#noop()} is used
/// automatically — zero allocation overhead, no SDK required.
public final class ProducerMetrics {

  private final LongCounter messagesSent;
  private final LongCounter messagesFailed;
  private final LongCounter dlqSent;

  public ProducerMetrics(final OpenTelemetry openTelemetry) {
    final var meter = openTelemetry.getMeter("org.kpipe.producer");
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
  }

  /// Creates a no-op instance — use when OTel is not configured.
  public static ProducerMetrics noop() {
    return new ProducerMetrics(OpenTelemetry.noop());
  }

  public void recordMessageSent() {
    messagesSent.add(1);
  }

  public void recordMessageFailed() {
    messagesFailed.add(1);
  }

  public void recordDlqSent() {
    dlqSent.add(1);
  }
}
