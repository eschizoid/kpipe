package io.github.eschizoid.kpipe.metrics.otel;

import static org.junit.jupiter.api.Assertions.*;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/// Asserts that [OtelProducerMetrics] emits each of the documented producer counters into a real
/// OTel SDK. Same rationale as [OtelConsumerMetricsTest] — the previous noop-only tests would
/// have let a dropped `counter.add(...)` regression land silently.
class OtelProducerMetricsTest {

  private record Fixture(OtelProducerMetrics metrics, InMemoryMetricReader reader) {
    Collection<MetricData> collect() {
      return reader.collectAllMetrics();
    }
  }

  private static Fixture fixture() {
    final var reader = InMemoryMetricReader.create();
    final var sdk = OpenTelemetrySdk.builder()
      .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(reader).build())
      .build();
    return new Fixture(new OtelProducerMetrics(sdk), reader);
  }

  private static long valueOf(final Collection<MetricData> metrics, final String name) {
    return metrics
      .stream()
      .filter(m -> m.getName().equals(name))
      .findFirst()
      .orElseThrow(() ->
        new AssertionError(
          "metric not emitted: " + name + " (saw: " + metrics.stream().map(MetricData::getName).sorted().toList() + ")"
        )
      )
      .getLongSumData()
      .getPoints()
      .iterator()
      .next()
      .getValue();
  }

  @Test
  void recordMessageSentEmitsCounter() {
    final var f = fixture();
    f.metrics().recordMessageSent();
    f.metrics().recordMessageSent();
    f.metrics().recordMessageSent();
    assertEquals(3L, valueOf(f.collect(), "kpipe.producer.messages.sent"));
  }

  @Test
  void recordMessageFailedEmitsCounter() {
    final var f = fixture();
    f.metrics().recordMessageFailed();
    assertEquals(1L, valueOf(f.collect(), "kpipe.producer.messages.failed"));
  }

  @Test
  void recordDlqSentEmitsCounter() {
    final var f = fixture();
    f.metrics().recordDlqSent();
    f.metrics().recordDlqSent();
    assertEquals(2L, valueOf(f.collect(), "kpipe.producer.dlq.sent"));
  }

  @Test
  void countersAreIndependent() {
    // A sent message must not accidentally tick the failed counter (or vice versa). The
    // previous noop tests couldn't catch a regression where someone wired the wrong field.
    final var f = fixture();
    f.metrics().recordMessageSent();
    f.metrics().recordMessageFailed();
    f.metrics().recordDlqSent();
    final var metrics = f.collect();
    assertEquals(1L, valueOf(metrics, "kpipe.producer.messages.sent"));
    assertEquals(1L, valueOf(metrics, "kpipe.producer.messages.failed"));
    assertEquals(1L, valueOf(metrics, "kpipe.producer.dlq.sent"));
  }
}
