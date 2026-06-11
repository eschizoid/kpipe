package io.github.eschizoid.kpipe.metrics.otel;

import static org.junit.jupiter.api.Assertions.*;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/// Asserts that [OtelConsumerMetrics] actually emits the documented counters and histogram into a
/// real OTel SDK — not just that the calls don't throw. Built around `InMemoryMetricReader` so
/// every assertion is on captured metric data: name, value, and attributes. Catches the class of
/// regression where someone accidentally drops a `counter.add(...)` call or changes a metric
/// name; the previous noop-only tests against `OpenTelemetry.noop()` would have happily passed.
class OtelConsumerMetricsTest {

  private static final AttributeKey<String> TOPIC_KEY = AttributeKey.stringKey("topic");
  private static final AttributeKey<String> PIPELINE_KEY = AttributeKey.stringKey("pipeline");
  private static final AttributeKey<String> CB_STATE_KEY = AttributeKey.stringKey("circuit_breaker.state");

  /// Per-test fixture so each metric capture is isolated.
  private record Fixture(OtelConsumerMetrics metrics, InMemoryMetricReader reader) {
    Collection<MetricData> collect() {
      return reader.collectAllMetrics();
    }
  }

  private static Fixture fixture(final String pipelineName) {
    final var reader = InMemoryMetricReader.create();
    final var sdk = OpenTelemetrySdk.builder()
      .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(reader).build())
      .build();
    return new Fixture(new OtelConsumerMetrics(sdk, () -> 0L, pipelineName), reader);
  }

  private static MetricData findByName(final Collection<MetricData> metrics, final String name) {
    return metrics
      .stream()
      .filter(m -> m.getName().equals(name))
      .findFirst()
      .orElseThrow(() ->
        new AssertionError(
          "metric not emitted: " + name + " (saw: " + metrics.stream().map(MetricData::getName).sorted().toList() + ")"
        )
      );
  }

  @Test
  void recordMessageReceivedEmitsCounter() {
    final var f = fixture("test");
    f.metrics().recordMessageReceived();
    final var counter = findByName(f.collect(), "kpipe.consumer.messages.received");
    final var point = counter.getLongSumData().getPoints().iterator().next();
    assertEquals(1L, point.getValue(), "expected one increment");
    assertEquals("test", point.getAttributes().get(PIPELINE_KEY), "pipeline attribute must be attached");
  }

  @Test
  void topicScopedRecordMessageReceivedAttachesTopicAttribute() {
    final var f = fixture("test");
    f.metrics().recordMessageReceived("orders");
    final var point = findByName(f.collect(), "kpipe.consumer.messages.received")
      .getLongSumData()
      .getPoints()
      .iterator()
      .next();
    assertEquals(1L, point.getValue());
    assertEquals("orders", point.getAttributes().get(TOPIC_KEY));
    assertEquals("test", point.getAttributes().get(PIPELINE_KEY));
  }

  @Test
  void recordMessageProcessedEmitsCounter() {
    final var f = fixture(null);
    f.metrics().recordMessageProcessed();
    final var point = findByName(f.collect(), "kpipe.consumer.messages.processed")
      .getLongSumData()
      .getPoints()
      .iterator()
      .next();
    assertEquals(1L, point.getValue());
    assertNull(
      point.getAttributes().get(PIPELINE_KEY),
      "null pipelineName must omit the attribute, not write the literal 'null'"
    );
  }

  @Test
  void recordProcessingErrorEmitsCounter() {
    final var f = fixture("test");
    f.metrics().recordProcessingError("orders");
    final var point = findByName(f.collect(), "kpipe.consumer.messages.errors")
      .getLongSumData()
      .getPoints()
      .iterator()
      .next();
    assertEquals(1L, point.getValue());
    assertEquals("orders", point.getAttributes().get(TOPIC_KEY));
  }

  @Test
  void recordProcessingDurationEmitsHistogram() {
    final var f = fixture("test");
    f.metrics().recordProcessingDuration(42L);
    f.metrics().recordProcessingDuration(58L);
    final var hist = findByName(f.collect(), "kpipe.consumer.processing.duration")
      .getHistogramData()
      .getPoints()
      .iterator()
      .next();
    assertEquals(2L, hist.getCount(), "two samples recorded");
    assertEquals(100.0, hist.getSum(), 0.001, "sum must be 42 + 58");
  }

  @Test
  void backpressureMetricsEmit() {
    final var f = fixture("test");
    f.metrics().recordBackpressurePause();
    f.metrics().recordBackpressureTime(150L);
    f.metrics().recordBackpressureTime(50L);
    final var metrics = f.collect();
    assertEquals(
      1L,
      findByName(metrics, "kpipe.consumer.backpressure.pauses")
        .getLongSumData()
        .getPoints()
        .iterator()
        .next()
        .getValue()
    );
    assertEquals(
      200L,
      findByName(metrics, "kpipe.consumer.backpressure.time").getLongSumData().getPoints().iterator().next().getValue(),
      "backpressure.time is a counter summing total paused ms"
    );
  }

  @Test
  void circuitBreakerTripsEmits() {
    final var f = fixture("test");
    f.metrics().recordCircuitBreakerTrip();
    f.metrics().recordCircuitBreakerTrip();
    final var point = findByName(f.collect(), "kpipe.consumer.circuit_breaker.trips")
      .getLongSumData()
      .getPoints()
      .iterator()
      .next();
    assertEquals(2L, point.getValue());
  }

  @Test
  void circuitBreakerStateChangeEmitsWithStateAttribute() {
    final var f = fixture("test");
    f.metrics().recordCircuitBreakerStateChange(CircuitState.OPEN);
    f.metrics().recordCircuitBreakerStateChange(CircuitState.HALF_OPEN);
    f.metrics().recordCircuitBreakerStateChange(CircuitState.OPEN);
    final var counter = findByName(f.collect(), "kpipe.consumer.circuit_breaker.state_changes");
    // Three increments expected, but they split across two distinct attribute sets (OPEN ×2,
    // HALF_OPEN ×1). Sum across the points must be 3 and each attribute set must match.
    final var points = counter.getLongSumData().getPoints();
    final var total = points
      .stream()
      .mapToLong(p -> p.getValue())
      .sum();
    assertEquals(3L, total, "all three state changes accounted for");
    final var openCount = points
      .stream()
      .filter(p -> "OPEN".equals(p.getAttributes().get(CB_STATE_KEY)))
      .mapToLong(p -> p.getValue())
      .sum();
    final var halfOpenCount = points
      .stream()
      .filter(p -> "HALF_OPEN".equals(p.getAttributes().get(CB_STATE_KEY)))
      .mapToLong(p -> p.getValue())
      .sum();
    assertEquals(2L, openCount, "two transitions into OPEN");
    assertEquals(1L, halfOpenCount, "one transition into HALF_OPEN");
  }

  @Test
  void circuitBreakerStateChangeIgnoresNull() {
    final var f = fixture("test");
    f.metrics().recordCircuitBreakerStateChange(null);
    final var sum = f
      .collect()
      .stream()
      .filter(m -> m.getName().equals("kpipe.consumer.circuit_breaker.state_changes"))
      .findFirst();
    assertTrue(
      sum.isEmpty() || sum.get().getLongSumData().getPoints().isEmpty(),
      "null state must not record anything — would otherwise attach a literal 'null' attribute"
    );
  }

  @Test
  void circuitBreakerTimeOpenEmits() {
    final var f = fixture("test");
    f.metrics().recordCircuitBreakerTimeOpen(5000L);
    final var point = findByName(f.collect(), "kpipe.consumer.circuit_breaker.time_open")
      .getLongSumData()
      .getPoints()
      .iterator()
      .next();
    assertEquals(5000L, point.getValue());
  }

  @Test
  void inFlightGaugeIsRegisteredAndUsesSupplier() {
    final var reader = InMemoryMetricReader.create();
    final var sdk = OpenTelemetrySdk.builder()
      .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(reader).build())
      .build();
    // Construct with a supplier that returns a fixed value; the gauge callback must fire on
    // collection and surface that value.
    @SuppressWarnings("unused")
    final var metrics = new OtelConsumerMetrics(sdk, () -> 17L, "test");
    final var point = findByName(reader.collectAllMetrics(), "kpipe.consumer.messages.inflight")
      .getLongGaugeData()
      .getPoints()
      .iterator()
      .next();
    assertEquals(17L, point.getValue(), "inflight gauge must read from the supplier");
  }

  @Test
  void topicAttributeCacheReturnsSameAttributesForRepeatedTopic() {
    // Recording the same topic many times shouldn't allocate a new Attributes per call.
    // This is observable as a stable per-topic point set: 1000 increments → one point with
    // value=1000 (not 1000 points each with value=1).
    final var f = fixture("test");
    for (int i = 0; i < 1000; i++) f.metrics().recordMessageReceived("orders");
    final var points = findByName(f.collect(), "kpipe.consumer.messages.received").getLongSumData().getPoints();
    assertEquals(1, points.size(), "all increments under the same topic attribute must merge into one point");
    assertEquals(1000L, points.iterator().next().getValue());
  }

  /// Minimal enum stand-in for a circuit-breaker state, since this module does not depend on
  /// `kpipe-consumer`.
  private enum CircuitState {
    CLOSED,
    OPEN,
    HALF_OPEN,
  }
}
