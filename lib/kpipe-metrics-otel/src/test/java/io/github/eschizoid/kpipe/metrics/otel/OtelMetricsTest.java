package io.github.eschizoid.kpipe.metrics.otel;

import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.consumer.CircuitBreakerState;
import io.github.eschizoid.kpipe.consumer.HealthSnapshot;
import io.github.eschizoid.kpipe.consumer.MetricsExporter;
import io.github.eschizoid.kpipe.consumer.MetricsSnapshot;
import io.github.eschizoid.kpipe.consumer.MetricsSource;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/// Exports through a real OTel SDK and reads the result back with an `InMemoryMetricReader`.
class OtelMetricsTest {

  private static final String GROUP = "orders-group";
  private static final String TOPIC = "orders";
  private static final Attributes GROUP_ONLY = Attributes.of(OtelMetrics.GROUP, GROUP);
  private static final Attributes ON_TOPIC = Attributes.of(OtelMetrics.GROUP, GROUP, OtelMetrics.TOPIC, TOPIC);

  /// A source whose snapshots the test replaces between collections.
  private static final class FakeSource implements MetricsSource {

    private final String group;
    volatile MetricsSnapshot metrics = MetricsSnapshot.empty();
    volatile CircuitBreakerState state = CircuitBreakerState.CLOSED;
    volatile RuntimeException failure;

    FakeSource(final String group) {
      this.group = group;
    }

    @Override
    public String consumerGroup() {
      return group;
    }

    @Override
    public MetricsSnapshot metrics() {
      if (failure != null) throw failure;
      return metrics;
    }

    @Override
    public HealthSnapshot health() {
      return new HealthSnapshot(true, false, Set.of(), state, metrics.inFlight());
    }
  }

  private record Fixture(
    OtelMetrics metrics,
    MetricsExporter.Export export,
    FakeSource source,
    InMemoryMetricReader reader
  ) {
    Collection<MetricData> collect() {
      return reader.collectAllMetrics();
    }
  }

  private static OpenTelemetrySdk sdk(final InMemoryMetricReader... readers) {
    final var provider = SdkMeterProvider.builder();
    for (final var reader : readers) provider.registerMetricReader(reader);
    return OpenTelemetrySdk.builder().setMeterProvider(provider.build()).build();
  }

  private static Fixture started(final String group) {
    final var reader = InMemoryMetricReader.create();
    final var source = new FakeSource(group);
    final var metrics = OtelMetrics.of(sdk(reader));
    return new Fixture(metrics, metrics.start(source), source, reader);
  }

  private static MetricsSnapshot snapshot(
    final MetricsSnapshot.Records records,
    final long inFlight,
    final MetricsSnapshot.Backpressure backpressure,
    final MetricsSnapshot.CircuitBreaker breaker
  ) {
    return new MetricsSnapshot(
      records,
      Map.of(TOPIC, records),
      inFlight,
      Map.of(TOPIC, 2 * inFlight),
      backpressure,
      breaker
    );
  }

  private static MetricsSnapshot processed(final long n, final long inFlight) {
    return snapshot(
      new MetricsSnapshot.Records(n, n, 0, 0, 0, 0, 0, Duration.ZERO),
      inFlight,
      new MetricsSnapshot.Backpressure(n, Duration.ZERO),
      new MetricsSnapshot.CircuitBreaker(0, Duration.ZERO)
    );
  }

  private static MetricsSnapshot processed(final long n) {
    return processed(n, 0);
  }

  private static Optional<MetricData> find(final Collection<MetricData> metrics, final String name) {
    return metrics
      .stream()
      .filter(m -> m.getName().equals(name))
      .findFirst();
  }

  private static MetricData metric(final Collection<MetricData> metrics, final String name) {
    return find(metrics, name).orElseThrow(() ->
      new AssertionError(
        "metric not emitted: " + name + " (saw: " + metrics.stream().map(MetricData::getName).sorted().toList() + ")"
      )
    );
  }

  private static <P extends PointData> Optional<P> findPoint(final Collection<P> points, final Attributes attributes) {
    return points
      .stream()
      .filter(p -> p.getAttributes().equals(attributes))
      .findFirst();
  }

  private static <P extends PointData> P point(final Collection<P> points, final Attributes attributes) {
    return findPoint(points, attributes).orElseThrow(() ->
      new AssertionError("no point with " + attributes + " in " + points)
    );
  }

  private static long sum(final Collection<MetricData> metrics, final String name, final Attributes attributes) {
    final Collection<LongPointData> points = metric(metrics, name).getLongSumData().getPoints();
    return point(points, attributes).getValue();
  }

  private static double doubleSum(final Collection<MetricData> metrics, final String name) {
    final Collection<DoublePointData> points = metric(metrics, name).getDoubleSumData().getPoints();
    return point(points, GROUP_ONLY).getValue();
  }

  private static Attributes inState(final String group, final String state) {
    return Attributes.of(OtelMetrics.GROUP, group, OtelMetrics.STATE, state);
  }

  private static long consumersIn(final Collection<MetricData> metrics, final String state) {
    return sum(metrics, "kpipe.consumer.circuit_breaker.state", inState(GROUP, state));
  }

  private static boolean hasPoint(
    final Collection<MetricData> metrics,
    final String name,
    final Attributes attributes
  ) {
    return find(metrics, name)
      .map(m -> findPoint(m.getLongSumData().getPoints(), attributes).isPresent())
      .orElse(false);
  }

  @Test
  void everyCountIsReadFromTheSnapshot() {
    final var f = started(GROUP);
    f.source().metrics = snapshot(
      new MetricsSnapshot.Records(10, 6, 2, 1, 4, 1, 0, Duration.ZERO),
      3,
      new MetricsSnapshot.Backpressure(5, Duration.ofMillis(1500)),
      new MetricsSnapshot.CircuitBreaker(2, Duration.ofMillis(250))
    );

    final var m = f.collect();
    assertEquals(10L, sum(m, "kpipe.consumer.records.received", ON_TOPIC));
    assertEquals(6L, sum(m, "kpipe.consumer.records.processed", ON_TOPIC));
    assertEquals(2L, sum(m, "kpipe.consumer.records.filtered", ON_TOPIC));
    assertEquals(1L, sum(m, "kpipe.consumer.records.failed", ON_TOPIC));
    assertEquals(4L, sum(m, "kpipe.consumer.records.retries", ON_TOPIC));
    assertEquals(1L, sum(m, "kpipe.consumer.dlq.sent", ON_TOPIC));
    assertEquals(0L, sum(m, "kpipe.consumer.dlq.failed", ON_TOPIC));
    assertEquals(3L, sum(m, "kpipe.consumer.records.in_flight", GROUP_ONLY));
    assertEquals(6L, sum(m, "kpipe.consumer.lag", ON_TOPIC));
    assertEquals(5L, sum(m, "kpipe.consumer.backpressure.pauses", GROUP_ONLY));
    assertEquals(1.5, doubleSum(m, "kpipe.consumer.backpressure.time"), 1e-9);
    assertEquals(2L, sum(m, "kpipe.consumer.circuit_breaker.trips", GROUP_ONLY));
    assertEquals(0.25, doubleSum(m, "kpipe.consumer.circuit_breaker.open_time"), 1e-9);
  }

  @Test
  void instrumentsCarryTheDocumentedUnitsAndKinds() {
    final var f = started(GROUP);
    f.source().metrics = processed(1);
    final var m = f.collect();
    assertEquals("{record}", metric(m, "kpipe.consumer.records.received").getUnit());
    assertEquals("{retry}", metric(m, "kpipe.consumer.records.retries").getUnit());
    assertEquals("s", metric(m, "kpipe.consumer.backpressure.time").getUnit());
    assertEquals("s", metric(m, "kpipe.consumer.circuit_breaker.open_time").getUnit());
    assertEquals("{consumer}", metric(m, "kpipe.consumer.circuit_breaker.state").getUnit());
    assertTrue(metric(m, "kpipe.consumer.records.received").getLongSumData().isMonotonic());
    assertFalse(metric(m, "kpipe.consumer.records.in_flight").getLongSumData().isMonotonic());
    assertFalse(metric(m, "kpipe.consumer.lag").getLongSumData().isMonotonic());
    assertFalse(metric(m, "kpipe.consumer.circuit_breaker.state").getLongSumData().isMonotonic());
  }

  @Test
  void eachCollectionReadsTheCurrentSnapshot() {
    final var f = started(GROUP);
    f.source().metrics = processed(1);
    assertEquals(1L, sum(f.collect(), "kpipe.consumer.records.processed", ON_TOPIC));

    f.source().metrics = processed(7);
    assertEquals(7L, sum(f.collect(), "kpipe.consumer.records.processed", ON_TOPIC));
  }

  @Test
  void theStateCountsTheConsumerInItsCurrentStateOnly() {
    final var f = started(GROUP);
    var m = f.collect();
    assertEquals(1L, consumersIn(m, "closed"));
    assertEquals(0L, consumersIn(m, "open"));
    assertEquals(0L, consumersIn(m, "half_open"));

    f.source().state = CircuitBreakerState.HALF_OPEN;
    m = f.collect();
    assertEquals(0L, consumersIn(m, "closed"));
    assertEquals(0L, consumersIn(m, "open"));
    assertEquals(1L, consumersIn(m, "half_open"));
  }

  @Test
  void processingTimeIsAHistogramInSecondsPerTopic() {
    final var f = started(GROUP);
    f.export().recordProcessingTime(TOPIC, Duration.ofMillis(20));
    f.export().recordProcessingTime(TOPIC, Duration.ofMillis(30));

    final var histogram = metric(f.collect(), "kpipe.consumer.processing.duration");
    assertEquals("s", histogram.getUnit());
    final Collection<HistogramPointData> points = histogram.getHistogramData().getPoints();
    final var p = point(points, ON_TOPIC);
    assertEquals(2L, p.getCount());
    assertEquals(0.05, p.getSum(), 1e-9);
    assertEquals(0.001, p.getBoundaries().getFirst(), "bucket advice in seconds, not the SDK's millisecond defaults");
    assertEquals(10.0, p.getBoundaries().getLast());
  }

  @Test
  void withoutAGroupIdOnlyTheTopicIsAttached() {
    final var f = started(null);
    f.source().metrics = processed(1, 2);
    final var m = f.collect();
    assertEquals(1L, sum(m, "kpipe.consumer.records.received", Attributes.of(OtelMetrics.TOPIC, TOPIC)));
    assertEquals(2L, sum(m, "kpipe.consumer.records.in_flight", Attributes.empty()));
  }

  @Test
  void oneInstanceServesManyConsumers() {
    final var reader = InMemoryMetricReader.create();
    final var metrics = OtelMetrics.of(sdk(reader));
    final var one = new FakeSource(GROUP);
    final var two = new FakeSource("other-group");
    final var sameGroup = new FakeSource(GROUP);
    final var exportOne = metrics.start(one);
    final var exportTwo = metrics.start(two);
    metrics.start(sameGroup);
    one.metrics = processed(10);
    two.metrics = processed(3);
    sameGroup.metrics = processed(30);
    two.state = CircuitBreakerState.OPEN;

    exportOne.recordProcessingTime(TOPIC, Duration.ofMillis(10));
    exportTwo.recordProcessingTime(TOPIC, Duration.ofMillis(10));

    final var m = reader.collectAllMetrics();
    final var otherOnTopic = Attributes.of(OtelMetrics.GROUP, "other-group", OtelMetrics.TOPIC, TOPIC);
    assertEquals(40L, sum(m, "kpipe.consumer.records.processed", ON_TOPIC), "consumers sharing a group add up");
    assertEquals(3L, sum(m, "kpipe.consumer.records.processed", otherOnTopic));
    assertEquals(40L, sum(m, "kpipe.consumer.backpressure.pauses", GROUP_ONLY));
    assertEquals(2L, consumersIn(m, "closed"), "both consumers in the group are closed");
    assertEquals(1L, sum(m, "kpipe.consumer.circuit_breaker.state", inState("other-group", "open")));
    final Collection<HistogramPointData> times = metric(m, "kpipe.consumer.processing.duration")
      .getHistogramData()
      .getPoints();
    assertEquals(1L, point(times, ON_TOPIC).getCount(), "each consumer's times carry its own group");
    assertEquals(1L, point(times, otherOnTopic).getCount());
  }

  @Test
  void aClosedConsumersFinalCountsStayInItsCounters() {
    final var f = started(GROUP);
    f.source().metrics = processed(1, 4);
    assertEquals(1L, sum(f.collect(), "kpipe.consumer.records.processed", ON_TOPIC));

    f.source().metrics = processed(5, 4);
    f.export().close();
    f.export().close();
    f.source().metrics = processed(99, 4);
    for (int i = 0; i < 2; i++) {
      final var m = f.collect();
      assertEquals(5L, sum(m, "kpipe.consumer.records.processed", ON_TOPIC), "the counts read at close, for good");
      assertEquals(5L, sum(m, "kpipe.consumer.backpressure.pauses", GROUP_ONLY));
      assertFalse(hasPoint(m, "kpipe.consumer.records.in_flight", GROUP_ONLY), "a closed consumer holds nothing");
      assertFalse(hasPoint(m, "kpipe.consumer.lag", ON_TOPIC), "a closed consumer has no lag");
      assertFalse(
        hasPoint(m, "kpipe.consumer.circuit_breaker.state", inState(GROUP, "closed")),
        "a closed consumer is in no state"
      );
    }
  }

  @Test
  void aSharedSeriesNeverGoesDownWhenOneOfItsConsumersCloses() {
    final var reader = InMemoryMetricReader.create();
    final var metrics = OtelMetrics.of(sdk(reader));
    final var stays = new FakeSource(GROUP);
    final var leaves = new FakeSource(GROUP);
    metrics.start(stays);
    final var leaving = metrics.start(leaves);
    stays.metrics = processed(100, 3);
    leaves.metrics = processed(50, 2);
    var m = reader.collectAllMetrics();
    assertEquals(150L, sum(m, "kpipe.consumer.records.processed", ON_TOPIC));
    assertEquals(5L, sum(m, "kpipe.consumer.records.in_flight", GROUP_ONLY));
    assertEquals(10L, sum(m, "kpipe.consumer.lag", ON_TOPIC), "consumers in a group add their partitions' lag");

    leaving.close();
    m = reader.collectAllMetrics();
    assertEquals(150L, sum(m, "kpipe.consumer.records.processed", ON_TOPIC));
    assertEquals(3L, sum(m, "kpipe.consumer.records.in_flight", GROUP_ONLY), "in-flight drops the closed consumer");
    assertEquals(1L, consumersIn(m, "closed"));

    stays.metrics = processed(120, 3);
    assertEquals(170L, sum(reader.collectAllMetrics(), "kpipe.consumer.records.processed", ON_TOPIC));
  }

  @Test
  void everyReaderSeesAClosedConsumersFinalCounts() {
    final var first = InMemoryMetricReader.create();
    final var second = InMemoryMetricReader.create();
    final var metrics = OtelMetrics.of(sdk(first, second));
    final var source = new FakeSource(GROUP);
    final var export = metrics.start(source);
    source.metrics = processed(9);
    export.close();

    assertEquals(9L, sum(first.collectAllMetrics(), "kpipe.consumer.records.processed", ON_TOPIC));
    assertEquals(9L, sum(second.collectAllMetrics(), "kpipe.consumer.records.processed", ON_TOPIC));
  }

  @Test
  void aSourceThatThrowsReportsItsLastReadingAndSparesTheOthers() {
    final var reader = InMemoryMetricReader.create();
    final var metrics = OtelMetrics.of(sdk(reader));
    final var failing = new FakeSource(GROUP);
    final var healthy = new FakeSource("other-group");
    final var failingExport = metrics.start(failing);
    metrics.start(healthy);
    failing.metrics = processed(7);
    healthy.metrics = processed(2);
    assertEquals(7L, sum(reader.collectAllMetrics(), "kpipe.consumer.records.processed", ON_TOPIC));

    failing.failure = new IllegalStateException("metrics bug");
    healthy.metrics = processed(4);
    final var m = reader.collectAllMetrics();
    assertEquals(7L, sum(m, "kpipe.consumer.records.processed", ON_TOPIC), "the last good reading, not a drop");
    assertEquals(
      4L,
      sum(
        m,
        "kpipe.consumer.records.processed",
        Attributes.of(OtelMetrics.GROUP, "other-group", OtelMetrics.TOPIC, TOPIC)
      )
    );

    failingExport.close();
    assertEquals(7L, sum(reader.collectAllMetrics(), "kpipe.consumer.records.processed", ON_TOPIC));
  }

  @Test
  void processingTimeAfterCloseIsDropped() {
    final var f = started(GROUP);
    f.export().close();
    f.export().recordProcessingTime(TOPIC, Duration.ofMillis(5));
    assertTrue(find(f.collect(), "kpipe.consumer.processing.duration").isEmpty());
  }

  @Test
  void theSchemaRegistryCacheIsExportedUntilItsRegistrationCloses() {
    final var reader = InMemoryMetricReader.create();
    final var hits = new AtomicLong(9);
    final var misses = new AtomicLong(2);
    final var size = new AtomicLong(3);
    final var registration = OtelMetrics.exportSchemaRegistryCache(sdk(reader), hits::get, misses::get, size::get);

    var m = reader.collectAllMetrics();
    assertEquals(9L, sum(m, "kpipe.schema_registry.cache.hits", Attributes.empty()));
    assertEquals(2L, sum(m, "kpipe.schema_registry.cache.misses", Attributes.empty()));
    final Collection<LongPointData> sizePoints = metric(m, "kpipe.schema_registry.cache.size")
      .getLongGaugeData()
      .getPoints();
    assertEquals(3L, point(sizePoints, Attributes.empty()).getValue());
    assertEquals("{lookup}", metric(m, "kpipe.schema_registry.cache.hits").getUnit());

    hits.set(10);
    assertEquals(10L, sum(reader.collectAllMetrics(), "kpipe.schema_registry.cache.hits", Attributes.empty()));

    registration.close();
    assertTrue(
      reader
        .collectAllMetrics()
        .stream()
        .noneMatch(d -> d.getName().startsWith("kpipe.schema_registry."))
    );
  }

  @Test
  void twoRegistrationsOverOneCacheDoubleItsCounts() {
    final var reader = InMemoryMetricReader.create();
    final var sdk = sdk(reader);
    final var hits = new AtomicLong(9);
    OtelMetrics.exportSchemaRegistryCache(sdk, hits::get, () -> 0, () -> 0);
    OtelMetrics.exportSchemaRegistryCache(sdk, hits::get, () -> 0, () -> 0);
    assertEquals(18L, sum(reader.collectAllMetrics(), "kpipe.schema_registry.cache.hits", Attributes.empty()));
  }

  @Test
  void twoInstancesOnOneSdkAddIntoTheSameSeries() {
    final var reader = InMemoryMetricReader.create();
    final var sdk = sdk(reader);
    final var one = new FakeSource(GROUP);
    final var two = new FakeSource(GROUP);
    OtelMetrics.of(sdk).start(one);
    OtelMetrics.of(sdk).start(two);
    one.metrics = processed(10);
    two.metrics = processed(30);
    assertEquals(40L, sum(reader.collectAllMetrics(), "kpipe.consumer.records.processed", ON_TOPIC));
  }
}
