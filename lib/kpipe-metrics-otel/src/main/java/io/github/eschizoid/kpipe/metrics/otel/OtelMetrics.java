package io.github.eschizoid.kpipe.metrics.otel;

import io.github.eschizoid.kpipe.consumer.CircuitBreakerState;
import io.github.eschizoid.kpipe.consumer.MetricsExporter;
import io.github.eschizoid.kpipe.consumer.MetricsSnapshot;
import io.github.eschizoid.kpipe.consumer.MetricsSource;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/// Exports consumers' metrics through OpenTelemetry.
///
/// ```java
/// final var metrics = OtelMetrics.of(openTelemetry);
///
/// KPipe.json("orders", props)
///     .withMetrics(metrics)
///     .toCustom(sink)
///     .start();
/// ```
///
/// Record, backpressure and circuit-breaker counts are observable instruments read from each
/// consumer's `MetricsSnapshot` in one batch callback per collection, so each consumer reports the
/// numbers its `Handle.metrics()` returns; the state instrument reads `Handle.health()`.
/// Processing time is a histogram recorded per processed record on per-record routes.
///
/// | Instrument | Kind | Unit |
/// | --- | --- | --- |
/// | `kpipe.consumer.records.received` | counter, per topic | `{record}` |
/// | `kpipe.consumer.records.processed` | counter, per topic | `{record}` |
/// | `kpipe.consumer.records.filtered` | counter, per topic | `{record}` |
/// | `kpipe.consumer.records.failed` | counter, per topic | `{record}` |
/// | `kpipe.consumer.records.retries` | counter, per topic | `{retry}` |
/// | `kpipe.consumer.dlq.sent` | counter, per topic | `{record}` |
/// | `kpipe.consumer.dlq.failed` | counter, per topic | `{record}` |
/// | `kpipe.consumer.records.in_flight` | up-down counter | `{record}` |
/// | `kpipe.consumer.processing.duration` | histogram, per topic | `s` |
/// | `kpipe.consumer.backpressure.pauses` | counter | `{pause}` |
/// | `kpipe.consumer.backpressure.time` | counter | `s` |
/// | `kpipe.consumer.circuit_breaker.trips` | counter | `{trip}` |
/// | `kpipe.consumer.circuit_breaker.open_time` | counter | `s` |
/// | `kpipe.consumer.circuit_breaker.state` | up-down counter, per state | `{consumer}` |
///
/// Every instrument carries `messaging.consumer.group.name` when the consumer has a `group.id`;
/// per-topic ones also carry `messaging.destination.name`, and the state instrument carries
/// `kpipe.circuit_breaker.state` (`closed`, `open` or `half_open`) and counts the consumers in
/// each state.
///
/// One instance serves any number of consumers, and consumers that share a `group.id` and topic
/// add up into the same series. A closed consumer's final counts stay in its counters for the
/// instance's lifetime, so a counter never goes down when one of its consumers closes; the
/// consumer drops out of `in_flight` and the state instrument. The instance stays registered with
/// the SDK, so create one per `OpenTelemetry` and share it rather than one per consumer.
public final class OtelMetrics implements MetricsExporter {

  static final String SCOPE = "io.github.eschizoid.kpipe";
  static final AttributeKey<String> GROUP = AttributeKey.stringKey("messaging.consumer.group.name");
  static final AttributeKey<String> TOPIC = AttributeKey.stringKey("messaging.destination.name");
  static final AttributeKey<String> STATE = AttributeKey.stringKey("kpipe.circuit_breaker.state");

  private static final Logger LOGGER = System.getLogger(OtelMetrics.class.getName());

  private static final List<Double> SECONDS_BUCKETS = List.of(
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0
  );

  /// Guards [#live] and the retired totals. A collection holds it from its first read to its last,
  /// so every consumer is counted exactly once: live or retired.
  private final Object lock = new Object();
  private final Set<ConsumerExport> live = new LinkedHashSet<>();
  private final Map<Attributes, TopicTotals> retiredTopics = new HashMap<>();
  private final Map<Attributes, GroupTotals> retiredGroups = new HashMap<>();
  private final DoubleHistogram processingTime;

  private OtelMetrics(final Meter meter) {
    processingTime = meter
      .histogramBuilder("kpipe.consumer.processing.duration")
      .setDescription("Time from receiving a record to its sink, retries included")
      .setUnit("s")
      .setExplicitBucketBoundariesAdvice(SECONDS_BUCKETS)
      .build();
    final var received = recordCounter(meter, "received", "Records the consumer started processing");
    final var processed = recordCounter(meter, "processed", "Records that passed every operator to the sink");
    final var filtered = recordCounter(meter, "filtered", "Records dropped on purpose: filtered or unrouted");
    final var failed = recordCounter(meter, "failed", "Records that failed after retries, or at once");
    final var retries = meter
      .counterBuilder("kpipe.consumer.records.retries")
      .setDescription("Retry attempts, not counting first attempts")
      .setUnit("{retry}")
      .buildObserver();
    final var dlqSent = dlqCounter(meter, "sent", "Failed records written to the dead-letter topic");
    final var dlqFailed = dlqCounter(meter, "failed", "Failed records whose dead-letter write failed");
    final var inFlight = meter
      .upDownCounterBuilder("kpipe.consumer.records.in_flight")
      .setDescription("Records the consumer holds: processing, queued behind their key, or buffered")
      .setUnit("{record}")
      .buildObserver();
    final var pauses = meter
      .counterBuilder("kpipe.consumer.backpressure.pauses")
      .setDescription("Times backpressure paused consumption")
      .setUnit("{pause}")
      .buildObserver();
    final var pausedTime = secondsCounter(meter, "kpipe.consumer.backpressure.time", "Time paused by backpressure");
    final var trips = meter
      .counterBuilder("kpipe.consumer.circuit_breaker.trips")
      .setDescription("Times the circuit breaker opened")
      .setUnit("{trip}")
      .buildObserver();
    final var openTime = secondsCounter(
      meter,
      "kpipe.consumer.circuit_breaker.open_time",
      "Time the circuit breaker spent open"
    );
    final var state = meter
      .upDownCounterBuilder("kpipe.consumer.circuit_breaker.state")
      .setDescription("Consumers whose circuit breaker is in each state")
      .setUnit("{consumer}")
      .buildObserver();

    meter.batchCallback(
      () -> {
        final var collection = collect();
        collection.topics.forEach((attributes, t) -> {
          received.record(t.received, attributes);
          processed.record(t.processed, attributes);
          filtered.record(t.filtered, attributes);
          failed.record(t.failed, attributes);
          retries.record(t.retries, attributes);
          dlqSent.record(t.dlqSent, attributes);
          dlqFailed.record(t.dlqFailed, attributes);
        });
        collection.groups.forEach((attributes, g) -> {
          pauses.record(g.pauses, attributes);
          pausedTime.record(g.pausedNanos / 1e9, attributes);
          trips.record(g.trips, attributes);
          openTime.record(g.openNanos / 1e9, attributes);
        });
        collection.inFlight.forEach((attributes, n) -> inFlight.record(n, attributes));
        collection.states.forEach((attributes, n) -> state.record(n, attributes));
      },
      received,
      processed,
      filtered,
      failed,
      retries,
      dlqSent,
      dlqFailed,
      inFlight,
      pauses,
      pausedTime,
      trips,
      openTime,
      state
    );
  }

  /// @param openTelemetry where to register the instruments
  /// @return an exporter for any number of consumers
  public static OtelMetrics of(final OpenTelemetry openTelemetry) {
    return new OtelMetrics(Objects.requireNonNull(openTelemetry, "openTelemetry").getMeter(SCOPE));
  }

  /// Exports a Schema Registry cache, such as `CachedSchemaResolver` in
  /// `kpipe-schema-registry-confluent`, as `kpipe.schema_registry.cache.hits` and `.misses`
  /// (counters, `{lookup}`) and `.size` (a gauge, `{schema}`), with no attributes. Call it once
  /// per cache: each call reports the cache, so two calls over one cache double its counts.
  ///
  /// ```java
  /// final var registration = OtelMetrics.exportSchemaRegistryCache(
  ///     openTelemetry, resolver::hitCount, resolver::missCount, () -> (long) resolver.size());
  /// ```
  ///
  /// @param openTelemetry where to register the instruments
  /// @param hits cumulative cache hits
  /// @param misses cumulative cache misses, each one a registry request
  /// @param size schemas currently cached
  /// @return the registration; close it to stop exporting the cache
  public static BatchCallback exportSchemaRegistryCache(
    final OpenTelemetry openTelemetry,
    final LongSupplier hits,
    final LongSupplier misses,
    final LongSupplier size
  ) {
    Objects.requireNonNull(hits, "hits");
    Objects.requireNonNull(misses, "misses");
    Objects.requireNonNull(size, "size");
    final var meter = Objects.requireNonNull(openTelemetry, "openTelemetry").getMeter(SCOPE);
    final var hitCount = cacheCounter(meter, "hits", "Cumulative Schema Registry cache hits");
    final var missCount = cacheCounter(meter, "misses", "Cumulative Schema Registry cache misses");
    final var cached = meter
      .gaugeBuilder("kpipe.schema_registry.cache.size")
      .setDescription("Schemas currently cached")
      .setUnit("{schema}")
      .ofLongs()
      .buildObserver();
    return meter.batchCallback(
      () -> {
        hitCount.record(hits.getAsLong());
        missCount.record(misses.getAsLong());
        cached.record(size.getAsLong());
      },
      hitCount,
      missCount,
      cached
    );
  }

  @Override
  public Export start(final MetricsSource source) {
    final var export = new ConsumerExport(Objects.requireNonNull(source, "source"));
    synchronized (lock) {
      live.add(export);
    }
    return export;
  }

  /// Sums every consumer's values per attribute set: the retired totals plus each live consumer's
  /// current reading.
  private Collection collect() {
    final var collection = new Collection();
    synchronized (lock) {
      retiredTopics.forEach((attributes, t) -> collection.topic(attributes).add(t));
      retiredGroups.forEach((attributes, g) -> collection.group(attributes).add(g));
      for (final var export : live) {
        final var reading = export.read();
        if (reading == null) continue;
        export.addCounts(reading, collection.topics, collection.groups);
        collection.inFlight.merge(export.group, reading.metrics().inFlight(), Long::sum);
        export.stateAttributes.forEach((s, attributes) ->
          collection.states.merge(attributes, s == reading.state() ? 1L : 0L, Long::sum)
        );
      }
    }
    return collection;
  }

  /// One collection's values, keyed by attribute set.
  private static final class Collection {

    final Map<Attributes, TopicTotals> topics = new HashMap<>();
    final Map<Attributes, GroupTotals> groups = new HashMap<>();
    final Map<Attributes, Long> inFlight = new HashMap<>();
    final Map<Attributes, Long> states = new HashMap<>();

    TopicTotals topic(final Attributes attributes) {
      return topics.computeIfAbsent(attributes, _ -> new TopicTotals());
    }

    GroupTotals group(final Attributes attributes) {
      return groups.computeIfAbsent(attributes, _ -> new GroupTotals());
    }
  }

  /// Cumulative per-topic counts.
  private static final class TopicTotals {

    long received;
    long processed;
    long filtered;
    long failed;
    long retries;
    long dlqSent;
    long dlqFailed;

    void add(final MetricsSnapshot.Records r) {
      received += r.received();
      processed += r.processed();
      filtered += r.filtered();
      failed += r.failed();
      retries += r.retries();
      dlqSent += r.dlqSent();
      dlqFailed += r.dlqFailed();
    }

    void add(final TopicTotals t) {
      received += t.received;
      processed += t.processed;
      filtered += t.filtered;
      failed += t.failed;
      retries += t.retries;
      dlqSent += t.dlqSent;
      dlqFailed += t.dlqFailed;
    }
  }

  /// Cumulative per-consumer-group counts.
  private static final class GroupTotals {

    long pauses;
    long pausedNanos;
    long trips;
    long openNanos;

    void add(final MetricsSnapshot m) {
      pauses += m.backpressure().pauses();
      pausedNanos += m.backpressure().time().toNanos();
      trips += m.circuitBreaker().trips();
      openNanos += m.circuitBreaker().openTime().toNanos();
    }

    void add(final GroupTotals g) {
      pauses += g.pauses;
      pausedNanos += g.pausedNanos;
      trips += g.trips;
      openNanos += g.openNanos;
    }
  }

  /// What one collection reads from a consumer.
  private record Reading(MetricsSnapshot metrics, CircuitBreakerState state) {}

  /// One consumer's attributes, the source its counts are read from, and its last good reading.
  private final class ConsumerExport implements Export {

    private final MetricsSource source;
    private final Attributes group;
    private final Map<CircuitBreakerState, Attributes> stateAttributes = new EnumMap<>(CircuitBreakerState.class);
    private final Map<String, Attributes> topicAttributes = new ConcurrentHashMap<>();
    private Reading last;
    private boolean readFailureLogged;
    private volatile boolean closed;

    ConsumerExport(final MetricsSource source) {
      this.source = source;
      this.group = source.consumerGroup() == null ? Attributes.empty() : Attributes.of(GROUP, source.consumerGroup());
      for (final var s : CircuitBreakerState.values()) {
        stateAttributes.put(s, group.toBuilder().put(STATE, s.name().toLowerCase(Locale.ROOT)).build());
      }
    }

    @Override
    public void recordProcessingTime(final String topic, final Duration time) {
      if (!closed) processingTime.record(time.toNanos() / 1e9, topicAttributes(topic));
    }

    /// Moves this consumer's final counts into the retired totals.
    @Override
    public void close() {
      synchronized (lock) {
        if (closed) return;
        closed = true;
        live.remove(this);
        final var reading = read();
        if (reading != null) addCounts(reading, retiredTopics, retiredGroups);
      }
    }

    /// Reads the consumer, falling back to its last good reading when the read throws, so a
    /// failed read never makes a counter go down. Called with [#lock] held.
    ///
    /// @return the reading, or null when the consumer has never been read successfully
    Reading read() {
      try {
        last = new Reading(source.metrics(), source.health().circuitBreaker());
      } catch (final RuntimeException e) {
        final var level = readFailureLogged ? Level.DEBUG : Level.WARNING;
        readFailureLogged = true;
        LOGGER.log(level, "Could not read a consumer's metrics; reporting its last reading", e);
      }
      return last;
    }

    void addCounts(
      final Reading reading,
      final Map<Attributes, TopicTotals> topics,
      final Map<Attributes, GroupTotals> groups
    ) {
      reading
        .metrics()
        .topics()
        .forEach((topic, r) -> topics.computeIfAbsent(topicAttributes(topic), _ -> new TopicTotals()).add(r));
      groups.computeIfAbsent(group, _ -> new GroupTotals()).add(reading.metrics());
    }

    private Attributes topicAttributes(final String topic) {
      return topicAttributes.computeIfAbsent(topic, t -> group.toBuilder().put(TOPIC, t).build());
    }
  }

  private static ObservableLongMeasurement recordCounter(final Meter meter, final String name, final String what) {
    return meter
      .counterBuilder("kpipe.consumer.records." + name)
      .setDescription(what)
      .setUnit("{record}")
      .buildObserver();
  }

  private static ObservableLongMeasurement dlqCounter(final Meter meter, final String name, final String what) {
    return meter.counterBuilder("kpipe.consumer.dlq." + name).setDescription(what).setUnit("{record}").buildObserver();
  }

  private static ObservableLongMeasurement cacheCounter(final Meter meter, final String name, final String what) {
    return meter
      .counterBuilder("kpipe.schema_registry.cache." + name)
      .setDescription(what)
      .setUnit("{lookup}")
      .buildObserver();
  }

  private static ObservableDoubleMeasurement secondsCounter(final Meter meter, final String name, final String what) {
    return meter.counterBuilder(name).setDescription(what).setUnit("s").ofDoubles().buildObserver();
  }
}
