package io.github.eschizoid.kpipe.consumer;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/// The consumer's counters: one set per topic plus the consumer-wide backpressure and circuit
/// breaker figures. Every metric KPipe reports is read from here, so each is counted once.
///
/// Record counters are [LongAdder]s, cheap under contention from many workers. Backpressure and
/// breaker periods are rare events, timed from this class's own clock under a lock, so a snapshot
/// sees each period exactly once: finished, or in progress up to the snapshot.
final class ConsumerCounters {

  /// One topic's record counters.
  static final class Topic {

    final LongAdder received = new LongAdder();
    final LongAdder processed = new LongAdder();
    final LongAdder filtered = new LongAdder();
    final LongAdder failed = new LongAdder();
    final LongAdder retries = new LongAdder();
    final LongAdder dlqSent = new LongAdder();
    final LongAdder dlqFailed = new LongAdder();
    final LongAdder processingNanos = new LongAdder();

    /// Reads the outcomes before [#received], so the snapshot never shows more outcomes than
    /// records received.
    MetricsSnapshot.Records snapshot() {
      final var processedNow = processed.sum();
      final var filteredNow = filtered.sum();
      final var failedNow = failed.sum();
      final var dlqSentNow = dlqSent.sum();
      final var dlqFailedNow = dlqFailed.sum();
      final var retriesNow = retries.sum();
      final var nanos = processingNanos.sum();
      return new MetricsSnapshot.Records(
        received.sum(),
        processedNow,
        filteredNow,
        failedNow,
        retriesNow,
        dlqSentNow,
        dlqFailedNow,
        Duration.ofNanos(nanos)
      );
    }
  }

  private final Map<String, Topic> topics = new ConcurrentHashMap<>();
  private final LongSupplier clock;
  private final Object periods = new Object();
  private long backpressurePauses;
  private long backpressureNanos;
  private long pausedSince = -1;
  private long circuitBreakerTrips;
  private long openNanos;
  private long openSince = -1;

  ConsumerCounters() {
    this(System::nanoTime);
  }

  ConsumerCounters(final LongSupplier clock) {
    this.clock = clock;
  }

  /// @param topic a topic name
  /// @return that topic's counters, created on first use
  Topic topic(final String topic) {
    return topics.computeIfAbsent(topic, _ -> new Topic());
  }

  void backpressurePaused() {
    synchronized (periods) {
      backpressurePauses++;
      if (pausedSince < 0) pausedSince = clock.getAsLong();
    }
  }

  void backpressureResumed() {
    synchronized (periods) {
      if (pausedSince < 0) return;
      backpressureNanos += clock.getAsLong() - pausedSince;
      pausedSince = -1;
    }
  }

  void circuitBreakerChanged(final CircuitBreakerState state) {
    synchronized (periods) {
      if (state == CircuitBreakerState.OPEN) {
        circuitBreakerTrips++;
        if (openSince < 0) openSince = clock.getAsLong();
      } else if (openSince >= 0) {
        openNanos += clock.getAsLong() - openSince;
        openSince = -1;
      }
    }
  }

  /// @param inFlight the consumer's in-flight count, read by the caller
  /// @return every counter, totals summed over topics
  MetricsSnapshot snapshot(final long inFlight) {
    final var perTopic = new TreeMap<String, MetricsSnapshot.Records>();
    topics.forEach((name, counters) -> perTopic.put(name, counters.snapshot()));
    final MetricsSnapshot.Backpressure backpressure;
    final MetricsSnapshot.CircuitBreaker circuitBreaker;
    synchronized (periods) {
      final var now = clock.getAsLong();
      backpressure = new MetricsSnapshot.Backpressure(
        backpressurePauses,
        Duration.ofNanos(backpressureNanos + (pausedSince < 0 ? 0 : now - pausedSince))
      );
      circuitBreaker = new MetricsSnapshot.CircuitBreaker(
        circuitBreakerTrips,
        Duration.ofNanos(openNanos + (openSince < 0 ? 0 : now - openSince))
      );
    }
    return new MetricsSnapshot(total(perTopic), perTopic, inFlight, backpressure, circuitBreaker);
  }

  private static MetricsSnapshot.Records total(final Map<String, MetricsSnapshot.Records> perTopic) {
    long received = 0;
    long processed = 0;
    long filtered = 0;
    long failed = 0;
    long retries = 0;
    long dlqSent = 0;
    long dlqFailed = 0;
    var time = Duration.ZERO;
    for (final var r : perTopic.values()) {
      received += r.received();
      processed += r.processed();
      filtered += r.filtered();
      failed += r.failed();
      retries += r.retries();
      dlqSent += r.dlqSent();
      dlqFailed += r.dlqFailed();
      time = time.plus(r.processingTime());
    }
    return new MetricsSnapshot.Records(received, processed, filtered, failed, retries, dlqSent, dlqFailed, time);
  }
}
