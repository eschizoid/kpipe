package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ConsumerCountersTest {

  private final AtomicLong now = new AtomicLong();
  private final ConsumerCounters counters = new ConsumerCounters(now::get);

  @Test
  void totalsSumTheTopics() {
    final var orders = counters.topic("orders");
    final var refunds = counters.topic("refunds");
    orders.received.add(3);
    orders.processed.add(2);
    orders.filtered.increment();
    refunds.received.add(2);
    refunds.failed.increment();
    refunds.processingNanos.add(Duration.ofMillis(40).toNanos());

    final var snapshot = counters.snapshot(1);

    assertEquals(new MetricsSnapshot.Records(5, 2, 1, 1, 0, 0, 0, Duration.ofMillis(40)), snapshot.records());
    assertEquals(3, snapshot.records("orders").received());
    assertEquals(1, snapshot.records("refunds").failed());
    assertEquals(1, snapshot.inFlight());
  }

  @Test
  void anUnknownTopicReadsAsZero() {
    assertEquals(0, counters.snapshot(0).records("never-seen").received());
  }

  @Test
  void averageProcessingTimeDividesByProcessedRecords() {
    final var topic = counters.topic("orders");
    topic.processed.add(4);
    topic.processingNanos.add(Duration.ofMillis(100).toNanos());

    assertEquals(Duration.ofMillis(25), counters.snapshot(0).records().averageProcessingTime());
    assertEquals(Duration.ZERO, new ConsumerCounters().snapshot(0).records().averageProcessingTime());
  }

  @Test
  void aBackpressurePauseInProgressCountsUpToTheSnapshot() {
    now.set(Duration.ofSeconds(10).toNanos());
    counters.backpressurePaused();
    now.set(Duration.ofSeconds(13).toNanos());

    final var during = counters.snapshot(0).backpressure();
    assertEquals(1, during.pauses());
    assertEquals(Duration.ofSeconds(3), during.time(), "still paused, so the open period counts");

    now.set(Duration.ofSeconds(15).toNanos());
    counters.backpressureResumed();
    now.set(Duration.ofSeconds(60).toNanos());

    assertEquals(Duration.ofSeconds(5), counters.snapshot(0).backpressure().time(), "time stops at the resume");
  }

  @Test
  void anOpenCircuitBreakerCountsUpToTheSnapshotAndEachOpeningIsATrip() {
    now.set(Duration.ofSeconds(1).toNanos());
    counters.circuitBreakerChanged(CircuitBreakerState.OPEN);
    now.set(Duration.ofSeconds(4).toNanos());
    counters.circuitBreakerChanged(CircuitBreakerState.HALF_OPEN);
    counters.circuitBreakerChanged(CircuitBreakerState.OPEN);
    now.set(Duration.ofSeconds(6).toNanos());

    final var breaker = counters.snapshot(0).circuitBreaker();
    assertEquals(2, breaker.trips(), "closed to open and half-open to open both trip");
    assertEquals(Duration.ofSeconds(5), breaker.openTime(), "three open seconds plus two still open");
  }

  @Test
  void outcomesNeverExceedReceivedWhileWritersRun() throws Exception {
    final var real = new ConsumerCounters();
    final var topic = real.topic("orders");
    final var stop = new AtomicBoolean();
    final var writers = new ArrayList<Thread>();
    for (int w = 0; w < 4; w++) {
      final var kind = w;
      writers.add(
        Thread.ofPlatform().start(() -> {
          while (!stop.get()) {
            topic.received.increment();
            switch (kind % 3) {
              case 0 -> topic.processed.increment();
              case 1 -> topic.filtered.increment();
              default -> topic.failed.increment();
            }
          }
        })
      );
    }
    try {
      final var deadline = System.nanoTime() + Duration.ofMillis(500).toNanos();
      var snapshots = 0;
      while (System.nanoTime() < deadline) {
        final var r = real.snapshot(0).records();
        assertTrue(
          r.received() >= r.processed() + r.filtered() + r.failed(),
          "received %d < outcomes %d".formatted(r.received(), r.processed() + r.filtered() + r.failed())
        );
        snapshots++;
      }
      assertTrue(snapshots > 100, "the reader ran " + snapshots + " snapshots");
    } finally {
      stop.set(true);
      for (final var t : writers) t.join();
    }
  }

  @Test
  void stopClocksEndsOpenPeriods() {
    now.set(Duration.ofSeconds(1).toNanos());
    counters.backpressurePaused();
    counters.circuitBreakerChanged(CircuitBreakerState.OPEN);
    now.set(Duration.ofSeconds(3).toNanos());
    counters.stopClocks();
    now.set(Duration.ofSeconds(60).toNanos());

    final var snapshot = counters.snapshot(0);
    assertEquals(Duration.ofSeconds(2), snapshot.backpressure().time(), "time stops when the consumer stops");
    assertEquals(Duration.ofSeconds(2), snapshot.circuitBreaker().openTime());
  }

  @Test
  void aPeriodThatStartsAfterTheClocksStopIsCountedButNotTimed() {
    counters.stopClocks();
    now.set(Duration.ofSeconds(1).toNanos());
    counters.circuitBreakerChanged(CircuitBreakerState.OPEN);
    counters.backpressurePaused();
    now.set(Duration.ofSeconds(60).toNanos());

    final var snapshot = counters.snapshot(0);
    assertEquals(1, snapshot.circuitBreaker().trips());
    assertEquals(Duration.ZERO, snapshot.circuitBreaker().openTime(), "a trip during close does not start a clock");
    assertEquals(Duration.ZERO, snapshot.backpressure().time());
  }
}
