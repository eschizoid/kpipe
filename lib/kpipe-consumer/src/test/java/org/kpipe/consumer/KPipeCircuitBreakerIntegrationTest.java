package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Integration tests for the circuit breaker. Uses Kafka's `MockConsumer` so the tests are fast,
/// deterministic, and have no Docker dependency — same pattern as
// `KPipeBackpressureIntegrationTest`.
///
/// Verifies the three transitions:
///
///   1. CLOSED → OPEN: sustained failure rate trips the breaker, consumer is paused, Kafka
///      `paused()` reflects it.
///   2. OPEN → HALF_OPEN: after `openDuration` elapses, the consumer resumes (the next record is
///      the probe).
///   3. HALF_OPEN → CLOSED: probe success returns to normal operation; stats reset.
class KPipeCircuitBreakerIntegrationTest {

  private static final String TOPIC = "cb-test-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "cb-test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("enable.auto.commit", "true");
  }

  @Test
  void sustainedFailuresTripBreakerAndPauseConsumer() throws InterruptedException {
    // Arrange: 12 records, every single one fails. windowSize=5, threshold=0.5 means the breaker
    // trips after the 5th failure (the first time the window fills with all-failures). Sequential
    // mode so outcomes arrive at the breaker in deterministic order.
    final var mc = buildMockConsumer(12);
    final var controller = new CircuitBreakerController(0.5, 5, Duration.ofSeconds(30));

    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withSequentialProcessing(true)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          throw new RuntimeException("simulated downstream failure");
        })
      )
      .withCircuitBreaker(controller)
      .withConsumer(() -> mc)
      .build();

    consumer.start();

    awaitCondition(() -> !mc.paused().isEmpty(), 5000);
    assertTrue(mc.paused().contains(PARTITION), "breaker should have tripped and paused the consumer");
    assertEquals(
      1L,
      consumer.getMetrics().get(KPipeConsumer.METRIC_CIRCUIT_BREAKER_TRIPS),
      "exactly one trip should have been recorded"
    );

    consumer.close();
  }

  @Test
  void halfOpenProbeResumesAfterOpenDuration() throws InterruptedException {
    // Arrange: short open duration so we can test the recovery path in <1s. 6 records all fail to
    // trip the breaker, then we observe the HALF_OPEN transition.
    final var mc = buildMockConsumer(6);
    final var controller = new CircuitBreakerController(0.5, 5, Duration.ofMillis(300));

    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withSequentialProcessing(true)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          throw new RuntimeException("downstream down");
        })
      )
      .withCircuitBreaker(controller)
      .withConsumer(() -> mc)
      .build();

    consumer.start();

    awaitCondition(() -> !mc.paused().isEmpty(), 5000);
    assertTrue(mc.paused().contains(PARTITION), "breaker tripped → paused");

    // After openDuration (300ms) the probe timer fires and the consumer is resumed.
    awaitCondition(() -> mc.paused().isEmpty(), 3000);
    assertTrue(mc.paused().isEmpty(), "after openDuration the breaker should probe and resume");

    final var timeOpenMs = (long) consumer.getMetrics().get(KPipeConsumer.METRIC_CIRCUIT_BREAKER_TIME_OPEN_MS);
    assertTrue(timeOpenMs > 0L, "circuitBreakerTimeOpenMs must be > 0 after a probe");

    consumer.close();
  }

  @Test
  void probeSuccessReturnsBreakerToClosedState() throws InterruptedException {
    // Arrange: pipeline fails on the first N records, then succeeds. We track the sequence so the
    // probe (the first record processed after resume) succeeds and the breaker closes.
    final var mc = buildMockConsumer(20);
    final var controller = new CircuitBreakerController(0.5, 5, Duration.ofMillis(300));
    final var processed = new AtomicInteger(0);
    final var FAIL_UNTIL = 5; // first 5 records fail to trip, rest succeed

    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withSequentialProcessing(true)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          final var seq = processed.incrementAndGet();
          if (seq <= FAIL_UNTIL) throw new RuntimeException("failure #" + seq);
          return v;
        })
      )
      .withCircuitBreaker(controller)
      .withConsumer(() -> mc)
      .build();

    consumer.start();

    // Wait for trip + probe + resume cycle to complete (at least 1 successful processing).
    awaitCondition(() -> !mc.paused().isEmpty(), 5000);
    awaitCondition(() -> mc.paused().isEmpty(), 3000);
    awaitCondition(() -> processed.get() > FAIL_UNTIL, 5000);

    // After a successful probe, the breaker should be CLOSED — no second trip even though the
    // pipeline saw more records. We sanity-check via the trips counter staying at 1.
    Thread.sleep(300); // grace period for any spurious trip to register
    final var trips = (long) consumer.getMetrics().get(KPipeConsumer.METRIC_CIRCUIT_BREAKER_TRIPS);
    assertEquals(1L, trips, "after recovery the breaker should stay closed; got " + trips + " trips");

    consumer.close();
  }

  @Test
  void breakerStaysClosedWhenFailureRateIsBelowThreshold() throws InterruptedException {
    // Arrange: 60% success, 40% failure → below the 50% threshold, breaker should never trip.
    final var mc = buildMockConsumer(20);
    final var controller = new CircuitBreakerController(0.5, 10, Duration.ofSeconds(30));
    final var processed = new AtomicInteger(0);

    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withSequentialProcessing(true)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          final var seq = processed.incrementAndGet();
          if (seq % 5 < 2) throw new RuntimeException("intermittent #" + seq); // 40% failure rate
          return v;
        })
      )
      .withCircuitBreaker(controller)
      .withConsumer(() -> mc)
      .build();

    consumer.start();

    awaitCondition(() -> processed.get() >= 20, 5000);
    Thread.sleep(200); // grace period for any delayed trip to land

    assertTrue(mc.paused().isEmpty(), "breaker should NOT have tripped at 40% failure rate");
    assertEquals(0L, consumer.getMetrics().get(KPipeConsumer.METRIC_CIRCUIT_BREAKER_TRIPS));

    consumer.close();
  }

  @Test
  void breakerDisabledWhenNotConfiguredIsZeroCost() throws InterruptedException {
    // A consumer with no CB should have CB metrics that stay at zero AND should never pause.
    final var mc = buildMockConsumer(5);
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withSequentialProcessing(true)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          throw new RuntimeException("every record fails");
        })
      )
      .withConsumer(() -> mc)
      .build();

    consumer.start();
    awaitCondition(() -> (long) consumer.getMetrics().get("processingErrors") >= 5, 5000);

    assertEquals(0L, consumer.getMetrics().get(KPipeConsumer.METRIC_CIRCUIT_BREAKER_TRIPS));
    assertTrue(mc.paused().isEmpty(), "no breaker configured → never pauses despite all failures");
    assertNotEquals(0L, consumer.getMetrics().get("processingErrors"));

    consumer.close();
  }

  private MockConsumer<String, byte[]> buildMockConsumer(final int recordCount) {
    final var mc = new MockConsumer<String, byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    mc.assign(List.of(PARTITION));
    mc.updateBeginningOffsets(Map.of(PARTITION, 0L));
    for (int i = 0; i < recordCount; i++) {
      mc.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k" + i, ("v" + i).getBytes()));
    }
    return mc;
  }

  private void awaitCondition(final BooleanSupplier condition, final long timeoutMs) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) Thread.sleep(50);
  }

  @FunctionalInterface
  interface BooleanSupplier {
    boolean getAsBoolean() throws InterruptedException;
  }
}
