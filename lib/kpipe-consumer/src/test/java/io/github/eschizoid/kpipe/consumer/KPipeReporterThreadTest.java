package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.metrics.KPipeMetricsReporter;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Pins the periodic-reporter-thread subsystem wired through
/// [KPipeConsumer.Builder#withMetricsReporters(Collection)] +
/// [KPipeConsumer.Builder#withMetricsInterval(Duration)].
///
/// The repo itself has no callers of these setters — they exist purely for downstream users
/// (documented in §17 of `CLAUDE.md` and the `docs/escape-hatches.md` capability table). The
/// deletion gate for documented public API requires both "no callers in repo" and "not in §17
/// / escape-hatches.md"; this subsystem fails the second half. The test names below explicitly
/// reference the Builder method so a future symbol-based dead-code audit trips over them.
class KPipeReporterThreadTest {

  private static final String TOPIC = "test-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  /// Short reporter cadence so the test runs in well under a second while still giving the
  /// thread room to fire three times. The post-close safety sleep is derived from this so the
  /// two values can't drift apart in future edits.
  private static final Duration REPORTER_INTERVAL = Duration.ofMillis(50);
  private static final long POST_CLOSE_SAFETY_MS = REPORTER_INTERVAL.multipliedBy(3).toMillis();

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("enable.auto.commit", "true");
  }

  /// Drives a real consumer through the bundled setter and asserts: (1) the reporter fires at
  /// least three times while `start()`ed, (2) it stops firing after `close()` returns. The
  /// short [#REPORTER_INTERVAL] keeps the test under a second; the 3-second latch timeout is
  /// the generous CI safety margin, not the expected runtime.
  @Test
  void shouldRunReportersPeriodicallyViaBundledWithMetricsReportersSetter() throws Exception {
    final var fires = new AtomicInteger();
    final var threeFires = new CountDownLatch(3);
    final KPipeMetricsReporter reporter = () -> {
      fires.incrementAndGet();
      threeFires.countDown();
    };

    final var mockConsumer = buildMockConsumer();
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> mockConsumer)
      .withMetricsReporters(List.of(reporter))
      .withMetricsInterval(REPORTER_INTERVAL)
      .build();

    consumer.start();
    try {
      assertTrue(threeFires.await(3, TimeUnit.SECONDS), "reporter should fire at least three times while running");
    } finally {
      consumer.close();
    }

    // After close() returns, stopMetricsReporterThread() has interrupted + joined the thread.
    // Sleep three intervals so any racing fire would land comfortably, then assert the count
    // is stable.
    final var firesAfterClose = fires.get();
    Thread.sleep(POST_CLOSE_SAFETY_MS);
    assertEquals(firesAfterClose, fires.get(), "reporter must not fire after close() returns");
  }

  /// Pins the null-rejection contract on the bundled setter so callers get a fast, specific
  /// failure rather than a deferred NPE on `start()`.
  @Test
  void withMetricsReportersRejectsNullArgs() {
    final var builder = KPipeConsumer.<String>builder().withProperties(properties).withTopic(TOPIC);
    assertThrows(NullPointerException.class, () -> builder.withMetricsReporters(null));
  }

  /// Pins the positive-duration guard on the bundled setter. The runtime defensively no-ops
  /// when `interval.toMillis() <= 0` (see `KPipeConsumer.startMetricsReporterThread`), so an
  /// invalid interval would silently disable the reporters that were just configured. The
  /// Builder rejects up front so the misconfig surfaces at `build()` time instead of at
  /// `start()` as a no-show feature.
  @Test
  void withMetricsIntervalRejectsZeroOrNegative() {
    final var builder = KPipeConsumer.<String>builder().withProperties(properties).withTopic(TOPIC);
    assertThrows(NullPointerException.class, () -> builder.withMetricsInterval(null));
    assertThrows(IllegalArgumentException.class, () -> builder.withMetricsInterval(Duration.ZERO));
    assertThrows(IllegalArgumentException.class, () -> builder.withMetricsInterval(Duration.ofMillis(-1)));
  }

  /// Mirrors the helper in `KPipeBackpressureIntegrationTest`: pre-assigns the partition so
  /// `subscribe()` is a no-op, and seeds the beginning offset. No records are added — the
  /// reporter-thread test doesn't need any.
  private MockConsumer<String, byte[]> buildMockConsumer() {
    final var mc = new MockConsumer<String, byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    mc.assign(List.of(PARTITION));
    mc.updateBeginningOffsets(Map.of(PARTITION, 0L));
    return mc;
  }
}
