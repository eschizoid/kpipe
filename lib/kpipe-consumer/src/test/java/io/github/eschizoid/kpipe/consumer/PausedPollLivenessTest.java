package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Liveness tests for the paused consumer loop.
///
/// Regression coverage for the lag-strategy wedge: in SEQUENTIAL mode the backpressure strategy
/// is lag-based, and a backpressure pause used to `LockSupport.park()` the consumer thread
/// indefinitely. Nothing ever unparked it — SEQUENTIAL has no asynchronous record completions
/// (processing runs inline on the parked thread itself), and a paused consumer's positions are
/// frozen while the broker's end offsets only grow, so the lag metric could never fall to the low
/// watermark on its own. One WARNING was logged and the consumer sat parked forever; past
/// `max.poll.interval.ms` the client also left the group, triggering a silent rebalance.
///
/// The fix keeps the paused consumer polling its (paused) partitions on the poll-timeout cadence:
/// each loop iteration re-evaluates backpressure, so an external lag drop (partition
/// reassignment, retention truncation, manual offset reset) resumes the consumer, and the
/// continued polling keeps the group membership alive. These tests drive the real consumer loop
/// through [MockConsumer]; every await is bounded so a regression fails fast instead of hanging.
class PausedPollLivenessTest {

  private static final String TOPIC = "test-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
  private static final Duration AWAIT = Duration.ofSeconds(5);

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  }

  /// The wedge itself: a lag-triggered pause must not park the consumer forever. When the lag
  /// drops externally (here: the test shrinks the broker end offsets, standing in for retention
  /// truncation or a manual offset reset), the paused loop's periodic re-check must observe
  /// lag ≤ low watermark, resume, and go back to delivering records.
  @Test
  void lagPauseResumesWhenLagDropsExternally() throws InterruptedException {
    final var mockConsumer = pausableMockConsumer();
    mockConsumer.updateEndOffsets(Map.of(PARTITION, 100L)); // lag = 100 - 0 >= high(5) -> PAUSE

    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withBackpressure(5, 2)
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
      .withConsumer(() -> mockConsumer)
      .build();

    try {
      consumer.start();

      // The first loop iteration ticks backpressure before the first poll, so the pause is
      // deterministic — no records have been fetched yet.
      TestAwaits.pollUntil(() -> !mockConsumer.paused().isEmpty(), AWAIT, "lag backpressure pauses the consumer");
      assertTrue(consumer.isPaused(), "Consumer state should be PAUSED");
      assertTrue(consumer.getMetrics().get(KPipeConsumer.METRIC_BACKPRESSURE_PAUSE_COUNT) >= 1);

      // External lag drop: end offsets shrink to the frozen position. Before the fix the
      // consumer thread was parked here and never re-read the metric — this await timed out.
      mockConsumer.updateEndOffsets(Map.of(PARTITION, 0L)); // lag = 0 <= low(2) -> RESUME

      TestAwaits.pollUntil(() -> mockConsumer.paused().isEmpty(), AWAIT, "consumer resumes after external lag drop");
      TestAwaits.pollUntil(() -> !consumer.isPaused(), AWAIT, "consumer state returns to RUNNING");

      // Full liveness: a record produced after the resume must flow end to end.
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k0", "v0".getBytes()));
      mockConsumer.updateEndOffsets(Map.of(PARTITION, 1L));
      TestAwaits.pollUntil(
        () -> consumer.getMetrics().get("messagesProcessed") == 1L,
        AWAIT,
        "record produced after resume is processed"
      );
      assertEquals(1L, consumer.getMetrics().get("messagesProcessed"));
    } finally {
      consumer.close();
    }
  }

  /// A lag-paused consumer must keep calling `poll()` — that is what keeps the group membership
  /// alive (a consumer that stops polling is evicted after `max.poll.interval.ms`). The polls
  /// must also fetch nothing: the records seeded below sit behind paused partitions the whole
  /// time. Before the fix the thread parked after the pause and the poll count froze.
  @Test
  void lagPausedConsumerKeepsPollingWithoutFetching() throws InterruptedException {
    final var polls = new AtomicLong();
    final var mockConsumer = new MockConsumer<String, byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}

      @Override
      public synchronized ConsumerRecords<String, byte[]> poll(final Duration timeout) {
        polls.incrementAndGet();
        return super.poll(timeout);
      }
    };
    mockConsumer.assign(List.of(PARTITION));
    mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
    // Records are present, but the pause decision runs before the first poll, so none of them
    // may ever be fetched while the pause holds.
    for (int i = 0; i < 3; i++) {
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k" + i, ("v" + i).getBytes()));
    }
    mockConsumer.updateEndOffsets(Map.of(PARTITION, 100L)); // lag = 100 >= high(5) -> PAUSE

    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withBackpressure(5, 2)
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
      .withConsumer(() -> mockConsumer)
      .build();

    try {
      consumer.start();
      TestAwaits.pollUntil(() -> !mockConsumer.paused().isEmpty(), AWAIT, "lag backpressure pauses the consumer");

      final var pollsAtPause = polls.get();
      TestAwaits.pollUntil(
        () -> polls.get() >= pollsAtPause + 5,
        AWAIT,
        "paused consumer keeps polling (group-membership liveness)"
      );

      // The pause contract still holds: all that polling fetched nothing.
      assertTrue(consumer.isPaused(), "Consumer should still be paused while lag stays high");
      assertEquals(0L, consumer.getMetrics().get("messagesReceived"), "No records may be fetched while paused");
    } finally {
      consumer.close();
    }
  }

  /// The same eviction hazard existed for manual pauses (and circuit-breaker opens, which share
  /// the pause path): a long `pause()` parked the thread until `resume()`, so past
  /// `max.poll.interval.ms` the client silently left the group. A manually paused consumer must
  /// keep polling its paused partitions too.
  @Test
  void manuallyPausedConsumerKeepsPolling() throws InterruptedException {
    final var polls = new AtomicLong();
    final var mockConsumer = new MockConsumer<String, byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}

      @Override
      public synchronized ConsumerRecords<String, byte[]> poll(final Duration timeout) {
        polls.incrementAndGet();
        return super.poll(timeout);
      }
    };
    mockConsumer.assign(List.of(PARTITION));
    mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
    mockConsumer.updateEndOffsets(Map.of(PARTITION, 0L));

    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> mockConsumer)
      .build();

    try {
      consumer.start();
      TestAwaits.pollUntil(() -> polls.get() > 0, AWAIT, "consumer loop starts polling");

      consumer.pause();
      TestAwaits.pollUntil(() -> !mockConsumer.paused().isEmpty(), AWAIT, "manual pause reaches the Kafka consumer");

      final var pollsAtPause = polls.get();
      TestAwaits.pollUntil(
        () -> polls.get() >= pollsAtPause + 5,
        AWAIT,
        "manually paused consumer keeps polling (group-membership liveness)"
      );
      assertTrue(consumer.isPaused(), "Consumer should still be paused until resume()");

      consumer.resume();
      TestAwaits.pollUntil(() -> mockConsumer.paused().isEmpty(), AWAIT, "resume() unpauses the partitions");
    } finally {
      consumer.close();
    }
  }

  /// MockConsumer with subscribe() stubbed to a no-op so the manual `assign` survives — the
  /// same pattern as the other backpressure tests in this package.
  private MockConsumer<String, byte[]> pausableMockConsumer() {
    final var mc = new MockConsumer<String, byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}
    };
    mc.assign(List.of(PARTITION));
    mc.updateBeginningOffsets(Map.of(PARTITION, 0L));
    return mc;
  }
}
