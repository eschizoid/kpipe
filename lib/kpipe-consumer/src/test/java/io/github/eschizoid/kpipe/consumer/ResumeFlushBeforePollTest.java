package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Regression coverage for the resume path reaching Kafka before the next poll.
///
/// `internalResume()` flips the consumer state to RUNNING and queues a `Resume` command rather
/// than calling `kafkaConsumer.resume()` inline — it is reachable off the consumer thread (the
/// public `resume()`, and the circuit breaker's OPEN → HALF_OPEN probe firing from the scheduler
/// thread), and only the consumer thread may touch the Kafka consumer. The queueing is therefore
/// required and is not what these tests constrain.
///
/// What they constrain is the flush. A queued `Resume` that is not drained before the poll leaves
/// the partitions paused at the Kafka level while the consumer believes it is running, so the
/// poll fetches nothing and the resume costs a full `pollTimeout` of not fetching — after every
/// backpressure release, every half-open probe, and every manual resume.
///
/// The window is observable without reaching into the consumer: a poll that runs while the state
/// has left PAUSED but the assignment is still paused at the Kafka level is a poll that cannot
/// fetch. This drives the real consumer loop through [MockConsumer] and requires that count to be
/// zero.
///
/// Only the tick-driven resume is covered, and deliberately so. A manual `resume()` runs on the
/// caller's thread, and between `internalResume()`'s state flip and its `offer(Resume)` — two
/// adjacent statements — the state reads RUNNING with no command yet queued. A poll landing in
/// that window fetches nothing no matter where the flush is placed, so asserting zero bad polls
/// on that path would test an invariant the consumer does not provide.
class ResumeFlushBeforePollTest {

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

  /// Backpressure release: the lag drops, the tick resumes the consumer, and the poll in that same
  /// iteration must already see un-paused partitions.
  @Test
  void backpressureReleaseResumesKafkaBeforeTheNextPoll() throws InterruptedException {
    final var consumerRef = new AtomicReference<KPipeConsumer>();
    final var pollsUnableToFetch = new AtomicLong();
    final var mockConsumer = instrumentedConsumer(consumerRef, pollsUnableToFetch);
    mockConsumer.updateEndOffsets(Map.of(PARTITION, 100L)); // lag = 100 >= high(5) -> PAUSE

    final var consumer = KPipeConsumer.builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withBackpressure(5, 2)
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
      .withConsumer(() -> mockConsumer)
      .build();
    consumerRef.set(consumer);

    try {
      consumer.start();
      TestAwaits.pollUntil(() -> !mockConsumer.paused().isEmpty(), AWAIT, "lag backpressure pauses the consumer");

      mockConsumer.updateEndOffsets(Map.of(PARTITION, 0L)); // lag = 0 <= low(2) -> RESUME
      TestAwaits.pollUntil(() -> mockConsumer.paused().isEmpty(), AWAIT, "resume reaches the Kafka consumer");
      TestAwaits.pollUntil(() -> !consumer.isPaused(), AWAIT, "consumer state returns to RUNNING");

      assertEquals(
        0L,
        pollsUnableToFetch.get(),
        "Every poll after the state left PAUSED must see un-paused partitions; a poll against a " +
          "still-paused assignment cannot fetch and costs a full pollTimeout"
      );
    } finally {
      consumer.close();
    }
  }

  /// The same flush drains `Close`, and a drained `Close` must stop the iteration rather than fall
  /// through to the poll. It sets CLOSING, not PAUSED, so `isPaused()` is false and the guard below
  /// the flush — along with the check inside it — is skipped. Without a check of its own between
  /// the flush and the poll, this iteration polls a consumer that has been told to stop.
  ///
  /// The delivery point is what makes this bite, so the test controls it rather than racing for it.
  /// A `Close` offered from outside is almost always drained by the flush at the top of the loop,
  /// where the existing check already catches it. The queue below hands the `Close` to the *second*
  /// drain instead — the one after the backpressure tick — which is the only site with no check of
  /// its own. `withCommandQueue` is public, so nothing here reaches into the consumer.
  ///
  /// The wait is on `awaitShutdown`, not on the state flag. `isRunning()` goes false the moment the
  /// command is applied, several steps before the poll it guards, so a test resuming there reads
  /// the counter while the consumer thread is still on its way to that poll. The shutdown latch is
  /// released in the consumer thread's own teardown, after the loop has exited, so the counter is
  /// final by the time it returns.
  @Test
  void aCloseDrainedByTheSecondFlushStopsTheIteration() throws InterruptedException {
    final var consumerRef = new AtomicReference<KPipeConsumer>();
    final var pollsAfterStop = new AtomicLong();

    // Which drain gets the Close decides whether this test means anything, so it is derived from
    // the loop's own position and never from timing. `processCommands` drains until poll() returns
    // null, so each null ends one drain, and the counter resets on every consumer poll. Drain 0 is
    // the flush at the top of the loop; drain 1 is the one after the backpressure tick — the site
    // with no check of its own. Delivery lands on the very first iteration, before any poll, so
    // there is no window to arm and nothing to race.
    final var drainsSincePoll = new AtomicInteger();
    final var delivered = new AtomicBoolean();
    final var queue = new ConcurrentLinkedQueue<ConsumerCommand>() {
      @Override
      public ConsumerCommand poll() {
        final var existing = super.poll();
        if (existing != null) return existing;
        if (drainsSincePoll.get() == 1 && delivered.compareAndSet(false, true)) {
          return new ConsumerCommand.Close();
        }
        drainsSincePoll.incrementAndGet();
        return null;
      }
    };

    final var mockConsumer = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}

      @Override
      public synchronized ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
        final var consumer = consumerRef.get();
        if (consumer != null && !consumer.isRunning()) pollsAfterStop.incrementAndGet();
        drainsSincePoll.set(0);
        return super.poll(timeout);
      }
    };
    mockConsumer.assign(List.of(PARTITION));
    mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
    mockConsumer.updateEndOffsets(Map.of(PARTITION, 0L));

    final var consumer = KPipeConsumer.builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withCommandQueue(queue)
      .withConsumer(() -> mockConsumer)
      .build();
    consumerRef.set(consumer);

    try {
      consumer.start();
      assertTrue(consumer.awaitShutdown(AWAIT), "the queued Close shuts the consumer down");
      assertEquals(0L, pollsAfterStop.get(), "no poll may be issued after a drained Close has stopped the consumer");
    } finally {
      consumer.close();
    }
  }

  /// Counts polls issued while the consumer state has left PAUSED but the assignment is still
  /// paused at the Kafka level — the window in which a poll is guaranteed to fetch nothing.
  private MockConsumer<byte[], byte[]> instrumentedConsumer(
    final AtomicReference<KPipeConsumer> consumerRef,
    final AtomicLong pollsUnableToFetch
  ) {
    final var mc = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}

      @Override
      public synchronized ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
        final var consumer = consumerRef.get();
        if (consumer != null && !consumer.isPaused() && !paused().isEmpty()) pollsUnableToFetch.incrementAndGet();
        return super.poll(timeout);
      }
    };
    mc.assign(List.of(PARTITION));
    mc.updateBeginningOffsets(Map.of(PARTITION, 0L));
    return mc;
  }
}
