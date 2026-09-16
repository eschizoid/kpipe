package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
/// fetch. These tests drive the real consumer loop through [MockConsumer] and require that count
/// to be zero.
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

  /// The manual path, which reaches `internalResume()` from the caller's thread rather than from
  /// the tick. The same flush has to cover it.
  @Test
  void manualResumeResumesKafkaBeforeTheNextPoll() throws InterruptedException {
    final var consumerRef = new AtomicReference<KPipeConsumer>();
    final var pollsUnableToFetch = new AtomicLong();
    final var mockConsumer = instrumentedConsumer(consumerRef, pollsUnableToFetch);
    mockConsumer.updateEndOffsets(Map.of(PARTITION, 0L));

    final var consumer = KPipeConsumer.builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> mockConsumer)
      .build();
    consumerRef.set(consumer);

    try {
      consumer.start();
      consumer.pause();
      TestAwaits.pollUntil(() -> !mockConsumer.paused().isEmpty(), AWAIT, "manual pause reaches the Kafka consumer");

      consumer.resume();
      TestAwaits.pollUntil(() -> mockConsumer.paused().isEmpty(), AWAIT, "manual resume reaches the Kafka consumer");

      assertEquals(0L, pollsUnableToFetch.get(), "A manual resume must also reach Kafka before the next poll");
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
