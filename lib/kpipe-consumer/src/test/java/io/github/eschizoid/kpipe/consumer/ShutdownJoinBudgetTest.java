package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/// With the default timeouts, `close()` must wait long enough for the consumer thread's teardown
/// to reach the final commit, even when a record is stuck and the dispatcher waits its full
/// termination timeout. Interrupting the thread first would make the final commit throw, as a real
/// Kafka consumer does when its thread is interrupted, so the commit would never be confirmed.
class ShutdownJoinBudgetTest {

  private static final String TOPIC = "join-budget";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
  private static final int RECORDS = 10;
  private static final int STUCK_OFFSET = 4;

  @Test
  void defaultJoinBudgetLetsTheFinalCommitRunWithAStuckRecord() throws InterruptedException {
    final var commits = new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>();
    final var mockConsumer = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void commitSync(
        final Map<TopicPartition, OffsetAndMetadata> offsets,
        final Duration timeout
      ) {
        // A real KafkaConsumer refuses to block on an interrupted thread.
        if (Thread.interrupted()) throw new InterruptException(new InterruptedException());
        super.commitSync(offsets, timeout);
        commits.putAll(offsets);
      }

      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}
    };
    mockConsumer.assign(List.of(PARTITION));
    mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
    for (int i = 0; i < RECORDS; i++) {
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, ("k" + i).getBytes(UTF_8), ("v" + i).getBytes(UTF_8)));
    }

    final var finished = new AtomicInteger();
    final var neverReleased = new CountDownLatch(1);
    final var properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "join-budget-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessingMode(ProcessingMode.PARALLEL)
      // Only the drain wait is shortened, to keep the test quick; the join budget and the
      // dispatcher's termination wait keep their defaults.
      .withWaitForMessagesTimeout(Duration.ofMillis(200))
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          if (new String(value, UTF_8).equals("v" + STUCK_OFFSET)) {
            try {
              neverReleased.await();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException("interrupted at shutdown", e);
            }
          }
          finished.incrementAndGet();
          return value;
        })
      )
      .withConsumer(() -> mockConsumer)
      .build();

    try {
      consumer.start();
      TestAwaits.pollUntil(() -> finished.get() == RECORDS - 1, Duration.ofSeconds(10), "every other record finishes");
      consumer.close();
    } finally {
      neverReleased.countDown();
    }

    final var committed = commits.get(PARTITION);
    assertNotNull(committed, "the final commit must run once the stuck record is abandoned");
    assertEquals(STUCK_OFFSET, committed.offset(), "the commit stops at the stuck record");
  }

  /// With the broker unreachable, the final commit and the Kafka client's close each wait out
  /// their whole timeout. The default budget leaves room for both, so the client finishes its
  /// close before `close()` returns. Interrupted instead, the client cuts the close short without
  /// leaving the group, and the group waits out the session timeout before it rebalances.
  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void kafkaClientClosesBeforeCloseReturnsWhenTheBrokerIsUnreachable(final ProcessingMode mode)
    throws InterruptedException {
    final var kafkaDefaultClose = Duration.ofSeconds(30);
    final var requestedTimeout = new AtomicReference<Duration>();
    final var closeCutShort = new AtomicBoolean();
    final var kafkaClosed = new CountDownLatch(1);
    final var mockConsumer = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}

      /// Not synchronized, so `close()`'s `wakeup()` is not held up behind it.
      @Override
      public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        if (!waitOut(timeout)) throw new InterruptException(new InterruptedException());
        throw new org.apache.kafka.common.errors.TimeoutException("the broker cannot be reached");
      }

      @Override
      public void close() {
        closeFor(kafkaDefaultClose);
      }

      @Override
      public void close(final Duration timeout) {
        closeFor(timeout);
      }

      @Override
      public void close(final CloseOptions options) {
        closeFor(options.timeout().orElse(kafkaDefaultClose));
      }

      private void closeFor(final Duration timeout) {
        requestedTimeout.set(timeout);
        if (!waitOut(timeout)) closeCutShort.set(true);
        kafkaClosed.countDown();
      }

      private boolean waitOut(final Duration timeout) {
        try {
          Thread.sleep(timeout);
          return true;
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    };
    mockConsumer.assign(List.of(PARTITION));
    mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k".getBytes(UTF_8), "v".getBytes(UTF_8)));

    final var processed = new CountDownLatch(1);
    final var properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "join-budget-kafka-close-" + mode);
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessingMode(mode)
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          processed.countDown();
          return value;
        })
      )
      .withConsumer(() -> mockConsumer)
      .build();

    consumer.start();
    assertTrue(processed.await(10, TimeUnit.SECONDS), "the record should be processed");
    consumer.close();

    assertEquals(0, kafkaClosed.getCount(), "the Kafka client must have closed by the time close() returns");
    assertFalse(closeCutShort.get(), "the Kafka client close must not be interrupted");
    assertEquals(ConsumerDefaults.KAFKA_CLIENT_CLOSE, requestedTimeout.get(), "the Kafka client close is bounded");
  }

  /// Each mode's default budget covers exactly the waits that mode's teardown runs, including the
  /// producer's close only for a consumer that has a producer to close.
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void defaultJoinBudgetCoversEachModesTeardown(final boolean hasProducer) {
    final var clientCloses = hasProducer
      ? ConsumerDefaults.KAFKA_CLIENT_CLOSE.plus(ConsumerDefaults.PRODUCER_CLOSE)
      : ConsumerDefaults.KAFKA_CLIENT_CLOSE;
    final var afterDispatch = ConsumerDefaults.COMMIT_SCHEDULER_STOP.plus(ConsumerDefaults.FINAL_COMMIT).plus(
      clientCloses
    );
    assertEquals(afterDispatch, ConsumerDefaults.threadTermination(ProcessingMode.SEQUENTIAL, hasProducer));
    assertEquals(
      ConsumerDefaults.WAIT_FOR_MESSAGES.plus(ConsumerDefaults.EXECUTOR_TERMINATION).plus(afterDispatch),
      ConsumerDefaults.threadTermination(ProcessingMode.PARALLEL, hasProducer)
    );
    assertEquals(
      ConsumerDefaults.WAIT_FOR_MESSAGES.plus(ConsumerDefaults.KEY_ORDERED_CLOSE_DRAIN).plus(afterDispatch),
      ConsumerDefaults.threadTermination(ProcessingMode.KEY_ORDERED, hasProducer)
    );
  }

  /// A record hung on a SEQUENTIAL consumer's own thread keeps its loop from ever reaching
  /// teardown, so waiting longer cannot help; `close()` must not hold for the dispatcher wait
  /// that only PARALLEL runs.
  @Test
  void hungSequentialRecordDoesNotHoldCloseForAParallelBudget() throws InterruptedException {
    final var mockConsumer = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}
    };
    mockConsumer.assign(List.of(PARTITION));
    mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k".getBytes(UTF_8), "v".getBytes(UTF_8)));

    final var started = new CountDownLatch(1);
    final var neverReleased = new CountDownLatch(1);
    final var properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "join-budget-sequential");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
      .withWaitForMessagesTimeout(Duration.ofMillis(200))
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          started.countDown();
          try {
            neverReleased.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted at shutdown", e);
          }
          return value;
        })
      )
      .withConsumer(() -> mockConsumer)
      .build();

    try {
      consumer.start();
      assertTrue(started.await(10, TimeUnit.SECONDS), "the record should start");
      final long begin = System.nanoTime();
      consumer.close();
      final var elapsed = Duration.ofNanos(System.nanoTime() - begin);
      // The teardown a SEQUENTIAL consumer runs once its loop exits: the scheduler stop, the final
      // commit and the Kafka client's close. Computed here rather than read from threadTermination,
      // so a wrong budget there cannot also move the bound this test checks.
      final var budget = ConsumerDefaults.COMMIT_SCHEDULER_STOP.plus(ConsumerDefaults.FINAL_COMMIT).plus(
        ConsumerDefaults.KAFKA_CLIENT_CLOSE
      );
      assertTrue(
        elapsed.compareTo(budget.plusSeconds(3)) < 0,
        "close() must give up after the SEQUENTIAL budget of " + budget + "; took " + elapsed
      );
    } finally {
      neverReleased.countDown();
    }
  }

  /// A slow but healthy SEQUENTIAL consumer stops between records once `close()` begins, so its
  /// teardown fits the SEQUENTIAL budget and the final commit lands at the last record it
  /// processed. Finishing the whole polled batch first would run past the budget and lose the
  /// commit to the interrupt.
  @Test
  void slowSequentialConsumerStopsBetweenRecordsAndCommitsWhatItProcessed() throws InterruptedException {
    final var commits = new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>();
    final var mockConsumer = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void commitSync(
        final Map<TopicPartition, OffsetAndMetadata> offsets,
        final Duration timeout
      ) {
        if (Thread.interrupted()) throw new InterruptException(new InterruptedException());
        super.commitSync(offsets, timeout);
        commits.putAll(offsets);
      }

      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}
    };
    mockConsumer.assign(List.of(PARTITION));
    mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
    // 400 records at 50 ms each is 20 s of work in one poll, twice the SEQUENTIAL budget.
    for (int i = 0; i < 400; i++) {
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, ("k" + i).getBytes(UTF_8), "v".getBytes(UTF_8)));
    }

    final var processed = new AtomicInteger();
    final var properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "join-budget-slow-sequential");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
      .withWaitForMessagesTimeout(Duration.ofMillis(200))
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          try {
            Thread.sleep(50);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted at shutdown", e);
          }
          processed.incrementAndGet();
          return value;
        })
      )
      .withConsumer(() -> mockConsumer)
      .build();

    consumer.start();
    TestAwaits.pollUntil(() -> processed.get() >= 1, Duration.ofSeconds(10), "the first record finishes");
    consumer.close();

    final var committed = commits.get(PARTITION);
    assertNotNull(committed, "the final commit must land, not be lost to an interrupt");
    assertEquals(processed.get(), committed.offset(), "the commit stops at the last record processed");
    assertTrue(
      processed.get() < 400,
      "close() must stop the batch rather than finish it; processed " + processed.get()
    );
  }
}
