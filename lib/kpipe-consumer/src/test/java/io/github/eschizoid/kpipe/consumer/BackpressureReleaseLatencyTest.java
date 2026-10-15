package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// Release latency of in-flight backpressure.
///
/// A real `KafkaConsumer` whose assigned partitions are all paused has nothing to fetch, so
/// `poll(timeout)` blocks for the whole timeout. [MockConsumer] returns at once, which hides any
/// wait spent inside a paused poll; the consumer here restores the broker's behaviour. Once the
/// in-flight count drains to the low watermark, the consumer has to resume without first sitting
/// out a paused poll, or every pause costs a full poll timeout of idle time.
class BackpressureReleaseLatencyTest {

  private static final String TOPIC = "test-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
  private static final int RECORDS = 100;
  private static final int RECORDS_PER_POLL = 25;
  private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

  @Test
  void drainedBackpressureResumesWithoutWaitingOutThePollTimeout() throws InterruptedException {
    final var mockConsumer = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}

      @Override
      public synchronized ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
        if (!assignment().isEmpty() && paused().containsAll(assignment())) {
          // A real consumer waits in a network selector, which LockSupport.unpark cannot end
          // early, and Thread.sleep has the same property. Only wakeup() or an interrupt can.
          try {
            Thread.sleep(timeout);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return ConsumerRecords.empty();
        }
        return super.poll(timeout);
      }
    };
    mockConsumer.assign(List.of(PARTITION));
    mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
    for (int i = 0; i < RECORDS; i++) {
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, ("k" + i).getBytes(UTF_8), "v".getBytes(UTF_8)));
    }
    mockConsumer.updateEndOffsets(Map.of(PARTITION, (long) RECORDS));
    mockConsumer.setMaxPollRecords(RECORDS_PER_POLL);

    final var processed = new AtomicInteger();
    final var properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    // Each poll hands over more records than the high watermark, so every poll trips a pause.
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(b -> {
          LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
          processed.incrementAndGet();
          return b;
        })
      )
      .withProcessingMode(ProcessingMode.PARALLEL)
      .withBackpressure(20, 10)
      .withPollTimeout(POLL_TIMEOUT)
      .withConsumer(() -> mockConsumer)
      .build();

    try {
      final long start = System.nanoTime();
      consumer.start();
      // Shorter than one poll timeout: a single paused poll that runs its course fails this.
      TestAwaits.pollUntil(
        () -> processed.get() == RECORDS,
        POLL_TIMEOUT.dividedBy(2),
        "every record is processed without a paused poll running its full timeout"
      );
      final var elapsed = Duration.ofNanos(System.nanoTime() - start);
      assertTrue(
        consumer.getMetrics().get(KPipeConsumer.METRIC_BACKPRESSURE_PAUSE_COUNT) >= RECORDS / RECORDS_PER_POLL - 1,
        "the workload must actually exercise backpressure; elapsed " + elapsed
      );
    } finally {
      consumer.close();
    }
  }
}
