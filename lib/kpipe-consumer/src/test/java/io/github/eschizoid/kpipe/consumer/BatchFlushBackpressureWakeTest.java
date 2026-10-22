package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// Release latency of in-flight backpressure when a batch flush, not a record completion, drains
/// the load.
///
/// Records on a batch route finish their per-record work as soon as they are buffered, while the
/// buffer still counts toward the in-flight load. An age-triggered flush runs on the scheduler and
/// is the event that brings the load under the low watermark, so it has to wake the consumer
/// thread the same way a record completion does. As in [BackpressureReleaseLatencyTest], the mock
/// consumer blocks for the whole timeout in a fully paused poll, as a real broker connection does.
class BatchFlushBackpressureWakeTest {

  private static final String TOPIC = "batch-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
  private static final int RECORDS = 100;
  private static final int RECORDS_PER_POLL = 25;
  private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

  @Test
  void ageTriggeredFlushThatDrainsTheLoadResumesWithoutWaitingOutThePollTimeout() throws InterruptedException {
    final var mockConsumer = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}

      @Override
      public synchronized ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
        if (!assignment().isEmpty() && paused().containsAll(assignment())) {
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

    final var flushed = new AtomicInteger();
    final var properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    // The size limit is never reached, so every flush is age-triggered on the scheduler thread.
    // Each poll hands over more records than the high watermark, so every poll trips a pause.
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties)
      .withBatchPipeline(
        TOPIC,
        TestPipelines.identity(),
        BatchSink.<byte[]>ofVoid(batch -> flushed.addAndGet(batch.size())),
        new BatchPolicy(1_000, Duration.ofMillis(100))
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
        () -> flushed.get() == RECORDS,
        POLL_TIMEOUT.dividedBy(2),
        "every record is flushed without a paused poll running its full timeout"
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
