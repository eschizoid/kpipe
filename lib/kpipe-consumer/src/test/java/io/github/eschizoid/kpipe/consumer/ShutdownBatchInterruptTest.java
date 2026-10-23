package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.junit.jupiter.api.Test;

/// A batch route's final flush at shutdown, cut short by `close()` interrupting the consumer
/// thread, must not cost the records it never delivered, nor the final commit. The commit has to
/// land at the first record of the interrupted batch: with no DLQ, failing that batch would commit
/// past it, and leaving the interrupt set would make the commit itself throw.
class ShutdownBatchInterruptTest {

  private static final String TOPIC = "batch-interrupt";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  @Test
  void interruptedShutdownFlushCommitsUpToTheUndeliveredBatch() throws InterruptedException {
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
    for (int i = 0; i < 8; i++) {
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, ("k" + i).getBytes(UTF_8), "v".getBytes(UTF_8)));
    }

    // The first flush (offsets 0 to 4, by size) delivers. The shutdown flush of 5 to 7 hangs until
    // close() interrupts the thread.
    final var flushes = new AtomicInteger();
    final var neverReleased = new CountDownLatch(1);
    final BatchSink<byte[]> sink = BatchSink.ofVoid(_ -> {
      if (flushes.incrementAndGet() == 1) return;
      try {
        neverReleased.await();
      } catch (final InterruptedException e) {
        throw new IllegalStateException("write aborted at shutdown", e);
      }
    });
    final var enqueued = new AtomicInteger();
    final var properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "batch-interrupt-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties)
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
      .withBatchPipeline(
        TOPIC,
        TestPipelines.sideEffect(value -> {
          enqueued.incrementAndGet();
          return value;
        }),
        sink,
        new BatchPolicy(5, Duration.ofMinutes(1))
      )
      .withWaitForMessagesTimeout(Duration.ofMillis(200))
      .withThreadTerminationTimeout(Duration.ofSeconds(1))
      .withConsumer(() -> mockConsumer)
      .build();

    try {
      consumer.start();
      TestAwaits.pollUntil(() -> enqueued.get() == 8, Duration.ofSeconds(10), "every record is buffered");
      consumer.close();
      TestAwaits.pollUntil(() -> commits.containsKey(PARTITION), Duration.ofSeconds(10), "the final commit lands");
    } finally {
      neverReleased.countDown();
    }

    final var committed = commits.get(PARTITION);
    assertNotNull(committed, "the final commit must land after the interrupted flush");
    assertEquals(5L, committed.offset(), "the commit stops at the first record of the undelivered batch");
  }
}
