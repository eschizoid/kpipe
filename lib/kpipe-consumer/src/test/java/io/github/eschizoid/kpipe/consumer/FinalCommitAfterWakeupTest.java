package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;

/// `KPipeConsumer.close()` wakes the Kafka consumer to break a blocking poll. When the consumer
/// thread is already past its last poll, the wakeup stays pending and fires in the next blocking
/// call, which is the offset manager's final commit. That commit must still go through.
class FinalCommitAfterWakeupTest {

  private static final TopicPartition PARTITION = new TopicPartition("topic", 0);

  @Test
  void finalCommitSurvivesAPendingWakeup() {
    final var commits = new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>();
    final var wakeupPending = new AtomicBoolean(true);
    final var consumer = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void commitSync(
        final Map<TopicPartition, OffsetAndMetadata> offsets,
        final Duration timeout
      ) {
        if (wakeupPending.getAndSet(false)) throw new WakeupException();
        super.commitSync(offsets, timeout);
        commits.putAll(offsets);
      }
    };
    final var manager = KafkaOffsetManager.builder(consumer).withCommitExecutor(TestCommitExecutors.unwired()).build();
    manager.start();
    final var record = new ConsumerRecord<>("topic", 0, 41L, "k".getBytes(UTF_8), "v".getBytes(UTF_8));
    manager.trackOffset(record);
    manager.markOffsetProcessed(record);

    manager.close();

    final var committed = commits.get(PARTITION);
    assertNotNull(committed, "the final commit must go through after the pending wakeup fires");
    assertEquals(42L, committed.offset());
  }
}
