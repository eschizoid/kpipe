package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// Regression guard for the at-least-once LOSS bug: a record interrupted while sleeping in the
/// retry backoff between attempts must NOT be acknowledged. If the interrupted record's offset
/// were marked processed, the commit point would advance past it and the record would be silently
/// dropped on the next poll — violating the no-loss / lowest-pending commit invariant.
///
/// Unlike the mock-offset-manager coverage that only checks `markOffsetProcessed` was never
/// invoked, this test wires a REAL [KafkaOffsetManager] and asserts the observable commit-point
/// invariant end-to-end: after the interrupt the offset is still pending and the partition's
/// `nextOffsetToCommit` points AT that offset (eligible for reprocessing), never past it.
class RetryInterruptTest {

  private static final String TOPIC = "retry-interrupt-topic";
  private static final int PARTITION = 0;
  private static final long OFFSET = 42L;
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);

  private static Properties consumerProperties() {
    final var props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "retry-interrupt-group");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return props;
  }

  @Test
  void interruptDuringRetryBackoffLeavesOffsetPendingForReprocessing() throws Exception {
    final var mockConsumer = new MockConsumer<String, byte[]>("earliest");
    final var commandQueue = new LinkedList<ConsumerCommand>();

    // First attempt throws -> consumer enters the retry-backoff sleep on the next loop iteration.
    final var sinkInvocations = new AtomicInteger(0);
    final var managerHolder = new AtomicReference<KafkaOffsetManager<String>>();

    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(consumerProperties())
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          sinkInvocations.incrementAndGet();
          throw new RuntimeException("transient sink failure");
        })
      )
      .withRetry(3, Duration.ofSeconds(30))
      .withConsumer(() -> mockConsumer)
      .withCommandQueue(commandQueue)
      .withOffsetManagerProvider(c -> {
        final var manager = KafkaOffsetManager.<String>builder(c).withCommandQueue(commandQueue).build();
        managerHolder.set(manager);
        return manager;
      })
      .build();

    final var offsetManager = managerHolder.get();
    final var record = new ConsumerRecord<>(TOPIC, PARTITION, OFFSET, "key", "value".getBytes());

    // The consumer thread tracks the offset before dispatch; mirror that here so the partition
    // state reflects an in-flight record, exactly as the real poll loop would have set it up.
    offsetManager.trackOffset(record);

    final var threadStarted = new CountDownLatch(1);
    final var threadFinished = new CountDownLatch(1);

    final var processingThread = Thread.ofVirtual().unstarted(() -> {
      threadStarted.countDown();
      try {
        consumer.processRecord(record);
      } finally {
        threadFinished.countDown();
      }
    });
    processingThread.start();

    assertTrue(threadStarted.await(2, TimeUnit.SECONDS), "processing thread never started");

    // Wait until the first sink attempt has failed, which means the worker is now parked in the
    // 30s retry backoff sleep — the exact window issue #72 is about.
    final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (sinkInvocations.get() == 0 && System.nanoTime() < deadline) {
      Thread.onSpinWait();
    }
    assertEquals(1, sinkInvocations.get(), "first attempt should have failed before the backoff sleep");

    processingThread.interrupt();
    assertTrue(threadFinished.await(2, TimeUnit.SECONDS), "interrupt did not unblock the backoff sleep");

    // The sink only ran once: the interrupt aborted the retry, it did not let a later attempt run.
    assertEquals(1, sinkInvocations.get(), "interrupt must not allow further retry attempts");

    // Core no-loss assertion: the offset is still pending and the commit point points AT it, so on
    // the next poll the record is re-fetched and reprocessed rather than silently acknowledged.
    final var state = offsetManager.getPartitionState(TOPIC_PARTITION);
    assertEquals(1, state.get("pendingCount"), "interrupted record must remain pending");
    assertEquals(OFFSET, state.get("lowestPendingOffset"), "lowest pending offset must be the interrupted record");
    assertEquals(
      OFFSET,
      state.get("nextOffsetToCommit"),
      "commit point must not advance past the interrupted, never-acknowledged offset"
    );
    assertEquals(
      -1L,
      state.get("highestProcessedOffset"),
      "no offset was successfully processed, so highestProcessed must stay unset"
    );

    consumer.close();
  }
}
