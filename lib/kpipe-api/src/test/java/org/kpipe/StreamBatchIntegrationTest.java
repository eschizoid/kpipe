package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.BatchSink;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/// End-to-end test for `Stream.toBatch(...)`. Spins up a real Kafka broker, produces JSON
/// records, and verifies that the batch sink receives them in chunks and that residual buffer
/// content is drained on shutdown.
@Testcontainers(disabledWithoutDocker = true)
class StreamBatchIntegrationTest {

  private static final String CONFLUENT_PLATFORM_VERSION = System.getProperty("confluentPlatformVersion", "8.2.0");

  @Container
  static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:%s".formatted(CONFLUENT_PLATFORM_VERSION))
  ).withStartupAttempts(3);

  @Test
  void batchSinkReceivesMessagesInChunksAndDrainsOnShutdown() throws Exception {
    final var topic = "kpipe-batch-test-" + UUID.randomUUID().toString().substring(0, 8);
    final var batches = new CopyOnWriteArrayList<List<Map<String, Object>>>();
    final var totalCaptured = new ArrayList<Map<String, Object>>();

    final BatchSink<Map<String, Object>> batchSink = BatchSink.ofVoid(batch -> {
      batches.add(List.copyOf(batch));
      synchronized (totalCaptured) {
        totalCaptured.addAll(batch);
      }
    });

    final var consumerProps = consumerProps("kpipe-batch-group-" + UUID.randomUUID());
    final var policy = new BatchPolicy(10, Duration.ofSeconds(2));

    final var handle = KPipe.json(topic, consumerProps)
      .withSequentialProcessing(true)
      .toBatch(batchSink, policy)
      .start();

    try {
      assertTrue(handle.isHealthy(), "Handle should be healthy after start()");

      // Produce 25 records — expect 2 size-triggered flushes (10+10), then a tail flush of 5 via
      // age trigger or shutdown drain.
      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
        for (int i = 1; i <= 25; i++) {
          final var json = """
            {"id":%d,"value":"v%d"}""".formatted(i, i).getBytes(StandardCharsets.UTF_8);
          producer.send(new ProducerRecord<>(topic, json)).get();
        }
        producer.flush();
      }

      // Wait until the consumer has polled all 25 records from Kafka — `messagesReceived` is
      // the post-poll counter. Without this gate, shutdown can race ahead of a slow fetch and
      // the trailing records never reach the buffer (failure surfaced after the partial-batch
      // flusher refactor reshuffled timing).
      waitFor(
        () -> handle.metrics().getOrDefault("messagesReceived", 0L) >= 25L,
        Duration.ofSeconds(20),
        "expected consumer to receive all 25 records"
      );

      // By now at least 20 should be flushed (two size-triggered batches); the remaining ≤5
      // sit in the buffer waiting for the age trigger or the shutdown drain.
      waitFor(() -> totalCaptured.size() >= 20, Duration.ofSeconds(5), "expected at least 20 captured records");

      // Verify size-triggered batches are exactly maxSize=10.
      final var sizeTriggered = batches.stream().filter(b -> b.size() == policy.maxSize()).toList();
      assertTrue(sizeTriggered.size() >= 2, "expected at least 2 size-triggered batches of 10, got: " + batches);

      // Trigger shutdown — the trailing 5 records must be drained.
      assertTrue(handle.shutdownGracefully(Duration.ofSeconds(10)), "shutdownGracefully should complete");

      assertEquals(25, totalCaptured.size(), "all 25 produced records should reach the batch sink");

      // Sanity-check ids 1..25 all observed (order across partitions not guaranteed but a single
      // partition + sequential consumer should preserve insertion order).
      final var observedIds = totalCaptured
        .stream()
        .map(m -> ((Number) m.get("id")).intValue())
        .sorted()
        .toList();
      for (int i = 1; i <= 25; i++) assertEquals(i, observedIds.get(i - 1), "missing id " + i);
    } finally {
      handle.shutdownGracefully(Duration.ofSeconds(2));
    }
  }

  private static Properties consumerProps(final String groupId) {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return props;
  }

  private static Properties producerProps() {
    final var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return props;
  }

  private static void waitFor(final BooleanCondition cond, final Duration timeout, final String failureMessage)
    throws InterruptedException {
    final var deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (cond.test()) return;
      TimeUnit.MILLISECONDS.sleep(200);
    }
    if (!cond.test()) throw new AssertionError(failureMessage);
  }

  @FunctionalInterface
  private interface BooleanCondition {
    boolean test();
  }
}
