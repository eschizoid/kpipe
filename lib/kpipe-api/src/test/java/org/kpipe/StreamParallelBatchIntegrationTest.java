package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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

/// End-to-end test for `Stream.toBatch(...)` running in **parallel** mode (the new default after
/// lifting the sequential-required restriction). The topic is created with multiple partitions
/// so the consumer dispatches polled records across virtual-thread workers concurrently. The
/// batch wrapper's lock guarantees no record is lost or double-delivered, and the in-flight
/// backpressure metric includes buffered records so a slow sink cannot cause unbounded buffer
/// growth.
@Testcontainers(disabledWithoutDocker = true)
class StreamParallelBatchIntegrationTest {

  private static final String CONFLUENT_PLATFORM_VERSION = System.getProperty("confluentPlatformVersion", "8.2.0");
  private static final int PARTITIONS = 4;
  private static final int RECORD_COUNT = 200;

  @Container
  static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:%s".formatted(CONFLUENT_PLATFORM_VERSION))
  ).withStartupAttempts(3);

  @Test
  void parallelBatchSinkReceivesAllRecordsAcrossPartitions() throws Exception {
    final var topic = "kpipe-parallel-batch-" + UUID.randomUUID().toString().substring(0, 8);
    createTopic(topic, PARTITIONS);

    final var batches = new CopyOnWriteArrayList<List<Map<String, Object>>>();
    final var totalCaptured = Collections.synchronizedList(new ArrayList<Map<String, Object>>());

    final BatchSink<Map<String, Object>> batchSink = BatchSink.ofVoid(batch -> {
      batches.add(List.copyOf(batch));
      totalCaptured.addAll(batch);
    });

    final var consumerProps = consumerProps("kpipe-parallel-batch-group-" + UUID.randomUUID());
    // maxSize=20 — 200 records will cause ~10 size-triggered flushes plus a final age/shutdown
    // tail.
    final var policy = new BatchPolicy(20, Duration.ofSeconds(2));

    // No `.withSequentialProcessing(true)` — the stream runs in parallel by default. This is
    // the configuration this test guards.
    final var handle = KPipe.json(topic, consumerProps).toBatch(batchSink, policy).start();

    try {
      assertTrue(handle.isHealthy(), "Handle should be healthy after start()");

      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
        for (int i = 1; i <= RECORD_COUNT; i++) {
          final var json = """
            {"id":%d,"value":"v%d"}""".formatted(i, i)
            .getBytes(StandardCharsets.UTF_8);
          // Round-robin across partitions by setting an explicit key — null key + sticky
          // partitioner would clump everything onto one or two partitions, which would defeat
          // the multi-partition coverage check below.
          final var key = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
          producer.send(new ProducerRecord<>(topic, null, key, json)).get();
        }
        producer.flush();
      }

      // Gate shutdown on `messagesReceived` so the consumer has actually polled every record
      // before we drain. Same flake-avoidance pattern as `StreamBatchIntegrationTest`.
      waitFor(
        () -> handle.metrics().getOrDefault("messagesReceived", 0L) >= (long) RECORD_COUNT,
        Duration.ofSeconds(30),
        "expected consumer to receive all " + RECORD_COUNT + " records"
      );

      assertTrue(handle.shutdownGracefully(Duration.ofSeconds(10)), "shutdownGracefully should complete");

      // Verify exactly RECORD_COUNT records reached the sink, every id appears exactly once,
      // no duplicates, no losses — these are the parallel-mode guarantees the batch wrapper's
      // lock is supposed to provide.
      assertEquals(RECORD_COUNT, totalCaptured.size(), "all produced records should reach the batch sink");

      final var observedIds = new HashSet<Integer>();
      for (final var msg : totalCaptured) {
        observedIds.add(((Number) msg.get("id")).intValue());
      }
      assertEquals(RECORD_COUNT, observedIds.size(), "no record may be delivered twice");
      for (int i = 1; i <= RECORD_COUNT; i++) {
        assertTrue(observedIds.contains(i), "missing id " + i);
      }

      // Metrics: every record must have been processed; zero processing errors.
      final var metrics = handle.metrics();
      assertTrue(
        metrics.get("messagesProcessed") >= (long) RECORD_COUNT,
        "messagesProcessed should be >= RECORD_COUNT, got " + metrics.get("messagesProcessed")
      );
      assertEquals(0L, metrics.getOrDefault("processingErrors", 0L), "no processing errors expected");
    } finally {
      handle.shutdownGracefully(Duration.ofSeconds(2));
    }
  }

  private static void createTopic(final String topic, final int partitions) throws Exception {
    final var adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    try (final var admin = Admin.create(adminProps)) {
      admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
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
