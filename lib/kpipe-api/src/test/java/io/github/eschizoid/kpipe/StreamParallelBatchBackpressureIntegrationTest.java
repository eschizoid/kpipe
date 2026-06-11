package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/// Verifies that buffered records participate in the in-flight backpressure metric when batch
/// sinks run in parallel mode. Tiny watermarks (50/25) plus a slow batch sink (10ms per flush)
/// are enough to push the in-flight + buffered count over the high watermark, which should
/// trigger at least one pause/resume cycle. All produced records must still be delivered after
/// the cycle settles.
@Testcontainers(disabledWithoutDocker = true)
class StreamParallelBatchBackpressureIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");
  private static final int PARTITIONS = 2;
  private static final int RECORD_COUNT = 500;

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION)).asCompatibleSubstituteFor("apache/kafka")
  ).withStartupAttempts(3);

  @Test
  void slowBatchSinkTriggersBackpressureViaBufferedRecords() throws Exception {
    final var topic = "kpipe-bp-batch-" + UUID.randomUUID().toString().substring(0, 8);
    createTopic(topic, PARTITIONS);

    final var totalCaptured = Collections.synchronizedList(new ArrayList<Map<String, Object>>());

    // Slow sink — 10ms per flush. Combined with maxSize=10 below, that means flushes drain
    // at roughly 1000 records/sec, well below the producer's burst rate. Buffered records
    // accumulate, the in-flight + buffered metric crosses 50 (the high watermark), and the
    // consumer pauses.
    final BatchSink<Map<String, Object>> batchSink = BatchSink.ofVoid(batch -> {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      totalCaptured.addAll(batch);
    });

    final var consumerProps = consumerProps("kpipe-bp-batch-group-" + UUID.randomUUID());
    // maxSize=10 keeps the buffer turning over so the buffered count actively contributes to
    // the metric rather than getting swept on every enqueue. Larger maxSize would also work
    // but the assertion remains cleaner with frequent partial flushes.
    final var policy = new BatchPolicy(10, Duration.ofSeconds(2));

    final var handle = KPipe.json(topic, consumerProps).withBackpressure(50, 25).toBatch(batchSink, policy).start();

    try {
      assertTrue(handle.isHealthy(), "Handle should be healthy after start()");

      // Aggressive burst — synchronous send, no flush between, so messages queue up at the
      // broker faster than the consumer can drain.
      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
        for (int i = 1; i <= RECORD_COUNT; i++) {
          final var json = """
            {"id":%d,"value":"v%d"}""".formatted(i, i)
            .getBytes(StandardCharsets.UTF_8);
          final var key = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
          producer.send(new ProducerRecord<>(topic, null, key, json));
        }
        producer.flush();
      }

      // Wait for every record to be received before shutdown so we don't race ahead of a slow
      // poll. Generous timeout because the slow sink throttles delivery to ~1000 rec/s.
      waitFor(
        () -> handle.metrics().getOrDefault("messagesReceived", 0L) >= (long) RECORD_COUNT,
        Duration.ofSeconds(60),
        "expected consumer to receive all " + RECORD_COUNT + " records"
      );

      assertTrue(handle.shutdownGracefully(Duration.ofSeconds(20)), "shutdownGracefully should complete");

      // Backpressure should have fired at least once. Without the buffered-count contribution,
      // the in-flight count alone would never cross 50 (records leave `inFlightCount` the
      // moment they're buffered, before the slow sink runs).
      final var metrics = handle.metrics();
      assertTrue(
        metrics.getOrDefault("backpressurePauseCount", 0L) > 0L,
        "expected at least one backpressure pause, got metrics=" + metrics
      );

      // Every record must still arrive — backpressure must never drop records.
      assertEquals(RECORD_COUNT, totalCaptured.size(), "all produced records should reach the batch sink");
      final var observedIds = new HashSet<Integer>();
      for (final var msg : totalCaptured) {
        observedIds.add(((Number) msg.get("id")).intValue());
      }
      assertEquals(RECORD_COUNT, observedIds.size(), "no record may be delivered twice");
      for (int i = 1; i <= RECORD_COUNT; i++) {
        assertTrue(observedIds.contains(i), "missing id " + i);
      }
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
