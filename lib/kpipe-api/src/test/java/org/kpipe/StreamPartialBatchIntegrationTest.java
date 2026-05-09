package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.BatchResult;
import org.kpipe.sink.PartialBatchSink;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/// End-to-end test for `Stream.toBatchPartial(...)`. Spins up a real Kafka broker, produces 10
/// JSON records, configures a [PartialBatchSink] that fails records with id `2` and `7`, and
/// verifies that:
///
/// 1. The 8 succeeded records pass through the sink with their offsets marked processed
///    (`messagesProcessed == 8`).
/// 2. The 2 failed records are routed to the configured DLQ topic — and only those two — so a
///    separate consumer reading from the DLQ topic sees exactly the two failed payloads.
/// 3. Consumer metrics show 2 `processingErrors`, 2 `dlqSent`, and 8 `messagesProcessed` —
///    the partial-batch path increments the same OTel/log instruments as the per-record path.
@Testcontainers(disabledWithoutDocker = true)
class StreamPartialBatchIntegrationTest {

  private static final String CONFLUENT_PLATFORM_VERSION = System.getProperty("confluentPlatformVersion", "8.2.0");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Container
  static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:%s".formatted(CONFLUENT_PLATFORM_VERSION))
  ).withStartupAttempts(3);

  @Test
  void partialBatchSinkRoutesOnlyFailedRecordsToDlq() throws Exception {
    final var topic = "kpipe-partial-batch-test-" + UUID.randomUUID().toString().substring(0, 8);
    final var dlqTopic = topic + "-dlq";
    final var failingIds = Set.of(2, 7);

    final var batches = new CopyOnWriteArrayList<List<Map<String, Object>>>();
    final var allDelivered = new CopyOnWriteArrayList<Map<String, Object>>();

    final PartialBatchSink<Map<String, Object>> sink = values -> {
      // Snapshot the batch for assertions and decide which positions to fail. Failures are
      // attributed by the json `id` field — which maps to the record id we produced — so the
      // assertion below can correlate DLQ payloads back to the source records.
      batches.add(List.copyOf(values));
      allDelivered.addAll(values);

      final var succeeded = new ArrayList<Integer>();
      final var failed = new HashMap<Integer, Exception>();
      for (int i = 0; i < values.size(); i++) {
        final var id = ((Number) values.get(i).get("id")).intValue();
        if (failingIds.contains(id)) {
          failed.put(i, new RuntimeException("simulated downstream failure for id=" + id));
        } else {
          succeeded.add(i);
        }
      }
      return new BatchResult<>(succeeded, failed);
    };

    final var consumerProps = consumerProps("kpipe-partial-batch-group-" + UUID.randomUUID());
    // maxSize = 10 so the entire test batch flushes in one shot — the partial-batch failure
    // path is what we are testing, not size-trigger semantics.
    final var policy = new BatchPolicy(10, Duration.ofSeconds(2));

    final var handle = KPipe
      .json(topic, consumerProps)
      .withDeadLetterTopic(dlqTopic)
      .toBatchPartial(sink, policy)
      .start();

    try {
      assertTrue(handle.isHealthy(), "Handle should be healthy after start()");

      // Produce 10 records with ids 1..10 — the sink will fail ids 2 and 7.
      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
        for (int i = 1; i <= 10; i++) {
          final var json = """
            {"id":%d,"value":"v%d"}""".formatted(i, i).getBytes(StandardCharsets.UTF_8);
          producer.send(new ProducerRecord<>(topic, json)).get();
        }
        producer.flush();
      }

      // Wait until the batch sink has seen all 10 records (one flush should suffice).
      waitFor(() -> allDelivered.size() >= 10, Duration.ofSeconds(20), "expected sink to receive all 10 records");

      // Drain the DLQ topic with a separate consumer. Two records expected (ids 2 and 7), no
      // more — succeeded records must NOT have leaked to the DLQ.
      final var dlqPayloads = drainDlqTopic(dlqTopic, 2, Duration.ofSeconds(15));

      // The DLQ payloads are the raw original record values. Parse them back to assert the
      // ids match the configured failing set.
      final var dlqIds = new HashSet<Integer>();
      for (final var raw : dlqPayloads) {
        @SuppressWarnings("unchecked")
        final var parsed = (Map<String, Object>) MAPPER.readValue(raw, Map.class);
        dlqIds.add(((Number) parsed.get("id")).intValue());
      }
      assertEquals(failingIds, dlqIds, "DLQ should contain exactly the failing ids and nothing else");

      // Consumer-side metrics: 8 processed, 2 errors, 2 DLQ sent. processingErrors and dlqSent
      // increment from the partial-batch path the same way the per-record path does.
      final var metrics = handle.metrics();
      assertEquals(8L, metrics.get("messagesProcessed"), "8 records succeeded => 8 messagesProcessed");
      assertEquals(2L, metrics.get("processingErrors"), "2 records failed => 2 processingErrors");
      assertEquals(2L, metrics.get("dlqSent"), "2 records routed to DLQ => 2 dlqSent");

      // And confirm we never sent a succeeded record to the DLQ — sanity check that the
      // failing-id set is exactly the metric-reported failure count.
      assertFalse(dlqIds.contains(1), "non-failing id 1 should not be in DLQ");
      assertFalse(dlqIds.contains(5), "non-failing id 5 should not be in DLQ");

      assertTrue(handle.shutdownGracefully(Duration.ofSeconds(10)), "shutdownGracefully should complete");
    } finally {
      handle.shutdownGracefully(Duration.ofSeconds(2));
    }
  }

  /// Polls a fresh consumer against `dlqTopic` until at least `expected` payloads have been
  /// observed or the deadline expires. Records are returned as raw byte[] payloads.
  private List<byte[]> drainDlqTopic(final String dlqTopic, final int expected, final Duration timeout) {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-drainer-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // ConcurrentHashMap.newKeySet() de-dupes payloads if the consumer ever rebalances mid-poll
    // and re-reads — DLQ records are uniquely identified by their raw bytes.
    final var observed = ConcurrentHashMap.<String>newKeySet();
    final var deadline = System.nanoTime() + timeout.toNanos();
    try (final var consumer = new KafkaConsumer<byte[], byte[]>(props)) {
      consumer.subscribe(List.of(dlqTopic));
      while (System.nanoTime() < deadline && observed.size() < expected) {
        final var records = consumer.poll(Duration.ofMillis(500));
        for (final var r : records) observed.add(new String(r.value(), StandardCharsets.UTF_8));
      }
    }
    final var out = new ArrayList<byte[]>(observed.size());
    for (final var s : observed) out.add(s.getBytes(StandardCharsets.UTF_8));
    return out;
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
