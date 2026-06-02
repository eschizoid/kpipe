package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.format.json.JsonFormat;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.BatchResult;
import org.kpipe.sink.BatchSink;
import org.kpipe.sink.MessageSink;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/// Verifies [MultiBuilder] supports a mix of non-batch + batch + partial-batch routes on one
/// consumer. The shape: produce known records to three topics, each routed differently, and
/// confirm exact-match dispatch with no cross-contamination.
@Testcontainers(disabledWithoutDocker = true)
class MultiBuilderBatchIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.2.0");

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION)).asCompatibleSubstituteFor("apache/kafka")
  ).withStartupAttempts(3);

  @Test
  void multiBuilderRoutesMixedNonBatchAndBatchTopicsThroughOneConsumer() throws Exception {
    final var nonBatchTopic = "kpipe-mb-nonbatch-" + UUID.randomUUID().toString().substring(0, 8);
    final var batchTopic = "kpipe-mb-batch-" + UUID.randomUUID().toString().substring(0, 8);
    final var bytesBatchTopic = "kpipe-mb-bytesbatch-" + UUID.randomUUID().toString().substring(0, 8);

    final var nonBatchCaptured = new CopyOnWriteArrayList<Map<String, Object>>();
    final var batchCaptured = new CopyOnWriteArrayList<Map<String, Object>>();
    final var bytesBatchCaptured = new CopyOnWriteArrayList<byte[]>();

    final BatchSink<Map<String, Object>> jsonBatchSink = BatchSink.ofVoid(batchCaptured::addAll);
    final BatchSink<byte[]> bytesBatchSink = BatchSink.ofVoid(bytesBatchCaptured::addAll);
    final MessageSink<Map<String, Object>> nonBatchSink = nonBatchCaptured::add;

    final var policy = new BatchPolicy(5, Duration.ofSeconds(2));

    final var handle = KPipe.multi(consumerProps("kpipe-mb-group-" + UUID.randomUUID()))
      .json(nonBatchTopic, s -> s.toCustom(nonBatchSink))
      .json(batchTopic, s -> s.toBatch(jsonBatchSink, policy))
      .bytes(bytesBatchTopic, s -> s.toBatch(bytesBatchSink, policy))
      .start();

    try {
      assertTrue(handle.isHealthy());

      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
        // Produce 6 to nonBatch, 12 to batch (two size-triggered flushes plus a 2-record drain),
        // 7 to bytesBatch (one size-triggered flush plus a 2-record drain).
        for (int i = 1; i <= 6; i++) producer
          .send(
            new ProducerRecord<>(
              nonBatchTopic,
              (
                """
                {"id":%d,"src":"nonbatch"}"""
              ).formatted(i)
                .getBytes(StandardCharsets.UTF_8)
            )
          )
          .get();
        for (int i = 1; i <= 12; i++) producer
          .send(
            new ProducerRecord<>(
              batchTopic,
              (
                """
                {"id":%d,"src":"batch"}"""
              ).formatted(i)
                .getBytes(StandardCharsets.UTF_8)
            )
          )
          .get();
        for (int i = 1; i <= 7; i++) producer
          .send(new ProducerRecord<>(bytesBatchTopic, ("raw-%d".formatted(i)).getBytes(StandardCharsets.UTF_8)))
          .get();
        producer.flush();
      }

      // Wait for the consumer to acknowledge it has polled all 25 records before shutting down,
      // matching the deterministic-shutdown pattern from StreamBatchIntegrationTest.
      waitFor(
        () -> handle.metrics().getOrDefault("messagesReceived", 0L) >= 25L,
        Duration.ofSeconds(20),
        "expected consumer to receive all 25 records"
      );

      assertTrue(handle.shutdownGracefully(Duration.ofSeconds(10)), "shutdownGracefully should drain all routes");

      // Exact-match dispatch — no cross-contamination.
      assertEquals(6, nonBatchCaptured.size(), "nonBatch sink saw the right count");
      assertEquals(12, batchCaptured.size(), "JSON batch sink saw the right count");
      assertEquals(7, bytesBatchCaptured.size(), "bytes batch sink saw the right count");

      assertTrue(nonBatchCaptured.stream().allMatch(m -> "nonbatch".equals(m.get("src"))));
      assertTrue(batchCaptured.stream().allMatch(m -> "batch".equals(m.get("src"))));
      assertTrue(bytesBatchCaptured.stream().allMatch(b -> new String(b, StandardCharsets.UTF_8).startsWith("raw-")));
    } finally {
      handle.shutdownGracefully(Duration.ofSeconds(2));
    }
  }

  /// Disjoint-topic invariant: a topic cannot be both a regular pipeline target and a batch
  /// route. The builder must reject the combo at `build()` rather than dispatching ambiguously.
  @Test
  void builderRejectsTopicConfiguredAsBothRegularAndBatch() {
    final var registry = new MessageProcessorRegistry();
    final var pipeline = registry.pipeline(JsonFormat.INSTANCE).build();
    final BatchSink<Map<String, Object>> sink = BatchSink.ofVoid(b -> {});

    final var ex = assertThrows(IllegalArgumentException.class, () ->
      KPipeConsumer.<byte[]>builder()
        .withProperties(consumerProps("test-disjoint"))
        .withTopic("dup-topic")
        .withPipeline(pipeline)
        .withBatchPipeline("dup-topic", pipeline, sink, BatchPolicy.ofSize(10))
        .build()
    );
    assertTrue(
      ex.getMessage().contains("'dup-topic'") && ex.getMessage().contains("both"),
      "error message should name the duplicated topic and the conflict; got: " + ex.getMessage()
    );
  }

  /// Two batch routes (one whole-batch via `ofVoid`, one returning per-record results) coexist
  /// on different topics.
  @Test
  void builderAcceptsTwoBatchRoutesOnDifferentTopics() {
    final var registry = new MessageProcessorRegistry();
    final var pipeline = registry.pipeline(JsonFormat.INSTANCE).build();
    final BatchSink<Map<String, Object>> wholeBatch = BatchSink.ofVoid(b -> {});
    final BatchSink<Map<String, Object>> perRecordBatch = b -> BatchResult.allSucceeded(b.size());

    // No exception: two batch routes on different topics build cleanly.
    KPipeConsumer.<byte[]>builder()
      .withProperties(consumerProps("test-mixed-batch"))
      .withBatchPipeline("topic-whole", pipeline, wholeBatch, BatchPolicy.ofSize(10))
      .withBatchPipeline("topic-per-record", pipeline, perRecordBatch, BatchPolicy.ofSize(10))
      .build();
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
