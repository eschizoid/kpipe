package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.kpipe.sink.MessageSink;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/// Testcontainers-based end-to-end test for the [KPipe] fluent facade.
///
/// Spins up a real Kafka broker, produces JSON payloads, and verifies that the facade-built
/// pipeline consumes, transforms, and sinks them through the [Handle] surface — including
/// metrics, health, and graceful shutdown.
///
/// Disabled automatically when Docker is unavailable (e.g. CI without Docker).
@Testcontainers(disabledWithoutDocker = true)
class KPipeFacadeIntegrationTest {

  private static final Logger LOG = System.getLogger(KPipeFacadeIntegrationTest.class.getName());
  private static final String CONFLUENT_PLATFORM_VERSION = System.getProperty("confluentPlatformVersion", "8.2.0");

  @Container
  static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:%s".formatted(CONFLUENT_PLATFORM_VERSION))
  ).withStartupAttempts(3);

  @Test
  void endToEndJsonStreamConsumesAndShutsDown() throws Exception {
    final var topic = "kpipe-facade-test";
    final var captured = new CopyOnWriteArrayList<Map<String, Object>>();
    final MessageSink<Map<String, Object>> captureSink = captured::add;

    final var consumerProps = consumerProps("kpipe-facade-test-group-" + UUID.randomUUID());

    final var handle = KPipe.json(topic, consumerProps)
      .pipe(msg -> {
        msg.put("processed", true);
        return msg;
      })
      .toCustom(captureSink)
      .start();

    try {
      // Verify health before producing.
      assertTrue(handle.isHealthy(), "Handle should be healthy after start()");

      // Produce 5 JSON messages and wait for them to be consumed.
      produceUntilConsumed(topic, 5, captured, Duration.ofSeconds(20));

      assertEquals(5, captured.size(), "Should have captured exactly 5 messages");

      // Verify each captured message has been transformed by the operator.
      for (final var msg : captured) {
        assertEquals(Boolean.TRUE, msg.get("processed"), "Each message must carry processed=true");
        assertNotNull(msg.get("id"), "Each message must retain its id field");
        assertNotNull(msg.get("value"), "Each message must retain its value field");
      }

      // Verify metrics reflect at least the 5 messages we produced.
      final var metrics = handle.metrics();
      assertNotNull(metrics, "metrics() must not be null");
      final var processed = metrics.getOrDefault("messagesProcessed", 0L);
      assertTrue(processed >= 5L, "messagesProcessed (%d) must be >= 5".formatted(processed));

      // Still healthy mid-flight.
      assertTrue(handle.isHealthy(), "Handle should remain healthy while running");

      // Graceful shutdown.
      final var clean = handle.shutdownGracefully(Duration.ofSeconds(5));
      assertTrue(clean, "shutdownGracefully should complete within timeout");

      // Post-shutdown the runner should no longer report healthy.
      assertFalse(handle.isHealthy(), "Handle should not be healthy after shutdown");
    } finally {
      // Idempotent close on failure paths — runner.shutdownGracefully is safe to call twice.
      handle.shutdownGracefully(Duration.ofSeconds(2));
    }
  }

  @Test
  void endToEndShortCircuitFilter() throws Exception {
    final var topic = "kpipe-facade-filter-test";
    final var captured = new CopyOnWriteArrayList<Map<String, Object>>();
    final MessageSink<Map<String, Object>> captureSink = captured::add;

    final var consumerProps = consumerProps("kpipe-facade-filter-group-" + UUID.randomUUID());

    final var handle = KPipe.json(topic, consumerProps)
      // Drop messages where id is even — only odd ids reach the sink.
      .filter(msg -> ((Number) msg.get("id")).intValue() % 2 == 1)
      .toCustom(captureSink)
      .start();

    try {
      assertTrue(handle.isHealthy(), "Handle should be healthy after start()");

      // Produce messages with ids 1..6; expect only 1, 3, 5 to make it through.
      produceFixedAndWait(topic, 6, captured, 3, Duration.ofSeconds(20));

      assertAll(
        "Filtered captures",
        () -> assertEquals(3, captured.size(), "Should have captured exactly 3 odd-id messages"),
        () -> captured.forEach(m -> assertTrue(((Number) m.get("id")).intValue() % 2 == 1, "Captured id must be odd"))
      );
    } finally {
      handle.shutdownGracefully(Duration.ofSeconds(5));
    }
  }

  @Test
  void endToEndMultiTopicJsonStream() throws Exception {
    final var topics = Set.of(
      "kpipe-facade-multi-a-" + UUID.randomUUID().toString().substring(0, 8),
      "kpipe-facade-multi-b-" + UUID.randomUUID().toString().substring(0, 8),
      "kpipe-facade-multi-c-" + UUID.randomUUID().toString().substring(0, 8)
    );
    final var captured = new CopyOnWriteArrayList<Map<String, Object>>();
    final MessageSink<Map<String, Object>> captureSink = captured::add;

    final var consumerProps = consumerProps("kpipe-facade-multi-group-" + UUID.randomUUID());

    final var handle = KPipe.json(topics, consumerProps)
      .pipe(msg -> {
        msg.put("processed", true);
        return msg;
      })
      .toCustom(captureSink)
      .start();

    try {
      assertTrue(handle.isHealthy(), "Handle should be healthy after start()");

      // Produce 3 messages per topic — 9 total.
      final var perTopic = 3;
      final var expected = perTopic * topics.size();
      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
        for (final var topic : topics) {
          for (int i = 1; i <= perTopic; i++) {
            final var json = """
              {"id":%d,"topic":"%s","value":"v%d"}""".formatted(i, topic, i)
              .getBytes(StandardCharsets.UTF_8);
            producer.send(new ProducerRecord<>(topic, json)).get();
          }
        }
        producer.flush();
      }

      waitFor(() -> captured.size() >= expected, Duration.ofSeconds(20), "expected %d messages".formatted(expected));

      assertEquals(expected, captured.size(), "Should have captured one message per topic per id");

      // Every captured message must come from one of the subscribed topics and be transformed.
      for (final var msg : captured) {
        assertEquals(Boolean.TRUE, msg.get("processed"), "Each message must carry processed=true");
        assertTrue(topics.contains(msg.get("topic")), "Captured topic must be one of the subscribed topics");
      }

      // Verify each topic contributed at least one message (otherwise we accidentally only consumed
      // one).
      for (final var topic : topics) {
        final var fromTopic = captured
          .stream()
          .filter(m -> topic.equals(m.get("topic")))
          .count();
        assertEquals(perTopic, fromTopic, "Each topic should contribute %d messages".formatted(perTopic));
      }
    } finally {
      handle.shutdownGracefully(Duration.ofSeconds(5));
    }
  }

  @Test
  void endToEndHeterogeneousMulti() throws Exception {
    final var jsonTopic = "kpipe-multi-json-" + UUID.randomUUID().toString().substring(0, 8);
    final var bytesTopic = "kpipe-multi-bytes-" + UUID.randomUUID().toString().substring(0, 8);

    final var jsonCaptured = new CopyOnWriteArrayList<Map<String, Object>>();
    final var bytesCaptured = new CopyOnWriteArrayList<byte[]>();

    final var consumerProps = consumerProps("kpipe-multi-group-" + UUID.randomUUID());

    final var handle = KPipe.multi(consumerProps)
      .json(jsonTopic, s ->
        s
          .pipe(msg -> {
            msg.put("processed", true);
            return msg;
          })
          .toCustom(jsonCaptured::add)
      )
      .bytes(bytesTopic, s -> s.toCustom(bytesCaptured::add))
      .start();

    try {
      assertTrue(handle.isHealthy(), "Handle should be healthy after start()");

      final var perTopic = 3;
      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
        for (int i = 1; i <= perTopic; i++) {
          final var json = """
            {"id":%d,"value":"v%d"}""".formatted(i, i)
            .getBytes(StandardCharsets.UTF_8);
          producer.send(new ProducerRecord<>(jsonTopic, json)).get();
          producer
            .send(new ProducerRecord<>(bytesTopic, ("raw-%d".formatted(i)).getBytes(StandardCharsets.UTF_8)))
            .get();
        }
        producer.flush();
      }

      waitFor(
        () -> jsonCaptured.size() >= perTopic && bytesCaptured.size() >= perTopic,
        Duration.ofSeconds(20),
        "expected %d JSON + %d byte messages".formatted(perTopic, perTopic)
      );

      assertEquals(perTopic, jsonCaptured.size(), "JSON sink should have captured exactly %d".formatted(perTopic));
      assertEquals(perTopic, bytesCaptured.size(), "bytes sink should have captured exactly %d".formatted(perTopic));

      // JSON pipeline transformed each message.
      for (final var msg : jsonCaptured) {
        assertEquals(Boolean.TRUE, msg.get("processed"), "JSON message must carry processed=true");
        assertNotNull(msg.get("id"));
      }

      // Byte sink received only the bytes-topic raw payloads.
      for (final var b : bytesCaptured) {
        final var s = new String(b, StandardCharsets.UTF_8);
        assertTrue(s.startsWith("raw-"), "Byte payload should start with 'raw-' but was: " + s);
      }
    } finally {
      handle.shutdownGracefully(Duration.ofSeconds(5));
    }
  }

  /// Builds default consumer properties pointing at the Testcontainers Kafka instance with a
  /// unique group id so each test starts at the earliest offset.
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

  /// Produces simple JSON payloads of the form `{"id":N,"value":"vN"}` and polls until at least
  /// `expected` messages have been observed by the sink, or the timeout elapses.
  private static void produceUntilConsumed(
    final String topic,
    final int expected,
    final List<Map<String, Object>> sink,
    final Duration timeout
  ) throws Exception {
    final var producerProps = producerProps();
    try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
      for (int i = 1; i <= expected; i++) {
        final var json = """
          {"id":%d,"value":"v%d"}""".formatted(i, i)
          .getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(topic, json)).get();
      }
      producer.flush();
    }
    waitFor(() -> sink.size() >= expected, timeout, "Timed out waiting for %d messages".formatted(expected));
  }

  /// Produces `count` JSON payloads with sequential ids 1..count and waits for `expected`
  /// messages to land in the sink (used by the filter test to confirm only odd-id messages pass).
  private static void produceFixedAndWait(
    final String topic,
    final int count,
    final List<Map<String, Object>> sink,
    final int expected,
    final Duration timeout
  ) throws Exception {
    final var producerProps = producerProps();
    try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
      for (int i = 1; i <= count; i++) {
        final var json = """
          {"id":%d,"value":"v%d"}""".formatted(i, i)
          .getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(topic, json)).get();
      }
      producer.flush();
    }
    // Wait until the sink stops growing OR we hit `expected`. Then give a small grace period to
    // ensure any extra (unwanted) messages would have arrived too.
    waitFor(() -> sink.size() >= expected, timeout, "Timed out waiting for %d filtered messages".formatted(expected));
    // Grace period: ensure no extra messages slip through after expected reached.
    TimeUnit.MILLISECONDS.sleep(500);
  }

  private static Properties producerProps() {
    final var producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return producerProps;
  }

  private static void waitFor(final BooleanCondition cond, final Duration timeout, final String failureMessage)
    throws InterruptedException {
    final var deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (cond.test()) return;
      TimeUnit.MILLISECONDS.sleep(200);
    }
    if (!cond.test()) {
      LOG.log(Level.ERROR, failureMessage);
      throw new AssertionError(failureMessage);
    }
  }

  @FunctionalInterface
  private interface BooleanCondition {
    boolean test();
  }
}
