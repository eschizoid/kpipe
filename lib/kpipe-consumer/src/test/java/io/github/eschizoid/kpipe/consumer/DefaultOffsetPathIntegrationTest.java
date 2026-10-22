package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/// At-least-once on the path users run: a consumer built without an explicit [OffsetManager],
/// with the consumer properties left at Kafka's defaults, which turn auto-commit on.
///
/// Consumer A works in PARALLEL mode and some of its records never finish. It shuts down while
/// they are still in flight. A's commit must stop at the first record it did not finish, and
/// consumer B, joining the same group afterwards, must receive every record A never finished.
/// Kafka's auto-commit would have committed the polled offsets past the unfinished records, so B
/// would start after them and they would be lost.
///
/// CI-RUN-REQUIRED: this is a Testcontainers test (needs a Docker daemon to start the Kafka
/// broker).
@Testcontainers
class DefaultOffsetPathIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");
  private static final int RECORD_COUNT = 200;
  /// Records at these offsets mod 10 never finish on consumer A. The first is offset 5, so the
  /// frontier A commits is 5, which a consumer starting from the beginning would not reach.
  private static final int STUCK_EVERY = 10;
  private static final int FIRST_STUCK = 5;

  @Container
  static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:%s".formatted(KAFKA_VERSION)));

  @Test
  void recordsStillInFlightAtShutdownAreRedeliveredToTheNextConsumer() throws Exception {
    final var topic = "default-path-" + System.nanoTime();
    final var groupId = "default-path-group-" + System.nanoTime();
    createTopic(topic);
    produceRecords(topic);

    final var stuck = IntStream.range(0, RECORD_COUNT)
      .filter(i -> i % STUCK_EVERY == FIRST_STUCK)
      .mapToObj(i -> "val-" + i)
      .collect(Collectors.toUnmodifiableSet());
    final var finishedByA = ConcurrentHashMap.<String>newKeySet();
    final var neverReleased = new CountDownLatch(1);

    final var consumerA = KPipeConsumer.builder()
      .withProperties(consumerProperties(groupId))
      .withTopic(topic)
      .withProcessingMode(ProcessingMode.PARALLEL)
      .withWaitForMessagesTimeout(Duration.ofMillis(200))
      // Longer than the dispatcher's own termination wait, so close() never interrupts the
      // consumer thread in the middle of its final commit.
      .withThreadTerminationTimeout(Duration.ofSeconds(30))
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          final var text = new String(value);
          if (stuck.contains(text)) {
            try {
              neverReleased.await();
            } catch (final InterruptedException e) {
              // The interrupt must stay in the cause chain: an interruption is not
              // a processing failure, so the record stays unmarked instead of being
              // logged and committed past.
              Thread.currentThread().interrupt();
              throw new IllegalStateException("interrupted at shutdown", e);
            }
          }
          finishedByA.add(text);
          return value;
        })
      )
      .build();

    consumerA.start();
    awaitSize(finishedByA, RECORD_COUNT - stuck.size(), Duration.ofSeconds(60));
    assertEquals(RECORD_COUNT - stuck.size(), finishedByA.size(), "A must finish every record that is not stuck");
    consumerA.close();

    try (final var admin = Admin.create(adminProperties())) {
      final var committed = admin
        .listConsumerGroupOffsets(groupId)
        .partitionsToOffsetAndMetadata()
        .get(15, TimeUnit.SECONDS)
        .get(new TopicPartition(topic, 0));
      assertNotNull(committed, "A must commit the frontier at shutdown");
      assertEquals(FIRST_STUCK, committed.offset(), "A's commit must stop at the first record it did not finish");
    }

    final var seenByB = ConcurrentHashMap.<String>newKeySet();
    final var consumerB = KPipeConsumer.builder()
      .withProperties(consumerProperties(groupId))
      .withTopic(topic)
      .withProcessingMode(ProcessingMode.PARALLEL)
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          seenByB.add(new String(value));
          return value;
        })
      )
      .build();

    try {
      consumerB.start();
      final var deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
      while (!seenByB.containsAll(stuck) && System.nanoTime() < deadline) {
        TimeUnit.MILLISECONDS.sleep(100);
      }
      final var missing = stuck
        .stream()
        .filter(v -> !seenByB.contains(v))
        .sorted()
        .toList();
      assertTrue(
        missing.isEmpty(),
        "records A never finished must be redelivered to B; missing " + missing.size() + ": " + missing
      );
    } finally {
      consumerB.close();
      neverReleased.countDown();
    }
  }

  private static void awaitSize(final Set<String> set, final int target, final Duration timeout)
    throws InterruptedException {
    final var deadline = System.nanoTime() + timeout.toNanos();
    while (set.size() < target && System.nanoTime() < deadline) {
      TimeUnit.MILLISECONDS.sleep(50);
    }
  }

  private static void createTopic(final String topic) throws Exception {
    try (final var admin = Admin.create(adminProperties())) {
      admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
    }
  }

  private static void produceRecords(final String topic) throws Exception {
    final var props = new Properties();
    props.put("bootstrap.servers", kafka.getBootstrapServers());
    props.put("acks", "all");
    try (final var producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
      final var sends = new ArrayList<Future<?>>();
      for (int i = 0; i < RECORD_COUNT; i++) {
        sends.add(producer.send(new ProducerRecord<>(topic, ("key-" + i).getBytes(), ("val-" + i).getBytes())));
      }
      producer.flush();
      for (final var send : sends) send.get(30, TimeUnit.SECONDS);
    }
  }

  private static Properties adminProperties() {
    final var props = new Properties();
    props.put("bootstrap.servers", kafka.getBootstrapServers());
    return props;
  }

  /// Only what a consumer needs to connect. `enable.auto.commit` is left unset, so Kafka's default
  /// (`true`) applies, with a short interval so an auto-commit would land well before shutdown.
  private static Properties consumerProperties(final String groupId) {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }
}
