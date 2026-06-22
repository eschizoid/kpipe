package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/// End-to-end backpressure hysteresis against a real broker, with a no-loss guarantee.
///
/// `WatermarkHysteresisTest` pins the pure threshold math, and
/// `KPipeBackpressureIntegrationTest` drives the full consumer loop against a `MockConsumer`.
/// Neither exercises a real broker: a MockConsumer's `pause()` is in-memory bookkeeping with
/// no fetcher behind it, so it cannot show that a paused consumer actually stops pulling bytes
/// off a live broker and then resumes and drains the backlog without dropping records.
///
/// This test fills that gap. A high inbound burst is produced to a real Kafka broker, the sink
/// is deliberately slow so the in-flight count climbs past the high watermark, and the consumer
/// is configured with tight custom watermarks in PARALLEL mode. The assertions are:
///
///   * **Pause fires.** The consumer pauses at least once — the backpressure pause counter
///     is positive, proving the in-flight count crossed the high watermark against real
///     fetch pressure.
///   * **Resume happens — no permanent stall.** The consumer is not left paused once the sink
///     drains; the backlog clears and the partition is unpaused.
///   * **No loss across the pause/resume cycle.** Every produced value is observed exactly
///     once by the sink (the consumer is the only one in its group, so at-least-once collapses
///     to exactly the produced set here). A record dropped while paused — or one never
///     re-fetched after resume — would make the observed set smaller than the produced set.
///
/// CI-RUN-REQUIRED: this is a Testcontainers test and needs a Docker daemon to start the Kafka
/// broker. It compiles locally but cannot run where Docker is unavailable; it runs in CI.
@Testcontainers(disabledWithoutDocker = true)
class KPipeRealBrokerBackpressureIntegrationTest {

  private static final System.Logger LOGGER = System.getLogger(
    KPipeRealBrokerBackpressureIntegrationTest.class.getName()
  );

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");

  private static final int PARTITIONS = 3;
  // A burst large enough that the slow sink keeps the in-flight count over the high watermark
  // long enough for the backpressure loop to observe it and pause, but small enough to drain
  // inside the test window.
  private static final int RECORD_COUNT = 600;

  // Tight watermarks so the burst trips the pause early. The hysteresis gap (high > low) is
  // what stops pause/resume thrashing.
  private static final long HIGH_WATERMARK = 50L;
  private static final long LOW_WATERMARK = 10L;

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION)).asCompatibleSubstituteFor("apache/kafka")
  ).withStartupAttempts(3);

  @Test
  void parallelBackpressurePausesAtHighWatermarkAndResumesWithoutLoss() throws Exception {
    final var topic = "bp-real-" + System.nanoTime();
    final var groupId = "bp-real-group-" + System.nanoTime();

    createTopic(topic);
    final var producedValues = produceRecords(topic);

    final var observed = ConcurrentHashMap.<String>newKeySet();
    final var duplicateObservations = ConcurrentHashMap.<String>newKeySet();

    final var consumer = KPipeConsumer.<byte[]>builder()
      .withProperties(consumerProperties(groupId))
      .withTopic(topic)
      .withProcessingMode(ProcessingMode.PARALLEL)
      // A slow sink so many records are simultaneously in-flight, driving the in-flight count
      // over the high watermark and forcing a real pause.
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          try {
            TimeUnit.MILLISECONDS.sleep(15);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          final var asString = new String(value);
          // Track duplicates so the no-loss check can tell coverage from re-delivery.
          if (!observed.add(asString)) {
            duplicateObservations.add(asString);
          }
          return value;
        })
      )
      .withBackpressure(HIGH_WATERMARK, LOW_WATERMARK)
      .build();

    final var thread = Thread.ofVirtual().name("bp-real-consumer").start(consumer::start);

    try {
      // Pause must fire: the in-flight count crossed the high watermark under real fetch load.
      awaitCondition(
        () -> consumer.getMetrics().get(KPipeConsumer.METRIC_BACKPRESSURE_PAUSE_COUNT) > 0,
        Duration.ofSeconds(30)
      );
      final var pauseCount = consumer.getMetrics().get(KPipeConsumer.METRIC_BACKPRESSURE_PAUSE_COUNT);
      assertTrue(
        pauseCount > 0,
        "Consumer must pause at least once under real-broker backpressure; pauses=" + pauseCount
      );

      // No loss: every produced value is eventually observed. If a record were dropped while
      // paused or never re-fetched after resume, observed would never reach the produced set.
      final var allObserved = awaitCondition(() -> observed.containsAll(producedValues), Duration.ofSeconds(90));
      assertTrue(
        allObserved,
        "Every produced record must be observed across the pause/resume cycle; saw %d of %d".formatted(
          observed.size(),
          producedValues.size()
        )
      );
      assertEquals(producedValues, observed, "Observed set must exactly cover the produced set (no loss)");

      // Resume must have happened: once the backlog drains the consumer is no longer paused.
      // The public `inFlight` gauge mirrors the count the backpressure controller reads.
      final var resumed = awaitCondition(() -> consumer.getMetrics().get("inFlight") == 0L, Duration.ofSeconds(30));
      assertTrue(resumed, "In-flight count must drain to zero after the burst is processed");

      LOGGER.log(
        System.Logger.Level.INFO,
        () ->
          "real-broker backpressure: pauses=" +
          pauseCount +
          " observed=" +
          observed.size() +
          " duplicates=" +
          duplicateObservations.size()
      );
    } finally {
      consumer.close();
      thread.join(5000);
    }
  }

  /// Polls a boolean condition until it holds or the deadline passes. Returns the final state.
  private boolean awaitCondition(final java.util.function.BooleanSupplier condition, final Duration timeout)
    throws InterruptedException {
    final var deadline = System.currentTimeMillis() + timeout.toMillis();
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
      TimeUnit.MILLISECONDS.sleep(50);
    }
    return condition.getAsBoolean();
  }

  private void createTopic(final String topic) throws Exception {
    try (final var admin = Admin.create(adminProperties())) {
      admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get(30, TimeUnit.SECONDS);
    }
  }

  /// Produces a burst of uniquely-valued records spread across all partitions. Returns the
  /// produced values so the sink can assert exact coverage.
  private Set<String> produceRecords(final String topic) throws Exception {
    final var values = ConcurrentHashMap.<String>newKeySet();
    try (
      final var producer = new KafkaProducer<>(
        producerProperties(),
        new ByteArraySerializer(),
        new ByteArraySerializer()
      )
    ) {
      final var sends = new ArrayList<Future<?>>();
      for (int i = 0; i < RECORD_COUNT; i++) {
        final var value = "val-" + i;
        values.add(value);
        final var key = ("key-" + (i % PARTITIONS)).getBytes();
        sends.add(producer.send(new ProducerRecord<>(topic, key, value.getBytes())));
      }
      producer.flush();
      for (final var send : sends) {
        send.get(30, TimeUnit.SECONDS);
      }
    }
    return Set.copyOf(values);
  }

  private Properties adminProperties() {
    final var props = new Properties();
    props.put("bootstrap.servers", kafka.getBootstrapServers());
    return props;
  }

  private Properties producerProperties() {
    final var props = new Properties();
    props.put("bootstrap.servers", kafka.getBootstrapServers());
    props.put("acks", "all");
    return props;
  }

  private Properties consumerProperties(final String groupId) {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // A small fetch cap so the burst arrives in many small polls rather than one giant batch.
    // This gives the in-flight count headroom to climb across watermark crossings instead of
    // jumping straight over both in a single poll.
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }
}
