package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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

/// Crash-restart reprocessing integration test — end-to-end at-least-once across a hard
/// consumer crash against a real broker.
///
/// This is the sibling of `ChaosRebalanceIntegrationTest`. That test proves the no-loss /
/// no-commit-ahead contract holds when a partition is handed off via a live rebalance (both
/// members stay up). This test proves the same contract holds across a *hard restart*: a fresh
/// consumer instance in the same group resumes from the committed offset and re-delivers the
/// uncommitted tail that the crashed instance had already processed but never committed. It is
/// the premise the whole at-least-once claim rests on — an uncommitted offset is reprocessed
/// after a crash, not lost.
///
/// What this test ASSERTS:
///
///   * **No loss — the primary guarantee.** Every produced record is observed by the sink at
///     least once across A ∪ B (exact set coverage of the union). The crash must never let the
///     committed offset advance past a record that A tracked but never marked terminal, so on
///     restart B re-fetches the unprocessed (and processed-but-uncommitted) tail rather than
///     silently skipping it.
///   * **Re-delivery of the uncommitted tail.** B must observe at least one record that A had
///     already observed — i.e. A processed past its last commit point and B reprocessed that
///     tail. Without this, the test would pass trivially even if A had drained everything and
///     committed it (which is not the crash scenario under test).
///   * **No commit-ahead — a sanity bound.** The final committed offset for each partition never
///     exceeds the log end. This is `<=`, NOT `==`: at-least-once permits a processed-but-not-yet-
///     committed tail (reprocessed on restart), so requiring the commit to reach exactly the log
///     end would assert a complete-drain / exactly-once property kpipe does not claim.
///
/// Crash simulation: consumer A runs in PARALLEL mode with a manual-commit `KafkaOffsetManager`
/// on a short (1s) commit interval, so it commits a prefix while running. A deliberate small
/// per-record delay keeps processing slower than the fetch, guaranteeing a processed-but-not-yet-
/// committed tail exists at crash time. The crash is then induced WITHOUT a graceful drain: the
/// offset manager is stopped (halting any further commit and turning `markOffsetProcessed` into a
/// no-op so no final commit fires) and A's consumer thread is interrupted and abandoned —
/// `shutdownGracefully` / `close` are never called on A. This mirrors a hard process kill where
/// the JVM dies between a periodic commit and the next one, leaving an uncommitted processed tail.
///
/// CI-RUN-REQUIRED: this is a Testcontainers test (needs a Docker daemon to start the Kafka
/// broker). It compiles locally but cannot run where Docker is unavailable; it runs in CI.
@Testcontainers
class CrashRestartReprocessingIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");

  private static final int PARTITIONS = 4;
  private static final int RECORD_COUNT = 400;

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION)).asCompatibleSubstituteFor("apache/kafka")
  );

  @Test
  void atLeastOnceSurvivesHardCrashRestart() throws Exception {
    final var topic = "crash-input-" + System.nanoTime();
    final var groupId = "crash-group-" + System.nanoTime();

    createTopic(topic);
    final var producedValues = produceRecords(topic);

    // Per-consumer observed sets. A record may be observed by both A and B (at-least-once allows
    // duplicates after a crash replays an uncommitted tail). The no-loss assertion checks the
    // union; the re-delivery assertion checks the intersection is non-empty.
    final var observedA = ConcurrentHashMap.<String>newKeySet();
    final var observedB = ConcurrentHashMap.<String>newKeySet();
    final var observedTotalA = new AtomicInteger(0);

    // Hold A's offset manager so the crash can stop it (halt commits) without a graceful close.
    final var offsetManagerA = new AtomicReference<KafkaOffsetManager<byte[]>>();

    final var consumerA = buildConsumerWithManagedOffsets(topic, groupId, observedA, observedTotalA, offsetManagerA);
    final var threadA = Thread.ofVirtual().name("crash-consumer-A").start(consumerA::start);

    // Let A claim partitions, process a slice, and commit at least one prefix (the 1s interval
    // fires while A is still draining the slow stream). We wait until A has both committed
    // something AND processed past that commit point, so a genuine uncommitted tail exists.
    final var committedBeforeCrash = awaitCommittedPrefixWithProcessedTail(groupId, observedA, Duration.ofSeconds(40));
    assertFalse(
      committedBeforeCrash.isEmpty(),
      "Consumer A must commit a prefix before the crash; saw no committed offsets."
    );

    final var observedByABeforeCrash = Set.copyOf(observedA);

    // CRASH: stop the offset manager (no further commits, marks become no-ops) and abandon the
    // consumer thread without a graceful drain. No close() / shutdownGracefully() on A.
    crash(offsetManagerA.get(), threadA);

    // The committed offset is frozen at the last periodic commit — the tail A processed after it
    // is uncommitted and must be re-delivered to B.
    final var committedAtCrash = committedOffsets(groupId);

    // Consumer B: fresh instance, SAME group, resumes from the committed offset.
    final var observedTotalB = new AtomicInteger(0);
    final var offsetManagerB = new AtomicReference<KafkaOffsetManager<byte[]>>();
    final var consumerB = buildConsumerWithManagedOffsets(topic, groupId, observedB, observedTotalB, offsetManagerB);
    final var threadB = Thread.ofVirtual().name("crash-consumer-B").start(consumerB::start);

    try {
      // No loss: A ∪ B observes every produced value at least once.
      final var union = ConcurrentHashMap.<String>newKeySet();
      final var allObserved = awaitUnionAtLeast(
        observedA,
        observedB,
        union,
        producedValues.size(),
        Duration.ofSeconds(90)
      );
      assertTrue(
        allObserved,
        "Every produced record must be observed at least once across A and B; saw %d of %d (A=%d B=%d)".formatted(
          union.size(),
          producedValues.size(),
          observedA.size(),
          observedB.size()
        )
      );
      assertEquals(
        producedValues,
        union,
        "Union of A and B observations must exactly cover the produced set (no loss)."
      );

      // Re-delivery of the uncommitted tail: B must reprocess at least one record A already saw.
      // Records A observed before the crash but never committed are re-fetched by B from the
      // frozen commit point. An empty intersection would mean either A committed everything it
      // processed (not a crash scenario) or B skipped the tail (loss) — both are failures here.
      final var redelivered = new HashSet<>(observedByABeforeCrash);
      redelivered.retainAll(observedB);
      assertFalse(
        redelivered.isEmpty(),
        "B must re-deliver at least one record A processed but never committed (the uncommitted tail); A observed %d before crash, B observed %d, overlap was empty.".formatted(
          observedByABeforeCrash.size(),
          observedB.size()
        )
      );
    } finally {
      consumerB.close();
      threadB.join(5000);
    }

    // No commit-ahead: the final committed offset for each partition never exceeds the log end.
    // It must be `<=`, NOT `==`. At-least-once does not promise the commit reaches log-end after a
    // crash + bounded restart drain: a processed-but-not-yet-committed tail is legitimate and is
    // simply reprocessed — the definition of at-least-once tolerance. Asserting `== logEnd` would
    // test an exactly-once / complete-drain property kpipe deliberately does not claim. The
    // at-least-once contract is no-loss (above) + no-commit-ahead (here).
    final var endOffsets = endOffsets(topic);
    final var committedFinal = committedOffsets(groupId);

    for (final var entry : endOffsets.entrySet()) {
      final var tp = entry.getKey();
      final var logEnd = entry.getValue();
      final var committedMeta = committedFinal.get(tp);
      assertNotNull(committedMeta, "Partition %s must have a committed offset after restart drain.".formatted(tp));
      assertTrue(
        committedMeta.offset() <= logEnd,
        "Committed offset for %s must not exceed the log end (no commit-ahead). committed=%d logEnd=%d".formatted(
          tp,
          committedMeta.offset(),
          logEnd
        )
      );
      // Sanity: the crash-time commit point must also obey the bound (commit never raced ahead).
      final var atCrash = committedAtCrash.get(tp);
      if (atCrash != null) {
        assertTrue(
          atCrash.offset() <= logEnd,
          "Crash-time committed offset for %s must not exceed the log end. committed=%d logEnd=%d".formatted(
            tp,
            atCrash.offset(),
            logEnd
          )
        );
      }
    }
  }

  /// Stops the offset manager (no further commits; `markOffsetProcessed` becomes a no-op) and
  /// abandons the consumer thread without a graceful drain. This is the in-process stand-in for a
  /// hard process kill: the committed offset is frozen at the last periodic commit and the
  /// processed tail beyond it stays uncommitted.
  private void crash(final KafkaOffsetManager<byte[]> offsetManager, final Thread consumerThread) {
    if (offsetManager != null) offsetManager.stop();
    consumerThread.interrupt();
    // Deliberately NOT calling consumerA.close() / shutdownGracefully(): the thread is abandoned.
  }

  private void createTopic(final String topic) throws Exception {
    try (final var admin = Admin.create(adminProperties())) {
      admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get(30, TimeUnit.SECONDS);
    }
  }

  /// Produces [#RECORD_COUNT] keyed records spread across all partitions. Keys are
  /// `key-<i % PARTITIONS>` so the default partitioner fans them out, and values are unique
  /// (`val-<i>`) so the sink can assert exact set coverage. Returns the set of produced values.
  private Set<String> produceRecords(final String topic) throws Exception {
    final var values = ConcurrentHashMap.<String>newKeySet();
    final var props = producerProperties();
    try (final var producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
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

  /// Builds a PARALLEL consumer with a manual-commit `KafkaOffsetManager` on a 1s commit interval,
  /// captured into `offsetManagerRef` so the test can stop it to simulate a crash. A small
  /// per-record delay keeps processing slower than the fetch so a processed-but-uncommitted tail
  /// reliably exists between commit ticks.
  private KPipeConsumer<byte[]> buildConsumerWithManagedOffsets(
    final String topic,
    final String groupId,
    final Set<String> observed,
    final AtomicInteger observedTotal,
    final AtomicReference<KafkaOffsetManager<byte[]>> offsetManagerRef
  ) {
    final var builder = KPipeConsumer.<byte[]>builder()
      .withProperties(consumerProperties(groupId))
      .withTopic(topic)
      .withProcessingMode(ProcessingMode.PARALLEL)
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          try {
            // Slow the sink so processing trails the fetch, guaranteeing an uncommitted
            // tail.
            TimeUnit.MILLISECONDS.sleep(5);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          observed.add(new String(value));
          observedTotal.incrementAndGet();
          return value;
        })
      );
    builder.withOffsetManagerProvider(consumer -> {
      final var manager = KafkaOffsetManager.<byte[]>builder(consumer)
        .withCommandQueue(builder.getCommandQueue())
        .withCommitInterval(Duration.ofSeconds(1))
        .build();
      offsetManagerRef.set(manager);
      return manager;
    });
    return builder.build();
  }

  /// Polls until consumer A has committed at least one offset AND has observed more records than
  /// the committed prefix accounts for (a genuine uncommitted tail). Returns the committed map
  /// once both hold, or whatever was committed at the deadline.
  private Map<TopicPartition, OffsetAndMetadata> awaitCommittedPrefixWithProcessedTail(
    final String groupId,
    final Set<String> observed,
    final Duration timeout
  ) throws Exception {
    final var deadline = System.currentTimeMillis() + timeout.toMillis();
    var committed = Map.<TopicPartition, OffsetAndMetadata>of();
    while (System.currentTimeMillis() < deadline) {
      committed = committedOffsets(groupId);
      var committedSum = 0L;
      for (final var meta : committed.values()) {
        committedSum += meta.offset();
      }
      // A committed a prefix and has already observed strictly more than that prefix → tail exists.
      if (committedSum > 0 && observed.size() > committedSum) {
        return committed;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    return committed;
  }

  /// Polls until the union of A's and B's observed sets reaches `target` distinct values or the
  /// deadline passes. `union` is filled with the live union for the assertion message.
  /// Returns true if the target was reached.
  private boolean awaitUnionAtLeast(
    final Set<String> observedA,
    final Set<String> observedB,
    final Set<String> union,
    final int target,
    final Duration timeout
  ) throws InterruptedException {
    final var deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      union.clear();
      union.addAll(observedA);
      union.addAll(observedB);
      if (union.size() >= target) {
        return true;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    union.clear();
    union.addAll(observedA);
    union.addAll(observedB);
    return union.size() >= target;
  }

  private Map<TopicPartition, Long> endOffsets(final String topic) {
    try (
      final var consumer = new KafkaConsumer<>(
        consumerProperties("end-offset-probe-" + System.nanoTime()),
        new ByteArrayDeserializer(),
        new ByteArrayDeserializer()
      )
    ) {
      final var partitions = new ArrayList<TopicPartition>();
      for (int p = 0; p < PARTITIONS; p++) {
        partitions.add(new TopicPartition(topic, p));
      }
      return consumer.endOffsets(partitions, Duration.ofSeconds(15));
    }
  }

  private Map<TopicPartition, OffsetAndMetadata> committedOffsets(final String groupId) throws Exception {
    try (final var admin = Admin.create(adminProperties())) {
      return admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get(15, TimeUnit.SECONDS);
    }
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
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }
}
