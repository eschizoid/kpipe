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
import java.util.stream.Collectors;
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

/// Crash-restart reprocessing integration test — end-to-end at-least-once across a consumer
/// crash against a real broker.
///
/// This is the sibling of `ChaosRebalanceIntegrationTest`. That test proves the no-loss /
/// no-commit-ahead contract holds when a partition is handed off via a live rebalance (both
/// members stay up). This test proves the same contract holds across a crash and restart: a fresh
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
///   * **Re-delivery of the uncommitted tail.** Every record A processed past the committed prefix
///     before the crash was never committed, so B must process it again.
///   * **No commit-ahead — a sanity bound.** The final committed offset for each partition never
///     exceeds the log end. This is `<=`, NOT `==`: at-least-once permits a processed-but-not-yet-
///     committed tail (reprocessed on restart), so requiring the commit to reach exactly the log
///     end would assert a complete-drain / exactly-once property kpipe does not claim.
///
/// Crash simulation: consumer A runs in SEQUENTIAL mode with a manual-commit `KafkaOffsetManager`
/// on a deliberately long commit interval, so the ONLY commit is a single manual one the test
/// drives after A has processed a small prefix. Everything A processes after that commit therefore
/// stays uncommitted — a deterministic processed-but-uncommitted tail, with no dependence on
/// commit timing. The crash stops the offset manager, which turns its close, its rebalance
/// listener and `markOffsetProcessed` into no-ops, and then closes A. A leaves the group without
/// committing anything past the prefix, as a killed process would.
///
/// CI-RUN-REQUIRED: this is a Testcontainers test (needs a Docker daemon to start the Kafka
/// broker). It compiles locally but cannot run where Docker is unavailable; it runs in CI.
@Testcontainers
class CrashRestartReprocessingIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");

  // A single partition makes the A→B handoff deterministic: there is no rebalance-split ambiguity
  // about which member ends up reading the partition that holds the uncommitted tail. B inherits
  // the one partition and resumes from the manually-committed prefix, so the re-delivery overlap is
  // deterministic. No-loss/no-commit-ahead don't depend on the partition count; the
  // rebalance-handoff angle is covered by ChaosRebalanceIntegrationTest.
  private static final int PARTITIONS = 1;
  // Enough records that sequential processing (5ms/record) is still mid-stream when we crash A,
  // so a genuine processed-but-uncommitted tail exists rather than A having drained everything.
  private static final int RECORD_COUNT = 1000;

  /// Records A processes before the single manual prefix commit. Kept small so the bulk of the
  /// stream lands in the uncommitted tail.
  private static final int COMMIT_PREFIX = 50;

  /// Records A must process BEYOND the committed prefix, forming the guaranteed uncommitted tail
  /// that B re-delivers. Large enough to be robust, small enough to build quickly.
  private static final int TAIL_MARGIN = 50;

  @Container
  static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:%s".formatted(KAFKA_VERSION)));

  @Test
  void atLeastOnceSurvivesCrashRestart() throws Exception {
    final var topic = "crash-input-" + System.nanoTime();
    final var groupId = "crash-group-" + System.nanoTime();

    createTopic(topic);
    final var producedValues = produceRecords(topic);

    // Per-consumer observed sets. A record may be observed by both A and B (at-least-once allows
    // duplicates after a crash replays an uncommitted tail). No-loss checks the union; re-delivery
    // checks the intersection — both hard assertions below.
    final var observedA = ConcurrentHashMap.<String>newKeySet();
    final var observedB = ConcurrentHashMap.<String>newKeySet();
    final var observedTotalA = new AtomicInteger(0);

    // Hold A's offset manager so the test can commit a prefix manually and stop it at crash time.
    final var offsetManagerA = new AtomicReference<KafkaOffsetManager>();

    final var consumerA = buildConsumerWithManagedOffsets(topic, groupId, observedA, observedTotalA, offsetManagerA);
    consumerA.start();

    // Deterministically build a processed-but-uncommitted tail. A's manager has a very long commit
    // interval, so the ONLY commit is the single manual one here — everything A processes
    // afterward stays uncommitted.
    awaitObservedGrowth(observedA, COMMIT_PREFIX, Duration.ofSeconds(40));
    assertTrue(
      offsetManagerA.get().commitSyncAndWait(Duration.ofSeconds(10)),
      "Consumer A's manual prefix commit must succeed before the crash."
    );
    final var committedAtCrash = committedOffsets(groupId);
    final var committedSum = committedAtCrash.values().stream().mapToLong(OffsetAndMetadata::offset).sum();
    assertTrue(committedSum > 0, "A must have committed a prefix before the crash; committed nothing.");

    // A keeps processing past the committed prefix with no further commit firing — that growth is
    // the guaranteed uncommitted tail B must re-deliver. Assert it explicitly so a failure to build
    // the tail surfaces here with a clear message, not later as a confusing empty-overlap failure.
    final var tailTarget = (int) committedSum + TAIL_MARGIN;
    awaitObservedGrowth(observedA, tailTarget, Duration.ofSeconds(40));
    assertTrue(
      observedA.size() >= tailTarget,
      "A must process at least %d records past the committed prefix (%d) to build the uncommitted tail; saw %d".formatted(
        TAIL_MARGIN,
        (int) committedSum,
        observedA.size()
      )
    );
    final var observedByABeforeCrash = Set.copyOf(observedA);

    // CRASH: stop the manager, then close A. A stopped manager commits nothing more — its
    // close(), its rebalance listener and markOffsetProcessed all become no-ops — so closing A
    // leaves the group without any commit past the manual prefix, which is what a killed process
    // leaves behind. B then inherits the partition and resumes from that prefix.
    offsetManagerA.get().stop();
    consumerA.close();

    // Consumer B: fresh instance, SAME group, resumes from the committed offset.
    final var observedTotalB = new AtomicInteger(0);
    final var offsetManagerB = new AtomicReference<KafkaOffsetManager>();
    final var consumerB = buildConsumerWithManagedOffsets(topic, groupId, observedB, observedTotalB, offsetManagerB);
    consumerB.start();

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

      // Re-delivery of the uncommitted tail: every record A processed past the committed prefix
      // before the crash was never committed, so B must process it again.
      final var committedPrefix = committedAtCrash.values().stream().mapToLong(OffsetAndMetadata::offset).sum();
      final var uncommittedTail = observedByABeforeCrash
        .stream()
        .filter(value -> Long.parseLong(value.substring("val-".length())) >= committedPrefix)
        .collect(Collectors.toSet());
      assertFalse(uncommittedTail.isEmpty(), "A must have processed records past the committed prefix");
      final var notRedelivered = new HashSet<>(uncommittedTail);
      notRedelivered.removeAll(observedB);
      assertTrue(
        notRedelivered.isEmpty(),
        "B must re-deliver the %d records A processed past the committed prefix; missing %d".formatted(
          uncommittedTail.size(),
          notRedelivered.size()
        )
      );
    } finally {
      consumerB.close();
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
      // A missing commit is an acceptable uncommitted tail (reprocessed on a later
      // restart); no-loss above already covers it. Only bound-check partitions that
      // actually committed.
      if (committedMeta == null) {
        continue;
      }
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

  /// Polls until `observed` reaches `target` distinct values or the deadline passes. Used to reach
  /// the prefix before the manual commit and to confirm A processes a tail beyond it.
  private void awaitObservedGrowth(final Set<String> observed, final int target, final Duration timeout)
    throws InterruptedException {
    final var deadline = System.currentTimeMillis() + timeout.toMillis();
    while (observed.size() < target && System.currentTimeMillis() < deadline) {
      TimeUnit.MILLISECONDS.sleep(50);
    }
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

  /// Builds a SEQUENTIAL consumer with a manual-commit `KafkaOffsetManager` on a long commit
  /// interval, captured into `offsetManagerRef` so the test commits a prefix manually and stops it
  /// to simulate a crash. The only commit is the manual one; everything A processes afterward stays
  /// uncommitted, forming the deterministic tail B re-delivers.
  private KPipeConsumer buildConsumerWithManagedOffsets(
    final String topic,
    final String groupId,
    final Set<String> observed,
    final AtomicInteger observedTotal,
    final AtomicReference<KafkaOffsetManager> offsetManagerRef
  ) {
    final var builder = KPipeConsumer.builder()
      .withProperties(consumerProperties(groupId))
      .withTopic(topic)
      // SEQUENTIAL on purpose: in-order processing means the committed prefix is a clean
      // contiguous range, and combined with the small max.poll.records (see
      // consumerProperties) the consumer drains the command queue between small batches so
      // the manual commit lands as a bounded prefix. At-least-once is mode-independent; the
      // PARALLEL path is covered by the offset property and Fray suites and by
      // DefaultOffsetPathIntegrationTest.
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
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
      final var manager = KafkaOffsetManager.builder(consumer)
        .withCommitExecutor(builder.getCommitExecutor())
        // Very long commit interval: the test drives the single commit manually, so the
        // commit point is deterministic and everything processed after it is a
        // guaranteed uncommitted tail.
        .withCommitInterval(Duration.ofHours(1))
        .build();
      offsetManagerRef.set(manager);
      return manager;
    });
    return builder.build();
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
    // Short session timeout, so a member that stops heartbeating is evicted quickly and B inherits
    // its partition well within the test window.
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
    // Small poll batches so SEQUENTIAL inline processing drains the command queue frequently. The
    // manual prefix commit is enqueued mid-stream; with a large batch the consumer wouldn't drain
    // (and thus commit) it until it finished processing the whole batch — committing the entire
    // poll's worth and leaving no uncommitted tail. A tiny batch bounds the committed prefix.
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }
}
