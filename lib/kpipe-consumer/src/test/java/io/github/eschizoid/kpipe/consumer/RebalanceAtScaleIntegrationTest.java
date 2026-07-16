package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
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

/// Rebalance-at-scale integration tests — end-to-end at-least-once across partition handoffs
/// against a real broker, covering paths the eager-rebalance chaos test does not.
///
/// `ChaosRebalanceIntegrationTest` already proves no-loss + no-commit-ahead under the *eager*
/// (stop-the-world) rebalance protocol. These tests extend that coverage to:
///
///   1. **Cooperative-sticky (incremental) rebalance.** With
///      `partition.assignment.strategy=CooperativeStickyAssignor`, a joining member only gets a
///      *subset* of partitions revoked from the incumbent rather than a full stop-the-world
///      revoke-everything cycle. The incumbent keeps draining its retained partitions across the
///      handoff. This asserts the same no-loss + no-commit-ahead contract holds when the revoke is
///      partial rather than total.
///   2. **Mid-flight revocation.** A slow sink keeps records actively in-flight while the
///      rebalance hits, so the incumbent revokes partitions whose tail is still processing. The
///      committed prefix must be correct (no commit-ahead) and the new owner re-fetches the rest.
///   3. **Single-writer command-queue invariant survives a real rebalance.** `onPartitionsRevoked`
///      must run on the consumer's poll thread (the thread that owns `commandQueue` mutations). An
///      `assert` inside `KafkaOffsetManager` would AssertionError if it ran off the poll thread
///      under `-ea`; this test additionally wraps the incumbent's OffsetManager so each revoke
///      callback records the thread it ran on, then asserts that thread is the named Kafka poll
///      thread — proving the invariant held under a genuine broker-driven rebalance, not a
///      synthetic `MockConsumer` rebalance.
///
/// All three assert the at-least-once contract with the same `<=` (no-commit-ahead) bound and
/// exact no-loss set coverage as the chaos test — deliberately NOT exact-once. A partition with
/// no commit at all is an acceptable uncommitted tail (reprocessed on restart); only partitions
/// that did commit are bound-checked.
///
/// CI-RUN-REQUIRED: these are Testcontainers tests (need a Docker daemon to start the broker).
/// They compile locally but cannot run where Docker is unavailable; they run in CI.
@Testcontainers(disabledWithoutDocker = true)
class RebalanceAtScaleIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");

  private static final int PARTITIONS = 6;
  private static final int RECORD_COUNT = 600;

  // Per-method broker (instance field, NOT static). Each of this class's three methods creates its
  // own 6-partition topic plus consumer groups and never tears them down; sharing one static broker
  // across all three let that state accumulate until a later method's producer could not get acks
  // within delivery.timeout.ms and failed at the produce step during setup. A fresh broker per
  // method keeps each test isolated. Do not make this static.
  @Container
  final KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION)).asCompatibleSubstituteFor("apache/kafka")
  );

  /// Cooperative-sticky (incremental) rebalance: a second member joins mid-stream and Kafka
  /// revokes only a *subset* of the incumbent's partitions instead of a stop-the-world
  /// revoke-everything cycle. The incumbent keeps draining its retained partitions throughout.
  /// The no-loss + no-commit-ahead contract must hold across the incremental handoff exactly as
  /// it does under eager rebalance.
  @Test
  void atLeastOnceSurvivesCooperativeStickyRebalance() throws Exception {
    final var topic = "coop-input-" + System.nanoTime();
    final var groupId = "coop-group-" + System.nanoTime();

    createTopic(topic);
    final var producedValues = produceRecords(topic);

    final var observed = ConcurrentHashMap.<String>newKeySet();
    final var observedTotal = new AtomicInteger(0);

    final var consumerA = buildConsumer(
      topic,
      cooperativeStickyProperties(groupId),
      observed,
      observedTotal,
      Duration.ZERO
    );
    final var threadA = Thread.ofVirtual().name("coop-consumer-A").start(consumerA::start);

    // Let A claim all partitions and process a slice before the second member joins.
    awaitObservedAtLeast(observed, RECORD_COUNT / 10, Duration.ofSeconds(20));

    // Second member joins the same group with the same cooperative-sticky strategy. Kafka moves a
    // subset of A's partitions to B incrementally.
    final var consumerB = buildConsumer(
      topic,
      cooperativeStickyProperties(groupId),
      observed,
      observedTotal,
      Duration.ZERO
    );
    final var threadB = Thread.ofVirtual().name("coop-consumer-B").start(consumerB::start);

    try {
      final var allObserved = awaitObservedAtLeast(observed, producedValues.size(), Duration.ofSeconds(60));
      assertTrue(
        allObserved,
        "Every produced record must be observed at least once after the cooperative-sticky rebalance; saw %d of %d (total deliveries %d)".formatted(
          observed.size(),
          producedValues.size(),
          observedTotal.get()
        )
      );
      assertEquals(producedValues, observed, "Observed set must exactly cover the produced set (no loss).");
    } finally {
      consumerA.close();
      consumerB.close();
      threadA.join(5000);
      threadB.join(5000);
    }

    assertNoCommitAhead(topic, groupId);
  }

  /// Mid-flight revocation: a slow sink keeps records actively in-flight while the rebalance hits,
  /// so the incumbent revokes partitions whose tail is still being processed. The committed prefix
  /// for each revoked partition must reflect only what genuinely finished (no commit-ahead), and
  /// the new owner re-fetches the unprocessed remainder (no loss). With per-record processing
  /// trailing the fetch, there is a real in-flight set at the moment of revoke, not a drained log.
  @Test
  void midFlightRevocationCommitsPrefixAndReassignsRest() throws Exception {
    final var topic = "midflight-input-" + System.nanoTime();
    final var groupId = "midflight-group-" + System.nanoTime();

    createTopic(topic);
    final var producedValues = produceRecords(topic);

    final var observed = ConcurrentHashMap.<String>newKeySet();
    final var observedTotal = new AtomicInteger(0);

    // A slow sink (2ms/record) keeps a tail of records in-flight when the rebalance lands, so the
    // revoke catches partitions mid-processing rather than fully drained.
    final var sinkDelay = Duration.ofMillis(2);

    final var consumerA = buildConsumer(topic, consumerProperties(groupId), observed, observedTotal, sinkDelay);
    final var threadA = Thread.ofVirtual().name("midflight-consumer-A").start(consumerA::start);

    awaitObservedAtLeast(observed, RECORD_COUNT / 10, Duration.ofSeconds(30));

    // Second member joins while A is still actively processing in-flight records.
    final var consumerB = buildConsumer(topic, consumerProperties(groupId), observed, observedTotal, sinkDelay);
    final var threadB = Thread.ofVirtual().name("midflight-consumer-B").start(consumerB::start);

    try {
      final var allObserved = awaitObservedAtLeast(observed, producedValues.size(), Duration.ofSeconds(90));
      assertTrue(
        allObserved,
        "Every produced record must be observed at least once across a mid-flight revocation; saw %d of %d (total deliveries %d)".formatted(
          observed.size(),
          producedValues.size(),
          observedTotal.get()
        )
      );
      assertEquals(producedValues, observed, "Observed set must exactly cover the produced set (no loss).");
    } finally {
      consumerA.close();
      consumerB.close();
      threadA.join(5000);
      threadB.join(5000);
    }

    assertNoCommitAhead(topic, groupId);
  }

  /// Single-writer command-queue invariant under a real rebalance. `onPartitionsRevoked` mutates
  /// `commandQueue` (draining commands for revoked partitions); that drain is only safe because it
  /// runs on the consumer's poll thread, the sole writer of the queue. The runtime guard is an
  /// `assert` inside `KafkaOffsetManager` that would AssertionError if the callback ever fired
  /// off-thread under `-ea`. This test forces a genuine broker-driven revoke and asserts the
  /// revoke log record's source thread is the named Kafka poll thread — proving the callback ran
  /// on the poll thread and the drain stayed correct (data drained, no loss / no commit-ahead).
  @Test
  void revokeRunsOnConsumerThreadUnderRealRebalance() throws Exception {
    final var topic = "single-writer-input-" + System.nanoTime();
    final var groupId = "single-writer-group-" + System.nanoTime();

    createTopic(topic);
    final var producedValues = produceRecords(topic);

    // Capture the revoke thread directly at the callback by wrapping the incumbent's OffsetManager
    // rebalance listener. onPartitionsRevoked fires inside the poll call, so its thread must be the
    // named Kafka poll thread (the single writer of the command queue). Capturing at the callback
    // is deterministic: no log-message match, no System.Logger to JUL bridge, no thread-id lookup.
    final var revokeThreads = new CopyOnWriteArrayList<String>();

    final var observed = ConcurrentHashMap.<String>newKeySet();
    final var observedTotal = new AtomicInteger(0);

    final var consumerA = buildConsumer(
      topic,
      consumerProperties(groupId),
      observed,
      observedTotal,
      Duration.ofMillis(1),
      revokeThreads
    );
    final var threadA = Thread.ofVirtual().name("single-writer-consumer-A").start(consumerA::start);

    awaitObservedAtLeast(observed, RECORD_COUNT / 10, Duration.ofSeconds(30));

    // A second member joins → A's poll loop fires onPartitionsRevoked for the moved partitions.
    final var consumerB = buildConsumer(
      topic,
      consumerProperties(groupId),
      observed,
      observedTotal,
      Duration.ofMillis(1)
    );
    final var threadB = Thread.ofVirtual().name("single-writer-consumer-B").start(consumerB::start);

    try {
      // Wait for a revoke to actually fire on the incumbent.
      final var sawRevoke = awaitCondition(() -> !revokeThreads.isEmpty(), Duration.ofSeconds(60));
      assertTrue(sawRevoke, "expected a partition revoke to fire on the incumbent under a real rebalance");

      // Every revoke callback must have run on the named Kafka poll thread — the single writer of
      // commandQueue. Anything else would mean the listener ran off-thread, which the production
      // single-writer assert would also have caught by AssertionError.
      for (final var threadName : revokeThreads) {
        assertTrue(
          threadName.startsWith("kafka-consumer-"),
          "onPartitionsRevoked must run on the Kafka poll thread (single-writer invariant); ran on '%s'".formatted(
            threadName
          )
        );
      }

      // The drain must stay correct end-to-end: every record still lands at least once.
      final var allObserved = awaitObservedAtLeast(observed, producedValues.size(), Duration.ofSeconds(60));
      assertTrue(
        allObserved,
        "Every produced record must be observed at least once after the revoke; saw %d of %d (total deliveries %d)".formatted(
          observed.size(),
          producedValues.size(),
          observedTotal.get()
        )
      );
      assertEquals(producedValues, observed, "Observed set must exactly cover the produced set (no loss).");
    } finally {
      consumerA.close();
      consumerB.close();
      threadA.join(5000);
      threadB.join(5000);
    }

    assertNoCommitAhead(topic, groupId);
  }

  /// No commit-ahead / no silent skip: where a partition has a committed offset, it must never
  /// exceed the log end. The bound is `<=`, NOT `==`, and a partition may legitimately have NO
  /// committed offset at all — at-least-once does not promise the commit reaches log-end at an
  /// arbitrary shutdown across a rebalance. A fully-processed-but-uncommitted partition is simply
  /// re-read and reprocessed on restart (at-least-once tolerance, not loss). Combined with the
  /// no-loss assertion at each call site, this is the at-least-once contract. Requiring a commit
  /// on every partition, or `== logEnd`, would assert an exactly-once / complete-drain property
  /// kpipe deliberately does not claim.
  private void assertNoCommitAhead(final String topic, final String groupId) throws Exception {
    final var endOffsets = endOffsets(topic);
    final var committed = committedOffsets(groupId);

    for (final var entry : endOffsets.entrySet()) {
      final var tp = entry.getKey();
      final var logEnd = entry.getValue();
      final var committedMeta = committed.get(tp);
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

  /// Builds a PARALLEL consumer over `topic`. A non-zero `sinkDelay` slows each record so the
  /// processing trails the fetch, keeping a tail of records in-flight when a rebalance lands.
  private KPipeConsumer buildConsumer(
    final String topic,
    final Properties props,
    final Set<String> observed,
    final AtomicInteger observedTotal,
    final Duration sinkDelay
  ) {
    return buildConsumer(topic, props, observed, observedTotal, sinkDelay, null);
  }

  /// Builds a PARALLEL consumer over `topic`. When `revokeThreads` is non-null, the consumer's
  /// OffsetManager is wrapped so each `onPartitionsRevoked` callback records the thread it ran on —
  /// the deterministic single-writer probe used by `revokeRunsOnConsumerThreadUnderRealRebalance`.
  private KPipeConsumer buildConsumer(
    final String topic,
    final Properties props,
    final Set<String> observed,
    final AtomicInteger observedTotal,
    final Duration sinkDelay,
    final List<String> revokeThreads
  ) {
    final var builder = KPipeConsumer.builder()
      .withProperties(props)
      .withTopic(topic)
      .withProcessingMode(ProcessingMode.PARALLEL)
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          if (!sinkDelay.isZero()) {
            try {
              TimeUnit.NANOSECONDS.sleep(sinkDelay.toNanos());
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          observed.add(new String(value));
          observedTotal.incrementAndGet();
          return value;
        })
      );
    if (revokeThreads != null) {
      builder.withOffsetManagerProvider(consumer ->
        new RevokeCapturingOffsetManager(
          KafkaOffsetManager.builder(consumer).withCommandQueue(builder.getCommandQueue()).build(),
          revokeThreads
        )
      );
    }
    return builder.build();
  }

  /// Decorator that records the calling thread of every non-empty `onPartitionsRevoked` into
  /// `revokeThreads`, then delegates. Everything else passes straight through to the real manager.
  /// Capturing at the callback is deterministic — no log-string match or thread-id resolution.
  private static final class RevokeCapturingOffsetManager implements OffsetManager {

    private final OffsetManager delegate;
    private final List<String> revokeThreads;

    RevokeCapturingOffsetManager(final OffsetManager delegate, final List<String> revokeThreads) {
      this.delegate = delegate;
      this.revokeThreads = revokeThreads;
    }

    @Override
    public ConsumerRebalanceListener createRebalanceListener() {
      final var inner = delegate.createRebalanceListener();
      return new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
          if (!partitions.isEmpty()) revokeThreads.add(Thread.currentThread().getName());
          inner.onPartitionsRevoked(partitions);
        }

        @Override
        public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
          inner.onPartitionsAssigned(partitions);
        }
      };
    }

    @Override
    public OffsetManager start() {
      delegate.start();
      return this;
    }

    @Override
    public OffsetManager stop() {
      delegate.stop();
      return this;
    }

    @Override
    public void trackOffset(final ConsumerRecord<byte[], byte[]> record) {
      delegate.trackOffset(record);
    }

    @Override
    public void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record) {
      delegate.markOffsetProcessed(record);
    }

    @Override
    public void notifyCommitComplete(final String commitId, final boolean success) {
      delegate.notifyCommitComplete(commitId, success);
    }

    @Override
    public OffsetState getState() {
      return delegate.getState();
    }

    @Override
    public boolean isRunning() {
      return delegate.isRunning();
    }

    @Override
    public OffsetStatistics getStatistics() {
      return delegate.getStatistics();
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  /// Polls until the observed set reaches `target` distinct values or the deadline passes.
  /// Returns true if the target was reached.
  private boolean awaitObservedAtLeast(final Set<String> observed, final int target, final Duration timeout)
    throws InterruptedException {
    return awaitCondition(() -> observed.size() >= target, timeout);
  }

  /// Polls a boolean condition until it holds or the deadline passes. Returns the final state.
  private boolean awaitCondition(final java.util.function.BooleanSupplier condition, final Duration timeout)
    throws InterruptedException {
    final var deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    return condition.getAsBoolean();
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

  /// Consumer properties with the CooperativeStickyAssignor → incremental rebalance: the
  /// incumbent keeps its retained partitions and only the moved subset is revoked, distinct from
  /// the eager chaos-test path. A fresh instance per call so two members never share Properties.
  private Properties cooperativeStickyProperties(final String groupId) {
    final var props = consumerProperties(groupId);
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
    return props;
  }

  private Properties consumerProperties(final String groupId) {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // Short session timeout so a joining member triggers the rebalance promptly within the window.
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }
}
