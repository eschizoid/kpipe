package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

/// Chaos rebalance integration test — end-to-end at-least-once under an induced rebalance
/// against a real broker.
///
/// This exercises the offset-lifecycle invariants from a black-box, end-to-end angle (the
/// jqwik property + concurrency-stress tests cover them at the unit level on `KafkaOffsetManager`
/// directly). What this test actually ASSERTS:
///
///   * **No loss (I3) — the primary guarantee.** Every produced record is observed by the sink
///     at least once across the rebalance (exact set coverage). A rebalance mid-stream must never
///     let the committed offset advance past a record that was tracked but never marked terminal,
///     so on reassignment the unprocessed tail is re-fetched rather than silently skipped.
///   * **No commit-ahead (I2) — a sanity bound.** The committed offset for each partition never
///     exceeds the log end. This is `<=`, not `==`: at-least-once permits a processed-but-not-yet-
///     committed tail at a bounded graceful-close shutdown (reprocessed on restart), so requiring
///     the commit to reach exactly the log end would assert a complete-drain / exactly-once
///     property kpipe does not claim. No-loss (above) plus this bound is the
///     at-least-once contract.
///
/// Revocation commit-and-clear (I4) is *exercised* here (the second consumer's join forces a
/// revoke from the first) but is asserted directly at the unit level in
/// `OffsetConcurrencyStressTest`; this test does not assert the mid-rebalance commit distinctly.
///
/// The rebalance is induced by starting a second `KPipeConsumer` in the same consumer group
/// mid-stream, which forces Kafka to reassign partitions across the two members. A
/// multi-partition topic guarantees the second member actually gets work and the first member
/// experiences a real revoke-then-reassign cycle.
@Testcontainers
class ChaosRebalanceIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");

  private static final int PARTITIONS = 4;
  private static final int RECORD_COUNT = 400;

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION)).asCompatibleSubstituteFor("apache/kafka")
  );

  @Test
  void atLeastOnceSurvivesRebalanceMidStream() throws Exception {
    final var topic = "chaos-input-" + System.nanoTime();
    final var groupId = "chaos-group-" + System.nanoTime();

    createTopic(topic);
    final var producedValues = produceRecords(topic);

    // CapturingSink — every value the pipeline observes, recorded for the no-loss assertion.
    // A record may be observed more than once (at-least-once allows duplicates after a
    // rebalance replays an uncommitted tail); the contract is "at least once", so we count
    // distinct values seen and require full coverage.
    final var observed = ConcurrentHashMap.<String>newKeySet();
    final var observedTotal = new AtomicInteger(0);

    // First consumer joins the group alone and starts draining all partitions.
    final var consumerA = buildConsumer(topic, groupId, observed, observedTotal);
    final var threadA = Thread.ofVirtual().name("chaos-consumer-A").start(consumerA::start);

    // Let A claim partitions and process a slice of the stream before chaos hits.
    awaitObservedAtLeast(observed, RECORD_COUNT / 10, Duration.ofSeconds(20));

    // Induce the rebalance: a second member joins the same group mid-stream. Kafka revokes a
    // share of A's partitions and assigns them to B. This is the moment I4 (commit-and-clear
    // on revoke) and I3 (no loss across the handoff) are under test.
    final var consumerB = buildConsumer(topic, groupId, observed, observedTotal);
    final var threadB = Thread.ofVirtual().name("chaos-consumer-B").start(consumerB::start);

    try {
      // No loss (I3): every produced value reaches the sink at least once across both members.
      final var allObserved = awaitObservedAtLeast(observed, producedValues.size(), Duration.ofSeconds(60));
      assertTrue(
        allObserved,
        "Every produced record must be observed at least once after the rebalance; saw %d of %d (total deliveries %d)".formatted(
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

    // No commit-ahead / no silent skip (I2): where a partition has a committed offset, it must
    // never exceed the log end. Combined with the no-loss assertion above (every record observed),
    // this is the at-least-once contract — the commit only ever advances over genuinely processed
    // offsets and never skips an unprocessed record to get ahead.
    //
    // The bound is `<=`, NOT `==`, and a partition may legitimately have NO committed offset at
    // all. At-least-once does not promise the commit reaches log-end — or that every partition
    // commits — at an arbitrary shutdown across an induced rebalance. A fully-processed-but-
    // uncommitted partition (its records were observed, but the owning member revoked or closed
    // before its commit flushed) is simply re-read from `auto.offset.reset=earliest` and
    // reprocessed on restart: that is at-least-once tolerance, not loss. No-loss (asserted above
    // over the observed set) is the real guarantee; this loop adds the no-commit-ahead bound for
    // every partition that did commit. Requiring a commit on every partition, or `== logEnd`,
    // would assert an exactly-once / complete-drain property kpipe deliberately does not claim.
    final var endOffsets = endOffsets(topic);
    final var committed = committedOffsets(groupId);

    for (final var entry : endOffsets.entrySet()) {
      final var tp = entry.getKey();
      final var logEnd = entry.getValue();
      final var committedMeta = committed.get(tp);
      // A missing commit is an acceptable uncommitted tail (reprocessed on restart); no-loss above
      // already covers it. Only bound-check partitions that actually committed.
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

  private KPipeConsumer<byte[]> buildConsumer(
    final String topic,
    final String groupId,
    final Set<String> observed,
    final AtomicInteger observedTotal
  ) {
    return KPipeConsumer.<byte[]>builder()
      .withProperties(consumerProperties(groupId))
      .withTopic(topic)
      .withProcessingMode(ProcessingMode.PARALLEL)
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          observed.add(new String(value));
          observedTotal.incrementAndGet();
          return value;
        })
      )
      .build();
  }

  /// Polls until the observed set reaches `target` distinct values or the deadline passes.
  /// Returns true if the target was reached.
  private boolean awaitObservedAtLeast(final Set<String> observed, final int target, final Duration timeout)
    throws InterruptedException {
    final var deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      if (observed.size() >= target) {
        return true;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    return observed.size() >= target;
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
