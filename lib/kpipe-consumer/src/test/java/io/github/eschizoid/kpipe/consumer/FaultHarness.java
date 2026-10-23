package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.producer.KPipeProducer;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/// The bookkeeping and checks the broker fault tests share: a scenario's topic and records, the
/// consumers it builds, and the two checks on the commit frontier, a [ConsumerInterceptor] that
/// sees every acknowledged commit and a sampler that reads the committed offsets on the broker.
/// A test supplies the [Cluster] to run against: where to reach the brokers directly, where the
/// consumer reaches them through the fault proxies, and where a scenario's partitions live.
final class FaultHarness {

  static final int PARTITIONS = 3;
  static final int RECORD_COUNT = 300;
  /// Keys repeat, so in `KEY_ORDERED` the records behind a held one wait for it.
  static final int KEYS = 30;
  /// Held in every partition and released while the fault is in effect.
  static final long RELEASED_DURING_FAULT = 7;
  /// Held in every partition and released once the link has been healthy for [#HEALTHY_WINDOW].
  static final long RELEASED_AFTER_HEAL = 57;
  /// A `SEQUENTIAL` consumer runs records on its own thread, so holding one stops its polls and
  /// commits. In the fault scenarios each record takes this long instead.
  static final Duration SEQUENTIAL_RECORD_TIME = Duration.ofMillis(20);
  static final Duration COMMIT_INTERVAL = Duration.ofMillis(200);
  /// Several commit intervals, so commits land while held records are outstanding.
  static final Duration HEALTHY_WINDOW = COMMIT_INTERVAL.multipliedBy(5);

  static final Map<String, Run> RUNS_BY_GROUP = new ConcurrentHashMap<>();

  /// Where a test reaches its brokers directly, where the consumer reaches them through the
  /// fault proxies, and the brokers that hold every partition of a scenario's topic, preferred
  /// leader first. With no brokers listed, each partition has one replica wherever Kafka puts it.
  record Cluster(String direct, String proxied, List<Integer> replicas) {
    NewTopic newTopic(final String topic) {
      if (replicas.isEmpty()) return new NewTopic(topic, PARTITIONS, (short) 1);
      final var assignment = new HashMap<Integer, List<Integer>>();
      for (int p = 0; p < PARTITIONS; p++) assignment.put(p, replicas);
      return new NewTopic(topic, assignment);
    }
  }

  private FaultHarness() {}

  static Set<String> missingFrom(final Run run, final Set<String> finished) {
    final var missing = new HashSet<>(run.partitionOf.keySet());
    missing.removeAll(finished);
    return Set.copyOf(missing);
  }

  /// Sees every commit a consumer in this test has had acknowledged. Kafka creates it from the
  /// consumer's configuration, so it finds its scenario through the group id.
  public static final class CommitRecorder implements ConsumerInterceptor<byte[], byte[]> {

    private Run run;

    @Override
    public void configure(final Map<String, ?> configs) {
      final var groupId = (String) configs.get(ConsumerConfig.GROUP_ID_CONFIG);
      run = RUNS_BY_GROUP.get(groupId);
      // Without its scenario the recorder would check nothing, and every assertion built on it
      // would pass vacuously.
      if (run == null) throw new IllegalStateException("no scenario registered for group " + groupId);
    }

    @Override
    public ConsumerRecords<byte[], byte[]> onConsume(final ConsumerRecords<byte[], byte[]> records) {
      return records;
    }

    @Override
    public void onCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
      if (run != null) run.onAcknowledgedCommit(offsets);
    }

    @Override
    public void close() {}
  }

  /// One scenario's topic, group and record bookkeeping.
  static final class Run implements AutoCloseable {

    final Cluster cluster;
    final ProcessingMode mode;
    final String topic = "fault-" + System.nanoTime();
    final String groupId;
    /// The partition and offset each produced value landed at.
    final Map<String, TopicPartition> partitionOf = new HashMap<>();
    final Map<String, Long> offsetOf = new HashMap<>();
    /// The offset each held value is released with, keyed by value.
    final Map<String, Long> heldUntil = new HashMap<>();
    final Map<Long, CountDownLatch> releases = new HashMap<>();
    /// Values whose processing always throws, so they can finish only in the dead-letter topic.
    final Set<String> failing = new HashSet<>();
    /// Values completed by any consumer of this scenario, and by each consumer separately.
    final Set<String> completed = ConcurrentHashMap.newKeySet();
    final Map<KPipeConsumer, Set<String>> completedBy = new ConcurrentHashMap<>();
    final List<String> acknowledgedViolations = new CopyOnWriteArrayList<>();
    final AtomicBoolean faultInEffect = new AtomicBoolean();
    final AtomicInteger acknowledgedDuringFault = new AtomicInteger();

    Run(final Cluster cluster, final ProcessingMode mode, final Set<Long> heldOffsets) throws Exception {
      this(cluster, mode, heldOffsets, Set.of());
    }

    Run(final Cluster cluster, final ProcessingMode mode, final Set<Long> heldOffsets, final Set<Long> failingOffsets)
      throws Exception {
      this(cluster, mode, heldOffsets, failingOffsets, "fault-group-" + System.nanoTime());
    }

    /// A scenario whose consumers join `groupId`, for a test that needs the group's coordinator to
    /// be a particular broker.
    Run(
      final Cluster cluster,
      final ProcessingMode mode,
      final Set<Long> heldOffsets,
      final Set<Long> failingOffsets,
      final String groupId
    ) throws Exception {
      this.groupId = groupId;
      this.cluster = cluster;
      this.mode = mode;
      for (final var offset : heldOffsets) releases.put(offset, new CountDownLatch(1));
      try (final var admin = Admin.create(directAdminProperties(cluster.direct()))) {
        admin.createTopics(List.of(cluster.newTopic(topic))).all().get(30, TimeUnit.SECONDS);
      }
      produce();
      for (final var value : offsetOf.keySet()) {
        final var offset = offsetOf.get(value);
        if (releases.containsKey(offset)) heldUntil.put(value, offset);
      }
      assertEquals(PARTITIONS * heldOffsets.size(), heldUntil.size(), "every partition holds each held offset");
      for (final var value : offsetOf.keySet()) {
        if (failingOffsets.contains(offsetOf.get(value))) failing.add(value);
      }
      assertEquals(PARTITIONS * failingOffsets.size(), failing.size(), "every partition has each failing offset");
      RUNS_BY_GROUP.put(groupId, this);
    }

    KPipeConsumer consumer() {
      return consumer(null, null);
    }

    /// A consumer that sends records it cannot process to `dlqTopic` through `dlqProducer`, or
    /// that has no dead-letter queue when they are null.
    KPipeConsumer consumer(final String dlqTopic, final KPipeProducer<byte[], byte[]> dlqProducer) {
      final var finished = ConcurrentHashMap.<String>newKeySet();
      final var builder = KPipeConsumer.builder()
        .withProperties(proxiedConsumerProperties(cluster.proxied(), groupId, mode))
        .withTopic(topic)
        .withProcessingMode(mode)
        .withCommitInterval(COMMIT_INTERVAL)
        .withPipeline(
          TestPipelines.sideEffect(value -> {
            final var text = new String(value);
            try {
              final var heldOffset = heldUntil.get(text);
              if (heldOffset != null) releases.get(heldOffset).await();
              if (mode == ProcessingMode.SEQUENTIAL && heldUntil.isEmpty()) {
                TimeUnit.MILLISECONDS.sleep(SEQUENTIAL_RECORD_TIME.toMillis());
              }
            } catch (final InterruptedException e) {
              // The flag stays cleared, so only the interruption in the cause
              // chain tells the consumer the record was cut short at shutdown.
              throw new IllegalStateException("interrupted at shutdown", e);
            }
            if (failing.contains(text)) throw new IllegalStateException("fails on purpose: " + text);
            completed.add(text);
            finished.add(text);
            return value;
          })
        );
      if (dlqTopic != null) builder.withDeadLetterQueue(dlqTopic, dlqProducer);
      final var consumer = builder.build();
      completedBy.put(consumer, finished);
      return consumer;
    }

    void release(final long heldOffset) {
      final var latch = releases.get(heldOffset);
      if (latch != null) latch.countDown();
    }

    void releaseAll() {
      releases.values().forEach(CountDownLatch::countDown);
    }

    void onAcknowledgedCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
      if (faultInEffect.get()) acknowledgedDuringFault.incrementAndGet();
      for (final var entry : offsets.entrySet()) {
        final var unfinished = unfinishedBelow(entry.getKey(), entry.getValue().offset());
        if (!unfinished.isEmpty()) {
          acknowledgedViolations.add(
            entry.getKey() + " acknowledged at " + entry.getValue().offset() + " past " + unfinished
          );
        }
      }
    }

    void assertNoAcknowledgedViolation() {
      assertTrue(
        acknowledgedViolations.isEmpty(),
        "acknowledged commits passed unfinished records: " + acknowledgedViolations
      );
    }

    void awaitCompleted(final int target, final Duration timeout) throws InterruptedException {
      final var deadline = System.nanoTime() + timeout.toNanos();
      while (completed.size() < target && System.nanoTime() < deadline) TimeUnit.MILLISECONDS.sleep(50);
      assertTrue(completed.size() >= target, "completed " + completed.size() + " of the " + target + " awaited");
    }

    /// Waits until `consumer` itself has completed every value in `expected`. Counting only that
    /// consumer's completions means a record still finishing on a consumer that was closed before
    /// cannot stand in for one that was redelivered.
    void awaitCompletedBy(final KPipeConsumer consumer, final Set<String> expected, final Duration timeout)
      throws InterruptedException {
      final var finished = completedBy.get(consumer);
      final var deadline = System.nanoTime() + timeout.toNanos();
      while (!finished.containsAll(expected) && System.nanoTime() < deadline) TimeUnit.MILLISECONDS.sleep(50);
      final var missing = expected
        .stream()
        .filter(value -> !finished.contains(value))
        .sorted()
        .toList();
      assertTrue(missing.isEmpty(), "the next consumer must receive every unfinished record; missing " + missing);
    }

    /// Waits for every partition's committed offset to reach `heldOffset`: commits
    /// resumed after the fault and stopped at the record still held there. The
    /// interceptor and the sampler fail the test if any commit passes it meanwhile.
    void awaitCommittedAt(final long heldOffset, final Duration timeout) throws Exception {
      final var expected = new HashMap<TopicPartition, Long>();
      for (int p = 0; p < PARTITIONS; p++) expected.put(new TopicPartition(topic, p), heldOffset);
      final var deadline = System.nanoTime() + timeout.toNanos();
      var committed = committedOffsets();
      while (!committed.equals(expected) && System.nanoTime() < deadline) {
        TimeUnit.MILLISECONDS.sleep(100);
        committed = committedOffsets();
      }
      assertEquals(expected, committed, "commits resume and stop at the held records");
    }

    /// Waits for the running consumer's periodic commits to reach the end of every partition, which
    /// shows commits resumed once the broker came back. It is checked before `close()`, because a
    /// graceful close may leave a processed but uncommitted tail.
    void awaitCommittedToLogEnd(final Duration timeout) throws Exception {
      final var end = endOffsets();
      final var deadline = System.nanoTime() + timeout.toNanos();
      var committed = committedOffsets();
      while (!committed.equals(end) && System.nanoTime() < deadline) {
        TimeUnit.MILLISECONDS.sleep(100);
        committed = committedOffsets();
      }
      assertEquals(end, committed, "commits resume and reach the end of every partition");
    }

    Map<TopicPartition, Long> committedOffsets() throws Exception {
      try (final var admin = Admin.create(directAdminProperties(cluster.direct()))) {
        return admin
          .listConsumerGroupOffsets(groupId)
          .partitionsToOffsetAndMetadata()
          .get(15, TimeUnit.SECONDS)
          .entrySet()
          .stream()
          .filter(entry -> entry.getValue() != null)
          .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
      }
    }

    Map<TopicPartition, Long> endOffsets() throws Exception {
      final var request = new HashMap<TopicPartition, OffsetSpec>();
      for (int p = 0; p < PARTITIONS; p++) request.put(new TopicPartition(topic, p), OffsetSpec.latest());
      try (final var admin = Admin.create(directAdminProperties(cluster.direct()))) {
        return admin
          .listOffsets(request)
          .all()
          .get(15, TimeUnit.SECONDS)
          .entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
      }
    }

    /// Values in `partition` below `committed` that have not completed: each one is a commit that
    /// passed an unfinished record.
    List<String> unfinishedBelow(final TopicPartition partition, final long committed) {
      return partitionOf
        .entrySet()
        .stream()
        .filter(entry -> entry.getValue().equals(partition))
        .map(Map.Entry::getKey)
        .filter(value -> offsetOf.get(value) < committed)
        .filter(value -> !completed.contains(value))
        .sorted()
        .toList();
    }

    /// Writes each partition's records in order, keys spread so each key stays in one partition.
    private void produce() throws Exception {
      final var props = new Properties();
      props.put("bootstrap.servers", cluster.direct());
      props.put("acks", "all");
      final var sends = new HashMap<String, Future<RecordMetadata>>();
      try (final var producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
        for (int i = 0; i < RECORD_COUNT; i++) {
          final var value = "val-" + i;
          final var key = i % KEYS;
          final var record = new ProducerRecord<>(topic, key % PARTITIONS, ("key-" + key).getBytes(), value.getBytes());
          sends.put(value, producer.send(record));
        }
        producer.flush();
      }
      for (final var send : sends.entrySet()) {
        final var metadata = send.getValue().get(30, TimeUnit.SECONDS);
        partitionOf.put(send.getKey(), new TopicPartition(topic, metadata.partition()));
        offsetOf.put(send.getKey(), metadata.offset());
      }
    }

    @Override
    public void close() {
      releaseAll();
      RUNS_BY_GROUP.remove(groupId);
    }
  }

  /// Reads the group's committed offsets every 100 ms on the broker's own listener and records any
  /// offset that passes a record not yet completed. A record completes before KPipe marks it, and
  /// the completed set only grows, so a commit that was legitimate when made always passes here.
  static final class CommitSampler implements AutoCloseable {

    final Run run;
    final List<String> violations = new CopyOnWriteArrayList<>();
    final AtomicInteger samples = new AtomicInteger();
    final AtomicBoolean running = new AtomicBoolean(true);
    final Thread thread;

    CommitSampler(final Run run) {
      this.run = run;
      this.thread = Thread.ofPlatform().daemon().name("commit-sampler").start(this::sample);
    }

    private void sample() {
      try (final var admin = Admin.create(directAdminProperties(run.cluster.direct()))) {
        while (running.get()) {
          try {
            final var committed = admin
              .listConsumerGroupOffsets(run.groupId)
              .partitionsToOffsetAndMetadata()
              .get(5, TimeUnit.SECONDS);
            for (final var entry : committed.entrySet()) {
              if (entry.getValue() == null) continue;
              final var unfinished = run.unfinishedBelow(entry.getKey(), entry.getValue().offset());
              if (!unfinished.isEmpty()) {
                violations.add(entry.getKey() + " committed at " + entry.getValue().offset() + " past " + unfinished);
              }
            }
            samples.incrementAndGet();
            TimeUnit.MILLISECONDS.sleep(100);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          } catch (final Exception e) {
            // A sample that cannot be read checks nothing; the next one tries again.
          }
        }
      }
    }

    void assertNoViolation() {
      assertTrue(samples.get() > 0, "the sampler must read the committed offsets at least once");
      assertTrue(violations.isEmpty(), "commits passed unfinished records: " + violations);
    }

    @Override
    public void close() throws InterruptedException {
      running.set(false);
      thread.join(Duration.ofSeconds(10));
    }
  }

  /// Whether a fresh client can reach the broker through `bootstrap` within `timeout`.
  static boolean probeReachesBroker(final String bootstrap, final Duration timeout) {
    final var props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeout.toMillis());
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeout.toMillis());
    try (final var admin = Admin.create(props)) {
      admin.describeCluster().nodes().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      return true;
    } catch (final Exception e) {
      return false;
    }
  }

  static Properties directAdminProperties(final String bootstrap) {
    final var props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    return props;
  }

  /// A client API call gives up after five seconds, so a commit stalled by a fault fails within the
  /// fault window instead of outlasting the test. A request a silent broker never answers is
  /// abandoned after five seconds too: the client looks for a new group coordinator only once its
  /// request to the old one times out, which at Kafka's default of 30 seconds outlasts the window.
  /// A `SEQUENTIAL` consumer commits only between polls, so it fetches a few records at a time and
  /// keeps committing through the fault.
  static Properties proxiedConsumerProperties(final String bootstrap, final String groupId, final ProcessingMode mode) {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, CommitRecorder.class.getName());
    if (mode == ProcessingMode.SEQUENTIAL) props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }
}
