package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.eschizoid.kpipe.metrics.ConsumerMetricKeys;
import io.github.eschizoid.kpipe.producer.KPipeProducer;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

/// The at-least-once guarantee under a broker that misbehaves: every connection the consumer makes
/// goes through Toxiproxy, which cuts, stalls, slows or resets it mid-run. The checks reach the
/// broker on its own listener, so a fault never hides what they read.
///
/// The property under test is the one the commit frontier exists for: no committed offset ever
/// passes a record that has not completed. Two checks enforce it. A [ConsumerInterceptor] sees
/// every commit the consumer has acknowledged, and a sampler reads the group's committed offsets on
/// the broker, which also catches a commit that landed but whose response the fault dropped.
///
/// Records are held so the frontier has somewhere it must stop. One record per partition is
/// released while the fault is in effect, so the frontier wants to advance while commits cannot
/// get through, and a later one is released only after the link has been healthy for a while, so
/// the resumed commits must stop below it.
///
/// CI-RUN-REQUIRED: Testcontainers starts a Kafka broker and a Toxiproxy container (needs a Docker
/// daemon).
class BrokerFaultInjectionIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");
  private static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0";
  private static final int PROXY_PORT = 8666;
  private static final String PROXIED_LISTENER = "kafka:19092";
  /// The dead-letter producer reaches the broker on a listener of its own, behind its own proxy,
  /// so the DLQ can be cut off while the consumer's own connections stay healthy.
  private static final int DLQ_PROXY_PORT = 8667;
  private static final String DLQ_LISTENER = "kafka:19093";

  private static final int PARTITIONS = 3;
  private static final int RECORD_COUNT = 300;
  /// Keys repeat, so in `KEY_ORDERED` the records behind a held one wait for it.
  private static final int KEYS = 30;
  /// Held in every partition and released while the fault is in effect.
  private static final long RELEASED_DURING_FAULT = 7;
  /// Held in every partition and released once the link has been healthy for [#HEALTHY_WINDOW].
  private static final long RELEASED_AFTER_HEAL = 57;

  /// A `SEQUENTIAL` consumer runs records on its own thread, so holding one stops its polls and
  /// commits. In the fault scenarios each record takes this long instead.
  private static final Duration SEQUENTIAL_RECORD_TIME = Duration.ofMillis(20);
  private static final Duration COMMIT_INTERVAL = Duration.ofMillis(200);
  /// Several commit intervals, so commits land while held records are outstanding.
  private static final Duration HEALTHY_WINDOW = COMMIT_INTERVAL.multipliedBy(5);
  private static final Duration FAULT_WINDOW = Duration.ofSeconds(5);
  private static final Duration SLOW_LINK_LATENCY = Duration.ofSeconds(1);

  private static final Map<String, Run> RUNS_BY_GROUP = new ConcurrentHashMap<>();

  private static Network network;
  private static ToxiproxyContainer toxiproxy;
  private static KafkaContainer kafka;
  private static Proxy proxy;
  private static String proxiedBootstrap;
  private static Proxy dlqProxy;
  private static String dlqBootstrap;

  @BeforeAll
  static void startBroker() throws IOException {
    network = Network.newNetwork();
    toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE).withNetwork(network);
    toxiproxy.start();
    // The broker advertises the proxy's host address on this listener, so after bootstrap every
    // connection the client opens still goes through the proxy rather than straight to the broker.
    proxiedBootstrap = toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(PROXY_PORT);
    dlqBootstrap = toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(DLQ_PROXY_PORT);
    kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:%s".formatted(KAFKA_VERSION)))
      .withNetwork(network)
      .withNetworkAliases("kafka")
      .withListener(PROXIED_LISTENER, () -> proxiedBootstrap)
      .withListener(DLQ_LISTENER, () -> dlqBootstrap);
    kafka.start();
    final var client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
    proxy = client.createProxy("kafka", "0.0.0.0:" + PROXY_PORT, PROXIED_LISTENER);
    dlqProxy = client.createProxy("kafka-dlq", "0.0.0.0:" + DLQ_PROXY_PORT, DLQ_LISTENER);
  }

  @AfterAll
  static void stopBroker() {
    if (kafka != null) kafka.stop();
    if (toxiproxy != null) toxiproxy.stop();
    if (network != null) network.close();
  }

  @AfterEach
  void healProxy() throws IOException {
    Fault.heal();
  }

  /// A broker fault applied to every connection the consumer has.
  enum Fault {
    /// Connections are refused: the broker is unreachable.
    UNREACHABLE(true) {
      @Override
      void apply() throws IOException {
        proxy.disable();
      }
    },
    /// Connections stay open but nothing comes back, so a commit request can reach the broker and
    /// still never be acknowledged.
    BLACK_HOLE(true) {
      @Override
      void apply() throws IOException {
        proxy.toxics().timeout("black-hole", ToxicDirection.DOWNSTREAM, 0);
      }
    },
    /// Every response is delayed, so commits are acknowledged late.
    SLOW_LINK(false) {
      @Override
      void apply() throws IOException {
        proxy.toxics().latency("slow-link", ToxicDirection.DOWNSTREAM, SLOW_LINK_LATENCY.toMillis());
      }

      @Override
      void assertEngaged() {
        final var started = System.nanoTime();
        assertTrue(
          probeReachesBroker(proxiedBootstrap, Duration.ofSeconds(20)),
          "the slow link still reaches the broker"
        );
        final var elapsed = Duration.ofNanos(System.nanoTime() - started);
        assertTrue(elapsed.compareTo(SLOW_LINK_LATENCY) >= 0, "a request through the slow link took " + elapsed);
      }
    },
    /// Every connection is reset as soon as data arrives.
    CONNECTION_RESET(true) {
      @Override
      void apply() throws IOException {
        proxy.toxics().resetPeer("connection-reset", ToxicDirection.DOWNSTREAM, 0);
      }
    };

    /// Whether no commit can be acknowledged while the fault is in effect.
    final boolean blocksAcknowledgements;

    Fault(final boolean blocksAcknowledgements) {
      this.blocksAcknowledgements = blocksAcknowledgements;
    }

    abstract void apply() throws IOException;

    /// Fails the test if the fault is not in effect, so a scenario cannot pass against a link that
    /// was never actually broken.
    void assertEngaged() {
      assertTrue(
        !probeReachesBroker(proxiedBootstrap, Duration.ofSeconds(3)),
        name() + " must stop requests reaching the broker"
      );
    }

    static void heal() throws IOException {
      proxy.enable();
      for (final var toxic : proxy.toxics().getAll()) toxic.remove();
      dlqProxy.enable();
    }
  }

  static List<Arguments> faultsByMode() {
    final var arguments = new ArrayList<Arguments>();
    for (final var mode : ProcessingMode.values()) {
      for (final var fault : Fault.values()) arguments.add(Arguments.of(mode, fault));
    }
    return arguments;
  }

  @ParameterizedTest(name = "{0} under {1}")
  @MethodSource("faultsByMode")
  void noCommitPassesAnUnfinishedRecordWhileTheBrokerMisbehaves(final ProcessingMode mode, final Fault fault)
    throws Exception {
    // A held SEQUENTIAL record would stop the consumer's commits, leaving nothing for the fault to
    // break, so that mode runs every record slowly and holds none.
    final var holds =
      mode == ProcessingMode.SEQUENTIAL ? Set.<Long>of() : Set.of(RELEASED_DURING_FAULT, RELEASED_AFTER_HEAL);
    try (final var run = new Run(mode, holds)) {
      final var consumer = run.consumer();
      try (final var sampler = new CommitSampler(run)) {
        consumer.start();
        try {
          run.awaitCompleted(5, Duration.ofSeconds(60));
          TimeUnit.MILLISECONDS.sleep(HEALTHY_WINDOW.toMillis());
          fault.apply();
          fault.assertEngaged();
          run.faultInEffect.set(true);
          run.release(RELEASED_DURING_FAULT);
          TimeUnit.MILLISECONDS.sleep(FAULT_WINDOW.toMillis());
          run.faultInEffect.set(false);
          Fault.heal();
          TimeUnit.MILLISECONDS.sleep(HEALTHY_WINDOW.toMillis());
          if (!holds.isEmpty()) run.awaitCommittedAt(RELEASED_AFTER_HEAL, Duration.ofSeconds(20));
          run.release(RELEASED_AFTER_HEAL);
          run.awaitCompleted(RECORD_COUNT, Duration.ofSeconds(120));
          run.awaitCommittedToLogEnd(Duration.ofSeconds(30));
        } finally {
          run.releaseAll();
          consumer.close();
        }
        sampler.assertNoViolation();
      }
      run.assertNoAcknowledgedViolation();
      if (fault.blocksAcknowledgements) {
        assertEquals(0, run.acknowledgedDuringFault.get(), fault + " must stop every commit acknowledgement");
      } else {
        assertTrue(run.acknowledgedDuringFault.get() > 0, "commits keep landing over the slow link");
      }
      assertEquals(RECORD_COUNT, run.completed.size(), "every record completes once the fault heals");
    }
  }

  /// When the broker comes back relative to a `close()` that started while it was unreachable.
  enum Return {
    /// After `close()` has returned. The final commit fails, unless `close()` had to interrupt
    /// the consumer thread: that thread is still tearing down and commits once the broker is
    /// back, as a `SEQUENTIAL` consumer held by a record does.
    AFTER_CLOSE,
    /// While `close()` is still draining, so the final commit lands and has to stop below every
    /// record the shutdown abandoned.
    DURING_CLOSE,
  }

  static List<Arguments> returnsByMode() {
    final var arguments = new ArrayList<Arguments>();
    for (final var mode : ProcessingMode.values()) {
      for (final var brokerReturns : Return.values()) arguments.add(Arguments.of(mode, brokerReturns));
    }
    return arguments;
  }

  @ParameterizedTest(name = "{0}, broker returns {1}")
  @MethodSource("returnsByMode")
  void recordsLeftUnfinishedWhenTheBrokerVanishesAtShutdownAreRedelivered(
    final ProcessingMode mode,
    final Return brokerReturns
  ) throws Exception {
    try (final var run = new Run(mode, Set.of(RELEASED_AFTER_HEAL))) {
      final var first = run.consumer();
      final Set<String> finishedByFirst;
      try (final var sampler = new CommitSampler(run)) {
        first.start();
        run.awaitCompleted(20, Duration.ofSeconds(60));
        TimeUnit.MILLISECONDS.sleep(HEALTHY_WINDOW.toMillis());
        Fault.UNREACHABLE.apply();
        Fault.UNREACHABLE.assertEngaged();
        // The held records are interrupted by the shutdown and stay unfinished.
        final var closing = CompletableFuture.runAsync(first::close);
        if (brokerReturns == Return.DURING_CLOSE) {
          TimeUnit.MILLISECONDS.sleep(HEALTHY_WINDOW.toMillis());
          Fault.heal();
        }
        closing.get(90, TimeUnit.SECONDS);
        Fault.heal();
        finishedByFirst = Set.copyOf(run.completed);
        assertTrue(finishedByFirst.size() < RECORD_COUNT, "the first consumer must stop with records unfinished");

        run.releaseAll();
        final var second = run.consumer();
        try {
          second.start();
          run.awaitCompletedBy(second, missingFrom(run, finishedByFirst), Duration.ofSeconds(120));
          run.awaitCommittedToLogEnd(Duration.ofSeconds(30));
        } finally {
          second.close();
        }
        sampler.assertNoViolation();
      }
      run.assertNoAcknowledgedViolation();
    }
  }

  /// A record that cannot be processed finishes only once it is in the dead-letter topic. While
  /// that topic cannot be reached, the record stays pending and pins the commit, the consumer goes
  /// on processing everything else, and the failed send is counted. Once the topic can be reached,
  /// the next consumer receives the record again and parks it there.
  @ParameterizedTest(name = "{0}")
  @MethodSource("modes")
  void aRecordWhoseDeadLetterSendFailsHoldsTheCommitUntilItIsParked(final ProcessingMode mode) throws Exception {
    final var failingOffsets = Set.of(RELEASED_DURING_FAULT, RELEASED_AFTER_HEAL);
    try (final var run = new Run(mode, Set.of(), failingOffsets)) {
      final var dlqTopic = run.topic + "-dlq";
      try (final var admin = Admin.create(directAdminProperties())) {
        admin.createTopics(List.of(new NewTopic(dlqTopic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
      }
      try (final var sampler = new CommitSampler(run)) {
        dlqProxy.disable();
        assertTrue(!probeReachesBroker(dlqBootstrap, Duration.ofSeconds(3)), "the DLQ must be unreachable");
        final var unreachableDlq = dlqProducer(run, true);
        final var first = run.consumer(
          dlqTopic,
          KPipeProducer.<byte[], byte[]>builder().withProducer(unreachableDlq).build()
        );
        first.start();
        try {
          run.awaitCompleted(RECORD_COUNT - run.failing.size(), Duration.ofSeconds(120));
          final var deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
          while (dlqFailed(first) < run.failing.size() && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(100);
          }
          assertTrue(
            dlqFailed(first) >= run.failing.size(),
            "every failed DLQ send is counted, got " + dlqFailed(first)
          );
          run.awaitCommittedAt(RELEASED_DURING_FAULT, Duration.ofSeconds(20));
          TimeUnit.MILLISECONDS.sleep(HEALTHY_WINDOW.toMillis());
          run.awaitCommittedAt(RELEASED_DURING_FAULT, Duration.ofSeconds(1));
        } finally {
          first.close();
          unreachableDlq.close(Duration.ZERO);
        }
        Fault.heal();

        final var reachableDlq = dlqProducer(run, false);
        final var second = run.consumer(
          dlqTopic,
          KPipeProducer.<byte[], byte[]>builder().withProducer(reachableDlq).build()
        );
        try {
          second.start();
          run.awaitCommittedToLogEnd(Duration.ofSeconds(60));
        } finally {
          second.close();
          reachableDlq.close();
        }
        sampler.assertNoViolation();
      }
      run.assertNoAcknowledgedViolation();
      final var parked = dlqValues(dlqTopic, run.failing.size(), Duration.ofSeconds(30));
      assertTrue(parked.containsAll(run.failing), "every failing record is parked in the DLQ; parked " + parked);
    }
  }

  static List<ProcessingMode> modes() {
    return List.of(ProcessingMode.values());
  }

  private static long dlqFailed(final KPipeConsumer consumer) {
    return consumer.getMetrics().getOrDefault(ConsumerMetricKeys.DLQ_FAILED, 0L);
  }

  /// A dead-letter producer on the DLQ's own proxy. A failing record counts as completed only once
  /// the broker has acknowledged it in the DLQ: Kafka runs a send's callback before the send's
  /// future completes, so a commit can pass the record only after it is parked. With
  /// `failFast`, a send that cannot reach the broker gives up within about two seconds rather
  /// than Kafka's default of a minute.
  private static KafkaProducer<byte[], byte[]> dlqProducer(final Run run, final boolean failFast) {
    final var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, dlqBootstrap);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    if (failFast) {
      props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "2000");
      props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
      props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "3000");
    }
    return new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer()) {
      @Override
      public Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record, final Callback callback) {
        final var value = new String(record.value());
        return super.send(record, (metadata, exception) -> {
          if (exception == null) run.completed.add(value);
          if (callback != null) callback.onCompletion(metadata, exception);
        });
      }
    };
  }

  /// The values in `dlqTopic`, read on the broker's own listener until `expected` have arrived.
  private static Set<String> dlqValues(final String dlqTopic, final int expected, final Duration timeout) {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    final var values = new HashSet<String>();
    try (final var reader = new KafkaConsumer<byte[], byte[]>(props)) {
      final var partition = new TopicPartition(dlqTopic, 0);
      reader.assign(List.of(partition));
      reader.seekToBeginning(List.of(partition));
      final var deadline = System.nanoTime() + timeout.toNanos();
      while (values.size() < expected && System.nanoTime() < deadline) {
        for (final var record : reader.poll(Duration.ofMillis(200))) values.add(new String(record.value()));
      }
    }
    return values;
  }

  private static Set<String> missingFrom(final Run run, final Set<String> finished) {
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
  private static final class Run implements AutoCloseable {

    private final ProcessingMode mode;
    private final String topic = "fault-" + System.nanoTime();
    private final String groupId = "fault-group-" + System.nanoTime();
    /// The partition and offset each produced value landed at.
    private final Map<String, TopicPartition> partitionOf = new HashMap<>();
    private final Map<String, Long> offsetOf = new HashMap<>();
    /// The offset each held value is released with, keyed by value.
    private final Map<String, Long> heldUntil = new HashMap<>();
    private final Map<Long, CountDownLatch> releases = new HashMap<>();
    /// Values whose processing always throws, so they can finish only in the dead-letter topic.
    private final Set<String> failing = new HashSet<>();
    /// Values completed by any consumer of this scenario, and by each consumer separately.
    private final Set<String> completed = ConcurrentHashMap.newKeySet();
    private final Map<KPipeConsumer, Set<String>> completedBy = new ConcurrentHashMap<>();
    private final List<String> acknowledgedViolations = new CopyOnWriteArrayList<>();
    private final AtomicBoolean faultInEffect = new AtomicBoolean();
    private final AtomicInteger acknowledgedDuringFault = new AtomicInteger();

    Run(final ProcessingMode mode, final Set<Long> heldOffsets) throws Exception {
      this(mode, heldOffsets, Set.of());
    }

    Run(final ProcessingMode mode, final Set<Long> heldOffsets, final Set<Long> failingOffsets) throws Exception {
      this.mode = mode;
      for (final var offset : heldOffsets) releases.put(offset, new CountDownLatch(1));
      try (final var admin = Admin.create(directAdminProperties())) {
        admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get(30, TimeUnit.SECONDS);
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
        .withProperties(proxiedConsumerProperties(groupId, mode))
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
      try (final var admin = Admin.create(directAdminProperties())) {
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
      try (final var admin = Admin.create(directAdminProperties())) {
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
      props.put("bootstrap.servers", kafka.getBootstrapServers());
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
  private static final class CommitSampler implements AutoCloseable {

    private final Run run;
    private final List<String> violations = new CopyOnWriteArrayList<>();
    private final AtomicInteger samples = new AtomicInteger();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread thread;

    CommitSampler(final Run run) {
      this.run = run;
      this.thread = Thread.ofPlatform().daemon().name("commit-sampler").start(this::sample);
    }

    private void sample() {
      try (final var admin = Admin.create(directAdminProperties())) {
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
  private static boolean probeReachesBroker(final String bootstrap, final Duration timeout) {
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

  private static Properties directAdminProperties() {
    final var props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    return props;
  }

  /// A client API call gives up after five seconds, so a commit stalled by a fault fails within the
  /// fault window instead of outlasting the test. A `SEQUENTIAL` consumer commits only between
  /// polls, so it fetches a few records at a time and keeps committing through the fault.
  private static Properties proxiedConsumerProperties(final String groupId, final ProcessingMode mode) {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, CommitRecorder.class.getName());
    if (mode == ProcessingMode.SEQUENTIAL) props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }
}
