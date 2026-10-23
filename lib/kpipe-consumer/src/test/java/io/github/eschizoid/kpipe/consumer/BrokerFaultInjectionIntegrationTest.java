package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
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
/// goes through Toxiproxy, which cuts, stalls, slows or resets it mid-run. The checks themselves
/// reach the broker on its own listener, so a fault never hides what they read.
///
/// The property under test is the one the commit frontier exists for: no committed offset ever
/// passes a record that has not completed. A sampler reads the group's committed offsets
/// throughout each run and checks every one against the set of completed records. Some records
/// are held until the fault has healed, so a commit that skipped past an unfinished record would
/// show up as a committed offset above a held one.
///
/// CI-RUN-REQUIRED: Testcontainers starts a Kafka broker and a Toxiproxy container (needs a Docker
/// daemon).
class BrokerFaultInjectionIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");
  private static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0";
  private static final int PROXY_PORT = 8666;
  private static final String PROXIED_LISTENER = "kafka:19092";

  private static final int PARTITIONS = 3;
  private static final int RECORD_COUNT = 300;
  /// Records at these offsets mod 50, in every partition, stay unfinished until the fault has
  /// healed and the link has stayed healthy for [#HEALTHY_WINDOW].
  private static final int HELD_EVERY = 50;
  private static final int HELD_AT = 7;
  /// A `SEQUENTIAL` consumer runs records on its own thread, so holding one would stop its polls
  // and
  /// commits for the whole fault. Each record takes this long instead, which keeps it mid-stream.
  private static final Duration SEQUENTIAL_RECORD_TIME = Duration.ofMillis(20);
  private static final Duration FAULT_WINDOW = Duration.ofSeconds(5);
  private static final Duration SLOW_LINK_LATENCY = Duration.ofSeconds(1);
  private static final Duration COMMIT_INTERVAL = Duration.ofMillis(200);
  /// How long the link stays healthy with held records outstanding, before the fault and again
  // after
  /// it heals: several commit intervals, so commits land while the frontier has to stop below them.
  private static final Duration HEALTHY_WINDOW = COMMIT_INTERVAL.multipliedBy(5);

  private static Network network;
  private static ToxiproxyContainer toxiproxy;
  private static KafkaContainer kafka;
  private static Proxy proxy;
  private static String proxiedBootstrap;

  @BeforeAll
  static void startBroker() throws IOException {
    network = Network.newNetwork();
    toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE).withNetwork(network);
    toxiproxy.start();
    // The broker advertises the proxy's host address on this listener, so after bootstrap every
    // connection the client opens still goes through the proxy rather than straight to the broker.
    proxiedBootstrap = toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(PROXY_PORT);
    kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:%s".formatted(KAFKA_VERSION)))
      .withNetwork(network)
      .withNetworkAliases("kafka")
      .withListener(PROXIED_LISTENER, () -> proxiedBootstrap);
    kafka.start();
    proxy = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort()).createProxy(
      "kafka",
      "0.0.0.0:" + PROXY_PORT,
      PROXIED_LISTENER
    );
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

  /// A broker fault applied to every connection the consumer has, and how to tell it took effect.
  enum Fault {
    /// Connections are refused: the broker is unreachable.
    UNREACHABLE {
      @Override
      void apply() throws IOException {
        proxy.disable();
      }
    },
    /// Connections stay open but nothing comes back, so a commit request can reach the broker and
    /// still time out on the client.
    BLACK_HOLE {
      @Override
      void apply() throws IOException {
        proxy.toxics().timeout("black-hole", ToxicDirection.DOWNSTREAM, 0);
      }
    },
    /// Every response is delayed.
    SLOW_LINK {
      @Override
      void apply() throws IOException {
        proxy.toxics().latency("slow-link", ToxicDirection.DOWNSTREAM, SLOW_LINK_LATENCY.toMillis());
      }

      @Override
      void assertEngaged() {
        final var started = System.nanoTime();
        assertTrue(probeReachesBroker(Duration.ofSeconds(20)), "the slow link still reaches the broker");
        final var elapsed = Duration.ofNanos(System.nanoTime() - started);
        assertTrue(elapsed.compareTo(SLOW_LINK_LATENCY) >= 0, "a request through the slow link took " + elapsed);
      }
    },
    /// Every connection is reset as soon as data arrives.
    CONNECTION_RESET {
      @Override
      void apply() throws IOException {
        proxy.toxics().resetPeer("connection-reset", ToxicDirection.DOWNSTREAM, 0);
      }
    };

    abstract void apply() throws IOException;

    /// Fails the test if the fault is not in effect, so a scenario cannot pass against a link that
    /// was never actually broken.
    void assertEngaged() {
      assertTrue(!probeReachesBroker(Duration.ofSeconds(3)), name() + " must stop requests reaching the broker");
    }

    static void heal() throws IOException {
      proxy.enable();
      for (final var toxic : proxy.toxics().getAll()) toxic.remove();
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
    final var run = new Run(mode);
    try (final var sampler = new CommitSampler(run)) {
      final var consumer = run.consumer();
      consumer.start();
      try {
        run.awaitCompleted(5, Duration.ofSeconds(60));
        TimeUnit.MILLISECONDS.sleep(HEALTHY_WINDOW.toMillis());
        fault.apply();
        fault.assertEngaged();
        TimeUnit.MILLISECONDS.sleep(FAULT_WINDOW.toMillis());
        Fault.heal();
        TimeUnit.MILLISECONDS.sleep(HEALTHY_WINDOW.toMillis());
        run.releaseHeld();
        run.awaitCompleted(RECORD_COUNT, Duration.ofSeconds(120));
        run.awaitCommittedToLogEnd(Duration.ofSeconds(30));
      } finally {
        run.releaseHeld();
        consumer.close();
      }
      sampler.assertNoViolation();
    }
    assertEquals(RECORD_COUNT, run.completed.size(), "every record completes once the fault heals");
  }

  /// When the broker comes back relative to a `close()` that started while it was unreachable.
  enum Return {
    /// After `close()` has returned, so the final commit fails.
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
    final var run = new Run(mode);
    try (final var sampler = new CommitSampler(run)) {
      final var first = run.consumer();
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
      assertTrue(run.completed.size() < RECORD_COUNT, "the first consumer must stop with records unfinished");

      run.releaseHeld();
      final var second = run.consumer();
      try {
        second.start();
        run.awaitCompleted(RECORD_COUNT, Duration.ofSeconds(120));
        run.awaitCommittedToLogEnd(Duration.ofSeconds(30));
      } finally {
        second.close();
      }
      sampler.assertNoViolation();
    }
    assertEquals(RECORD_COUNT, run.completed.size(), "the next consumer receives every record left unfinished");
  }

  /// One scenario's topic, group and record bookkeeping.
  private static final class Run {

    private final ProcessingMode mode;
    private final String topic = "fault-" + System.nanoTime();
    private final String groupId = "fault-group-" + System.nanoTime();
    /// Where each produced value landed, so a committed offset can be checked record by record.
    private final Map<String, RecordMetadata> positions;
    private final Set<String> held;
    private final Set<String> completed = ConcurrentHashMap.newKeySet();
    private final CountDownLatch release = new CountDownLatch(1);

    Run(final ProcessingMode mode) throws Exception {
      this.mode = mode;
      try (final var admin = Admin.create(directAdminProperties())) {
        admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get(30, TimeUnit.SECONDS);
      }
      positions = produce();
      held =
        mode == ProcessingMode.SEQUENTIAL
          ? Set.of()
          : positions
              .keySet()
              .stream()
              .filter(value -> positions.get(value).offset() % HELD_EVERY == HELD_AT)
              .collect(Collectors.toUnmodifiableSet());
    }

    KPipeConsumer consumer() {
      return KPipeConsumer.builder()
        .withProperties(proxiedConsumerProperties(groupId))
        .withTopic(topic)
        .withProcessingMode(mode)
        .withCommitInterval(COMMIT_INTERVAL)
        .withPipeline(
          TestPipelines.sideEffect(value -> {
            final var text = new String(value);
            try {
              if (held.contains(text)) release.await();
              if (mode == ProcessingMode.SEQUENTIAL) TimeUnit.MILLISECONDS.sleep(SEQUENTIAL_RECORD_TIME.toMillis());
            } catch (final InterruptedException e) {
              // Keep the interruption in the cause chain, so the record is abandoned at
              // shutdown
              // rather than failed and committed past.
              Thread.currentThread().interrupt();
              throw new IllegalStateException("interrupted at shutdown", e);
            }
            completed.add(text);
            return value;
          })
        )
        .build();
    }

    void releaseHeld() {
      release.countDown();
    }

    void awaitCompleted(final int target, final Duration timeout) throws InterruptedException {
      final var deadline = System.nanoTime() + timeout.toNanos();
      while (completed.size() < target && System.nanoTime() < deadline) TimeUnit.MILLISECONDS.sleep(50);
      assertTrue(completed.size() >= target, "completed " + completed.size() + " of the " + target + " awaited");
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

    /// Records whose partition is `partition` and whose offset is below `committed` but which have
    /// not completed: each one is a commit that passed an unfinished record.
    List<String> unfinishedBelow(final TopicPartition partition, final long committed) {
      return positions
        .entrySet()
        .stream()
        .filter(entry -> entry.getValue().partition() == partition.partition())
        .filter(entry -> entry.getValue().offset() < committed)
        .map(Map.Entry::getKey)
        .filter(value -> !completed.contains(value))
        .sorted()
        .toList();
    }

    private Map<String, RecordMetadata> produce() throws Exception {
      final var props = new Properties();
      props.put("bootstrap.servers", kafka.getBootstrapServers());
      props.put("acks", "all");
      final var sends = new HashMap<String, java.util.concurrent.Future<RecordMetadata>>();
      try (final var producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
        for (int i = 0; i < RECORD_COUNT; i++) {
          final var value = "val-" + i;
          sends.put(value, producer.send(new ProducerRecord<>(topic, ("key-" + i).getBytes(), value.getBytes())));
        }
        producer.flush();
      }
      final var landed = new HashMap<String, RecordMetadata>();
      for (final var send : sends.entrySet()) landed.put(send.getKey(), send.getValue().get(30, TimeUnit.SECONDS));
      return Map.copyOf(landed);
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

  /// Whether a fresh client can reach the broker through the proxy within `timeout`.
  private static boolean probeReachesBroker(final Duration timeout) {
    final var props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrap);
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
  /// fault window instead of outlasting the test.
  private static Properties proxiedConsumerProperties(final String groupId) {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }
}
