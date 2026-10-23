package io.github.eschizoid.kpipe.consumer;

import static io.github.eschizoid.kpipe.consumer.FaultHarness.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.rekawek.toxiproxy.ToxiproxyClient;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

/// The at-least-once guarantee on a three-broker cluster whose brokers stop answering mid-run.
/// A separate controller node holds the KRaft quorum, so pausing brokers never costs the cluster
/// its controller and the controller goes on fencing brokers and shrinking ISRs. Each scenario's
/// topic keeps every partition on all three brokers, led by broker 1, and `__consumer_offsets`
/// requires two in-sync replicas. A fault pauses one or two
/// broker containers, which freezes them the way a hung host does, and resumes them once the
/// fault window ends, so each broker comes back with its data.
///
/// The checks are the single-broker fault tests' (see [FaultHarness]): no committed offset may
/// pass a record that has not completed, records are held so the frontier has somewhere it must
/// stop, and once the cluster recovers every record completes and the commits reach the log end.
/// Each fault also fixes where the group's coordinator is, and whether commits must keep landing
/// while the fault lasts.
///
/// CI-RUN-REQUIRED: Testcontainers starts a controller, three Kafka brokers and a Toxiproxy
/// container (needs a Docker daemon).
class MultiBrokerFaultIntegrationTest {

  private static final String KAFKA_IMAGE = "apache/kafka:" + System.getProperty("kafkaVersion", "4.3.0");
  private static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0";
  private static final int BROKERS = 3;
  private static final int CONTROLLER_ID = 4;
  private static final int FIRST_PROXY_PORT = 8666;
  private static final String CLUSTER_ID = "MkU3OEVBNTcwNTJENDM2Qk";
  /// Long enough for the controller to fence a paused broker and move its leaderships, for the
  /// client's request to a silent coordinator to time out, and for commits to resume on the
  /// brokers left.
  private static final Duration FAULT_WINDOW = Duration.ofSeconds(20);
  /// Acknowledgements still in flight when the fault starts are not counted against it.
  private static final Duration FAULT_SETTLE = Duration.ofSeconds(1);

  private static Network network;
  private static ToxiproxyContainer toxiproxy;
  private static GenericContainer<?> controller;
  private static final List<GenericContainer<?>> brokers = new ArrayList<>();
  private static String bootstrap;
  /// Broker 1 alone, for a check made while the other brokers are paused: a client that tried a
  /// paused broker first would wait out its request timeout.
  private static String broker1;

  @BeforeAll
  static void startCluster() throws IOException {
    network = Network.newNetwork();
    toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE).withNetwork(network);
    toxiproxy.start();
    final var client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
    final var addresses = new ArrayList<String>();
    for (int id = 1; id <= BROKERS; id++) {
      // Each broker advertises a proxy's host port. The proxies inject no faults here; they give
      // every broker an address the test JVM can reach that is known before the broker starts.
      final var port = FIRST_PROXY_PORT + id - 1;
      final var external = toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(port);
      addresses.add(external);
      client.createProxy("kafka" + id, "0.0.0.0:" + port, "kafka" + id + ":19092");
      brokers.add(broker(id, external));
    }
    bootstrap = String.join(",", addresses);
    broker1 = addresses.get(0);
    controller = kafkaNode(CONTROLLER_ID, "controller")
      .withEnv("KAFKA_PROCESS_ROLES", "controller")
      .withEnv("KAFKA_LISTENERS", "CONTROLLER://:9093")
      .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT");
    final var nodes = new ArrayList<GenericContainer<?>>(brokers);
    nodes.add(controller);
    Startables.deepStart(nodes).join();
    requireTwoInSyncOffsetReplicas();
  }

  /// Sets `min.insync.replicas` to two on `__consumer_offsets` itself. In this cluster the topic
  /// otherwise comes up requiring one in-sync replica, and a commit is then held to the leader's
  /// copy alone, so a fault could never leave the commits below their minimum.
  private static void requireTwoInSyncOffsetReplicas() {
    try (final var admin = Admin.create(directAdminProperties(bootstrap))) {
      // Looking up a group's offsets makes the brokers create `__consumer_offsets`.
      admin.listConsumerGroupOffsets("bootstrap").partitionsToOffsetAndMetadata().get(60, TimeUnit.SECONDS);
      final var resource = new ConfigResource(ConfigResource.Type.TOPIC, "__consumer_offsets");
      final var minIsr = new AlterConfigOp(new ConfigEntry("min.insync.replicas", "2"), AlterConfigOp.OpType.SET);
      admin.incrementalAlterConfigs(Map.of(resource, List.of(minIsr))).all().get(30, TimeUnit.SECONDS);
    } catch (final Exception e) {
      throw new IllegalStateException("could not require two in-sync replicas on __consumer_offsets", e);
    }
  }

  private static GenericContainer<?> broker(final int id, final String external) {
    return kafkaNode(id, "kafka" + id)
      .withEnv("KAFKA_PROCESS_ROLES", "broker")
      .withEnv("KAFKA_LISTENERS", "INTERNAL://:9092,EXTERNAL://:19092")
      .withEnv("KAFKA_ADVERTISED_LISTENERS", "INTERNAL://kafka" + id + ":9092,EXTERNAL://" + external)
      .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT")
      .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
      .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3")
      .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3")
      .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2")
      .withEnv("KAFKA_MIN_INSYNC_REPLICAS", "2")
      .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
      // Drop a lagging follower from the ISR within a few seconds, so each fault's effect on the
      // ISR lands inside the fault window.
      .withEnv("KAFKA_REPLICA_LAG_TIME_MAX_MS", "5000");
  }

  private static GenericContainer<?> kafkaNode(final int id, final String alias) {
    return new GenericContainer<>(KAFKA_IMAGE)
      .withNetwork(network)
      .withNetworkAliases(alias)
      .withEnv("CLUSTER_ID", CLUSTER_ID)
      .withEnv("KAFKA_NODE_ID", String.valueOf(id))
      .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", CONTROLLER_ID + "@controller:9093")
      .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
      // Fence a broker that stops heartbeating within a few seconds.
      .withEnv("KAFKA_BROKER_HEARTBEAT_INTERVAL_MS", "1000")
      .withEnv("KAFKA_BROKER_SESSION_TIMEOUT_MS", "4000")
      .withEnv("KAFKA_HEAP_OPTS", "-Xmx512m -Xms512m")
      .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1).withStartupTimeout(Duration.ofMinutes(3)));
  }

  @AfterAll
  static void stopCluster() {
    brokers.forEach(GenericContainer::stop);
    if (controller != null) controller.stop();
    if (toxiproxy != null) toxiproxy.stop();
    if (network != null) network.close();
  }

  @AfterEach
  void resumeBrokers() {
    for (final var broker : brokers) resume(broker);
  }

  /// Hands every partition back to its preferred replica, so each scenario starts from the same
  /// leaders whatever the one before it paused.
  @BeforeEach
  void restorePreferredLeaders() throws Exception {
    try (final var admin = Admin.create(directAdminProperties(bootstrap))) {
      final var deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
      while (true) {
        try {
          admin.electLeaders(ElectionType.PREFERRED, null).all().get(15, TimeUnit.SECONDS);
        } catch (final ExecutionException e) {
          // A partition already led by its preferred replica reports an error; the check below
          // is what decides.
        }
        final var topics = admin
          .listTopics(new ListTopicsOptions().listInternal(true))
          .names()
          .get(15, TimeUnit.SECONDS);
        final var off = admin
          .describeTopics(topics)
          .allTopicNames()
          .get(15, TimeUnit.SECONDS)
          .values()
          .stream()
          .flatMap(topic -> topic.partitions().stream())
          .filter(p -> p.leader() == null || p.leader().id() != p.replicas().get(0).id())
          .count();
        if (off == 0) return;
        if (System.nanoTime() > deadline) throw new AssertionError(
          off + " partitions are not led by their preferred replica"
        );
        TimeUnit.MILLISECONDS.sleep(500);
      }
    }
  }

  /// Every partition of a scenario's topic is replicated on all three brokers with broker 1 as its
  /// preferred leader, so each fault knows which broker leads and which follow.
  private static Cluster cluster() {
    return new Cluster(bootstrap, bootstrap, List.of(1, 2, 3));
  }

  /// A fault on the cluster: the brokers it pauses, where the group's coordinator must be for the
  /// fault to test what it says, and whether commits must keep landing while it lasts.
  enum BrokerFault {
    /// The broker leading every partition of the topic stops answering, so the controller elects
    /// new leaders among the followers. The coordinator is elsewhere, so commits carry on.
    LEADER_PAUSED(List.of(1), coordinator -> coordinator != 1, true),
    /// A follower stops answering, so the controller drops it from the ISR and the partitions
    /// carry on with two in-sync replicas.
    FOLLOWER_PAUSED(List.of(3), coordinator -> coordinator != 3, true),
    /// The broker coordinating the consumer group stops answering, so the group moves to the
    /// broker that takes over its `__consumer_offsets` partition. The topic's leader is not it.
    COORDINATOR_PAUSED(List.of(), coordinator -> coordinator != 1, true),
    /// Both followers stop answering. Broker 1 leads the topic and coordinates the group, and the
    /// controller shrinks the coordinator's `__consumer_offsets` partition to broker 1 alone, below
    /// the two in-sync replicas it requires, so the coordinator refuses every commit.
    TWO_FOLLOWERS_PAUSED(List.of(2, 3), coordinator -> coordinator == 1, false);

    private final List<Integer> fixedBrokers;
    final IntPredicate acceptsCoordinator;
    /// Whether commits must keep being acknowledged while the fault lasts.
    final boolean commitsContinue;

    BrokerFault(
      final List<Integer> fixedBrokers,
      final IntPredicate acceptsCoordinator,
      final boolean commitsContinue
    ) {
      this.fixedBrokers = fixedBrokers;
      this.acceptsCoordinator = acceptsCoordinator;
      this.commitsContinue = commitsContinue;
    }

    List<Integer> brokersToPause(final int coordinator) {
      return fixedBrokers.isEmpty() ? List.of(coordinator) : fixedBrokers;
    }
  }

  static List<Arguments> faultsByMode() {
    final var arguments = new ArrayList<Arguments>();
    for (final var mode : ProcessingMode.values()) {
      for (final var fault : BrokerFault.values()) arguments.add(Arguments.of(mode, fault));
    }
    return arguments;
  }

  @ParameterizedTest(name = "{0} with {1}")
  @MethodSource("faultsByMode")
  void noCommitPassesAnUnfinishedRecordWhileBrokersStopAnswering(final ProcessingMode mode, final BrokerFault fault)
    throws Exception {
    // A held SEQUENTIAL record would stop the consumer's commits, leaving nothing for the fault to
    // test, so that mode runs every record slowly and holds none.
    final var holds =
      mode == ProcessingMode.SEQUENTIAL ? Set.<Long>of() : Set.of(RELEASED_DURING_FAULT, RELEASED_AFTER_HEAL);
    final var groupId = groupCoordinatedWhere(fault.acceptsCoordinator);
    try (final var run = new Run(cluster(), mode, holds, Set.of(), groupId)) {
      assertLedByBroker1(run);
      final var coordinator = expectedCoordinatorOf(groupId);
      final var paused = fault.brokersToPause(coordinator);
      final var consumer = run.consumer();
      try (final var sampler = new CommitSampler(run)) {
        consumer.start();
        try {
          run.awaitCompleted(5, Duration.ofSeconds(60));
          assertEquals(coordinator, coordinatorOf(groupId), "the group is coordinated where the fault expects");
          TimeUnit.MILLISECONDS.sleep(HEALTHY_WINDOW.toMillis());
          for (final var id : paused) pause(brokers.get(id - 1));
          TimeUnit.MILLISECONDS.sleep(FAULT_SETTLE.toMillis());
          run.faultInEffect.set(true);
          run.release(RELEASED_DURING_FAULT);
          TimeUnit.MILLISECONDS.sleep(FAULT_WINDOW.minus(FAULT_SETTLE).toMillis());
          if (fault == BrokerFault.TWO_FOLLOWERS_PAUSED) assertOffsetsPartitionBelowMinIsr(groupId);
          run.faultInEffect.set(false);
          for (final var id : paused) resume(brokers.get(id - 1));
          if (!holds.isEmpty()) run.awaitCommittedAt(RELEASED_AFTER_HEAL, Duration.ofSeconds(60));
          run.release(RELEASED_AFTER_HEAL);
          run.awaitCompleted(RECORD_COUNT, Duration.ofSeconds(120));
          run.awaitCommittedToLogEnd(Duration.ofSeconds(60));
        } finally {
          run.releaseAll();
          consumer.close();
        }
        sampler.assertNoViolation();
      }
      run.assertNoAcknowledgedViolation();
      if (fault.commitsContinue) {
        assertTrue(run.acknowledgedDuringFault.get() > 0, "commits resume on the brokers left under " + fault);
      } else {
        assertEquals(0, run.acknowledgedDuringFault.get(), fault + " must leave no commit acknowledged");
      }
      assertEquals(RECORD_COUNT, run.completed.size(), "every record completes once the brokers return");
    }
  }

  /// A fresh group id whose coordinator satisfies `accepts`. Kafka places a group's coordinator by
  /// hashing its id onto a `__consumer_offsets` partition, so trying ids finds one in a few tries.
  private static String groupCoordinatedWhere(final IntPredicate accepts) throws Exception {
    for (int attempt = 0; attempt < 100; attempt++) {
      final var candidate = "fault-group-" + System.nanoTime();
      if (accepts.test(expectedCoordinatorOf(candidate))) return candidate;
    }
    throw new AssertionError("no group id found with a suitable coordinator");
  }

  /// The broker that will coordinate `groupId`: the leader of the `__consumer_offsets` partition
  /// Kafka places the group on, `abs(groupId.hashCode())` modulo the topic's partition count. A
  /// group with no members has no description to ask, so this is worked out rather than asked;
  /// [#coordinatorOf] confirms it once the consumer has joined.
  private static int expectedCoordinatorOf(final String groupId) throws Exception {
    try (final var admin = Admin.create(directAdminProperties(bootstrap))) {
      // Looking up a group's offsets makes the brokers create `__consumer_offsets` if it is not
      // there yet.
      admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
      final var partitions = admin
        .describeTopics(List.of("__consumer_offsets"))
        .allTopicNames()
        .get(15, TimeUnit.SECONDS)
        .get("__consumer_offsets")
        .partitions();
      final var hash = groupId.hashCode();
      final var index = (hash == Integer.MIN_VALUE ? 0 : Math.abs(hash)) % partitions.size();
      return partitions.get(index).leader().id();
    }
  }

  /// Checks that the fault really left the group's `__consumer_offsets` partition below the
  /// in-sync replicas it requires: one in-sync replica, against a minimum of two.
  private static void assertOffsetsPartitionBelowMinIsr(final String groupId) throws Exception {
    final var props = directAdminProperties(broker1);
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");
    try (final var admin = Admin.create(props)) {
      final var partitions = admin
        .describeTopics(List.of("__consumer_offsets"))
        .allTopicNames()
        .get(15, TimeUnit.SECONDS)
        .get("__consumer_offsets")
        .partitions();
      final var hash = groupId.hashCode();
      final var partition = partitions.get((hash == Integer.MIN_VALUE ? 0 : Math.abs(hash)) % partitions.size());
      assertEquals(
        1,
        partition.isr().size(),
        "the group's offsets partition shrinks to one replica; ISR " + partition.isr()
      );
      final var resource = new ConfigResource(ConfigResource.Type.TOPIC, "__consumer_offsets");
      final var minIsr = admin
        .describeConfigs(List.of(resource))
        .all()
        .get(15, TimeUnit.SECONDS)
        .get(resource)
        .get("min.insync.replicas")
        .value();
      assertEquals("2", minIsr, "__consumer_offsets requires two in-sync replicas");
    }
  }

  /// The broker coordinating `groupId`, as Kafka reports it once the group has members.
  private static int coordinatorOf(final String groupId) throws Exception {
    try (final var admin = Admin.create(directAdminProperties(bootstrap))) {
      return admin
        .describeConsumerGroups(List.of(groupId))
        .all()
        .get(15, TimeUnit.SECONDS)
        .get(groupId)
        .coordinator()
        .id();
    }
  }

  /// Waits for broker 1, every partition's preferred replica, to lead them all, so a fault that
  /// pauses broker 1 pauses the leader and one that pauses the others leaves the leader alone.
  private static void assertLedByBroker1(final Run run) throws Exception {
    try (final var admin = Admin.create(directAdminProperties(bootstrap))) {
      final var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      List<Integer> leaders;
      do {
        leaders = admin
          .describeTopics(List.of(run.topic))
          .allTopicNames()
          .get(15, TimeUnit.SECONDS)
          .get(run.topic)
          .partitions()
          .stream()
          .map(partition -> partition.leader() == null ? -1 : partition.leader().id())
          .toList();
        if (leaders.stream().allMatch(leader -> leader == 1)) return;
        TimeUnit.MILLISECONDS.sleep(200);
      } while (System.nanoTime() < deadline);
      throw new AssertionError("broker 1 must lead every partition before the fault; leaders " + leaders);
    }
  }

  private static void pause(final GenericContainer<?> broker) {
    broker.getDockerClient().pauseContainerCmd(broker.getContainerId()).exec();
  }

  private static void resume(final GenericContainer<?> broker) {
    final var state = broker.getDockerClient().inspectContainerCmd(broker.getContainerId()).exec().getState();
    if (Boolean.TRUE.equals(state.getPaused())) {
      broker.getDockerClient().unpauseContainerCmd(broker.getContainerId()).exec();
    }
  }
}
