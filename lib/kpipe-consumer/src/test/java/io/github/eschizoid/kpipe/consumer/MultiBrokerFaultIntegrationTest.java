package io.github.eschizoid.kpipe.consumer;

import static io.github.eschizoid.kpipe.consumer.FaultHarness.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.rekawek.toxiproxy.ToxiproxyClient;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

/// The at-least-once guarantee on a three-broker cluster whose brokers stop answering mid-run.
/// Each scenario's topic keeps three replicas of every partition, led by broker 1, and the brokers
/// require two in-sync replicas, as `__consumer_offsets` does. A fault pauses one or two broker
/// containers, which freezes them the way a hung host does, and resumes them once the fault window
/// ends, so each broker comes back with its data.
///
/// The checks are the single-broker fault tests' (see [FaultHarness]): no committed offset may
/// pass a record that has not completed, records are held so the frontier has somewhere it must
/// stop, and once the cluster recovers every record completes and the commits reach the log end.
/// Each fault also states whether commits must keep landing while it lasts.
///
/// CI-RUN-REQUIRED: Testcontainers starts three Kafka brokers and a Toxiproxy container (needs a
/// Docker daemon).
class MultiBrokerFaultIntegrationTest {

  private static final String KAFKA_IMAGE = "apache/kafka:" + System.getProperty("kafkaVersion", "4.3.0");
  private static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0";
  private static final int BROKERS = 3;
  private static final int FIRST_PROXY_PORT = 8666;
  private static final String CLUSTER_ID = "MkU3OEVBNTcwNTJENDM2Qk";
  /// Long enough for the controller to fence a paused broker and move its leaderships, for the
  /// group to find a new coordinator, and for commits to resume on the brokers left.
  private static final Duration FAULT_WINDOW = Duration.ofSeconds(15);

  private static Network network;
  private static ToxiproxyContainer toxiproxy;
  private static final List<GenericContainer<?>> brokers = new ArrayList<>();
  private static String bootstrap;

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
    Startables.deepStart(brokers).join();
  }

  private static GenericContainer<?> broker(final int id, final String external) {
    return new GenericContainer<>(KAFKA_IMAGE)
      .withNetwork(network)
      .withNetworkAliases("kafka" + id)
      .withEnv("CLUSTER_ID", CLUSTER_ID)
      .withEnv("KAFKA_NODE_ID", String.valueOf(id))
      .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
      .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka1:9093,2@kafka2:9093,3@kafka3:9093")
      .withEnv("KAFKA_LISTENERS", "INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:19092")
      .withEnv("KAFKA_ADVERTISED_LISTENERS", "INTERNAL://kafka" + id + ":9092,EXTERNAL://" + external)
      .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT")
      .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
      .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
      .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3")
      .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3")
      .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2")
      .withEnv("KAFKA_MIN_INSYNC_REPLICAS", "2")
      .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
      // Fence a silent broker and drop a lagging follower from the ISR within a few seconds, so
      // each failover fits the fault window.
      .withEnv("KAFKA_BROKER_HEARTBEAT_INTERVAL_MS", "1000")
      .withEnv("KAFKA_BROKER_SESSION_TIMEOUT_MS", "4000")
      .withEnv("KAFKA_REPLICA_LAG_TIME_MAX_MS", "5000")
      .withEnv("KAFKA_HEAP_OPTS", "-Xmx512m -Xms512m")
      .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1).withStartupTimeout(Duration.ofMinutes(3)));
  }

  @AfterAll
  static void stopCluster() {
    brokers.forEach(GenericContainer::stop);
    if (toxiproxy != null) toxiproxy.stop();
    if (network != null) network.close();
  }

  @AfterEach
  void resumeBrokers() {
    for (final var broker : brokers) resume(broker);
  }

  /// Every partition of a scenario's topic is replicated on all three brokers with broker 1 as its
  /// preferred leader, so each fault knows which broker leads and which follow.
  private static Cluster cluster() {
    return new Cluster(bootstrap, bootstrap, List.of(1, 2, 3));
  }

  /// A fault on the cluster, and whether commits must keep landing while it lasts.
  enum BrokerFault {
    /// The broker leading every partition of the topic stops answering, so the controller elects
    /// new leaders among the followers.
    LEADER_PAUSED(true) {
      @Override
      List<Integer> brokersToPause(final Run run) {
        return List.of(1);
      }
    },
    /// A follower stops answering, so it drops out of the ISR and the partitions carry on with two
    /// in-sync replicas.
    FOLLOWER_PAUSED(true) {
      @Override
      List<Integer> brokersToPause(final Run run) {
        return List.of(3);
      }
    },
    /// The broker coordinating the consumer group stops answering, so the group moves to the
    /// broker that takes over its `__consumer_offsets` partition.
    COORDINATOR_PAUSED(true) {
      @Override
      List<Integer> brokersToPause(final Run run) throws Exception {
        return List.of(coordinatorOf(run));
      }
    },
    /// Both followers stop answering. The leader still serves fetches, but `__consumer_offsets`
    /// has one in-sync replica against the two it requires, so the brokers refuse every commit.
    TWO_FOLLOWERS_PAUSED(false) {
      @Override
      List<Integer> brokersToPause(final Run run) {
        return List.of(2, 3);
      }
    };

    /// Whether commits must keep being acknowledged while the fault lasts.
    final boolean commitsContinue;

    BrokerFault(final boolean commitsContinue) {
      this.commitsContinue = commitsContinue;
    }

    abstract List<Integer> brokersToPause(Run run) throws Exception;
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
    var coordinator = -1;
    try (final var run = new Run(cluster(), mode, holds)) {
      final var consumer = run.consumer();
      try (final var sampler = new CommitSampler(run)) {
        consumer.start();
        try {
          run.awaitCompleted(5, Duration.ofSeconds(60));
          TimeUnit.MILLISECONDS.sleep(HEALTHY_WINDOW.toMillis());
          coordinator = coordinatorOf(run);
          final var paused = fault.brokersToPause(run);
          for (final var id : paused) pause(brokers.get(id - 1));
          run.faultInEffect.set(true);
          run.release(RELEASED_DURING_FAULT);
          TimeUnit.MILLISECONDS.sleep(FAULT_WINDOW.toMillis());
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
        assertTrue(
          run.acknowledgedDuringFault.get() > 0,
          "commits resume on the brokers left under " + fault + " (coordinator was broker " + coordinator + ")"
        );
      } else {
        assertEquals(0, run.acknowledgedDuringFault.get(), fault + " must leave no commit acknowledged");
      }
      assertEquals(RECORD_COUNT, run.completed.size(), "every record completes once the brokers return");
    }
  }

  /// The broker currently coordinating the scenario's consumer group.
  private static int coordinatorOf(final Run run) throws Exception {
    try (final var admin = Admin.create(directAdminProperties(bootstrap))) {
      return admin
        .describeConsumerGroups(List.of(run.groupId))
        .all()
        .get(15, TimeUnit.SECONDS)
        .get(run.groupId)
        .coordinator()
        .id();
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
