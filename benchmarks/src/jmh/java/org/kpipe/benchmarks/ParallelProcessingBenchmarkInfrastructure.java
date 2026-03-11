package org.kpipe.benchmarks;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.kpipe.consumer.KPipeConsumer;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/// Shared infrastructure for `ParallelProcessingBenchmark`.
///
/// This class holds all non-benchmark mechanics so `ParallelProcessingBenchmark` can focus
/// only on measured code paths.
///
/// ### What lives here
/// - Embedded Kafka lifecycle (startup/shutdown)
/// - Topic creation and seed-data publishing
/// - Common consumer property construction
/// - KPipe and Confluent invocation-scoped setup/teardown
/// - Shared wait/timeout utility used by benchmark methods
///
/// ### Why this split exists
/// - Keeps benchmark methods small and comparable.
/// - Prevents accidental infrastructure drift between KPipe and Confluent paths.
/// - Makes JMH state lifecycles explicit (`Trial` vs `Invocation`).
/// - Improves reliability by centralizing deterministic teardown logic.
public final class ParallelProcessingBenchmarkInfrastructure {

  /// Topic used by both benchmark implementations.
  static final String TOPIC = "benchmark-topic";

  /// Number of records seeded and awaited per invocation.
  static final int TARGET_MESSAGES = 10_000;

  /// Topic partitions used to expose parallel scheduler behavior.
  static final int TOPIC_PARTITIONS = 8;

  /// Safety timeout for per-invocation message completion checks.
  private static final long MAX_WAIT_NANOS = TimeUnit.SECONDS.toNanos(30);

  /// Bounded close timeout for Confluent resources.
  private static final Duration PC_CLOSE_TIMEOUT = Duration.ofSeconds(5);

  private ParallelProcessingBenchmarkInfrastructure() {}

  /// Waits until `TARGET_MESSAGES` are observed for the current invocation.
  ///
  /// @param benchmarkName name used in timeout diagnostics
  /// @param processedCount invocation-scoped progress counter
  static void awaitProcessedMessages(final String benchmarkName, final AtomicInteger processedCount) {
    final long deadline = System.nanoTime() + MAX_WAIT_NANOS;
    while (processedCount.get() < TARGET_MESSAGES) {
      if (System.nanoTime() >= deadline) {
        throw new IllegalStateException(
          "%s timed out waiting for %d messages; processed=%d".formatted(
              benchmarkName,
              TARGET_MESSAGES,
              processedCount.get()
            )
        );
      }
      Thread.onSpinWait();
    }
  }

  /// Trial-scoped Kafka test environment.
  ///
  /// A single embedded broker is started once per JMH trial to avoid startup overhead in the
  /// measured invocation path.
  @State(Scope.Benchmark)
  public static class KafkaContext {

    private AutoCloseable backend;
    private String bootstrapServers;
    private Properties clientProperties;

    /// Starts embedded Kafka and seeds benchmark data once per trial.
    @Setup(Level.Trial)
    public void setup() {
      final var embeddedBackend = new EmbeddedKafkaBackend();
      embeddedBackend.start();
      backend = embeddedBackend;
      clientProperties = embeddedBackend.getClientProperties();
      bootstrapServers = clientProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
      seedTopic(clientProperties);
    }

    /// Stops embedded Kafka once the trial completes.
    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
      if (backend != null) backend.close();
    }

    /// Builds baseline consumer properties for benchmark consumers.
    ///
    /// All consumers get consistent deserializers, offset behavior, and isolated group IDs.
    ///
    /// @param groupPrefix prefix used to build a unique invocation-safe group id
    /// @return a mutable properties instance for consumer construction
    public Properties consumerProps(final String groupPrefix) {
      final var props = new Properties();
      props.putAll(clientProperties);
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "%s-%s".formatted(groupPrefix, UUID.randomUUID()));
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
      return props;
    }

    /// Seeds the shared benchmark topic with `TARGET_MESSAGES` JSON records.
    private static void seedTopic(final Properties clientProperties) {
      createTopicIfMissing(clientProperties);

      final var producerProps = new Properties();
      producerProps.putAll(clientProperties);
      producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
        final var value =
          """
                  {
                    "id": 12345,
                    "message": "Benchmark message"
                  }
                  """.getBytes(
              StandardCharsets.UTF_8
            );

        for (int i = 0; i < TARGET_MESSAGES; i++) {
          producer.send(new ProducerRecord<>(TOPIC, value)).get();
        }
      } catch (final Exception e) {
        throw new IllegalStateException("Unable to seed benchmark topic", e);
      }
    }

    /// Creates the benchmark topic if it does not already exist.
    private static void createTopicIfMissing(final Properties clientProperties) {
      final var adminProps = new Properties();
      adminProps.putAll(clientProperties);

      try (final var admin = Admin.create(adminProps)) {
        admin.createTopics(Collections.singletonList(new NewTopic(TOPIC, TOPIC_PARTITIONS, (short) 1))).all().get();
      } catch (final Exception e) {
        throw new IllegalStateException("Unable to create benchmark topic", e);
      }
    }
  }

  /// Invocation-scoped KPipe runtime.
  ///
  /// The consumer is built in setup and started in the benchmark method so processing begins
  /// inside measured time (same timing model as Confluent path).
  @State(Scope.Thread)
  public static class KpipeInvocationContext {

    private AtomicInteger processedCount;
    private KPipeConsumer<byte[], byte[]> consumer;

    @Setup(Level.Invocation)
    public void setup(final KafkaContext kafkaContext) {
      processedCount = new AtomicInteger(0);
      final var kpipeProps = kafkaContext.consumerProps("kpipe-group");
      kpipeProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

      consumer =
        KPipeConsumer
          .<byte[], byte[]>builder()
          .withProperties(kpipeProps)
          .withTopic(TOPIC)
          .withMessageSink((record, processedValue) -> {})
          .withProcessor(val -> {
            processedCount.incrementAndGet();
            return val;
          })
          .withSequentialProcessing(false)
          .build();
    }

    void start() {
      consumer.start();
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
      if (consumer != null) consumer.close();
    }

    AtomicInteger processedCount() {
      return processedCount;
    }
  }

  /// Invocation-scoped Confluent Parallel Consumer runtime.
  ///
  /// `Scope.Thread` + `Level.Invocation` ensures each measured invocation receives fresh Confluent
  /// runtime state and deterministic shutdown.
  @State(Scope.Thread)
  public static class ConfluentInvocationContext {

    private AtomicInteger processedCount;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private ParallelStreamProcessor<byte[], byte[]> processor;

    /// Creates Confluent runtime for one invocation.
    @Setup(Level.Invocation)
    public void setup(final KafkaContext kafkaContext) {
      processedCount = new AtomicInteger(0);

      final var consumerProps = kafkaContext.consumerProps("confluent-group");
      consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

      kafkaConsumer = new KafkaConsumer<>(consumerProps);
      processor =
        ParallelStreamProcessor.createEosStreamProcessor(
          ParallelConsumerOptions
            .<byte[], byte[]>builder()
            .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
            .maxConcurrency(100)
            .ignoreReflectiveAccessExceptionsForAutoCommitDisabledCheck(true)
            .consumer(kafkaConsumer)
            .build()
        );

      processor.subscribe(Collections.singletonList(TOPIC));
    }

    /// Starts Confluent polling inside measured benchmark time.
    void start() {
      processor.poll(pollContext -> processedCount.incrementAndGet());
    }

    /// Deterministically tears down Confluent resources for one invocation.
    ///
    /// `closeDontDrainFirst` is intentional in benchmarks: we prioritize prompt, bounded shutdown
    /// to avoid JMH fork-exit warnings from background executor threads.
    @TearDown(Level.Invocation)
    public void tearDown() {
      if (processor != null) processor.closeDontDrainFirst(PC_CLOSE_TIMEOUT);
      if (kafkaConsumer != null) kafkaConsumer.close();
    }

    /// @return invocation-scoped processed message counter
    AtomicInteger processedCount() {
      return processedCount;
    }
  }

  /// Thin wrapper around Kafka test-kit bootstrap and shutdown.
  private static final class EmbeddedKafkaBackend implements AutoCloseable {

    private KafkaClusterTestKit cluster;

    /// Starts a single-node combined KRaft cluster configured for replication factor 1 internals.
    void start() {
      try {
        final var nodes = new TestKitNodes.Builder()
          .setCombined(true)
          .setNumBrokerNodes(1)
          .setNumControllerNodes(1)
          .build();

        cluster =
          new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp("auto.create.topics.enable", "true")
            .setConfigProp("offsets.topic.replication.factor", "1")
            .setConfigProp("transaction.state.log.replication.factor", "1")
            .setConfigProp("transaction.state.log.min.isr", "1")
            .setConfigProp("min.insync.replicas", "1")
            .build();

        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
      } catch (final Exception e) {
        throw new IllegalStateException("Unable to start embedded Kafka broker", e);
      }
    }

    /// @return client properties required to connect to this embedded cluster
    Properties getClientProperties() {
      final var copy = new Properties();
      copy.putAll(cluster.clientProperties());
      return copy;
    }

    /// Stops the embedded cluster and releases file/network resources.
    @Override
    public void close() throws Exception {
      if (cluster != null) cluster.close();
    }
  }
}
