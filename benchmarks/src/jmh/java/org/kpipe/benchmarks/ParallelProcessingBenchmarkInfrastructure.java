package org.kpipe.benchmarks;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
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
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.MessageProcessorRegistry;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

/// Shared infrastructure for the parallel-consumer competitive benchmarks.
///
/// Hosts the embedded Kafka broker, topic seeding, and the four invocation-scoped runtimes the
/// benchmarks compare:
///
///   * KPipe (`Stream<byte[]>` + virtual threads)
///   * Confluent Parallel Consumer (`ProcessingOrder.UNORDERED`, configurable max concurrency)
///   * Reactor Kafka (`Flux<ReceiverRecord>` on the Reactor `parallel` scheduler)
///   * Raw `KafkaConsumer` + `newVirtualThreadPerTaskExecutor` (the hand-rolled baseline)
///
/// Each runtime simulates a per-record workload of `workMicros` microseconds via
/// `LockSupport.parkNanos`. Setting `workMicros=0` reduces the bench to a pure framework-overhead
/// comparison; non-zero values expose how each runtime schedules blocking work.
public final class ParallelProcessingBenchmarkInfrastructure {

  static final String TOPIC = "benchmark-topic";

  /// Default number of records seeded per trial. 10k matches the prior baseline so cross-run
  /// comparisons stay meaningful. The in-process broker shares CPU cores with the consumer under
  /// test; on commodity dev hardware (Apple Silicon, 10c) pushing past 10–25k makes the broker
  /// the bottleneck rather than the consumer, group-join latency stretches past the safety
  /// timeout, and the run never completes a warmup iteration. For a "real" 100k+ steady-state
  /// number, externalise the broker (Testcontainers Kafka with `--cpus` constraints or a
  /// dedicated sidecar) and bump this constant.
  static final int TARGET_MESSAGES = 10_000;

  /// Topic partitions used to expose parallel scheduler behavior.
  static final int TOPIC_PARTITIONS = 8;

  /// Confluent Parallel Consumer max concurrency (number of worker threads it spins up).
  static final int CONFLUENT_MAX_CONCURRENCY = 100;

  /// Safety timeout for per-invocation completion checks. The in-process broker can stutter on
  /// group-join and metadata-update; leave headroom for a slow first poll.
  private static final long MAX_WAIT_NANOS = TimeUnit.MINUTES.toNanos(3);

  private static final Duration PC_CLOSE_TIMEOUT = Duration.ofSeconds(5);

  private ParallelProcessingBenchmarkInfrastructure() {}

  /// Simulates per-record work. `LockSupport.parkNanos` is the right primitive for "this thread
  /// is blocked for N µs" — it lets the JVM schedule something else on the carrier thread and is
  /// the closest cheap approximation of an I/O wait (JDBC commit, HTTP round-trip).
  static void simulateWork(final int workMicros) {
    if (workMicros > 0) LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(workMicros));
  }

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

  /// Trial-scoped Kafka test environment. One embedded broker per JMH trial; the seed payload is
  /// produced once.
  @State(Scope.Benchmark)
  public static class KafkaContext {

    private AutoCloseable backend;
    private String bootstrapServers;
    private Properties clientProperties;

    @Setup(Level.Trial)
    public void setup() {
      final var embeddedBackend = new EmbeddedKafkaBackend();
      embeddedBackend.start();
      backend = embeddedBackend;
      clientProperties = embeddedBackend.getClientProperties();
      bootstrapServers = clientProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
      seedTopic(clientProperties);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
      if (backend != null) backend.close();
    }

    public Properties consumerProps(final String groupPrefix) {
      final var props = new Properties();
      props.putAll(clientProperties);
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "%s-%s".formatted(groupPrefix, UUID.randomUUID()));
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      return props;
    }

    private static void seedTopic(final Properties clientProperties) {
      createTopicIfMissing(clientProperties);
      final var producerProps = new Properties();
      producerProps.putAll(clientProperties);
      producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      // Async send + single flush() at the end. Sync per-record `.send().get()` is ~50x slower
      // on the in-process broker and turns trial setup into the bottleneck of the whole run.
      producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "10");
      producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
        final var value = """
          {
            "id": 12345,
            "message": "Benchmark message"
          }
          """.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < TARGET_MESSAGES; i++) {
          producer.send(new ProducerRecord<>(TOPIC, value));
        }
        producer.flush();
      } catch (final Exception e) {
        throw new IllegalStateException("Unable to seed benchmark topic", e);
      }
    }

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

  /// Workload parameter shared by every invocation context. Exposed as a JMH `@Param` on the
  /// benchmark methods so the parameter sweep stays at the benchmark layer.
  @State(Scope.Benchmark)
  public static class WorkloadParams {

    @Param({ "0", "100", "1000" })
    public int workMicros;
  }

  @State(Scope.Thread)
  public static class KpipeInvocationContext {

    private static final MessageProcessorRegistry REGISTRY = new MessageProcessorRegistry();

    private AtomicInteger processedCount;
    private KPipeConsumer<byte[]> consumer;

    @Setup(Level.Invocation)
    public void setup(final KafkaContext kafkaContext, final WorkloadParams params) {
      processedCount = new AtomicInteger(0);
      final var props = kafkaContext.consumerProps("kpipe-group");
      final var workMicros = params.workMicros;
      final var pipeline = REGISTRY.pipeline(MessageFormat.bytes())
        .add(b -> {
          simulateWork(workMicros);
          processedCount.incrementAndGet();
          return b;
        })
        .build();
      consumer = KPipeConsumer.<byte[]>builder()
        .withProperties(props)
        .withTopic(TOPIC)
        .withPipeline(pipeline)
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

  @State(Scope.Thread)
  public static class ConfluentInvocationContext {

    private AtomicInteger processedCount;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private ParallelStreamProcessor<byte[], byte[]> processor;
    private int workMicros;

    @Setup(Level.Invocation)
    public void setup(final KafkaContext kafkaContext, final WorkloadParams params) {
      processedCount = new AtomicInteger(0);
      workMicros = params.workMicros;
      final var props = kafkaContext.consumerProps("confluent-group");
      kafkaConsumer = new KafkaConsumer<>(props);
      processor = ParallelStreamProcessor.createEosStreamProcessor(
        ParallelConsumerOptions.<byte[], byte[]>builder()
          .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
          .maxConcurrency(CONFLUENT_MAX_CONCURRENCY)
          .ignoreReflectiveAccessExceptionsForAutoCommitDisabledCheck(true)
          .consumer(kafkaConsumer)
          .build()
      );
      processor.subscribe(Collections.singletonList(TOPIC));
    }

    void start() {
      processor.poll(ctx -> {
        simulateWork(workMicros);
        processedCount.incrementAndGet();
      });
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
      if (processor != null) processor.closeDontDrainFirst(PC_CLOSE_TIMEOUT);
      if (kafkaConsumer != null) kafkaConsumer.close();
    }

    AtomicInteger processedCount() {
      return processedCount;
    }
  }

  /// Reactor Kafka runtime. `KafkaReceiver.receive()` produces a `Flux<ReceiverRecord>`; per-record
  /// work runs on Reactor's `parallel` scheduler via `flatMap`. Concurrency is bounded by the
  /// flatMap's `Queues.SMALL_BUFFER_SIZE` plus the scheduler's worker count.
  @State(Scope.Thread)
  public static class ReactorInvocationContext {

    private AtomicInteger processedCount;
    private reactor.core.Disposable subscription;
    private int workMicros;

    @Setup(Level.Invocation)
    public void setup(final KafkaContext kafkaContext, final WorkloadParams params) {
      processedCount = new AtomicInteger(0);
      workMicros = params.workMicros;
      final var props = kafkaContext.consumerProps("reactor-group");
      final var receiverOptions = ReceiverOptions
        .<byte[], byte[]>create(props)
        .subscription(Collections.singletonList(TOPIC));
      subscription = null;
      this.receiver = KafkaReceiver.create(receiverOptions);
    }

    private KafkaReceiver<byte[], byte[]> receiver;

    void start() {
      subscription = receiver
        .receive()
        .parallel(CONFLUENT_MAX_CONCURRENCY)
        .runOn(reactor.core.scheduler.Schedulers.parallel())
        .doOnNext(record -> {
          simulateWork(workMicros);
          processedCount.incrementAndGet();
          record.receiverOffset().acknowledge();
        })
        .sequential()
        .subscribe();
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
      if (subscription != null && !subscription.isDisposed()) subscription.dispose();
    }

    AtomicInteger processedCount() {
      return processedCount;
    }
  }

  /// Hand-rolled baseline: plain `KafkaConsumer.poll()` loop with `newVirtualThreadPerTaskExecutor`
  /// dispatching one task per record. No framework, no offset manager. This is the floor of "what
  /// if you just wrote the consumer loop yourself on Loom?" — the implicit comparison every
  /// framework lives next to.
  @State(Scope.Thread)
  public static class RawInvocationContext {

    private AtomicInteger processedCount;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private ExecutorService executor;
    private Thread pollLoop;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private int workMicros;

    @Setup(Level.Invocation)
    public void setup(final KafkaContext kafkaContext, final WorkloadParams params) {
      processedCount = new AtomicInteger(0);
      workMicros = params.workMicros;
      running.set(true);
      final var props = kafkaContext.consumerProps("raw-group");
      kafkaConsumer = new KafkaConsumer<>(props);
      kafkaConsumer.subscribe(Collections.singletonList(TOPIC));
      executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    void start() {
      pollLoop = Thread.ofPlatform().daemon().start(() -> {
        while (running.get()) {
          final var records = kafkaConsumer.poll(Duration.ofMillis(100));
          for (final var record : records) {
            executor.submit(() -> {
              simulateWork(workMicros);
              processedCount.incrementAndGet();
            });
          }
        }
      });
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
      running.set(false);
      if (pollLoop != null) {
        try {
          pollLoop.join(Duration.ofSeconds(5).toMillis());
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (executor != null) {
        executor.shutdownNow();
        try {
          executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (kafkaConsumer != null) kafkaConsumer.close();
    }

    AtomicInteger processedCount() {
      return processedCount;
    }
  }

  private static final class EmbeddedKafkaBackend implements AutoCloseable {

    private KafkaClusterTestKit cluster;

    void start() {
      try {
        final var nodes = new TestKitNodes.Builder()
          .setCombined(true)
          .setNumBrokerNodes(1)
          .setNumControllerNodes(1)
          .build();
        cluster = new KafkaClusterTestKit.Builder(nodes)
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

    Properties getClientProperties() {
      final var copy = new Properties();
      copy.putAll(cluster.clientProperties());
      return copy;
    }

    @Override
    public void close() throws Exception {
      if (cluster != null) cluster.close();
    }
  }
}
