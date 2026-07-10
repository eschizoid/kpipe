package io.github.eschizoid.kpipe.benchmarks;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

/// Shared infrastructure for the parallel-consumer competitive benchmarks.
///
/// Hosts the embedded Kafka broker, topic seeding, and the invocation-scoped runtimes the
/// benchmarks compare:
///
///   * KPipe PARALLEL (virtual-thread-per-record) and KEY_ORDERED (per-key serial queues)
///   * Kafka Share Consumer (KIP-932) — single consumer + virtual-thread fan-out
///   * Confluent Parallel Consumer (`ProcessingOrder.UNORDERED`, configurable max concurrency)
///   * Reactor Kafka (`Flux<ReceiverRecord>` on the Reactor `parallel` scheduler)
///   * Raw `KafkaConsumer` + `newVirtualThreadPerTaskExecutor` (the hand-rolled baseline)
///
/// Each runtime simulates a per-record workload of `workMicros` microseconds via
/// `LockSupport.parkNanos`. Setting `workMicros=0` reduces the bench to a pure framework-overhead
/// comparison; non-zero values expose how each runtime schedules blocking work. All arms except
/// KEY_ORDERED are unordered — read KEY_ORDERED's gap as the cost of per-key ordering.
public final class ParallelProcessingBenchmarkInfrastructure {

  static final String TOPIC = "benchmark-topic";

  /// Default number of records seeded per trial. 25k pushes the harness into the steady-state
  /// regime where group-join and first-poll cost are statistically small but the run still
  /// completes in a useful timeframe. Now that the broker runs in a Docker container (not
  /// in-process), the broker stops fighting the consumer for cores and the consumer is the
  /// thing being measured.
  static final int TARGET_MESSAGES = 25_000;

  /// Topic partitions used to expose parallel scheduler behavior.
  static final int TOPIC_PARTITIONS = 8;

  /// Distinct record keys seeded into the topic (`key = i % KEY_CARDINALITY`). Only the
  /// KEY_ORDERED arm reads keys — the unordered arms (PARALLEL, Confluent, Reactor, raw, share)
  /// ignore them, so the same seeded data drives every runtime. 1,000 keys over TARGET_MESSAGES
  /// gives ~25 records/key: enough per-key queue depth for ordering to actually constrain
  /// scheduling, while staying well under the dispatcher's 10,000-key LRU cap (no eviction).
  static final int KEY_CARDINALITY = 1_000;

  /// Confluent Parallel Consumer max concurrency (number of worker threads it spins up).
  static final int CONFLUENT_MAX_CONCURRENCY = 100;

  /// Safety timeout for per-invocation completion checks. Generous so a single slow first poll
  /// on container startup doesn't kill an iteration. Steady-state iterations finish well under
  /// this; the timeout is a circuit-breaker, not a measurement.
  private static final long MAX_WAIT_NANOS = TimeUnit.MINUTES.toNanos(2);

  private static final Duration PC_CLOSE_TIMEOUT = Duration.ofSeconds(5);

  /// How long teardown waits for the poll loop thread and the worker executor to drain. Five
  /// seconds is generous; iterations that need longer are paying a measurement cost outside the
  /// steady-state regime the bench is supposed to capture.
  private static final long TEARDOWN_TIMEOUT_SECONDS = 5;

  private ParallelProcessingBenchmarkInfrastructure() {}

  /// Shared teardown helper used by every invocation-context inner class that owns a poll loop +
  /// worker executor pair. Lowers the `running` flag, joins the poll loop, then shuts down the
  /// executor and waits for it to terminate. Restores the interrupt flag on `InterruptedException`
  /// per the cooperative-cancellation rule; logs at WARNING when `awaitTermination` returns false
  /// so a slow executor doesn't disappear from the report.
  static void stopPollLoopAndExecutor(
    final AtomicBoolean running,
    final Thread pollLoop,
    final ExecutorService executor
  ) {
    running.set(false);
    if (pollLoop != null) {
      try {
        pollLoop.join(Duration.ofSeconds(TEARDOWN_TIMEOUT_SECONDS).toMillis());
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (executor != null) {
      executor.shutdownNow();
      try {
        if (!executor.awaitTermination(TEARDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          System.err.println(
            "[bench teardown] executor did not terminate within " +
              TEARDOWN_TIMEOUT_SECONDS +
              "s; benchmark measurements may include teardown overhead"
          );
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

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

    /// Admin client props (bootstrap only). Used by the share arm to set the share-group's
    /// start-offset policy before it joins.
    public Properties adminProps() {
      final var props = new Properties();
      props.putAll(clientProperties);
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      return props;
    }

    /// Minimal props for a [org.apache.kafka.clients.consumer.KafkaShareConsumer]. Deliberately
    /// excludes `auto.offset.reset` / `enable.auto.commit` — those are plain-consumer configs
    /// that don't apply to share groups (the group's start offset is a group config, see the
    /// share context's Admin call).
    public Properties shareConsumerProps(final String groupId) {
      final var props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
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
      // Disable idempotence for the seed. The Kafka 4.x default enables it, which requires the
      // broker to track producer ID + sequence numbers; under the test-kit broker the PID epoch
      // gets reset on retry and produces a flood of OUT_OF_ORDER_SEQUENCE_NUMBER errors. We do
      // not need exactly-once for trial seeding — at-least-once is fine.
      producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
      producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
        final var value = """
          {
            "id": 12345,
            "message": "Benchmark message"
          }
          """.getBytes(StandardCharsets.UTF_8);
        // Precompute the key set once; reuse the byte[] per record (keys are immutable here).
        final var keys = new byte[KEY_CARDINALITY][];
        for (int k = 0; k < KEY_CARDINALITY; k++) keys[k] = Integer.toString(k).getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < TARGET_MESSAGES; i++) {
          producer.send(new ProducerRecord<>(TOPIC, keys[i % KEY_CARDINALITY], value));
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

    /// Per-record simulated work (`LockSupport.parkNanos`). The first three values are the
    /// original framework-overhead → 1ms sweep; the higher values cover the 10-100ms regime
    /// where most real Kafka consumers actually live (HTTP/DB hops). 100ms is also where the
    /// lag-based-backpressure pitch becomes load-bearing — so it's the column the numbers most
    /// need to be honest about.
    @Param({ "0", "100", "1000", "10000", "35000", "50000", "100000" })
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
        .withProcessingMode(ProcessingMode.PARALLEL)
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

  /// KPipe in KEY_ORDERED mode: same single consumer + virtual threads as the PARALLEL arm, but
  /// records are dispatched to per-key serial queues so processing within a key is strictly
  /// ordered. This is the ONLY arm in the suite that provides an ordering guarantee — it exists
  /// to quantify the per-key ordering tax relative to the unordered runtimes (PARALLEL, share,
  /// Confluent, Reactor, raw). It is NOT a like-for-like rival of the ShareConsumer arm, which
  /// offers no ordering at all; read the gap as "cost of ordering," not "X beats Y".
  @State(Scope.Thread)
  public static class KpipeKeyOrderedInvocationContext {

    private static final MessageProcessorRegistry REGISTRY = new MessageProcessorRegistry();

    private AtomicInteger processedCount;
    private KPipeConsumer<byte[]> consumer;

    @Setup(Level.Invocation)
    public void setup(final KafkaContext kafkaContext, final WorkloadParams params) {
      processedCount = new AtomicInteger(0);
      final var props = kafkaContext.consumerProps("kpipe-keyordered-group");
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
        .withProcessingMode(ProcessingMode.KEY_ORDERED)
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
      processor.poll(_ -> {
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

  /// Confluent Parallel Consumer with `ProcessingOrder.KEY` — at most one in-flight record per
  /// record key, but different keys process concurrently up to `maxConcurrency`. The cross-library
  /// counterpart to KPipe KEY_ORDERED for the "what does per-key ordering cost?" question. Same
  /// 100-worker platform-thread pool as the UNORDERED arm; only the ordering changes.
  @State(Scope.Thread)
  public static class ConfluentKeyInvocationContext {

    private AtomicInteger processedCount;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private ParallelStreamProcessor<byte[], byte[]> processor;
    private int workMicros;

    @Setup(Level.Invocation)
    public void setup(final KafkaContext kafkaContext, final WorkloadParams params) {
      processedCount = new AtomicInteger(0);
      workMicros = params.workMicros;
      final var props = kafkaContext.consumerProps("confluent-key-group");
      kafkaConsumer = new KafkaConsumer<>(props);
      processor = ParallelStreamProcessor.createEosStreamProcessor(
        ParallelConsumerOptions.<byte[], byte[]>builder()
          .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
          .maxConcurrency(CONFLUENT_MAX_CONCURRENCY)
          .ignoreReflectiveAccessExceptionsForAutoCommitDisabledCheck(true)
          .consumer(kafkaConsumer)
          .build()
      );
      processor.subscribe(Collections.singletonList(TOPIC));
    }

    void start() {
      processor.poll(_ -> {
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

  /// Confluent Parallel Consumer with `ProcessingOrder.PARTITION` — at most one in-flight record
  /// per partition; parallelism is capped at the partition count (8 here). Strict per-partition
  /// offset ordering, the strongest ordering CPC offers.
  @State(Scope.Thread)
  public static class ConfluentPartitionInvocationContext {

    private AtomicInteger processedCount;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private ParallelStreamProcessor<byte[], byte[]> processor;
    private int workMicros;

    @Setup(Level.Invocation)
    public void setup(final KafkaContext kafkaContext, final WorkloadParams params) {
      processedCount = new AtomicInteger(0);
      workMicros = params.workMicros;
      final var props = kafkaContext.consumerProps("confluent-partition-group");
      kafkaConsumer = new KafkaConsumer<>(props);
      processor = ParallelStreamProcessor.createEosStreamProcessor(
        ParallelConsumerOptions.<byte[], byte[]>builder()
          .ordering(ParallelConsumerOptions.ProcessingOrder.PARTITION)
          .maxConcurrency(CONFLUENT_MAX_CONCURRENCY)
          .ignoreReflectiveAccessExceptionsForAutoCommitDisabledCheck(true)
          .consumer(kafkaConsumer)
          .build()
      );
      processor.subscribe(Collections.singletonList(TOPIC));
    }

    void start() {
      processor.poll(_ -> {
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
      final var receiverOptions = ReceiverOptions.<byte[], byte[]>create(props).subscription(
        Collections.singletonList(TOPIC)
      );
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
      pollLoop = Thread.ofPlatform()
        .daemon()
        .start(() -> {
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
      stopPollLoopAndExecutor(running, pollLoop, executor);
      if (kafkaConsumer != null) kafkaConsumer.close();
    }

    AtomicInteger processedCount() {
      return processedCount;
    }
  }

  /// Single-threaded consumer baseline. One `KafkaConsumer` polls and processes inline — no
  /// executor, no fan-out, no framework. This is the default pattern most teams ship with out of
  /// the box, and the floor that makes every framework's value visible: a framework that can't
  /// beat this isn't earning its overhead. Distinct from the [RawInvocationContext] arm, which
  /// adds virtual-thread fan-out (i.e., what you get from "I just wrote the loop myself, but with
  /// Loom"). Commits sync after each non-empty poll for honest at-least-once.
  @State(Scope.Thread)
  public static class SingleThreadInvocationContext {

    private AtomicInteger processedCount;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private Thread pollLoop;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private int workMicros;

    @Setup(Level.Invocation)
    public void setup(final KafkaContext kafkaContext, final WorkloadParams params) {
      processedCount = new AtomicInteger(0);
      workMicros = params.workMicros;
      running.set(true);
      final var props = kafkaContext.consumerProps("single-thread-group");
      kafkaConsumer = new KafkaConsumer<>(props);
      kafkaConsumer.subscribe(Collections.singletonList(TOPIC));
    }

    void start() {
      pollLoop = Thread.ofPlatform()
        .daemon()
        .start(() -> {
          while (running.get()) {
            final var records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (final var record : records) {
              simulateWork(workMicros);
              processedCount.incrementAndGet();
            }
            if (!records.isEmpty()) {
              try {
                kafkaConsumer.commitSync();
              } catch (final Exception e) {
                // benchmark-best-effort: don't crash the run on a commit failure
              }
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
      if (kafkaConsumer != null) kafkaConsumer.close();
    }

    AtomicInteger processedCount() {
      return processedCount;
    }
  }

  /// Kafka 4.x Share Consumer (KIP-932) runtime. A single `KafkaShareConsumer` polls the topic;
  /// each polled batch is fanned out to virtual threads, then the loop waits for the whole batch
  /// to finish before polling again. The next `poll()` implicitly acknowledges the prior batch as
  /// ACCEPT, so the barrier is what keeps this at-least-once: a record is only acked after its
  /// work completes.
  ///
  /// ### Apples-to-apples framing
  ///
  /// This is the controlled comparison against KPipe-PARALLEL: one consuming process, work fanned
  /// to virtual threads, same broker / seed / workload / metric. The only thing that differs is
  /// the fetch+durability protocol — share-acquire + per-record acknowledgement here, versus
  /// fetch + lowest-pending offset commit in KPipe. Neither this arm nor PARALLEL provides
  /// ordering. The batch barrier is the honest cost of safely processing a share batch
  /// concurrently; it is not an artificial handicap.
  ///
  /// ### Share-group start offset (verify on first Docker run)
  ///
  /// A fresh share group's start offset is the GROUP config `share.auto.offset.reset` (broker
  /// default `latest`), NOT the consumer's `auto.offset.reset`. Left at `latest`, the group would
  /// skip every pre-seeded record and the bench would time out. We set it to `earliest` per group
  /// via `incrementalAlterConfigs` before subscribing. If the broker rejects that key, the
  /// fallback is a broker-level default for share groups — check the 4.3.0 docs for the exact
  /// name.
  @State(Scope.Thread)
  public static class ShareConsumerInvocationContext {

    private AtomicInteger processedCount;
    private KafkaShareConsumer<byte[], byte[]> shareConsumer;
    private ExecutorService executor;
    private Thread pollLoop;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private int workMicros;

    @Setup(Level.Invocation)
    public void setup(final KafkaContext kafkaContext, final WorkloadParams params) {
      processedCount = new AtomicInteger(0);
      workMicros = params.workMicros;
      running.set(true);
      final var groupId = "kpipe-share-group-%s".formatted(UUID.randomUUID());

      // Make this fresh share group read the pre-seeded records (default is `latest` → skip all).
      try (final var admin = Admin.create(kafkaContext.adminProps())) {
        final var resource = new ConfigResource(ConfigResource.Type.GROUP, groupId);
        admin
          .incrementalAlterConfigs(
            Map.of(
              resource,
              List.of(
                new AlterConfigOp(new ConfigEntry("share.auto.offset.reset", "earliest"), AlterConfigOp.OpType.SET)
              )
            )
          )
          .all()
          .get();
      } catch (final Exception e) {
        throw new IllegalStateException(
          "Unable to set share.auto.offset.reset=earliest for group " + groupId + " (share-group support configured?)",
          e
        );
      }

      shareConsumer = new KafkaShareConsumer<>(kafkaContext.shareConsumerProps(groupId));
      shareConsumer.subscribe(Collections.singletonList(TOPIC));
      executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    void start() {
      pollLoop = Thread.ofPlatform()
        .daemon()
        .start(() -> {
          while (running.get()) {
            final var records = shareConsumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) continue;
            final var batch = new CountDownLatch(records.count());
            for (final var record : records) {
              executor.submit(() -> {
                try {
                  simulateWork(workMicros);
                  processedCount.incrementAndGet();
                } finally {
                  batch.countDown();
                }
              });
            }
            // Barrier: finish the batch before the next poll() acks it.
            try {
              batch.await();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
          }
        });
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
      stopPollLoopAndExecutor(running, pollLoop, executor);
      // Safe to close from this thread: the poll loop has joined, so nothing else touches it.
      if (shareConsumer != null) shareConsumer.close();
    }

    AtomicInteger processedCount() {
      return processedCount;
    }
  }

  /// Testcontainers-backed Kafka broker. Replaces the prior in-process `KafkaClusterTestKit`
  /// because that harness collapsed under benchmark load on shared cores — the broker, the KRaft
  /// controller, the group coordinator, and the consumer under test all fought for the same
  /// CPUs and the consumer never reached its real throughput. Running the broker in a Docker
  /// container puts it on its own JVM with its own cores, so the consumer is the bottleneck the
  /// bench is actually trying to measure.
  ///
  /// Pinned to the `apache/kafka:4.3.0` image to match the `kafka-clients` version on the
  /// classpath. Auto-create topics is on so the seed step doesn't have to fight a race with
  /// topic-metadata propagation.
  ///
  /// Package-private (not class-private) so [PerRecordLatencyHarness] can reuse the same broker
  /// setup without duplicating the container configuration.
  static final class EmbeddedKafkaBackend implements AutoCloseable {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("apache/kafka:4.3.0");

    private KafkaContainer container;

    void start() {
      container = new KafkaContainer(KAFKA_IMAGE)
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .withEnv("KAFKA_MIN_INSYNC_REPLICAS", "1")
        // Share-group (KIP-932) support for the share arm. The group coordinator
        // must offer the "share" rebalance protocol, and __share_group_state needs
        // single-broker replication. VERIFY ON FIRST RUN: if a share group never
        // forms, check these env names vs the 4.3.0 broker configs + broker log.
        .withEnv("KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS", "classic,consumer,share")
        .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1");
      container.start();
    }

    Properties getClientProperties() {
      final var props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, container.getBootstrapServers());
      return props;
    }

    @Override
    public void close() {
      if (container != null) container.stop();
    }
  }
}
