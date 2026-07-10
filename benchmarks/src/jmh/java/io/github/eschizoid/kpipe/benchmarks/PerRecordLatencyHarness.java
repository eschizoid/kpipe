package io.github.eschizoid.kpipe.benchmarks;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/// TRUE per-record latency harness — measures the **per-record latency distribution** (p50 / p95 /
/// p99 / max), not the batch-amortized figures that [ParallelProcessingLatencyBenchmark] reports.
///
/// ### Why this is not a JMH benchmark
///
/// JMH's `SampleTime` mode samples the duration of `@Benchmark` invocations. Our invocation is
/// "drain N records", so dividing by `@OperationsPerInvocation(N)` yields whole-batch completion
/// time amortized per record — a throughput number in latency clothing (see the methodological
/// note in `results/2026-07-09.md`). A per-record distribution needs one timestamp pair *per
/// record*: stamp at produce, observe at the sink. JMH has no way to feed 20k externally-measured
/// samples into its `SampleTime` histogram, so this harness measures and reports them itself:
/// exact percentiles from full sample retention (every latency kept in a preallocated `long[]`,
/// sorted once at the end — no histogram binning error, trivially affordable at these counts).
///
/// ### What one arm run looks like
///
/// 1. Fresh topic (8 partitions) on the shared Testcontainers broker.
/// 2. Start the consumer arm (KPipe PARALLEL / KPipe KEY_ORDERED / Confluent PC UNORDERED / raw
///    `KafkaConsumer` + virtual threads).
/// 3. **Warmup phase**: produce `warmupRecords` at the paced rate; wait until the sink has seen
///    them all. This absorbs group join, first-poll, and JIT warmth *outside* the measurement —
///    without it, the first measured records would carry consumer-group-join backlog as fake
///    latency.
/// 4. **Measurement phase**: produce `measuredRecords` at the paced rate, each stamped with
///    `System.nanoTime()`; the sink computes `now - stamp` *after* the simulated per-record work
///    and records it.
/// 5. Sort, report percentiles, append to the JSON result.
///
/// ### Clock semantics — read before quoting numbers
///
/// * **Same-process `nanoTime` only.** `System.nanoTime()` values are comparable only within one
///   JVM. Producer and every consumer arm run in *this* process (the broker in Docker is just a
///   pipe), so `now - stamp` is valid. Do NOT split producer and consumer into separate processes
///   and keep this scheme — cross-process `nanoTime` deltas are meaningless (each JVM has its own
///   arbitrary origin) and wall-clock (`currentTimeMillis`) is NTP-steered and millisecond-coarse.
/// * **The stamp is the *intended* send time, not the actual send time.** Records are paced at
///   `ratePerSecond`; each record is stamped with its schedule slot. If the producer falls behind
///   (buffer full, broker stall), later records carry older stamps and the delay shows up as
///   latency — the standard coordinated-omission correction. Stamping the actual send time would
///   silently excuse producer stalls from the distribution.
/// * **The measured quantity is intended-produce → work-complete**: producer batching
///   (`linger.ms=0` here), broker hop over Docker loopback, consumer poll, framework dispatch,
///   and the simulated `workMicros` of per-record work. It is an end-to-end in-process figure —
///   compare arms against each other, not against absolute production SLAs.
/// * **Rate must stay below every arm's saturation throughput.** Above saturation the queue grows
///   without bound and percentiles measure backlog depth, not the framework. The defaults
///   (2,000 rec/s, 100 µs work) are far below every arm's measured throughput; if you raise
///   `workMicros` or the rate, sanity-check against the throughput bench first.
///
/// ### Running
///
/// ```bash
/// ./gradlew :benchmarks:perRecordLatency \
///   -Platency.workMicros=100 \
///   -Platency.ratePerSecond=2000 \
///   -Platency.warmupRecords=5000 \
///   -Platency.measuredRecords=20000 \
///   -Platency.arms=kpipeParallel,kpipeKeyOrdered,confluentUnordered,rawVirtualThreads
/// ```
///
/// All properties are optional; the values above are the defaults. Results print as a table and
/// are written as JSON to `benchmarks/build/results/per-record-latency/results.json` (override
/// with `-Platency.output=<path>`). Docker is required (Testcontainers broker).
public final class PerRecordLatencyHarness {

  /// Payload layout: `[0..7]` big-endian `nanoTime` stamp, `[8]` phase marker, rest zero padding
  /// up to 50 bytes to match the throughput suite's payload size.
  private static final int PAYLOAD_BYTES = 50;

  private static final byte PHASE_WARMUP = 0;
  private static final byte PHASE_MEASURED = 1;

  /// Circuit-breaker for phase completion, over and above the paced production time itself.
  private static final Duration PHASE_GRACE = Duration.ofMinutes(2);

  private PerRecordLatencyHarness() {}

  public static void main(final String[] args) throws Exception {
    final var config = Config.fromSystemProperties();
    System.out.println("Per-record latency harness — config: " + config);

    final var backend = new ParallelProcessingBenchmarkInfrastructure.EmbeddedKafkaBackend();
    backend.start();
    try {
      final var bootstrap = backend.getClientProperties().getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
      final var results = new ArrayList<ArmResult>();
      for (final var arm : config.arms) {
        results.add(runArm(arm, bootstrap, config));
      }
      printTable(results);
      writeJson(results, config);
    } finally {
      backend.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Arm orchestration
  // ---------------------------------------------------------------------------------------------

  private static ArmResult runArm(final Arm arm, final String bootstrap, final Config config) throws Exception {
    final var topic = "latency-%s-%s".formatted(arm.name().toLowerCase(Locale.ROOT), UUID.randomUUID());
    createTopic(bootstrap, topic);

    final var recorder = new LatencyRecorder(config.measuredRecords);
    final var workMicros = config.workMicros;
    final Consumer<byte[]> sink = value -> {
      ParallelProcessingBenchmarkInfrastructure.simulateWork(workMicros);
      recorder.observe(value);
    };

    System.out.printf("[%s] starting consumer on %s%n", arm, topic);
    try (final var ignored = startArm(arm, topic, bootstrap, sink)) {
      final var producerProps = producerProps(bootstrap);
      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
        // Warmup: absorbs group join + first poll + JIT outside the measurement window.
        producePaced(producer, topic, config.warmupRecords, config.ratePerSecond, PHASE_WARMUP);
        awaitCount(arm + " warmup", recorder::warmupCount, config.warmupRecords, config.ratePerSecond);

        // Measurement: every record stamped with its intended send time.
        producePaced(producer, topic, config.measuredRecords, config.ratePerSecond, PHASE_MEASURED);
        awaitCount(arm + " measurement", recorder::measuredCount, config.measuredRecords, config.ratePerSecond);
      }
    }

    final var result = ArmResult.of(arm, recorder.sortedSamples());
    System.out.printf("[%s] %s%n", arm, result.summaryLine());
    return result;
  }

  private static AutoCloseable startArm(
    final Arm arm,
    final String topic,
    final String bootstrap,
    final Consumer<byte[]> sink
  ) {
    return switch (arm) {
      case kpipeParallel -> startKpipe(topic, bootstrap, ProcessingMode.PARALLEL, sink);
      case kpipeKeyOrdered -> startKpipe(topic, bootstrap, ProcessingMode.KEY_ORDERED, sink);
      case confluentUnordered -> startConfluent(topic, bootstrap, sink);
      case rawVirtualThreads -> startRaw(topic, bootstrap, sink);
    };
  }

  private static AutoCloseable startKpipe(
    final String topic,
    final String bootstrap,
    final ProcessingMode mode,
    final Consumer<byte[]> sink
  ) {
    final var pipeline = new MessageProcessorRegistry()
      .pipeline(MessageFormat.bytes())
      .add(bytes -> {
        sink.accept(bytes);
        return bytes;
      })
      .build();
    final var consumer = KPipeConsumer.<byte[]>builder()
      .withProperties(consumerProps(bootstrap, "latency-kpipe"))
      .withTopic(topic)
      .withPipeline(pipeline)
      .withProcessingMode(mode)
      .build();
    consumer.start();
    return consumer::close;
  }

  private static AutoCloseable startConfluent(final String topic, final String bootstrap, final Consumer<byte[]> sink) {
    final var kafkaConsumer = new KafkaConsumer<byte[], byte[]>(consumerProps(bootstrap, "latency-confluent"));
    final var processor = ParallelStreamProcessor.createEosStreamProcessor(
      ParallelConsumerOptions.<byte[], byte[]>builder()
        .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
        .maxConcurrency(ParallelProcessingBenchmarkInfrastructure.CONFLUENT_MAX_CONCURRENCY)
        .ignoreReflectiveAccessExceptionsForAutoCommitDisabledCheck(true)
        .consumer(kafkaConsumer)
        .build()
    );
    processor.subscribe(Collections.singletonList(topic));
    processor.poll(context -> sink.accept(context.value()));
    return () -> {
      processor.closeDontDrainFirst(Duration.ofSeconds(5));
      kafkaConsumer.close();
    };
  }

  private static AutoCloseable startRaw(final String topic, final String bootstrap, final Consumer<byte[]> sink) {
    final var kafkaConsumer = new KafkaConsumer<byte[], byte[]>(consumerProps(bootstrap, "latency-raw"));
    kafkaConsumer.subscribe(Collections.singletonList(topic));
    final var executor = Executors.newVirtualThreadPerTaskExecutor();
    final var running = new AtomicBoolean(true);
    final var pollLoop = Thread.ofPlatform()
      .daemon()
      .start(() -> {
        while (running.get()) {
          final var records = kafkaConsumer.poll(Duration.ofMillis(100));
          for (final var record : records) {
            executor.submit(() -> sink.accept(record.value()));
          }
        }
      });
    return () -> {
      ParallelProcessingBenchmarkInfrastructure.stopPollLoopAndExecutor(running, pollLoop, executor);
      kafkaConsumer.close();
    };
  }

  // ---------------------------------------------------------------------------------------------
  // Paced production
  // ---------------------------------------------------------------------------------------------

  /// Produces `count` records at `ratePerSecond`, each stamped with its **intended** send slot
  /// (`start + i * interval`, in `nanoTime` terms). Stamping the schedule slot rather than the
  /// actual send time is the coordinated-omission correction: producer stalls surface as latency
  /// instead of being silently excused.
  private static void producePaced(
    final KafkaProducer<byte[], byte[]> producer,
    final String topic,
    final int count,
    final int ratePerSecond,
    final byte phase
  ) {
    final var keys = new byte[ParallelProcessingBenchmarkInfrastructure.KEY_CARDINALITY][];
    for (int k = 0; k < keys.length; k++) keys[k] = Integer.toString(k).getBytes(StandardCharsets.UTF_8);
    final var intervalNanos = TimeUnit.SECONDS.toNanos(1) / ratePerSecond;
    final var start = System.nanoTime();
    for (int i = 0; i < count; i++) {
      final var intended = start + i * intervalNanos;
      var now = System.nanoTime();
      while (now < intended) {
        LockSupport.parkNanos(intended - now);
        now = System.nanoTime();
      }
      final var payload = new byte[PAYLOAD_BYTES];
      ByteBuffer.wrap(payload).putLong(intended).put(phase);
      producer.send(new ProducerRecord<>(topic, keys[i % keys.length], payload));
    }
    producer.flush();
  }

  private static void awaitCount(
    final String what,
    final IntSupplier counter,
    final int expected,
    final int ratePerSecond
  ) {
    final var pacedNanos = (TimeUnit.SECONDS.toNanos(1) * expected) / ratePerSecond;
    final var deadline = System.nanoTime() + pacedNanos + PHASE_GRACE.toNanos();
    while (counter.getAsInt() < expected) {
      if (System.nanoTime() >= deadline) {
        throw new IllegalStateException(
          "%s timed out waiting for %d records; observed=%d".formatted(what, expected, counter.getAsInt())
        );
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Kafka plumbing
  // ---------------------------------------------------------------------------------------------

  private static void createTopic(final String bootstrap, final String topic) {
    final var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    try (final var admin = Admin.create(props)) {
      admin
        .createTopics(
          Collections.singletonList(
            new NewTopic(topic, ParallelProcessingBenchmarkInfrastructure.TOPIC_PARTITIONS, (short) 1)
          )
        )
        .all()
        .get();
    } catch (final Exception e) {
      throw new IllegalStateException("Unable to create latency topic " + topic, e);
    }
  }

  private static Properties consumerProps(final String bootstrap, final String groupPrefix) {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "%s-%s".formatted(groupPrefix, UUID.randomUUID()));
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return props;
  }

  /// Latency-producer props. Unlike the throughput seed (linger=10ms, big batches), this producer
  /// must not batch-delay records: `linger.ms=0` so producer batching doesn't dominate the very
  /// distribution being measured. Idempotence stays off per the seed-producer note (we don't need
  /// exactly-once for a bench, and the test broker's PID-epoch resets flood the log otherwise).
  private static Properties producerProps(final String bootstrap) {
    final var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    return props;
  }

  // ---------------------------------------------------------------------------------------------
  // Recording + reporting
  // ---------------------------------------------------------------------------------------------

  /// Concurrent per-record latency recorder with full sample retention. Sinks may call [#observe]
  /// from many virtual/platform threads: each measured sample reserves a slot via
  /// `getAndIncrement`, writes it, then publishes via a second atomic increment — the reader
  /// spins on `measuredCount()` reaching the target, and that acquire pairs with the writer's
  /// release, making every slot write visible before [#sortedSamples] runs. Redeliveries beyond
  /// the target (at-least-once) are dropped at the bounds check.
  private static final class LatencyRecorder {

    private final long[] samples;
    private final AtomicInteger reserved = new AtomicInteger();
    private final AtomicInteger published = new AtomicInteger();
    private final AtomicInteger warmupSeen = new AtomicInteger();

    LatencyRecorder(final int measuredRecords) {
      samples = new long[measuredRecords];
    }

    void observe(final byte[] payload) {
      final var now = System.nanoTime();
      final var buffer = ByteBuffer.wrap(payload);
      final var stamp = buffer.getLong();
      final var phase = buffer.get();
      if (phase == PHASE_WARMUP) {
        warmupSeen.incrementAndGet();
        return;
      }
      final var slot = reserved.getAndIncrement();
      if (slot >= samples.length) return;
      samples[slot] = now - stamp;
      published.incrementAndGet();
    }

    int warmupCount() {
      return warmupSeen.get();
    }

    int measuredCount() {
      return published.get();
    }

    long[] sortedSamples() {
      final var snapshot = Arrays.copyOf(samples, Math.min(published.get(), samples.length));
      Arrays.sort(snapshot);
      return snapshot;
    }
  }

  private enum Arm {
    kpipeParallel,
    kpipeKeyOrdered,
    confluentUnordered,
    rawVirtualThreads,
  }

  private record ArmResult(Arm arm, int samples, long p50, long p95, long p99, long max) {
    static ArmResult of(final Arm arm, final long[] sorted) {
      return new ArmResult(
        arm,
        sorted.length,
        percentile(sorted, 0.50),
        percentile(sorted, 0.95),
        percentile(sorted, 0.99),
        sorted[sorted.length - 1]
      );
    }

    /// Nearest-rank percentile on a sorted array of nanos.
    private static long percentile(final long[] sorted, final double quantile) {
      final var rank = (int) Math.ceil(quantile * sorted.length);
      return sorted[Math.max(0, rank - 1)];
    }

    String summaryLine() {
      return "samples=%d p50=%s p95=%s p99=%s max=%s".formatted(
        samples,
        micros(p50),
        micros(p95),
        micros(p99),
        micros(max)
      );
    }

    private static String micros(final long nanos) {
      return String.format(Locale.ROOT, "%.1fµs", nanos / 1_000.0);
    }
  }

  private static void printTable(final List<ArmResult> results) {
    System.out.println();
    System.out.printf("%-22s %10s %12s %12s %12s %12s%n", "arm", "samples", "p50(µs)", "p95(µs)", "p99(µs)", "max(µs)");
    for (final var r : results) {
      System.out.printf(
        Locale.ROOT,
        "%-22s %10d %12.1f %12.1f %12.1f %12.1f%n",
        r.arm(),
        r.samples(),
        r.p50() / 1_000.0,
        r.p95() / 1_000.0,
        r.p99() / 1_000.0,
        r.max() / 1_000.0
      );
    }
  }

  private static void writeJson(final List<ArmResult> results, final Config config) throws Exception {
    final var out = Path.of(System.getProperty("latency.output", "build/results/per-record-latency/results.json"));
    Files.createDirectories(out.toAbsolutePath().getParent());
    final var json = new StringBuilder();
    json.append("{\n");
    json.append("  \"harness\": \"PerRecordLatencyHarness\",\n");
    json.append("  \"timestamp\": \"").append(Instant.now()).append("\",\n");
    json
      .append("  \"config\": {\"workMicros\": ")
      .append(config.workMicros)
      .append(", \"ratePerSecond\": ")
      .append(config.ratePerSecond)
      .append(", \"warmupRecords\": ")
      .append(config.warmupRecords)
      .append(", \"measuredRecords\": ")
      .append(config.measuredRecords)
      .append("},\n");
    json.append("  \"latencyUnit\": \"nanoseconds\",\n");
    json.append("  \"results\": [\n");
    for (int i = 0; i < results.size(); i++) {
      final var r = results.get(i);
      json
        .append("    {\"arm\": \"")
        .append(r.arm())
        .append("\", \"samples\": ")
        .append(r.samples())
        .append(", \"p50\": ")
        .append(r.p50())
        .append(", \"p95\": ")
        .append(r.p95())
        .append(", \"p99\": ")
        .append(r.p99())
        .append(", \"max\": ")
        .append(r.max())
        .append(i < results.size() - 1 ? "},\n" : "}\n");
    }
    json.append("  ]\n}\n");
    Files.writeString(out, json.toString());
    System.out.println("\nJSON written to " + out.toAbsolutePath());
  }

  private record Config(List<Arm> arms, int workMicros, int ratePerSecond, int warmupRecords, int measuredRecords) {
    static Config fromSystemProperties() {
      final var armsCsv = System.getProperty(
        "latency.arms",
        "kpipeParallel,kpipeKeyOrdered,confluentUnordered,rawVirtualThreads"
      );
      final var arms = Arrays.stream(armsCsv.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(Arm::valueOf)
        .toList();
      return new Config(
        arms,
        Integer.getInteger("latency.workMicros", 100),
        Integer.getInteger("latency.ratePerSecond", 2_000),
        Integer.getInteger("latency.warmupRecords", 5_000),
        Integer.getInteger("latency.measuredRecords", 20_000)
      );
    }
  }
}
