package io.github.eschizoid.kpipe.benchmarks;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.eschizoid.kpipe.consumer.ConsumerCommand;
import io.github.eschizoid.kpipe.consumer.KafkaOffsetManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/// JMH benchmark for the `KafkaOffsetManager` hot path — `trackOffset` + `markOffsetProcessed`.
///
/// ### What it measures
///
/// Per-record cost of the two `OffsetManager` calls that fire on every Kafka record. Pre-cache
/// each call allocated a fresh `TopicPartition(topic, partition)` to use as a `ConcurrentHashMap`
/// key — at ~100k rec/s that's ~200k disposable `TopicPartition` instances/sec from this manager
/// alone. The cache (a `ConcurrentHashMap<String, ConcurrentHashMap<Integer, TopicPartition>>`
/// keyed by topic then partition) collapses those allocations to one instance per partition for
/// the lifetime of the assignment.
///
/// The bench drives the public `KafkaOffsetManager` API directly with synthetic `ConsumerRecord`
/// instances (no Kafka, no MockConsumer poll loop, no SerDe). The cache is pre-warmed in
/// `@Setup` so the steady-state fast path is the only thing measured — first-call allocations
/// are excluded from the measurement window.
///
/// ### Parameters
///
/// - `partitions ∈ {8}` — a small assignment that pre-warms the cache. Matches the audit's
///   "partition set is warm in steady state" assumption. Wider sweeps (1, 8, 64) aren't
///   informative here because the cache lookup is O(1) per record regardless of partition count.
///
/// ### Companion profilers
///
/// Run with `-prof gc` to see the allocation-rate drop directly:
///
/// ```bash
/// ./gradlew :benchmarks:jmh \
///   -Pjmh.includes='OffsetManagerBoxingBenchmark' \
///   -Pjmh.profilers='gc'
/// ```
///
/// The `gc.alloc.rate.norm` column (bytes allocated per op) is the primary signal — the cache
/// should remove one `TopicPartition` instance worth of allocation per record. Read the actual
/// per-op byte delta from the profiler rather than estimating the header size, which varies by
/// JVM version and compressed-oops mode. Throughput improvement is secondary; the JIT may already
/// optimize the constructor away in the baseline, in which case the alloc-rate delta is the only
/// signal.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class OffsetManagerBoxingBenchmark {

  private static final String TOPIC = "kpipe-offsetmgr-bench";
  private static final int PARTITION_COUNT = 8;

  /// Records driven through the manager per `@Benchmark` invocation. Sized to amortize the
  /// JMH per-invocation overhead while keeping the bench cell short enough that JIT/GC noise
  /// stays bounded. Each record exercises both `trackOffset` and `markOffsetProcessed`.
  private static final int RECORDS_PER_INVOCATION = 10_000;

  private static final byte[] PAYLOAD = "v".getBytes();

  @Param({ "8" })
  public int partitions;

  private KafkaOffsetManager offsetManager;
  private Queue<ConsumerCommand> commandQueue;
  private MockConsumer<byte[], byte[]> mockConsumer;
  private ConsumerRecord<byte[], byte[]>[] records;

  @Setup(Level.Trial)
  @SuppressWarnings("unchecked")
  public void setup() {
    commandQueue = new ConcurrentLinkedQueue<>();
    mockConsumer = new MockConsumer<>("earliest");
    final var assignment = new ArrayList<TopicPartition>(partitions);
    for (var p = 0; p < partitions; p++) assignment.add(new TopicPartition(TOPIC, p));
    mockConsumer.assign(assignment);
    final var beginningOffsets = new HashMap<TopicPartition, Long>();
    for (final var tp : assignment) beginningOffsets.put(tp, 0L);
    mockConsumer.updateBeginningOffsets(beginningOffsets);

    offsetManager = KafkaOffsetManager.builder(mockConsumer).withCommandQueue(commandQueue).build();
    offsetManager.start();

    // Pre-build the synthetic record set so the bench loop does no allocation outside the
    // manager — only the cache-hit path is on the timed path.
    records = (ConsumerRecord<byte[], byte[]>[]) new ConsumerRecord<?, ?>[RECORDS_PER_INVOCATION];
    for (var i = 0; i < RECORDS_PER_INVOCATION; i++) {
      records[i] = new ConsumerRecord<>(TOPIC, i % partitions, i, ("k-" + i).getBytes(UTF_8), PAYLOAD);
    }

    // Warm the cache: one track per partition populates `topicPartitionCache` so the
    // measurement loop only exercises the fast path.
    for (var p = 0; p < partitions; p++) {
      offsetManager.trackOffset(new ConsumerRecord<>(TOPIC, p, -1L, "warmup".getBytes(UTF_8), PAYLOAD));
      offsetManager.markOffsetProcessed(new ConsumerRecord<>(TOPIC, p, -1L, "warmup".getBytes(UTF_8), PAYLOAD));
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (offsetManager != null) offsetManager.close();
    if (mockConsumer != null) mockConsumer.close();
    if (commandQueue != null) commandQueue.clear();
  }

  /// Drives `trackOffset` then `markOffsetProcessed` for each pre-built `ConsumerRecord`. The
  /// `@OperationsPerInvocation` count is `RECORDS_PER_INVOCATION` so the reported throughput
  /// is in records/sec rather than invocations/sec.
  @Benchmark
  @OperationsPerInvocation(RECORDS_PER_INVOCATION)
  public void trackAndMark() {
    final var rs = records;
    final var mgr = offsetManager;
    for (var i = 0; i < rs.length; i++) {
      final var r = rs[i];
      mgr.trackOffset(r);
      mgr.markOffsetProcessed(r);
    }
  }

}
