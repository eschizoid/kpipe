package io.github.eschizoid.kpipe.benchmarks;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;

/// Competitive parallel-consumer throughput benchmark. Twelve arms consume the same seeded
/// topic from the same Testcontainers-managed Kafka broker:
///
///   * **Single-threaded `KafkaConsumer`** — poll loop with inline processing, no fan-out. The
///     default pattern most teams ship with; the floor that motivates the rest.
///   * **KPipe (PARALLEL)** — virtual-thread-per-record on Loom. Unordered.
///   * **KPipe (KEY_ORDERED)** — per-key serial queues on virtual threads. Rivals: `atleonKey`
///     and `confluentKey`.
///   * **Kafka Share Consumer (KIP-932)** — one share consumer + virtual-thread fan-out, the
///     controlled apples-to-apples rival of KPipe PARALLEL. Unordered.
///   * **Confluent Parallel Consumer (UNORDERED)** — `ProcessingOrder.UNORDERED`.
///   * **Confluent Parallel Consumer (KEY)** — `ProcessingOrder.KEY`, per-key FIFO.
///   * **Confluent Parallel Consumer (PARTITION)** — `ProcessingOrder.PARTITION`.
///   * **Reactor Kafka** — `Flux<ReceiverRecord>` on the Reactor `parallel` scheduler with a
///     100 rails. Re-enabled after Reactor Kafka 1.3.25 (Nov 2025), the first
///     release to drop the deprecated `ConsumerRecord` ctor that was removed in
///     `kafka-clients:4.x`. The dependency's POM still pins `kafka-clients:3.9.1` but the new
///     binary works when Gradle conflict-resolves to our 4.3.0.
///   * **Atleon** — `atleonPartition`, `atleonParallel`, and `atleonKey` use Alo processing
///     with virtual-thread schedulers; grouped modes consume each group serially.
///   * **Raw `KafkaConsumer` + virtual threads** — hand-rolled loop with VT fan-out. The
///     Loom-only floor; what you get without a framework but with Loom.
///
/// Each invocation processes [ParallelProcessingBenchmarkInfrastructure#TARGET_MESSAGES] records.
/// The `workMicros` `@Param` injects per-record simulated work via `LockSupport.parkNanos` so
/// the bench covers the following workload regimes:
///
///   * `0 µs` — pure framework overhead (no work per record).
///   * `100 µs` — typical local enrichment (in-memory transform, no I/O).
///   * `1000 µs` — typical blocking I/O (HTTP / JDBC / S3 round trip).
///   * `10000`, `35000`, `50000`, and `100000 µs` — longer blocking I/O waits.
///
/// Run every runtime across the parameter sweep:
///
/// ```bash
/// ./gradlew :benchmarks:jmh -Pjmh.includes='ParallelProcessingBenchmark'
/// ```
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ParallelProcessingBenchmark {

  private static final int TARGET_MESSAGES = ParallelProcessingBenchmarkInfrastructure.TARGET_MESSAGES;

  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void kpipe(final ParallelProcessingBenchmarkInfrastructure.KpipeInvocationContext context) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("kpipe", context.processedCount());
  }

  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void confluent(final ParallelProcessingBenchmarkInfrastructure.ConfluentInvocationContext context) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("confluent", context.processedCount());
  }

  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void reactor(final ParallelProcessingBenchmarkInfrastructure.ReactorInvocationContext context) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("reactor", context.processedCount());
  }

  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void atleonPartition(
    final ParallelProcessingBenchmarkInfrastructure.AtleonPartitionInvocationContext context
  ) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("atleon-partition", context.processedCount());
  }

  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void atleonParallel(final ParallelProcessingBenchmarkInfrastructure.AtleonParallelInvocationContext context) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("atleon-parallel", context.processedCount());
  }

  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void atleonKey(final ParallelProcessingBenchmarkInfrastructure.AtleonKeyOrderedInvocationContext context) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("atleon-key", context.processedCount());
  }

  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void raw(final ParallelProcessingBenchmarkInfrastructure.RawInvocationContext context) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("raw", context.processedCount());
  }

  /// Kafka 4.x Share Consumer (KIP-932), single consumer + virtual-thread fan-out. The
  /// controlled apples-to-apples comparison against `kpipe` (PARALLEL): same broker, seed,
  /// workload, and metric; only the fetch+acknowledge protocol differs. Both are unordered.
  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void share(final ParallelProcessingBenchmarkInfrastructure.ShareConsumerInvocationContext context) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("share", context.processedCount());
  }

  /// KPipe in KEY_ORDERED mode — per-key FIFO guarantee. Rivals: `atleonKey` and `confluentKey`.
  /// Read the gap versus unordered arms as the cost of ordering, not a head-to-head loss against
  /// unordered runtime.
  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void kpipeKeyOrdered(
    final ParallelProcessingBenchmarkInfrastructure.KpipeKeyOrderedInvocationContext context
  ) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("kpipe-key-ordered", context.processedCount());
  }

  /// Single-threaded `KafkaConsumer` poll loop with inline processing — no framework, no fan-out.
  /// The default pattern most teams ship with; the floor that motivates every framework's
  /// existence. Per-partition ordering is implicit (one thread).
  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void singleThread(final ParallelProcessingBenchmarkInfrastructure.SingleThreadInvocationContext context) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("single-thread", context.processedCount());
  }

  /// Confluent Parallel Consumer, `ProcessingOrder.KEY` — per-key FIFO. The cross-library rival
  /// of `kpipeKeyOrdered` for the "what does key ordering cost?" question.
  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void confluentKey(final ParallelProcessingBenchmarkInfrastructure.ConfluentKeyInvocationContext context) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("confluent-key", context.processedCount());
  }

  /// Confluent Parallel Consumer, `ProcessingOrder.PARTITION` — strict per-partition ordering,
  /// parallelism capped at the partition count (8 here).
  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void confluentPartition(
    final ParallelProcessingBenchmarkInfrastructure.ConfluentPartitionInvocationContext context
  ) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("confluent-partition", context.processedCount());
  }
}
