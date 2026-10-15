package io.github.eschizoid.kpipe.benchmarks;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;

/// Latency-mode companion to [ParallelProcessingBenchmark]. Same seeded topic, but reports the
/// **distribution** of "time to drain N records" rather than steady-state throughput.
///
/// JMH `SampleTime` mode publishes the percentile histogram per benchmark — `p(0.50)`, `p(0.95)`,
/// `p(0.99)`, `p(0.999)`, `p(1.00)` — which are the numbers competitive evaluators actually want
/// from a parallel-Kafka library. Average throughput is half the story; tail latency is the other
/// half, and the two libraries can rank differently on each.
///
/// Run:
///
/// ```bash
/// ./gradlew :benchmarks:jmh -Pjmh.includes='ParallelProcessingLatencyBenchmark'
/// ```
///
/// Every arm uses `@OperationsPerInvocation(TARGET_MESSAGES)`, so JMH already divides batch
/// completion time by the record count and reports amortized milliseconds per record. Multiply
/// the reported value by `TARGET_MESSAGES` to recover batch-drain time. The samples describe
/// batches, not individual record latency; [PerRecordLatencyHarness] measures the latter.
@BenchmarkMode({ Mode.SampleTime, Mode.AverageTime })
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ParallelProcessingLatencyBenchmark {

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

  /// KPipe in KEY_ORDERED mode. Its per-key rivals are `atleonKey` and `confluentKey`.
  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void kpipeKeyOrdered(
    final ParallelProcessingBenchmarkInfrastructure.KpipeKeyOrderedInvocationContext context
  ) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("kpipe-key-ordered", context.processedCount());
  }

  /// Confluent Parallel Consumer with `ProcessingOrder.KEY`, the per-key rival of
  // `kpipeKeyOrdered`.
  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void confluentKey(final ParallelProcessingBenchmarkInfrastructure.ConfluentKeyInvocationContext context) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("confluent-key", context.processedCount());
  }

  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void raw(final ParallelProcessingBenchmarkInfrastructure.RawInvocationContext context) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("raw", context.processedCount());
  }
}
