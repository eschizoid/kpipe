package org.kpipe.benchmarks;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;

/// Latency-mode companion to [ParallelProcessingBenchmark]. Same four runtimes, same seeded
/// topic, but reports the **distribution** of "time to drain N records" rather than steady-state
/// throughput.
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
/// The reported time-per-op is "time to process [#TARGET_MESSAGES] records", not per-record
/// latency. Divide by `TARGET_MESSAGES` for the per-record figure; the percentile *shape* (mean
/// vs p99) is the headline, not the absolute number.
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

  // `reactor` row disabled — Reactor Kafka 1.3.23 (latest stable) is incompatible with
  // kafka-clients 4.x. Re-enable when upstream ships a 4.x-compatible build.

  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void raw(final ParallelProcessingBenchmarkInfrastructure.RawInvocationContext context) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("raw", context.processedCount());
  }
}
