package org.kpipe.benchmarks;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;

/// Competitive parallel-consumer throughput benchmark. Six runtimes consume the same seeded
/// topic from the same Testcontainers-managed Kafka broker:
///
///   * **KPipe (PARALLEL)** — virtual-thread-per-record on Loom. Unordered.
///   * **KPipe (KEY_ORDERED)** — per-key serial queues on virtual threads. The only arm with an
///     ordering guarantee; included to measure the per-key ordering tax, not as a head-to-head.
///   * **Kafka Share Consumer (KIP-932)** — one share consumer + virtual-thread fan-out, the
///     controlled apples-to-apples rival of KPipe PARALLEL. Unordered.
///   * **Confluent Parallel Consumer** — `ProcessingOrder.UNORDERED`, 100-worker pool.
///   * **Reactor Kafka** — `Flux<ReceiverRecord>` on the Reactor `parallel` scheduler with a
///     matching concurrency limit. Re-enabled after Reactor Kafka 1.3.25 (Nov 2025) shipped the
///     fix for issue #420 (avoid the deprecated `ConsumerRecord` ctor that was removed in
///     `kafka-clients:4.x`); the dependency's POM still pins `kafka-clients:3.9.1` but the new
///     binary works when Gradle conflict-resolves to our 4.2.0.
///   * **Raw `KafkaConsumer` + virtual threads** — the hand-rolled baseline. No framework, no
///     offset manager. Establishes the floor of "what if I just wrote the loop myself?"
///
/// Each invocation processes [ParallelProcessingBenchmarkInfrastructure#TARGET_MESSAGES] records.
/// The `workMicros` `@Param` injects per-record simulated work via `LockSupport.parkNanos` so
/// the bench covers three workload regimes:
///
///   * `0 µs` — pure framework overhead (no work per record).
///   * `100 µs` — typical local enrichment (in-memory transform, no I/O).
///   * `1000 µs` — typical blocking I/O (HTTP / JDBC / S3 round trip).
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

  /// KPipe in KEY_ORDERED mode — the only arm with an ordering guarantee (per-key FIFO). Read
  /// the gap versus the unordered arms as the cost of ordering, NOT as a head-to-head loss: no
  /// other runtime in this suite (share included) can provide per-key ordering at all.
  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void kpipeKeyOrdered(
    final ParallelProcessingBenchmarkInfrastructure.KpipeKeyOrderedInvocationContext context
  ) {
    context.start();
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("kpipe-key-ordered", context.processedCount());
  }
}
