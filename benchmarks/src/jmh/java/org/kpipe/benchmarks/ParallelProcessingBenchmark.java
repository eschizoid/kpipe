package org.kpipe.benchmarks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.kpipe.consumer.FunctionalConsumer;
import org.openjdk.jmh.annotations.*;

/// JMH Benchmark for comparing KPipe's parallel processing engine against the
/// Confluent Parallel Consumer.
///
/// This benchmark measures raw throughput (messages processed per second) by
/// simulating a high-concurrency message ingestion scenario using an embedded
/// Kafka broker powered by Apache Kafka's test kit.
///
/// ### Scenarios:
/// 1. **KPipe Parallel Mode**: Leverages Java 24 Virtual Threads (Project Loom) for
///    record-level parallelism with minimal overhead.
/// 2. **Confluent Parallel Consumer**: Industry-standard library for parallel
///    processing, using traditional platform thread pools.
///
/// ### Design Integrity:
/// - Both frameworks start from the beginning of the topic (`earliest`) for each iteration.
/// - Both process exactly **1,000 messages** per iteration.
/// - KPipe uses Loom; Confluent uses a max concurrency of **100**.
///
/// ### Running the Benchmark:
/// ```bash
/// ./gradlew :benchmarks:jmh -Pjmh.includes='ParallelProcessingBenchmark'
/// ```
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ParallelProcessingBenchmark {

  private static final String TOPIC = ParallelProcessingBenchmarkInfrastructure.TOPIC;
  private static final int TARGET_MESSAGES = ParallelProcessingBenchmarkInfrastructure.TARGET_MESSAGES;

  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void kpipeParallelProcessing(final ParallelProcessingBenchmarkInfrastructure.KafkaContext context) {
    final var currentProcessedCount = new AtomicInteger(0);

    final var kpipeProps = context.consumerProps("kpipe-group");
    kpipeProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    try (
      final var consumer = FunctionalConsumer
        .<byte[], byte[]>builder()
        .withProperties(kpipeProps)
        .withTopic(TOPIC)
        .withMessageSink((_, __) -> {})
        .withProcessor(val -> {
          currentProcessedCount.incrementAndGet();
          return val;
        })
        .withSequentialProcessing(false)
        .build()
    ) {
      consumer.start();
      ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages("kpipeParallelProcessing", currentProcessedCount);
    }
  }

  @Benchmark
  @OperationsPerInvocation(TARGET_MESSAGES)
  public void confluentParallelProcessing(
    final ParallelProcessingBenchmarkInfrastructure.ConfluentInvocationContext context
  ) {
    ParallelProcessingBenchmarkInfrastructure.awaitProcessedMessages(
      "confluentParallelProcessing",
      context.processedCount()
    );
  }
}
