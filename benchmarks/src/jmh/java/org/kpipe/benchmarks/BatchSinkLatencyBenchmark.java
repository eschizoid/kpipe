package org.kpipe.benchmarks;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.consumer.ProcessingMode;
import org.kpipe.format.json.JsonFormat;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.BatchSink;
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
import org.openjdk.jmh.infra.Blackhole;

/// JMH benchmark for evaluating the throughput win of batch sinks when the destination has a
/// non-trivial per-call latency.
///
/// ### Why this benchmark exists
///
/// A naive bench that compares a no-op `MessageSink` against a no-op `BatchSink` produces a
/// statistical tie at small batch sizes and a regression at large ones (GC + buffer overhead). The
/// real win of batching is **amortizing per-call cost when the destination has nontrivial
/// latency** — a JDBC commit, an HTTP POST, an S3 PUT. This bench simulates that destination via
/// `LockSupport.parkNanos(sinkLatencyMicros * 1000L)`:
///
/// - Single-record sink: parks once **per record** (control).
/// - Batch sink: parks once **per batch** (regardless of batch size — that's the point).
///
/// ### Parameters
///
/// - `sinkLatencyMicros ∈ {10, 100, 1000}` — fast (in-memory hash), medium (local JDBC), slow
///   (HTTP / S3) destinations.
/// - `batchSize ∈ {1, 10, 100}` — `1` is the per-call control (the batch sink fires per record,
///   matching the single-record path); `10` and `100` should show the batching win at higher
///   latencies.
///
/// ### Wrapper-access strategy
///
/// The bench drives the `BatchPipelineWrapper` indirectly through the public
/// `KPipeConsumer.Builder.withBatchPipeline(...)` entry point — the same API the fluent facade
/// (`Stream.toBatch(...)`) delegates to. This keeps `BatchPipelineWrapper` package-private. The
/// fake Kafka backend is Apache Kafka's `MockConsumer`, which we pre-prime with the workload via
/// `withConsumer(() -> mockConsumer)` (a public builder hook). No new test bridges are needed in
/// `kpipe-consumer`.
///
/// ### Running
///
/// ```bash
/// ./gradlew :benchmarks:jmh -Pjmh.includes='BatchSinkLatencyBenchmark'
/// ```
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class BatchSinkLatencyBenchmark {

  private static final String TOPIC = "kpipe-batch-bench";
  private static final int PARTITION = 0;

  /// Records primed into the MockConsumer per invocation. Sized to keep the slowest cell
  /// (single-record path at 1ms latency) below ~500ms while still giving the fastest cell
  /// (batch=100 at 10µs latency) enough work to dwarf JMH invocation overhead.
  private static final int RECORDS_PER_INVOCATION = 500;

  /// Pre-serialized JSON payload reused across every invocation.
  private static final byte[] JSON_PAYLOAD = """
    {
      "id": 12345,
      "name": "John Doe",
      "email": "john.doe@example.com",
      "active": true,
      "balance": 1250.50
    }
    """.getBytes(StandardCharsets.UTF_8);

  @Param({ "10", "100", "1000" })
  public int sinkLatencyMicros;

  @Param({ "1", "10", "100" })
  public int batchSize;

  /// Per-invocation context. Each measured invocation gets a fresh consumer / mock-consumer pair so
  /// (a) JMH's `Level.Invocation` overhead is bounded by the real workload (≥ 5ms in the worst
  /// cell), and (b) we don't have to model post-poll backpressure / record reuse.
  @State(Scope.Thread)
  public static class InvocationContext {

    KPipeConsumer<byte[]> consumer;
    CountDownLatch processedLatch;

    @Setup(Level.Invocation)
    public void setup(final BatchSinkLatencyBenchmark trial) {
      processedLatch = new CountDownLatch(RECORDS_PER_INVOCATION);

      final var partition = new TopicPartition(TOPIC, PARTITION);
      // The KPipeConsumer subscribe path requires a stub-able subscribe; we override both arities
      // and hand-assign the partition before adding records (matches the existing
      // KPipeConsumerMockingTest pattern).
      final var mockConsumer = new MockConsumer<byte[], byte[]>("earliest") {
        @Override
        public synchronized void subscribe(final Collection<String> topics) {
          // pre-assigned below
        }

        @Override
        public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {
          // pre-assigned below
        }
      };
      mockConsumer.assign(List.of(partition));
      mockConsumer.updateBeginningOffsets(Map.of(partition, 0L));
      for (int i = 0; i < RECORDS_PER_INVOCATION; i++) {
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, PARTITION, i, null, JSON_PAYLOAD));
      }

      final var nanos = trial.sinkLatencyMicros * 1_000L;

      // The pipeline is identical across cells: deserialize JSON once, no operators. Construction
      // happens in setup so it doesn't pollute measured time.
      final var registry = new MessageProcessorRegistry();
      final var pipeline = registry.pipeline(JsonFormat.INSTANCE).build();

      final var props = baseProps();

      final var builder = KPipeConsumer.<byte[]>builder()
        .withProperties(props)
        .withConsumer(() -> mockConsumer)
        .withProcessingMode(ProcessingMode.SEQUENTIAL)
        .withPollTimeout(Duration.ofMillis(10));

      if (trial.batchSize == 1) {
        // Per-record control path — wire a normal MessageSink that parks once per record. This
        // exercises the same byte → JSON deserialization path the batch path uses; the only
        // difference is the terminal sink.
        final var perRecordPipeline = registry
          .pipeline(JsonFormat.INSTANCE)
          .toSink((final Map<String, Object> v) -> {
            LockSupport.parkNanos(nanos);
            processedLatch.countDown();
          })
          .build();
        consumer = builder.withTopic(TOPIC).withPipeline(perRecordPipeline).build();
      } else {
        // Batch path — flush parks once per batch regardless of batch size. The latch counts down
        // by batch.size() so the latch resolution semantics are identical between cells.
        final BatchSink<Map<String, Object>> batchSink = BatchSink.ofVoid(batch -> {
          LockSupport.parkNanos(nanos);
          for (int i = 0; i < batch.size(); i++) processedLatch.countDown();
        });
        // A permissive 1-minute age cap means flush is purely size-driven within an invocation,
        // which keeps the parked-call count deterministic at exactly RECORDS / batchSize.
        final var policy = new BatchPolicy(trial.batchSize, Duration.ofMinutes(1));
        consumer = builder.withBatchPipeline(TOPIC, pipeline, batchSink, policy).build();
      }
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
      if (consumer != null) consumer.close();
    }

    void start() {
      consumer.start();
    }

    private static Properties baseProps() {
      final var props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ignored:9092");
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "kpipe-batch-bench");
      props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer"
      );
      props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer"
      );
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      return props;
    }
  }

  /// A reference to silence the "unused field" warning when the bench is run in IDE form. The
  /// real workload counters live in `InvocationContext`; this consume keeps the params hot in the
  /// dead-code-elimination model JMH uses.
  @Benchmark
  @OperationsPerInvocation(RECORDS_PER_INVOCATION)
  public void run(final InvocationContext ctx, final Blackhole bh) throws InterruptedException {
    ctx.start();
    if (!ctx.processedLatch.await(60, TimeUnit.SECONDS)) {
      throw new IllegalStateException(
        "BatchSinkLatencyBenchmark timed out: latency=" + sinkLatencyMicros + "µs, batchSize=" + batchSize
      );
    }
    bh.consume(ctx.processedLatch);
  }
}
