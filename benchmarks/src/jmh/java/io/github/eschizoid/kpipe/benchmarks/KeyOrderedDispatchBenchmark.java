package io.github.eschizoid.kpipe.benchmarks;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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

/// JMH benchmark for `ProcessingMode.KEY_ORDERED` dispatcher throughput. Originally built to
/// establish the lock-contention baseline of the v1 single-`ReentrantLock` dispatcher; that
/// baseline arbitrated the v2 rewrite (ConcurrentHashMap + per-queue monitors), which measured
/// +122% at 10k distinct keys / +112% at 100 keys in an interleaved A/B at stable control
/// (2026-07-21, see `benchmarks/results/`). The bench remains the gate for any future
/// dispatcher work: re-run it and demonstrate a measurable win before landing.
///
/// ### What it measures
///
/// End-to-end throughput of `KPipeConsumer` driving a no-op pipeline through MockConsumer. The
/// pipeline itself is byte-passthrough (no SerDe), so per-record cost is dominated by the
/// dispatcher: enqueue under lock + worker dequeue under lock + in-flight counter ops. PARALLEL
/// mode is included as the control — same code path, no per-key lock-protected queue, so its
/// throughput represents "what KEY_ORDERED would achieve if the lock weren't in the way."
///
/// ### Parameters
///
/// - `mode ∈ {PARALLEL, KEY_ORDERED}` — control vs. system-under-test. PARALLEL has no per-key
///   structure so its dispatcher path is the floor for lock-attributable cost.
/// - `distinctKeys ∈ {1, 100, 10000}` — single hot key (all work serialized on one queue),
///   moderate fan-out (typical app), and max fan-out (one record per key, filling the LRU to
///   its default 10,000 cap — the highest lock churn for the single-lock design). `distinctKeys`
///   never exceeds `RECORDS_PER_INVOCATION`, which is sized to match so every cell is reachable
///   (keys are `i % distinctKeys`, so a too-small record count would silently cap the key count).
///
/// ### Wrapper-access strategy
///
/// Drives the public `KPipeConsumer` end-to-end via the same `MockConsumer` setup used by
/// `BatchSinkLatencyBenchmark` and `DispatcherIntegrationTest`. `KeyOrderedDispatcher` is
/// package-private; we measure it through the front door. Dispatcher lock contention manifests
/// as a KEY_ORDERED-throughput gap relative to PARALLEL — that's the signal future optimization
/// work targets.
///
/// ### Running
///
/// ```bash
/// ./gradlew :benchmarks:jmh -Pjmh.includes='KeyOrderedDispatchBenchmark'
/// ```
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class KeyOrderedDispatchBenchmark {

  private static final String TOPIC = "kpipe-keyordered-bench";
  private static final int PARTITION = 0;

  /// Records primed into MockConsumer per invocation. Must be >= the largest `distinctKeys`
  /// value, or that cell silently caps its key count (keys are `i % distinctKeys`). 10,000
  /// matches both the top `distinctKeys` value and the default KEY_ORDERED LRU cap, so the
  /// max-fan-out cell genuinely fills the LRU.
  private static final int RECORDS_PER_INVOCATION = 10_000;

  private static final byte[] PAYLOAD = "v".getBytes();

  @Param({ "PARALLEL", "KEY_ORDERED" })
  public ProcessingMode mode;

  @Param({ "1", "100", "10000" })
  public int distinctKeys;

  /// Per-invocation context. Fresh consumer per JMH invocation so MockConsumer state and
  /// dispatcher LRU don't leak between measurements.
  @State(Scope.Thread)
  public static class InvocationContext {

    KPipeConsumer consumer;
    CountDownLatch processedLatch;

    @Setup(Level.Invocation)
    public void setup(final KeyOrderedDispatchBenchmark trial) {
      processedLatch = new CountDownLatch(RECORDS_PER_INVOCATION);

      final var partition = new TopicPartition(TOPIC, PARTITION);
      final var mockConsumer = new MockConsumer<byte[], byte[]>("earliest") {
        @Override
        public synchronized void subscribe(final Collection<String> topics) {}

        @Override
        public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
      };
      mockConsumer.assign(List.of(partition));
      mockConsumer.updateBeginningOffsets(Map.of(partition, 0L));
      for (int i = 0; i < RECORDS_PER_INVOCATION; i++) {
        final var key = ("k-" + (i % trial.distinctKeys)).getBytes(UTF_8);
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, PARTITION, i, key, PAYLOAD));
      }

      // Pure byte-passthrough format keeps the per-record work inside the dispatcher path,
      // so the bench isolates lock contention from SerDe cost.
      final var registry = new MessageProcessorRegistry();
      final var pipeline = registry
        .pipeline(MessageFormat.bytes())
        .toSink((final byte[] _) -> processedLatch.countDown())
        .build();

      consumer = KPipeConsumer.builder()
        .withProperties(baseProps())
        .withConsumer(() -> mockConsumer)
        .withProcessingMode(trial.mode)
        .withTopic(TOPIC)
        .withPipeline(pipeline)
        .withPollTimeout(Duration.ofMillis(10))
        .build();
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
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "kpipe-keyordered-bench");
      props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer"
      );
      props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer"
      );
      return props;
    }
  }

  @Benchmark
  @OperationsPerInvocation(RECORDS_PER_INVOCATION)
  public void dispatch(final InvocationContext ctx) throws InterruptedException {
    ctx.start();
    if (!ctx.processedLatch.await(60, TimeUnit.SECONDS)) {
      throw new IllegalStateException("bench cell did not finish within 60s");
    }
  }
}
