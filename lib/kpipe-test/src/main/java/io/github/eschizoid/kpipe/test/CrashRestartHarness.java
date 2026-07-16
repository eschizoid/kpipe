package io.github.eschizoid.kpipe.test;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.OffsetManager;
import io.github.eschizoid.kpipe.consumer.OffsetState;
import io.github.eschizoid.kpipe.consumer.OffsetStatistics;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.UnaryOperator;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/// A Docker-free driver that exercises a KPipe pipeline's resume-window delivery path across
/// a simulated consumer crash — deterministically, with no broker and no timing races.
///
/// **What it exercises.** A first consumer (A) processes a prefix `[0, P)` and commits only
/// `[0, k)`, then crashes; a fresh consumer (B) is driven over the resume window `[k, N)` —
/// the log a broker replays from committed offset `k` — and runs the full
/// poll → dispatch → operator → sink path on it. The uncommitted tail `[k, P)` therefore
/// appears in B's output, which is where a pipeline's own idempotency / dedup gets tested.
///
/// **Scope.** The harness *supplies* B's resume window from `k`; it does not assert that the
/// consumer *seeks* to a committed offset, nor the commit-frontier math (lowest-pending, no
/// commit-ahead). Those are covered by the offset jcstress/property suites and the broker E2E
/// `CrashRestartReprocessingIntegrationTest`. What this pins deterministically is the
/// resume-window delivery path — the piece a live-broker test can only observe flakily.
///
/// **Why it's deterministic.** The crash geometry is expressed as seeded ranges, not
/// wall-clock timing:
///
///   * consumer A is seeded with exactly `[0, P)`, so it processes those `P` and then idles
///     — the stopping point is the seed, not a "crash it at the right instant" race;
///   * A commits only through offset `k` (`k < P`), leaving `[k, P)` uncommitted;
///   * consumer B is seeded with `[k, N)` — the log a broker replays from offset `k`.
///
/// Each phase runs a real [KPipeConsumer] over a subscribe-stubbed `MockConsumer`. The crash
/// is modelled without leaking threads: each phase uses an offset manager that never commits
/// on its own, and phase A commits exactly `k` explicitly before `close()`. Because the
/// manager never advanced past `k`, the tail stays uncommitted — as an abrupt kill would.
///
/// ```java
/// final var result = CrashRestartHarness.builder(JsonFormat.INSTANCE)
///     .withProcessingMode(ProcessingMode.SEQUENTIAL)
///     .pipe(enrich)
///     .seed(values)        // N records
///     .commitUpTo(3)       // A commits through offset 3
///     .crashAfter(7)       // A processes [0,7), then crashes
///     .restart();          // B's resume window is [3,N)
///
/// // B's resume window includes the uncommitted tail [3,7).
/// assertTrue(result.secondRun().containsAll(result.uncommittedTail()));
/// ```
///
/// @param <T> the pipeline value type
public final class CrashRestartHarness<T> {

  private static final Duration QUIESCE_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration CLOSE_AGE_CAP = Duration.ofDays(1);
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(10);
  private static final String TOPIC = "kpipe-crash-restart-topic";

  private static final String METRIC_RECEIVED = "messagesReceived";
  private static final String METRIC_PROCESSED = "messagesProcessed";
  private static final String METRIC_ERRORS = "processingErrors";
  private static final String METRIC_IN_FLIGHT = "inFlight";

  private final MessageFormat<T> format;
  private final List<UnaryOperator<T>> operators = new ArrayList<>();
  private ProcessingMode processingMode = ProcessingMode.SEQUENTIAL;
  private List<T> seed;
  private int commitUpTo = -1;
  private int crashAfter = -1;
  private int batchMaxSize = 0;
  private boolean ran = false;

  private CrashRestartHarness(final MessageFormat<T> format) {
    this.format = Objects.requireNonNull(format, "format cannot be null");
  }

  /// Creates a harness over the given format.
  ///
  /// @param format the SerDe for the pipeline value type
  /// @param <T> the pipeline value type
  /// @return a new harness
  public static <T> CrashRestartHarness<T> builder(final MessageFormat<T> format) {
    return new CrashRestartHarness<>(format);
  }

  /// Overrides the processing mode (default [ProcessingMode#SEQUENTIAL]). Under
  /// [ProcessingMode#PARALLEL] / [ProcessingMode#KEY_ORDERED] the captured order is not
  /// the seed order, so assertions on the result must be order-insensitive.
  ///
  /// @param mode the processing mode
  /// @return this harness
  public CrashRestartHarness<T> withProcessingMode(final ProcessingMode mode) {
    this.processingMode = Objects.requireNonNull(mode, "mode cannot be null");
    return this;
  }

  /// Appends a transformation operator to the pipeline both consumer instances run.
  ///
  /// @param op the operator
  /// @return this harness
  public CrashRestartHarness<T> pipe(final UnaryOperator<T> op) {
    operators.add(Objects.requireNonNull(op, "operator cannot be null"));
    return this;
  }

  /// Sets the full stream `[0, N)` of typed values to drive through the pipeline.
  ///
  /// @param values the seeded values (must be non-empty)
  /// @return this harness
  public CrashRestartHarness<T> seed(final List<T> values) {
    this.seed = List.copyOf(Objects.requireNonNull(values, "seed cannot be null"));
    return this;
  }

  /// Sets the offset consumer A durably commits before it crashes (`k`). Must satisfy
  /// `0 <= k < crashAfter`.
  ///
  /// @param k the committed offset
  /// @return this harness
  public CrashRestartHarness<T> commitUpTo(final int k) {
    this.commitUpTo = k;
    return this;
  }

  /// Sets how many records consumer A processes before crashing (`P`). Must satisfy
  /// `commitUpTo < P <= N`.
  ///
  /// @param p the count A processes before the crash
  /// @return this harness
  public CrashRestartHarness<T> crashAfter(final int p) {
    this.crashAfter = p;
    return this;
  }

  /// Routes both phases through a batch sink flushing every `maxSize` records (the trailing
  /// partial batch flushes on the crash/close). Because the offset manager never commits,
  /// records buffered at crash time are uncommitted and appear in B's resume window — the
  /// batch analogue of the per-record crash.
  ///
  /// @param maxSize the size-flush threshold (must be ≥ 1)
  /// @return this harness
  public CrashRestartHarness<T> toBatch(final int maxSize) {
    if (maxSize < 1) throw new IllegalArgumentException("maxSize must be >= 1, got " + maxSize);
    this.batchMaxSize = maxSize;
    return this;
  }

  /// Runs both phases and returns what each delivered: A over `[0, P)` commits `k` then
  /// crashes, B over the resume window `[k, N)`. Single-use — build a fresh harness per
  /// scenario.
  ///
  /// @return the crash-restart result
  /// @throws IllegalArgumentException if the geometry is invalid (`0 <= k < P <= N` and a
  ///     non-empty seed are required)
  /// @throws IllegalStateException if called more than once
  public CrashRestartResult<T> restart() {
    if (ran) throw new IllegalStateException("restart() already called — build a fresh harness per scenario");
    Objects.requireNonNull(seed, "seed(...) is required before restart()");
    final var n = seed.size();
    if (n == 0) throw new IllegalArgumentException("seed must be non-empty");
    if (crashAfter < 0) throw new IllegalArgumentException("crashAfter(P) is required before restart()");
    if (commitUpTo < 0) throw new IllegalArgumentException("commitUpTo(k) is required before restart()");
    if (!(commitUpTo < crashAfter && crashAfter <= n)) throw new IllegalArgumentException(
      "require 0 <= commitUpTo(" + commitUpTo + ") < crashAfter(" + crashAfter + ") <= seedSize(" + n + ")"
    );
    ran = true;

    final var first = runPhase(0, crashAfter, commitUpTo);
    final var second = runPhase(commitUpTo, n, -1);
    // The uncommitted tail is defined by offset — the values at offsets [k, P) — not by A's
    // capture order, which is not offset order under PARALLEL / KEY_ORDERED. It is the tail as
    // the sink sees it (operators applied, filtered records dropped), matching secondRun's shape.
    final var tail = new ArrayList<T>();
    for (final var value : seed.subList(commitUpTo, crashAfter)) {
      final var transformed = applyOperators(value);
      if (transformed != null) tail.add(transformed);
    }
    return new CrashRestartResult<>(first, second, commitUpTo, tail);
  }

  /// Applies the configured operators to one seeded value in order, returning the
  /// transformed value — or `null` if an operator filtered it out, so the expected tail
  /// matches what the sink actually delivers.
  private T applyOperators(final T value) {
    var current = value;
    for (final var op : operators) {
      if (current == null) return null;
      current = op.apply(current);
    }
    return current;
  }

  /// Runs one consumer over the seeded window `[fromOffset, toExclusive)` and returns what
  /// its sink captured. When `commitOffset >= 0` (phase A) the mock is committed to that
  /// offset before close, modelling the durable commit that survives the crash.
  private List<T> runPhase(final int fromOffset, final int toExclusive, final int commitOffset) {
    final var count = toExclusive - fromOffset;
    final var sink = new CapturingSink<T>();

    final var pipelineBuilder = new MessageProcessorRegistry().pipeline(format);
    for (final var op : operators) pipelineBuilder.add(op);
    if (batchMaxSize == 0) pipelineBuilder.toSink(sink);
    final var pipeline = pipelineBuilder.build();

    final var mock = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, (long) fromOffset));
    for (var i = 0; i < count; i++) {
      final var value = seed.get(fromOffset + i);
      mock.addRecord(new ConsumerRecord<>(TOPIC, 0, fromOffset + i, null, format.serialize(value)));
    }

    final var consumerBuilder = KPipeConsumer.builder()
      .withProperties(props())
      .withProcessingMode(processingMode)
      .withConsumer(() -> mock)
      .withOffsetManager(new NoCommitOffsetManager())
      .withPollTimeout(POLL_TIMEOUT);
    if (batchMaxSize == 0) {
      consumerBuilder.withTopic(TOPIC).withPipeline(pipeline);
    } else {
      consumerBuilder.withBatchPipeline(
        TOPIC,
        pipeline,
        BatchSink.ofVoid(batch -> batch.forEach(sink::accept)),
        new BatchPolicy(batchMaxSize, CLOSE_AGE_CAP)
      );
    }

    try (final var consumer = consumerBuilder.build()) {
      consumer.start();
      awaitQuiescent(consumer, count);
      if (commitOffset >= 0) mock.commitSync(Map.of(tp, new OffsetAndMetadata(commitOffset)));
    }
    return sink.captured();
  }

  /// Blocks until every seeded record has been received and is accounted for (terminal or
  /// buffered) and the dispatcher has drained. Mirrors [TestStream]'s quiescence check:
  /// records move strictly forward, so seeing "all accounted for" before "in-flight
  /// drained" cannot yield a false positive.
  private static void awaitQuiescent(final KPipeConsumer consumer, final long target) {
    final var deadline = System.nanoTime() + QUIESCE_TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      if (quiescent(consumer, target)) return;
      try {
        //noinspection BusyWait — deadline-bounded quiescence poll, not a spin
        Thread.sleep(5);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("crash-restart phase interrupted awaiting " + target + " records", e);
      }
    }
    throw new AssertionError("crash-restart phase timed out awaiting " + target + " records: " + consumer.getMetrics());
  }

  private static boolean quiescent(final KPipeConsumer consumer, final long target) {
    final var snapshot = consumer.getMetrics();
    if (snapshot.get(METRIC_RECEIVED) < target) return false;
    if (accountedFor(snapshot) < target) return false;
    if (!consumer.waitForInFlightDrain(Duration.ofMillis(1))) return false;
    final var settled = consumer.getMetrics();
    return settled.get(METRIC_RECEIVED) >= target && accountedFor(settled) >= target;
  }

  private static long accountedFor(final Map<String, Long> snapshot) {
    return snapshot.get(METRIC_PROCESSED) + snapshot.get(METRIC_ERRORS) + snapshot.get(METRIC_IN_FLIGHT);
  }

  /// Minimal, never-contacted config — the `MockConsumer` supplier bypasses the real
  /// client, so only the properties object's presence matters.
  private static Properties props() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "kpipe-crash-restart");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("enable.auto.commit", "false");
    return p;
  }

  /// An [OffsetManager] that never commits: `trackOffset` / `markOffsetProcessed` are
  /// no-ops, so the consumer's own bookkeeping can never advance the commit point. The
  /// harness commits exactly `k` explicitly on the mock; everything past `k` therefore
  /// stays uncommitted through the crash, which is the whole point.
  private static final class NoCommitOffsetManager implements OffsetManager {

    @Override
    public OffsetManager start() {
      return this;
    }

    @Override
    public OffsetManager stop() {
      return this;
    }

    @Override
    public void trackOffset(final ConsumerRecord<byte[], byte[]> record) {}

    @Override
    public void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record) {}

    @Override
    public void notifyCommitComplete(final String commitId, final boolean success) {}

    @Override
    public OffsetState getState() {
      return OffsetState.RUNNING;
    }

    @Override
    public boolean isRunning() {
      return true;
    }

    @Override
    public OffsetStatistics getStatistics() {
      return OffsetStatistics.empty();
    }

    @Override
    public void close() {}
  }
}
