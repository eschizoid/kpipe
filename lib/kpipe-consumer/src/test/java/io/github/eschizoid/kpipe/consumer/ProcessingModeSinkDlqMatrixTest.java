package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchResult;
import io.github.eschizoid.kpipe.sink.BatchSink;
import io.github.eschizoid.kpipe.sink.CompositeMessageSink;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

/// The combinatorial cell of the lifecycle matrix that isn't already pinned elsewhere:
/// [ProcessingMode] × sink shape (single / multi / batch) × DLQ on/off × retry × circuit breaker,
/// each driven end-to-end through a started [KPipeConsumer] against a [MockConsumer] (no Docker).
/// Every cell asserts the at-least-once invariants:
///
///   * **No loss, no silent drop** — every seeded record lands in exactly one terminal bucket
///     (success sink OR dead-letter), and the two buckets together cover the full seeded
///     offset set.
///   * **Offsets advance** — `markOffsetProcessed` is called for every record so the commit offset
///     advances past the batch (no infinite re-fetch).
///
/// Cells deliberately NOT duplicated here (already covered, with the owning test named so a future
/// reader doesn't re-add them):
///
///   * ProcessingMode × ordering / per-key serialization / offset-drain correctness —
///     `DispatcherIntegrationTest` (all three modes, plus KEY_ORDERED × retry and KEY_ORDERED ×
///     error-handler failure).
///   * Circuit-breaker trip / half-open probe / recovery, including the batch-failure trip path —
///     `KPipeCircuitBreakerIntegrationTest` (SEQUENTIAL + batch). Not re-run per mode here: the
///     breaker observes pipeline outcomes through the same `recordSuccess` / `recordFailure` hooks
///     regardless of dispatcher, so a third copy per mode would add cost without a new property.
///   * DLQ send mechanics via `processRecord` directly (synchronous send, throwing send, null
///     pipeline, bundled `withDeadLetterQueue` setter) — `KPipeDlqTest`.
///
/// Dropped cells (logged here rather than silently truncated): the full 3×3×2×2×2 = 72-cell product
/// is not enumerated. Retry+CB are toggled together with the "all features" cell rather than as
/// independent axes because their interaction with the dispatcher is identical to their interaction
/// already proven in the single-axis suites; the high-value gap is the sink-shape × DLQ × mode
/// face, which is what this class fills.
class ProcessingModeSinkDlqMatrixTest {

  private static final String TOPIC = "matrix-topic";
  private static final String DLQ_TOPIC = "matrix-dlq";
  private static final int RECORDS = 12;

  private static Properties props() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "matrix-group");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("enable.auto.commit", "false");
    return p;
  }

  /// Records every `markOffsetProcessed` offset so we can assert the commit offset would advance
  /// past every seeded record (no re-fetch loop).
  private static final class MarkRecordingOffsetManager implements OffsetManager {

    final Set<Long> markedOffsets = ConcurrentHashMap.newKeySet();

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
    public void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record) {
      markedOffsets.add(record.offset());
    }

    @Override
    public void notifyCommitComplete(final String commitId, final boolean success) {}

    @Override
    public OffsetState getState() {
      return OffsetState.CREATED;
    }

    @Override
    public boolean isRunning() {
      return true;
    }

    @Override
    public Map<String, Object> getStatistics() {
      return Map.of();
    }

    @Override
    public void close() {}
  }

  private static MockConsumer<byte[], byte[]> seeded(final int recordCount) {
    final var mock = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    for (int i = 0; i < recordCount; i++) {
      // Value encodes the offset so the pipeline can decide pass/fail per record.
      mock.addRecord(new ConsumerRecord<>(TOPIC, 0, i, ("k" + i).getBytes(UTF_8), ("v" + i).getBytes(StandardCharsets.UTF_8)));
    }
    mock.updateEndOffsets(Map.of(tp, (long) recordCount));
    return mock;
  }

  private static long offsetOf(final byte[] value) {
    // Strip the leading "v" and parse the offset the seeder encoded.
    return Long.parseLong(new String(value, StandardCharsets.UTF_8).substring(1));
  }

  /// A DLQ producer that records every send into a concurrent list and completes the future, so a
  /// started consumer's synchronous DLQ send succeeds and we can read back which offsets parked.
  @SuppressWarnings("unchecked")
  private static Producer<byte[], byte[]> recordingDlqProducer(final List<Long> dlqOffsets) {
    final Producer<byte[], byte[]> producer = Mockito.mock(Producer.class);
    Mockito.lenient()
      .when(producer.send(Mockito.any(ProducerRecord.class)))
      .thenAnswer(inv -> {
        final ProducerRecord<byte[], byte[]> rec = inv.getArgument(0);
        dlqOffsets.add(offsetOf(rec.value()));
        return CompletableFuture.completedFuture(Mockito.mock(RecordMetadata.class));
      });
    return producer;
  }

  private static void awaitCondition(final BooleanSupplier cond, final long timeoutMs) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() >= deadline) throw new AssertionError(
        "awaitCondition timed out after " + timeoutMs + "ms — invariant assertions would have run on a partial dataset"
      );
      Thread.sleep(10);
    }
  }

  /// Every offset in `[0, RECORDS)` must appear in exactly one of the two terminal buckets, the two
  /// buckets must be disjoint, and every offset must have been marked processed. This is the
  /// at-least-once-without-loss-and-without-duplicate-terminal-bucket invariant for the cell.
  private static void assertExactlyOnceTerminal(
    final Collection<Long> succeeded,
    final Collection<Long> deadLettered,
    final Set<Long> marked,
    final String cellLabel
  ) {
    final var union = new HashSet<Long>();
    union.addAll(succeeded);
    union.addAll(deadLettered);

    final var overlap = new HashSet<>(succeeded);
    overlap.retainAll(deadLettered);
    assertTrue(
      overlap.isEmpty(),
      () -> cellLabel + ": offsets reached BOTH success and DLQ (double terminal): " + overlap
    );

    for (long off = 0; off < RECORDS; off++) {
      final long o = off;
      assertTrue(
        union.contains(o),
        () ->
          cellLabel +
          ": offset " +
          o +
          " reached NO terminal bucket (silent loss). success=" +
          succeeded +
          " dlq=" +
          deadLettered
      );
      assertTrue(
        marked.contains(o),
        () -> cellLabel + ": offset " + o + " was never marked processed (would re-fetch forever). marked=" + marked
      );
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Cell 1: single sink × all-success × no DLQ — baseline at-least-once per mode
  // ────────────────────────────────────────────────────────────────────────────────

  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void singleSinkAllSuccessMarksEveryOffset(final ProcessingMode mode) throws Exception {
    final var sinkSaw = new CopyOnWriteArrayList<Long>();
    final var manager = new MarkRecordingOffsetManager();
    final var mock = seeded(RECORDS);
    final MessageSink<byte[]> sink = v -> sinkSaw.add(offsetOf(v));

    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withProcessingMode(mode)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          sink.accept(v);
          return v;
        })
      )
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      .withPollTimeout(Duration.ofMillis(5))
      .build();

    try {
      consumer.start();
      awaitCondition(() -> sinkSaw.size() >= RECORDS && manager.markedOffsets.size() >= RECORDS, 10_000);

      assertExactlyOnceTerminal(sinkSaw, List.of(), manager.markedOffsets, "single-sink/all-success/" + mode);
    } finally {
      consumer.close();
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Cell 2: multi-sink fanout × all-success — every sink sees every record
  // ────────────────────────────────────────────────────────────────────────────────

  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void multiSinkFanoutEverySinkSeesEveryRecord(final ProcessingMode mode) throws Exception {
    // Drive both terminal sinks through the real CompositeMessageSink broadcast (the explicit-API
    // multi-sink shape), not two inline adds — so the assertion reflects the fanout machinery's
    // delivery, not the test harness. Both sinks must see all records regardless of dispatcher,
    // and offsets still mark exactly once.
    final var sinkA = new CopyOnWriteArrayList<Long>();
    final var sinkB = new CopyOnWriteArrayList<Long>();
    final MessageSink<byte[]> recordA = v -> sinkA.add(offsetOf(v));
    final MessageSink<byte[]> recordB = v -> sinkB.add(offsetOf(v));
    final var fanout = new CompositeMessageSink<>(List.of(recordA, recordB));
    final var manager = new MarkRecordingOffsetManager();
    final var mock = seeded(RECORDS);

    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withProcessingMode(mode)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          fanout.accept(v);
          return v;
        })
      )
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      .withPollTimeout(Duration.ofMillis(5))
      .build();

    try {
      consumer.start();
      awaitCondition(
        () -> sinkA.size() >= RECORDS && sinkB.size() >= RECORDS && manager.markedOffsets.size() >= RECORDS,
        10_000
      );

      assertExactlyOnceTerminal(sinkA, List.of(), manager.markedOffsets, "multi-sink-A/" + mode);
      assertEquals(
        new HashSet<>(sinkA),
        new HashSet<>(sinkB),
        () -> "both sinks in the fanout must see the same offset set for mode " + mode
      );
    } finally {
      consumer.close();
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Cell 3: single sink × retry-then-DLQ — failing records park, successes sink
  // ────────────────────────────────────────────────────────────────────────────────

  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void retryThenDlqRoutesFailuresAndKeepsAtLeastOnce(final ProcessingMode mode) throws Exception {
    // Records whose offset is even fail every attempt → after retries exhaust they must reach the
    // DLQ. Odd offsets succeed → reach the success sink. The two buckets must exactly partition the
    // seeded set, every offset must be marked. This is the per-mode DLQ-through-a-started-consumer
    // gap (KPipeDlqTest only drives processRecord directly on the default mode).
    final var sinkSaw = new CopyOnWriteArrayList<Long>();
    final var dlqOffsets = new CopyOnWriteArrayList<Long>();
    final var manager = new MarkRecordingOffsetManager();
    final var mock = seeded(RECORDS);
    final var dlqProducer = recordingDlqProducer(dlqOffsets);

    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withProcessingMode(mode)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          final var off = offsetOf(v);
          if (off % 2 == 0) throw new RuntimeException("deliberate failure at offset " + off);
          sinkSaw.add(off);
          return v;
        })
      )
      .withRetry(1, Duration.ofMillis(1))
      .withDeadLetterTopic(DLQ_TOPIC)
      .withKafkaProducer(dlqProducer)
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      .withPollTimeout(Duration.ofMillis(5))
      .build();

    try {
      consumer.start();
      final var expectedSuccess = RECORDS / 2;
      final var expectedDlq = RECORDS - expectedSuccess;
      awaitCondition(
        () ->
          sinkSaw.size() >= expectedSuccess &&
          dlqOffsets.size() >= expectedDlq &&
          manager.markedOffsets.size() >= RECORDS,
        10_000
      );

      assertExactlyOnceTerminal(sinkSaw, dlqOffsets, manager.markedOffsets, "retry-then-dlq/" + mode);
      // Sanity: the partition matches the even/odd rule we encoded.
      assertTrue(sinkSaw.stream().allMatch(o -> o % 2 == 1), () -> "only odd offsets should succeed: " + sinkSaw);
      assertTrue(dlqOffsets.stream().allMatch(o -> o % 2 == 0), () -> "only even offsets should DLQ: " + dlqOffsets);
    } finally {
      consumer.close();
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Cell 4: batch sink × per-record DLQ — partial-batch failure routing per mode
  // ────────────────────────────────────────────────────────────────────────────────

  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void batchSinkPartialFailureRoutesPerRecordDlq(final ProcessingMode mode) throws Exception {
    // A batch sink that fails the even-offset records within each batch via a per-index
    // BatchResult. The wrapper must route exactly those to the DLQ and mark the succeeded ones,
    // covering the full seeded set. Exercises the batch coverage-contract path under each
    // dispatcher. RECORDS=12 with size 5 gives batches [5, 5, 2] — the trailing 2 are a genuine
    // partial batch that only flushes at teardown, so this also exercises the close()-drain path.
    final var sinkSaw = new CopyOnWriteArrayList<Long>();
    final var dlqOffsets = new CopyOnWriteArrayList<Long>();
    final var manager = new MarkRecordingOffsetManager();
    final var mock = seeded(RECORDS);
    final var dlqProducer = recordingDlqProducer(dlqOffsets);

    final BatchSink<byte[]> batchSink = batch -> {
      final var succeeded = new ArrayList<Integer>();
      final var failed = new HashMap<Integer, Exception>();
      for (int i = 0; i < batch.size(); i++) {
        final var off = offsetOf(batch.get(i));
        if (off % 2 == 0) {
          failed.put(i, new RuntimeException("batch failure at offset " + off));
        } else {
          succeeded.add(i);
          sinkSaw.add(off);
        }
      }
      return new BatchResult(succeeded, failed);
    };

    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withBatchPipeline(TOPIC, TestPipelines.sideEffect(v -> v), batchSink, BatchPolicy.ofSize(5))
      .withProcessingMode(mode)
      .withDeadLetterTopic(DLQ_TOPIC)
      .withKafkaProducer(dlqProducer)
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      .withPollTimeout(Duration.ofMillis(5))
      .build();

    try {
      consumer.start();
      final var expectedSuccess = RECORDS / 2;
      final var expectedDlq = RECORDS - expectedSuccess;
      // Two batches flush on size(5) (10 records); the trailing partial batch of 2 flushes at
      // teardown — so we close() to force the final flush, then assert. Wait for the size-triggered
      // flushes first to avoid racing the close.
      awaitCondition(() -> sinkSaw.size() + dlqOffsets.size() >= 10, 10_000);
    } finally {
      consumer.close();
    }

    // After close() the trailing buffered batch has flushed (teardown drain). Now the full set is
    // terminal.
    assertExactlyOnceTerminal(sinkSaw, dlqOffsets, manager.markedOffsets, "batch-partial-dlq/" + mode);
    assertTrue(
      sinkSaw.stream().allMatch(o -> o % 2 == 1),
      () -> "only odd offsets should succeed in batch: " + sinkSaw
    );
    assertTrue(
      dlqOffsets.stream().allMatch(o -> o % 2 == 0),
      () -> "only even offsets should DLQ in batch: " + dlqOffsets
    );
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Cell 5: the "everything on" cell — PARALLEL + retry + CB + DLQ + single sink
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void allFeaturesEnabledStillSatisfiesAtLeastOnce() throws Exception {
    // The densest realistic config: PARALLEL dispatch, retry, a circuit breaker (configured but
    // never tripping because the failure rate stays below threshold), DLQ for the few failures, a
    // single success sink. Every record must still reach exactly one terminal bucket and be marked.
    final var sinkSaw = new CopyOnWriteArrayList<Long>();
    final var dlqOffsets = new CopyOnWriteArrayList<Long>();
    final var manager = new MarkRecordingOffsetManager();
    final var mock = seeded(RECORDS);
    final var dlqProducer = recordingDlqProducer(dlqOffsets);
    final var attempts = new ConcurrentHashMap<Long, AtomicInteger>();

    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withProcessingMode(ProcessingMode.PARALLEL)
      // High windowSize/threshold so sparse failures never trip the breaker here.
      .withCircuitBreaker(new CircuitBreakerController(0.9, 50, Duration.ofSeconds(30)))
      .withRetry(2, Duration.ofMillis(1))
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          final var off = offsetOf(v);
          // Offset 3 recovers on retry; offset 7 fails permanently to the DLQ.
          final var n = attempts.computeIfAbsent(off, _ -> new AtomicInteger(0)).incrementAndGet();
          if (off == 3 && n == 1) throw new RuntimeException("transient failure, recovers on retry");
          if (off == 7) throw new RuntimeException("permanent failure → DLQ");
          sinkSaw.add(off);
          return v;
        })
      )
      .withDeadLetterTopic(DLQ_TOPIC)
      .withKafkaProducer(dlqProducer)
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      .withPollTimeout(Duration.ofMillis(5))
      .build();

    try {
      consumer.start();
      awaitCondition(
        () -> sinkSaw.size() >= RECORDS - 1 && dlqOffsets.contains(7L) && manager.markedOffsets.size() >= RECORDS,
        10_000
      );

      assertExactlyOnceTerminal(sinkSaw, dlqOffsets, manager.markedOffsets, "all-features");
      assertEquals(List.of(7L), dlqOffsets, "only the permanently-failing offset 7 should reach the DLQ");
      assertTrue(sinkSaw.contains(3L), "offset 3 must succeed on retry, not DLQ");
      assertEquals(
        0L,
        consumer.getMetrics().get(KPipeConsumer.METRIC_CIRCUIT_BREAKER_TRIPS),
        "the breaker must not trip at this sparse failure rate"
      );
    } finally {
      consumer.close();
    }
  }
}
