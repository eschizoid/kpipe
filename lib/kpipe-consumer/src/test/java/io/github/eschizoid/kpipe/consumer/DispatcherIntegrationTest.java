package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/// Mini property-style suite that exercises every [ProcessingMode] through its [Dispatcher]
/// implementation end-to-end, driving a real [KPipeConsumer] against a [MockConsumer] (no
/// Docker required). Each property is asserted as a parameterized test where relevant so the
/// same invariant is checked across applicable modes.
///
/// Properties verified:
///
///   * **Liveness** — every seeded record gets dispatched exactly once. Asserted for all three
///     modes.
///   * **Strict per-partition offset ordering** — SEQUENTIAL only. Records within a partition
///     finish in offset order.
///   * **Strict per-key offset ordering** — KEY_ORDERED. Records sharing a key finish in
///     offset order regardless of how many other keys are interleaved.
///   * **No concurrent processing** — SEQUENTIAL only. At most one record running at any moment.
///   * **Cross-key concurrency** — PARALLEL and KEY_ORDERED. Records on different keys can run
///     concurrently.
///   * **Null-key sentinel queue** — KEY_ORDERED. Records with `null` keys all serialize through
///     a single queue, in offset order.
///
/// The dispatch path mocks the broker via [MockConsumer] but drives the genuine
/// `KPipeConsumer.processRecords` → `Dispatcher.dispatch` → `processRecord` →
/// `pipeline.process` → sink-invocation chain. The mode-specific assertions are what distinguish
/// whether the corresponding dispatcher is correctly preserving the property the mode promises.
class DispatcherIntegrationTest {

  private static final String TOPIC = "test-topic";

  /// One per-record observation. `startNs` and `endNs` bracket the side-effect lambda's
  /// execution — used to detect concurrent processing by comparing intervals.
  private record Observation(int partition, long offset, String key, long startNs, long endNs) {}

  // ────────────────────────────────────────────────────────────────────────────────
  // Test setup helpers
  // ────────────────────────────────────────────────────────────────────────────────

  private static Properties consumerProps() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "test-group");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return p;
  }

  /// Wraps a MockConsumer with no-op subscribe so KPipeConsumer's `subscribe(topic, listener)`
  /// path doesn't blow up when the test driver doesn't pre-assign partitions itself.
  private static MockConsumer<byte[], byte[]> newMockConsumer() {
    return new MockConsumer<>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
  }

  /// Seeds `recordsPerPartition * partitions` records into MockConsumer. Each record's key is
  /// `keyFor(partition, offset)`, value is `"v-<partition>-<offset>"`. Returns the configured
  /// consumer with finite end offsets so polling settles.
  private static MockConsumer<byte[], byte[]> seededConsumer(
    final int partitions,
    final int recordsPerPartition,
    final BiFunction<Integer, Long, String> keyFor
  ) {
    final var mc = newMockConsumer();
    final var topicPartitions = new ArrayList<TopicPartition>();
    final var beginning = new HashMap<TopicPartition, Long>();
    final var end = new HashMap<TopicPartition, Long>();
    for (int p = 0; p < partitions; p++) {
      final var tp = new TopicPartition(TOPIC, p);
      topicPartitions.add(tp);
      beginning.put(tp, 0L);
      end.put(tp, (long) recordsPerPartition);
    }
    mc.assign(topicPartitions);
    mc.updateBeginningOffsets(beginning);
    for (int p = 0; p < partitions; p++) {
      for (long offset = 0; offset < recordsPerPartition; offset++) {
        final var key = keyFor.apply(p, offset);
        final var keyBytes = key == null ? null : key.getBytes(UTF_8);
        final var value = ("v-" + p + "-" + offset).getBytes();
        mc.addRecord(new ConsumerRecord<byte[], byte[]>(TOPIC, p, offset, keyBytes, value));
      }
    }
    mc.updateEndOffsets(end);
    return mc;
  }

  /// Runs a KPipeConsumer to completion for the given mode + mocked broker, capturing every
  /// observation. Returns the captured list once `expectedCount` records have been processed.
  /// Throws via `awaitCondition` if processing doesn't reach `expectedCount` within 10s, so
  /// callers never see a partial dataset.
  private static List<Observation> runUntilProcessed(
    final ProcessingMode mode,
    final MockConsumer<byte[], byte[]> mock,
    final int expectedCount,
    final long perRecordSleepMs,
    final BiFunction<Integer, Long, String> keyFor
  ) throws InterruptedException {
    final var observations = new CopyOnWriteArrayList<Observation>();
    final var processed = new AtomicInteger(0);

    final var consumer = KPipeConsumer.builder()
      .withProperties(consumerProps())
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          // Value encodes partition+offset ("v-<p>-<off>"), parsed back here.
          // Key is rebuilt via the seeder's keyFor, so Observation.key matches
          // the dispatched record key for any key scheme.
          final var parts = new String(v).split("-", -1);
          final var p = Integer.parseInt(parts[1]);
          final var offset = Long.parseLong(parts[2]);
          final var startNs = System.nanoTime();
          if (perRecordSleepMs > 0) {
            try {
              Thread.sleep(perRecordSleepMs);
            } catch (final InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
          }
          final var endNs = System.nanoTime();
          observations.add(new Observation(p, offset, keyFor.apply(p, offset), startNs, endNs));
          processed.incrementAndGet();
          return v;
        })
      )
      .withProcessingMode(mode)
      .withConsumer(() -> mock)
      .build();

    consumer.start();
    awaitCondition(() -> processed.get() >= expectedCount, 10_000);
    consumer.close();
    return new ArrayList<>(observations);
  }

  /// Convention used both at seed time and at observation reconstruction. Keep this in lockstep
  /// with the seeder's `keyFor` lambda or assertions will compare apples to oranges.
  private static String keyAt(final int partition, final long offset) {
    return "key-" + partition + "-" + (offset % 5);
  }

  /// Waits until `cond` becomes true. Throws `AssertionError` on timeout — silently passing
  /// would let downstream property assertions (per-key order, no-overlap) succeed vacuously
  /// against a partial dataset.
  private static void awaitCondition(final BooleanSupplier cond, final long timeoutMs) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() >= deadline) throw new AssertionError(
        "awaitCondition timed out after " + timeoutMs + "ms — downstream assertions would have run on a partial dataset"
      );
      Thread.sleep(20);
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property 1: liveness — every seeded record gets processed
  // ────────────────────────────────────────────────────────────────────────────────

  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void allSeededRecordsAreEventuallyProcessed(final ProcessingMode mode) throws InterruptedException {
    final var partitions = 3;
    final var recordsPerPartition = 10;
    final var expected = partitions * recordsPerPartition;
    final var mock = seededConsumer(partitions, recordsPerPartition, DispatcherIntegrationTest::keyAt);

    final var observations = runUntilProcessed(mode, mock, expected, 0, DispatcherIntegrationTest::keyAt);

    assertEquals(expected, observations.size(), () -> "mode " + mode + " should process every seeded record");
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property 2: SEQUENTIAL preserves strict per-partition offset order
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void sequentialPreservesPerPartitionOffsetOrder() throws InterruptedException {
    final var partitions = 3;
    final var recordsPerPartition = 8;
    final var mock = seededConsumer(partitions, recordsPerPartition, DispatcherIntegrationTest::keyAt);

    final var observations = runUntilProcessed(
      ProcessingMode.SEQUENTIAL,
      mock,
      partitions * recordsPerPartition,
      0,
      DispatcherIntegrationTest::keyAt
    );

    final var byPartition = observations
      .stream()
      .collect(Collectors.groupingBy(Observation::partition, Collectors.toList()));
    for (final var entry : byPartition.entrySet()) {
      assertStrictlyAscendingOffsets(entry.getValue(), "partition " + entry.getKey());
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property 3: SEQUENTIAL never overlaps two records in time
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void sequentialDoesNotOverlapProcessing() throws InterruptedException {
    // Per-record sleep widens the window so any concurrent processing would be visible
    // through overlapping (startNs, endNs) intervals.
    final var partitions = 2;
    final var recordsPerPartition = 5;
    final var sleepMs = 30L;
    final var mock = seededConsumer(partitions, recordsPerPartition, DispatcherIntegrationTest::keyAt);

    final var observations = runUntilProcessed(
      ProcessingMode.SEQUENTIAL,
      mock,
      partitions * recordsPerPartition,
      sleepMs,
      DispatcherIntegrationTest::keyAt
    );
    assertNoOverlappingIntervals(observations);
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property 4: KEY_ORDERED preserves strict per-key offset order
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void keyOrderedPreservesPerKeyOffsetOrder() throws InterruptedException {
    // Use a single partition so the only ordering source under test is the key dispatcher
    // (multi-partition would let Kafka assign records to partitions, which is its own
    // ordering source). Multiple distinct keys, multiple records per key.
    final var partitions = 1;
    final var recordsPerPartition = 30;
    final var mock = seededConsumer(partitions, recordsPerPartition, DispatcherIntegrationTest::keyAt);

    final var observations = runUntilProcessed(
      ProcessingMode.KEY_ORDERED,
      mock,
      partitions * recordsPerPartition,
      5,
      DispatcherIntegrationTest::keyAt
    );

    final var byKey = observations.stream().collect(Collectors.groupingBy(Observation::key, Collectors.toList()));
    for (final var entry : byKey.entrySet()) {
      assertStrictlyAscendingOffsets(entry.getValue(), "key " + entry.getKey());
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property 5: PARALLEL + KEY_ORDERED exhibit cross-record concurrency
  // ────────────────────────────────────────────────────────────────────────────────

  private static Stream<ProcessingMode> concurrentModes() {
    return Stream.of(ProcessingMode.PARALLEL, ProcessingMode.KEY_ORDERED);
  }

  @ParameterizedTest
  @MethodSource("concurrentModes")
  void concurrentModesProcessMultipleRecordsSimultaneously(final ProcessingMode mode) throws InterruptedException {
    // Many distinct keys + per-record sleep so the dispatcher has many independent units of
    // work to run concurrently. If any two intervals overlap, we've proven cross-record
    // concurrency.
    final var partitions = 1;
    final var recordsPerPartition = 12;
    // Force every record to a distinct key so KEY_ORDERED can fan them out.
    final BiFunction<Integer, Long, String> distinctKeys = (p, off) -> "key-" + p + "-" + off;
    final var mock = seededConsumer(partitions, recordsPerPartition, distinctKeys);
    final var sleepMs = 40L;

    final var observations = runUntilProcessed(mode, mock, recordsPerPartition, sleepMs, distinctKeys);

    assertHasOverlappingIntervals(observations, mode);
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property 6: KEY_ORDERED serializes null-keyed records through one sentinel queue
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void keyOrderedSerializesNullKeysThroughSentinelQueue() throws InterruptedException {
    final var partitions = 1;
    final var recordsPerPartition = 10;
    // Every record gets a null key — should all serialize through the same sentinel queue
    // in offset order, never concurrent.
    final BiFunction<Integer, Long, String> allNull = (_, _) -> null;
    final var mock = seededConsumer(partitions, recordsPerPartition, allNull);

    final var observations = runUntilProcessed(ProcessingMode.KEY_ORDERED, mock, recordsPerPartition, 20, allNull);

    // All null-keyed records serialize → no overlapping intervals.
    assertNoOverlappingIntervals(observations);
    // And they're in offset order.
    assertStrictlyAscendingOffsets(observations, "null-key sentinel queue");
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property 7: offset-manager pending set drains and commit offset advances for all modes
  // ────────────────────────────────────────────────────────────────────────────────

  /// Proves the dispatch refactor preserves `KafkaOffsetManager` invariants regardless of mode.
  /// Wires a real [KafkaOffsetManager] into the consumer, processes records, then inspects the
  /// manager's per-partition state. The pending set must be empty (every dispatched record had
  /// `markOffsetProcessed` called on it), `highestProcessedOffset` must equal the last seeded
  /// offset, and `nextOffsetToCommit` must be one past it — i.e. the consumer would correctly
  /// resume from the next record on restart.
  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void offsetManagerDrainsCleanlyAcrossAllModes(final ProcessingMode mode) throws InterruptedException {
    final var partitions = 3;
    final var recordsPerPartition = 10;
    final var expected = partitions * recordsPerPartition;
    final var mock = seededConsumer(partitions, recordsPerPartition, DispatcherIntegrationTest::keyAt);
    final var processed = new AtomicInteger(0);

    final var builder = KPipeConsumer.builder()
      .withProperties(consumerProps())
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          processed.incrementAndGet();
          return v;
        })
      )
      .withProcessingMode(mode)
      .withConsumer(() -> mock);

    final var managerRef = new AtomicReference<KafkaOffsetManager>();
    builder.withOffsetManagerProvider(c -> {
      final var mgr = KafkaOffsetManager.builder(c).withCommandQueue(builder.getCommandQueue()).build();
      managerRef.set(mgr);
      return mgr;
    });

    final var consumer = builder.build();
    try {
      consumer.start();
      awaitCondition(() -> processed.get() >= expected, 10_000);

      // The side-effect lambda increments `processed` before markOffsetProcessed runs on the
      // offset manager, so a few marks may still be racing here. Wait for the manager state to
      // converge before asserting. Read state BEFORE consumer.close(): close() routes through
      // offsetManager.close() which clears pendingOffsets + highestProcessedOffsets.
      final var manager = managerRef.get();
      assertNotNull(manager, "offset manager should have been captured via the provider");
      final var topicPartitions = new ArrayList<TopicPartition>();
      for (int p = 0; p < partitions; p++) topicPartitions.add(new TopicPartition(TOPIC, p));
      awaitCondition(
        () ->
          topicPartitions
            .stream()
            .allMatch(tp -> {
              final var st = manager.getPartitionState(tp);
              return (
                ((Number) st.get("pendingCount")).intValue() == 0 &&
                ((Number) st.get("highestProcessedOffset")).longValue() == recordsPerPartition - 1
              );
            }),
        5_000
      );

      for (final var tp : topicPartitions) {
        final var state = manager.getPartitionState(tp);
        assertEquals(
          0,
          ((Number) state.get("pendingCount")).intValue(),
          () -> "mode " + mode + " left pending offsets on " + tp + ": " + state
        );
        assertEquals(
          (recordsPerPartition - 1),
          ((Number) state.get("highestProcessedOffset")).longValue(),
          () -> "mode " + mode + " did not mark all offsets processed on " + tp + ": " + state
        );
        assertEquals(
          recordsPerPartition,
          ((Number) state.get("nextOffsetToCommit")).longValue(),
          () -> "mode " + mode + " did not advance commit offset on " + tp + ": " + state
        );
      }
    } finally {
      consumer.close();
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property 8: KEY_ORDERED head-of-line isolation — a slow key must not block other keys
  // ────────────────────────────────────────────────────────────────────────────────

  /// THE defining property of KEY_ORDERED: a slow record on key A must not delay records on
  /// keys B, C, D... If it does, KEY_ORDERED has silently degraded into "single global
  /// worker." Seeds one 200ms record on key "slow" plus many fast (~1ms) records on distinct
  /// keys, asserts that the overwhelming majority of fast records finish BEFORE the slow one
  /// completes.
  @Test
  void keyOrderedSlowKeyDoesNotBlockOtherKeys() throws InterruptedException {
    final var fastCount = 20;
    final var totalRecords = fastCount + 1;
    final var observations = new CopyOnWriteArrayList<Observation>();
    final var processed = new AtomicInteger(0);

    final var mock = newMockConsumer();
    final var partition = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(partition));
    mock.updateBeginningOffsets(Map.of(partition, 0L));
    // Slow record first at offset 0 so its dispatch can't be "won" by a fast record getting
    // there first.
    mock.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "slow".getBytes(UTF_8), "v-0-0".getBytes()));
    for (long offset = 1; offset <= fastCount; offset++) {
      mock.addRecord(
        new ConsumerRecord<>(TOPIC, 0, offset, ("fast-" + offset).getBytes(UTF_8), ("v-0-" + offset).getBytes())
      );
    }
    mock.updateEndOffsets(Map.of(partition, (long) totalRecords));

    final var consumer = KPipeConsumer.builder()
      .withProperties(consumerProps())
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          final var off = Long.parseLong(new String(v).split("-", -1)[2]);
          final var key = off == 0 ? "slow" : "fast-" + off;
          final var startNs = System.nanoTime();
          try {
            Thread.sleep(off == 0 ? 200 : 1);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
          observations.add(new Observation(0, off, key, startNs, System.nanoTime()));
          processed.incrementAndGet();
          return v;
        })
      )
      .withProcessingMode(ProcessingMode.KEY_ORDERED)
      .withConsumer(() -> mock)
      .build();

    try {
      consumer.start();
      awaitCondition(() -> processed.get() >= totalRecords, 10_000);

      final var slowEndNs = observations
        .stream()
        .filter(o -> o.offset() == 0)
        .findFirst()
        .orElseThrow()
        .endNs();
      final var fastFinishedBeforeSlow = observations
        .stream()
        .filter(o -> o.offset() != 0 && o.endNs() < slowEndNs)
        .count();
      assertTrue(
        fastFinishedBeforeSlow >= 18,
        () ->
          "head-of-line isolation broken: expected ≥18/20 fast records to finish before the " +
          "slow record (200ms); got " +
          fastFinishedBeforeSlow
      );
    } finally {
      consumer.close();
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property 9: KEY_ORDERED — same-key records never overlap in time
  // ────────────────────────────────────────────────────────────────────────────────

  /// Belt for the existing suspenders. The per-key-order assertion implies no-overlap GIVEN
  /// today's dispatcher design — but a future refactor (e.g. changing `pollFirst` to a
  /// non-blocking variant, or splitting work across multiple workers per queue) could break
  /// the no-overlap invariant without breaking the order assertion. This test asserts
  /// no-overlap directly so such a regression fails CI.
  @Test
  void keyOrderedSameKeyRecordsDoNotOverlapInTime() throws InterruptedException {
    final var partitions = 1;
    final var recordsPerPartition = 10;
    // Constant key across all records — forces all into a single per-key queue.
    final BiFunction<Integer, Long, String> constantKey = (_, _) -> "single";
    final var mock = seededConsumer(partitions, recordsPerPartition, constantKey);

    final var observations = runUntilProcessed(ProcessingMode.KEY_ORDERED, mock, recordsPerPartition, 20, constantKey);

    assertNoOverlappingIntervals(observations);
    assertStrictlyAscendingOffsets(observations, "single-key queue");
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property 10: KEY_ORDERED — same key across multiple partitions still serializes
  // ────────────────────────────────────────────────────────────────────────────────

  /// Real edge case: during partition reassignment or after a topic resize, the same key bytes
  /// can land on two partitions simultaneously. The dispatcher keys by `record.key()` only —
  /// not by `(partition, key)` — so both routes must serialize through the same worker. If a
  /// future change keyed by `(partition, key)` instead, two workers would race on the same
  /// logical key and break the user contract.
  @Test
  void keyOrderedSameKeyAcrossPartitionsStillSerializes() throws InterruptedException {
    final var partitions = 2;
    final var recordsPerPartition = 5;
    final BiFunction<Integer, Long, String> sharedKey = (_, _) -> "shared";
    final var mock = seededConsumer(partitions, recordsPerPartition, sharedKey);

    final var observations = runUntilProcessed(
      ProcessingMode.KEY_ORDERED,
      mock,
      partitions * recordsPerPartition,
      15,
      sharedKey
    );

    // Across all observations (regardless of partition), no two same-key intervals may overlap.
    // Here every observation IS same-key, so global no-overlap suffices.
    assertNoOverlappingIntervals(observations);
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property 11: KEY_ORDERED — a failed record does not block same-key successors
  // ────────────────────────────────────────────────────────────────────────────────

  /// When a record on key A fails (retries exhausted, error handler invoked, offset marked
  /// processed), the worker must continue draining its queue — records (A, offset+1) onward
  /// must still process in order. Tests with `withErrorHandler` rather than full DLQ wiring;
  /// the same `handleProcessingError → markOffsetProcessed → worker continues drain` path
  /// exercises whether dispatcher state survives a failure.
  @Test
  void keyOrderedFailureDoesNotBlockSameKeySuccessors() throws InterruptedException {
    final var partitions = 1;
    final var recordsPerPartition = 10;
    final var failingOffset = 5L;
    final BiFunction<Integer, Long, String> singleKey = (_, _) -> "key-A";
    final var mock = seededConsumer(partitions, recordsPerPartition, singleKey);

    final var processedSuccessfully = new CopyOnWriteArrayList<Long>();
    final var errored = new CopyOnWriteArrayList<Long>();
    final var totalCompleted = new AtomicInteger(0);

    final var consumer = KPipeConsumer.builder()
      .withProperties(consumerProps())
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          final var off = Long.parseLong(new String(v).split("-", -1)[2]);
          if (off == failingOffset) throw new RuntimeException("deliberate failure at offset " + off);
          processedSuccessfully.add(off);
          return v;
        })
      )
      .withProcessingMode(ProcessingMode.KEY_ORDERED)
      .withRetry(0, Duration.ofMillis(1)) // fail immediately, no retries
      .withErrorHandler(err -> {
        errored.add(err.record().offset());
        totalCompleted.incrementAndGet();
      })
      .withConsumer(() -> mock)
      .build();

    try {
      consumer.start();
      awaitCondition(() -> processedSuccessfully.size() + errored.size() >= recordsPerPartition, 10_000);

      assertEquals(List.of(failingOffset), errored, "exactly the seeded failing offset should hit the error handler");
      assertEquals(
        recordsPerPartition - 1,
        processedSuccessfully.size(),
        () -> "all non-failing records must process; got " + processedSuccessfully
      );
      // Successors of the failing record must appear in the success log — proving the worker
      // didn't get stuck.
      for (long off = failingOffset + 1; off < recordsPerPartition; off++) {
        final long offset = off;
        assertTrue(
          processedSuccessfully.contains(offset),
          () -> "successor offset " + offset + " missing from successful set: " + processedSuccessfully
        );
      }
      // And per-key order is preserved through the failure (success list ascending modulo gap).
      for (int i = 1; i < processedSuccessfully.size(); i++) {
        assertTrue(
          processedSuccessfully.get(i) > processedSuccessfully.get(i - 1),
          () -> "order broken across failure: " + processedSuccessfully
        );
      }
    } finally {
      consumer.close();
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property 12: KEY_ORDERED — retry on a record holds same-key successors in queue
  // ────────────────────────────────────────────────────────────────────────────────

  /// During retry attempts on (key A, offset N), records (A, offset N+1..) must wait in the
  /// queue — neither processing concurrently nor lost. Proves the worker correctly serializes
  /// retries with the same-key queue. Pipeline fails offset 2 on its first attempt, succeeds
  /// on retry; assertions: (a) all records ultimately succeed, (b) offsets after 2 start AFTER
  /// 2's final completion, (c) offset 2 took at least 1 retry-backoff.
  @Test
  void keyOrderedRetryHoldsSameKeySuccessorsInQueue() throws InterruptedException {
    final var partitions = 1;
    final var recordsPerPartition = 5;
    final var retryingOffset = 2L;
    final var retryBackoff = Duration.ofMillis(50);
    final BiFunction<Integer, Long, String> singleKey = (_, _) -> "key-A";
    final var mock = seededConsumer(partitions, recordsPerPartition, singleKey);

    final var attemptsPerOffset = new ConcurrentHashMap<Long, AtomicInteger>();
    final var firstAttemptStartNs = new ConcurrentHashMap<Long, Long>();
    final var observations = new CopyOnWriteArrayList<Observation>();
    final var processed = new AtomicInteger(0);
    // Sleep per attempt — gives observations non-zero intervals so successor-startNs checks
    // (and overall elapsed-time checks against retryBackoff) measure something real instead
    // of just nanoTime monotonicity.
    final var workMillis = 5L;

    final var consumer = KPipeConsumer.builder()
      .withProperties(consumerProps())
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          final var off = Long.parseLong(new String(v).split("-", -1)[2]);
          final var attempt = attemptsPerOffset.computeIfAbsent(off, _ -> new AtomicInteger(0)).incrementAndGet();
          final var attemptStartNs = System.nanoTime();
          firstAttemptStartNs.putIfAbsent(off, attemptStartNs);
          try {
            Thread.sleep(workMillis);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
          if (off == retryingOffset && attempt == 1) {
            throw new RuntimeException("first attempt fails on offset " + off);
          }
          observations.add(new Observation(0, off, "key-A", attemptStartNs, System.nanoTime()));
          processed.incrementAndGet();
          return v;
        })
      )
      .withProcessingMode(ProcessingMode.KEY_ORDERED)
      .withRetry(2, retryBackoff)
      .withConsumer(() -> mock)
      .build();

    try {
      consumer.start();
      awaitCondition(() -> processed.get() >= recordsPerPartition, 10_000);

      assertEquals(recordsPerPartition, observations.size(), "all records must ultimately succeed");
      assertEquals(2, attemptsPerOffset.get(retryingOffset).get(), "retrying offset must have exactly 2 attempts");

      // (b) Successors of the retrying record must START after the retrying record's final
      // attempt completed (proves the dispatcher serialized retries with same-key successors).
      final var retried = observations
        .stream()
        .filter(o -> o.offset() == retryingOffset)
        .findFirst()
        .orElseThrow();
      for (final var obs : observations) {
        if (obs.offset() > retryingOffset) {
          assertTrue(
            obs.startNs() >= retried.endNs(),
            () -> "successor offset " + obs.offset() + " started before retried offset finished"
          );
        }
      }

      // (c) The retrying offset must have waited at least one retry-backoff between attempts.
      // Measured as (final-attempt-end) - (first-attempt-start) ≥ workMillis + retryBackoff —
      // the first attempt runs workMillis, then waits retryBackoff, then the second attempt
      // runs workMillis again.
      final var retriedFirstStart = firstAttemptStartNs.get(retryingOffset);
      final var retriedTotalNanos = retried.endNs() - retriedFirstStart;
      final var minExpectedNanos = retryBackoff.toNanos();
      assertTrue(
        retriedTotalNanos >= minExpectedNanos,
        () ->
          "retrying offset total elapsed " +
          (retriedTotalNanos / 1_000_000) +
          "ms is less than retryBackoff " +
          retryBackoff.toMillis() +
          "ms — backoff not observed"
      );
    } finally {
      consumer.close();
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Property assertion helpers
  // ────────────────────────────────────────────────────────────────────────────────

  /// Asserts that the observations sorted by completion time appear in ascending offset order.
  /// Equivalent to "earlier offsets finished earlier" given that observations are appended in
  /// completion order by the side-effect lambda.
  private static void assertStrictlyAscendingOffsets(final List<Observation> obs, final String groupLabel) {
    for (int i = 1; i < obs.size(); i++) {
      final var prev = obs.get(i - 1);
      final var cur = obs.get(i);
      assertTrue(
        cur.offset() > prev.offset(),
        () ->
          "Offset order violated in " +
          groupLabel +
          ": offset " +
          cur.offset() +
          " observed after " +
          prev.offset() +
          " (full sequence: " +
          obs
            .stream()
            .map(o -> Long.toString(o.offset()))
            .collect(Collectors.joining(",")) +
          ")"
      );
    }
  }

  /// Asserts that no two observations have overlapping `[startNs, endNs]` intervals. Used to
  /// prove single-threaded execution (SEQUENTIAL or null-keyed serial queue).
  private static void assertNoOverlappingIntervals(final List<Observation> obs) {
    final var sorted = new ArrayList<>(obs);
    sorted.sort(Comparator.comparingLong(Observation::startNs));
    for (int i = 1; i < sorted.size(); i++) {
      final var prev = sorted.get(i - 1);
      final var cur = sorted.get(i);
      assertTrue(
        cur.startNs() >= prev.endNs(),
        () ->
          "Concurrent processing detected: record at offset " +
          cur.offset() +
          " started at " +
          cur.startNs() +
          " before predecessor at offset " +
          prev.offset() +
          " ended at " +
          prev.endNs()
      );
    }
  }

  /// Asserts that at least one pair of observations has overlapping intervals (proves
  /// concurrency happens). Reports a clear failure when concurrency was expected but not
  /// observed.
  private static void assertHasOverlappingIntervals(final List<Observation> obs, final ProcessingMode mode) {
    final var sorted = new ArrayList<>(obs);
    sorted.sort(Comparator.comparingLong(Observation::startNs));
    for (int i = 0; i < sorted.size(); i++) {
      for (int j = i + 1; j < sorted.size(); j++) {
        final var a = sorted.get(i);
        final var b = sorted.get(j);
        if (a.endNs() > b.startNs() && a.startNs() < b.endNs()) return;
      }
    }
    fail(
      "Mode " +
        mode +
        " did not exhibit concurrent processing — no two observation intervals overlapped. " +
        "Observation count: " +
        obs.size()
    );
  }
}
