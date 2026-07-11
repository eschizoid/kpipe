package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchResult;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Invariant-rigor companion to [BatchPipelineWrapperCoverageContractTest]. Where that suite pins
/// the headline cases (trailing-gap, out-of-range, null result, age tick, close drain), this one
/// hammers the *partial-coverage* surface of the `succeededIndexes ∪ failedByIndex` contract: the
/// disjoint union of those two sets must equal `[0, batchSize)`, and any index NOT in that union is
/// a contract violation that the wrapper must route to the failure callback (DLQ) with a synthetic
/// [IllegalStateException] — never silently `markProcessed`.
///
/// The binding invariant exercised across every case below:
///
/// > For every position `i` in `[0, batchSize)`, exactly one callback fires for the record at `i`.
/// > It is `markProcessed` if and only if `i` is a valid succeeded index; otherwise it is
/// > `onBatchFailure`. A missing index (neither succeeded nor failed) must reach `onBatchFailure`
/// > carrying an `IllegalStateException`, and the count of `markProcessed + onBatchFailure` must
/// > equal `batchSize` with no record fired twice.
///
/// Edges covered here that the sibling suite does not:
///
/// * Interior gaps — succeeded = {0, 2, 4}, missing = {1, 3} (not just a trailing gap).
/// * Mixed explicit-failure + missing — explicit failures keep their own per-record cause, missing
///   indexes carry the synthetic coverage-violation cause.
/// * Overlap — an index named in BOTH succeeded and failed: pins the observed precedence
///   (succeeded wins → `markProcessed`).
/// * Empty coverage — both lists empty for a non-empty batch: every index is a failure.
/// * Negative out-of-range succeeded index: ignored as coverage, so the real index stays uncovered
///   and routes to the failure path.
/// * Exactly-one-record batches with the single index missing.
class BatchCoverageContractInvariantTest {

  private ScheduledExecutorService scheduler;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  /// Records each callback by offset so a test can assert the exact partition of the batch into
  /// processed vs failed, and detect a record that fired on both callbacks (double-fire bug).
  private static final class RecordingCallbacks implements BatchPipelineWrapper.BatchCallbacks {

    final CopyOnWriteArrayList<Long> processed = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<Long> failed = new CopyOnWriteArrayList<>();
    final Map<Long, Exception> failureCauseByOffset = new HashMap<>();

    @Override
    public void markProcessed(final ConsumerRecord<byte[], byte[]> record) {
      processed.add(record.offset());
    }

    @Override
    public synchronized void onBatchFailure(final ConsumerRecord<byte[], byte[]> record, final Exception cause) {
      failed.add(record.offset());
      failureCauseByOffset.put(record.offset(), cause);
    }
  }

  private static ConsumerRecord<byte[], byte[]> record(final String topic, final long offset) {
    final var bytes = Long.toString(offset).getBytes();
    return new ConsumerRecord<>(topic, 0, offset, ("k-" + offset).getBytes(UTF_8), bytes);
  }

  /// Drives a size-triggered flush: enqueues exactly `batchSize` records under a policy whose
  /// `maxSize == batchSize` so the flush fires inline on the final `enqueue`. A long `maxAge`
  /// keeps the scheduler tick from racing the size trigger. Returns after the flush has completed.
  private void runFlush(
    final String topic,
    final int batchSize,
    final BatchSink<byte[]> sink,
    final RecordingCallbacks callbacks
  ) {
    final var policy = new BatchPolicy(batchSize, Duration.ofMinutes(10));
    final var wrapper = new BatchPipelineWrapper<>(topic, TestPipelines.identity(), sink, policy, scheduler, callbacks);
    wrapper.start();
    try {
      for (int i = 0; i < batchSize; i++) {
        final var rec = record(topic, i);
        wrapper.enqueue(rec, rec.value());
      }
    } finally {
      wrapper.close();
    }
  }

  /// Asserts the binding invariant: every offset in `[0, batchSize)` fired exactly one callback,
  /// the processed set equals `expectedProcessed`, and the failed set is exactly the complement.
  /// Also asserts no offset fired on both callbacks and the total fired count equals `batchSize`.
  private static void assertExactPartition(
    final int batchSize,
    final RecordingCallbacks callbacks,
    final Set<Long> expectedProcessed
  ) {
    final var processedSet = new HashSet<>(callbacks.processed);
    final var failedSet = new HashSet<>(callbacks.failed);

    assertEquals(
      callbacks.processed.size(),
      processedSet.size(),
      "no offset may be marked processed more than once; processed=" + callbacks.processed
    );
    assertEquals(
      callbacks.failed.size(),
      failedSet.size(),
      "no offset may be routed to the failure path more than once; failed=" + callbacks.failed
    );

    for (long off = 0; off < batchSize; off++) {
      final var inProcessed = processedSet.contains(off);
      final var inFailed = failedSet.contains(off);
      assertFalse(inProcessed && inFailed, "offset " + off + " fired on BOTH callbacks (double-fire)");
      assertTrue(inProcessed || inFailed, "offset " + off + " fired on NEITHER callback (silently dropped)");
    }

    assertEquals(
      batchSize,
      callbacks.processed.size() + callbacks.failed.size(),
      "exactly batchSize callbacks must fire across both paths"
    );

    final var expectedFailed = new HashSet<Long>();
    for (long off = 0; off < batchSize; off++) {
      if (!expectedProcessed.contains(off)) expectedFailed.add(off);
    }
    assertEquals(expectedProcessed, processedSet, "processed set must match expectation");
    assertEquals(expectedFailed, failedSet, "failed set must be the exact complement of processed");
  }

  /// **Interior gap.** Succeeded = {0, 2, 4}, leaving {1, 3} unaccounted-for in a batch of 5. The
  /// existing suite only covers a *trailing* gap; this pins that the wrapper handles gaps in the
  /// *middle* of the index space, marking 0/2/4 processed and routing 1/3 to the DLQ with a
  /// synthetic [IllegalStateException]. A naive contiguous-prefix implementation would mishandle
  /// this; the wrapper walks every index and consults the coverage bitset per position.
  @Test
  void interiorGapsRouteOnlyMissingIndexesToDlq() {
    final var topic = "inv-interior-gap";
    final var callbacks = new RecordingCallbacks();
    final BatchSink<byte[]> sink = batch -> new BatchResult(List.of(0, 2, 4), Map.of());

    runFlush(topic, 5, sink, callbacks);

    assertExactPartition(5, callbacks, Set.of(0L, 2L, 4L));
    for (final var missing : List.of(1L, 3L)) {
      final var cause = callbacks.failureCauseByOffset.get(missing);
      assertInstanceOf(IllegalStateException.class, cause, "missing index " + missing + " must carry an ISE");
      assertTrue(
        cause.getMessage().contains("did not account for"),
        "synthetic cause must name the coverage violation; got: " + cause.getMessage()
      );
    }
    // The single coverage-violation exception is shared across every missing index — it names the
    // whole missing set, not one index, so the wrapper reuses one instance.
    assertSame(
      callbacks.failureCauseByOffset.get(1L),
      callbacks.failureCauseByOffset.get(3L),
      "all missing indexes in one batch share the single synthetic coverage-violation cause"
    );
  }

  /// **Mixed explicit-failure + missing.** Succeeded = {0}, explicit failure at index 1 (with its
  /// own distinct cause), index 2 missing. The invariant: index 0 processed; index 1 routed with
  /// ITS OWN cause (not the synthetic one); index 2 routed with the synthetic coverage cause. This
  /// pins that the synthetic-violation cause does not clobber a real per-record failure cause.
  @Test
  void explicitFailureKeepsOwnCauseWhileMissingGetsSyntheticCause() {
    final var topic = "inv-mixed";
    final var callbacks = new RecordingCallbacks();
    final var explicitCause = new RuntimeException("sink-reported failure at index 1");
    final BatchSink<byte[]> sink = batch -> new BatchResult(List.of(0), Map.of(1, explicitCause));

    runFlush(topic, 3, sink, callbacks);

    assertExactPartition(3, callbacks, Set.of(0L));

    assertSame(
      explicitCause,
      callbacks.failureCauseByOffset.get(1L),
      "an explicitly-failed index must carry the sink's own per-record cause verbatim"
    );

    final var missingCause = callbacks.failureCauseByOffset.get(2L);
    assertInstanceOf(IllegalStateException.class, missingCause, "the missing index must carry the synthetic ISE");
    assertTrue(
      missingCause.getMessage().contains("did not account for"),
      "missing-index cause must be the coverage violation; got: " + missingCause.getMessage()
    );
  }

  /// **Overlap precedence.** Index 1 appears in BOTH the succeeded list and the failure map. The
  /// `BatchResult` Javadoc declares overlap "undefined," but the wrapper's behavior is
  /// deterministic and worth pinning so a refactor can't silently flip it: the succeeded check is
  /// consulted first, so an overlapping index is marked processed and its failure-map entry is
  /// ignored. Coverage is still complete (no missing indexes), so no record hits the DLQ.
  @Test
  void overlappingIndexIsMarkedProcessedSucceededWins() {
    final var topic = "inv-overlap";
    final var callbacks = new RecordingCallbacks();
    final var ignoredCause = new RuntimeException("should be ignored — index 1 also succeeded");
    final BatchSink<byte[]> sink = batch -> new BatchResult(List.of(0, 1, 2), Map.of(1, ignoredCause));

    runFlush(topic, 3, sink, callbacks);

    // Every index succeeded; the overlap on index 1 resolves to processed, so the batch is clean.
    assertExactPartition(3, callbacks, Set.of(0L, 1L, 2L));
    assertEquals(0, callbacks.failed.size(), "overlap must not route any record to the DLQ when succeeded wins");
  }

  /// **Empty coverage.** Both lists empty for a non-empty batch of 4 — the maximal coverage
  /// violation. Every index is missing, so every record must route to the DLQ with a synthetic
  /// cause and NONE may be marked processed. This is the case that most directly guards the
  /// silent-failure trap: a sink that reports nothing must not be read as "everything succeeded."
  @Test
  void emptyCoverageRoutesEveryRecordToDlq() {
    final var topic = "inv-empty";
    final var callbacks = new RecordingCallbacks();
    final BatchSink<byte[]> sink = batch -> new BatchResult(List.of(), Map.of());

    runFlush(topic, 4, sink, callbacks);

    assertExactPartition(4, callbacks, Set.of());
    assertEquals(0, callbacks.processed.size(), "empty coverage must mark NOTHING processed");
    for (long off = 0; off < 4; off++) {
      final var cause = callbacks.failureCauseByOffset.get(off);
      assertInstanceOf(IllegalStateException.class, cause, "offset " + off + " must carry the synthetic ISE");
    }
  }

  /// **Negative out-of-range succeeded index.** Succeeded = {-1, 0} in a batch of 2. The negative
  /// index is out of range and must be ignored as coverage (not crash, not wrap-around). Index 0
  /// is valid → processed; index 1 is then uncovered → DLQ. Pins that a negative index can't be
  /// mistaken for a valid position via signed-int arithmetic.
  @Test
  void negativeOutOfRangeSucceededIndexIsIgnoredAsCoverage() {
    final var topic = "inv-negative";
    final var callbacks = new RecordingCallbacks();
    final var succeeded = new ArrayList<Integer>();
    succeeded.add(-1);
    succeeded.add(0);
    final BatchSink<byte[]> sink = batch -> new BatchResult(succeeded, Map.of());

    runFlush(topic, 2, sink, callbacks);

    assertExactPartition(2, callbacks, Set.of(0L));
    assertInstanceOf(
      IllegalStateException.class,
      callbacks.failureCauseByOffset.get(1L),
      "the uncovered index 1 must route to the DLQ with a synthetic ISE"
    );
  }

  /// **Single-record batch, index missing.** The smallest non-trivial batch: size 1 with an empty
  /// result. The lone record must route to the DLQ — a boundary the larger batches don't exercise
  /// (off-by-one in the `[0, size)` walk would either skip the only index or over-iterate).
  @Test
  void singleRecordMissingIndexRoutesToDlq() {
    final var topic = "inv-single";
    final var callbacks = new RecordingCallbacks();
    final BatchSink<byte[]> sink = batch -> new BatchResult(List.of(), Map.of());

    runFlush(topic, 1, sink, callbacks);

    assertExactPartition(1, callbacks, Set.of());
    final var cause = callbacks.failureCauseByOffset.get(0L);
    assertInstanceOf(IllegalStateException.class, cause, "the lone missing record must carry a synthetic ISE");
    assertNotNull(cause.getMessage(), "synthetic cause must carry a diagnostic message");
  }

  /// **Partial success with a trailing run of explicit failures plus an interior missing index.**
  /// A composite stress case: size 6, succeeded = {0, 1}, explicit failures = {4, 5} (own cause),
  /// missing = {2, 3}. Asserts the full three-way partition holds simultaneously: processed {0,1},
  /// explicit-cause failures {4,5}, synthetic-cause failures {2,3}. This is the realistic shape of
  /// a sink that wrote a prefix, hit an error partway, and under-reported the tail.
  @Test
  void compositePartialSuccessExplicitFailureAndMissing() {
    final var topic = "inv-composite";
    final var callbacks = new RecordingCallbacks();
    final var explicitCause = new RuntimeException("downstream rejected indexes 4 and 5");
    final BatchSink<byte[]> sink = batch -> new BatchResult(List.of(0, 1), Map.of(4, explicitCause, 5, explicitCause));

    runFlush(topic, 6, sink, callbacks);

    assertExactPartition(6, callbacks, Set.of(0L, 1L));

    for (final var explicit : List.of(4L, 5L)) {
      assertSame(
        explicitCause,
        callbacks.failureCauseByOffset.get(explicit),
        "explicitly-failed index " + explicit + " must keep its own cause"
      );
    }
    for (final var missing : List.of(2L, 3L)) {
      final var cause = callbacks.failureCauseByOffset.get(missing);
      assertInstanceOf(IllegalStateException.class, cause, "missing index " + missing + " must carry a synthetic ISE");
      assertTrue(
        cause.getMessage().contains("did not account for"),
        "missing-index cause must be the coverage violation; got: " + cause.getMessage()
      );
    }
  }
}
