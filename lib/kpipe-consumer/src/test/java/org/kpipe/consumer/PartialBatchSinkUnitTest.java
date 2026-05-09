package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.registry.MessagePipeline;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.BatchResult;
import org.kpipe.sink.PartialBatchSink;

/// Unit tests for [BatchPipelineWrapper]'s partial-batch flush path. Drives the wrapper
/// directly with a fake [PartialBatchSink] and a recording [BatchPipelineWrapper.BatchCallbacks]
/// so we can assert the exact sequence of `markProcessed` / `onBatchFailure` calls.
///
/// **What we are testing.** The flush leaf for partial-batch sinks is the new code path; every
/// other layer (pipeline build, lock/buffer/tick, scheduler) is shared with the v1 whole-batch
/// path and covered by the existing batch tests. These tests therefore go through the wrapper's
/// public API (`enqueue` triggering a size-1 flush) rather than reaching into private state.
class PartialBatchSinkUnitTest {

  private ScheduledExecutorService scheduler;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  /// Records each callback invocation in order so tests can assert the exact sequence and
  /// content. Uses `String` keys because we drive the wrapper with byte[] records — keys are
  /// just the test ids.
  private static final class RecordingCallbacks implements BatchPipelineWrapper.BatchCallbacks<String> {

    final List<ConsumerRecord<String, byte[]>> processed = Collections.synchronizedList(new ArrayList<>());
    final List<ConsumerRecord<String, byte[]>> failed = Collections.synchronizedList(new ArrayList<>());
    final List<Exception> failureCauses = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void markProcessed(final ConsumerRecord<String, byte[]> record) {
      processed.add(record);
    }

    @Override
    public void onBatchFailure(final ConsumerRecord<String, byte[]> record, final Exception cause) {
      failed.add(record);
      failureCauses.add(cause);
    }
  }

  /// Returns the values 0..size-1 as a list of `Integer` indexes — readable shorthand for
  /// "every position in the batch."
  private static List<Integer> range(final int size) {
    final var out = new ArrayList<Integer>(size);
    for (int i = 0; i < size; i++) out.add(i);
    return out;
  }

  /// Builds a fake byte[] pipeline that just returns its input unchanged. We use byte[] as the
  /// `T` because it minimizes setup; the partial-batch flush path doesn't care what `T` is.
  private static MessagePipeline<byte[]> identityPipeline() {
    return TestPipelines.identity();
  }

  /// Constructs a `ConsumerRecord` with key=`id` and value=the same bytes. Lets us match
  /// records in assertions without depending on object identity.
  private static ConsumerRecord<String, byte[]> rec(final String topic, final long offset, final String id) {
    return new ConsumerRecord<>(topic, 0, offset, id, id.getBytes());
  }

  /// Drains a batch of `size` records through the wrapper. Returns the records in the order
  /// they were enqueued so assertions can refer to them by index.
  private static List<ConsumerRecord<String, byte[]>> enqueueBatch(
    final BatchPipelineWrapper<String, byte[]> wrapper,
    final String topic,
    final int size
  ) {
    final var records = new ArrayList<ConsumerRecord<String, byte[]>>(size);
    for (int i = 0; i < size; i++) {
      final var r = rec(topic, i, "id-" + i);
      records.add(r);
      wrapper.enqueue(r, r.value());
    }
    return records;
  }

  @Test
  void flushDispatchesPerRecordSuccessAndFailure() {
    // Given a batch of 10 records, where the sink reports indexes 2 and 7 as failed and the
    // remaining 8 as succeeded. This is the canonical partial-failure case (think: JDBC bulk
    // insert with two unique-constraint violations).
    final var topic = "partial-batch-test";
    final var callbacks = new RecordingCallbacks();
    final var fail2 = new RuntimeException("constraint at idx 2");
    final var fail7 = new RuntimeException("constraint at idx 7");

    final PartialBatchSink<byte[]> sink = values -> {
      assertEquals(10, values.size(), "sink should receive the full batch");
      final var failures = new HashMap<Integer, Exception>();
      failures.put(2, fail2);
      failures.put(7, fail7);
      final var succeeded = new ArrayList<Integer>();
      for (int i = 0; i < 10; i++) if (i != 2 && i != 7) succeeded.add(i);
      return new BatchResult<>(succeeded, failures);
    };

    final var wrapper = new BatchPipelineWrapper<>(
      topic,
      identityPipeline(),
      sink,
      new BatchPolicy(10, Duration.ofSeconds(1)),
      scheduler,
      callbacks
    );
    final var records = enqueueBatch(wrapper, topic, 10);

    // Then exactly 8 markProcessed calls, in input order, for indexes 0,1,3,4,5,6,8,9.
    final var expectedProcessed = List.of(0, 1, 3, 4, 5, 6, 8, 9);
    assertEquals(expectedProcessed.size(), callbacks.processed.size(), "expected 8 processed records");
    for (int i = 0; i < expectedProcessed.size(); i++) {
      assertSame(
        records.get(expectedProcessed.get(i)),
        callbacks.processed.get(i),
        "processed record at position %d should match input index %d".formatted(i, expectedProcessed.get(i))
      );
    }

    // And exactly 2 onBatchFailure calls with the right per-record exceptions, in input order.
    assertEquals(2, callbacks.failed.size(), "expected 2 failed records");
    assertSame(records.get(2), callbacks.failed.get(0), "first failure should be the record at index 2");
    assertSame(records.get(7), callbacks.failed.get(1), "second failure should be the record at index 7");
    assertSame(fail2, callbacks.failureCauses.get(0), "exception attribution for index 2");
    assertSame(fail7, callbacks.failureCauses.get(1), "exception attribution for index 7");
  }

  @Test
  void sinkThrowsFallsBackToWholeBatchFailure() {
    // Given a sink that itself throws (unrecoverable infrastructure error: connection died,
    // auth expired). v1's whole-batch failure semantics are the right fallback — every record
    // should go to onBatchFailure with that throwable as the cause.
    final var topic = "sink-throws-test";
    final var callbacks = new RecordingCallbacks();
    final var boom = new RuntimeException("connection pool exhausted");

    final PartialBatchSink<byte[]> throwingSink = values -> {
      throw boom;
    };

    final var wrapper = new BatchPipelineWrapper<>(
      topic,
      identityPipeline(),
      throwingSink,
      new BatchPolicy(5, Duration.ofSeconds(1)),
      scheduler,
      callbacks
    );
    final var records = enqueueBatch(wrapper, topic, 5);

    // Then every record goes to onBatchFailure with the thrown exception, in input order.
    assertEquals(0, callbacks.processed.size(), "no record should be marked processed");
    assertEquals(5, callbacks.failed.size(), "every record should be failed");
    for (int i = 0; i < 5; i++) {
      assertSame(records.get(i), callbacks.failed.get(i), "failure dispatch order should match input order");
      assertSame(boom, callbacks.failureCauses.get(i), "every record's failure cause should be the thrown exception");
    }
  }

  @Test
  void uncoveredIndexesAreTreatedAsFailuresNotSilentlyMarkedProcessed() {
    // Given a sink whose return value violates the coverage contract: succeededIndexes = [0..4],
    // failedByIndex = {5, 6}, indexes 7-9 unaccounted. Silently marking 7-9 processed would
    // mask the bug and lose data — we want them sent to the DLQ with a synthetic
    // IllegalStateException that operators can attribute back to the contract violation.
    final var topic = "coverage-violation-test";
    final var callbacks = new RecordingCallbacks();
    final var fail5 = new RuntimeException("real failure at 5");
    final var fail6 = new RuntimeException("real failure at 6");

    final PartialBatchSink<byte[]> buggySink = values -> {
      final var succeeded = List.of(0, 1, 2, 3, 4);
      final var failures = new HashMap<Integer, Exception>();
      failures.put(5, fail5);
      failures.put(6, fail6);
      // Note: indexes 7, 8, 9 are deliberately left out — this is the bug under test.
      return new BatchResult<>(succeeded, failures);
    };

    final var wrapper = new BatchPipelineWrapper<>(
      topic,
      identityPipeline(),
      buggySink,
      new BatchPolicy(10, Duration.ofSeconds(1)),
      scheduler,
      callbacks
    );
    final var records = enqueueBatch(wrapper, topic, 10);

    // Indexes 0..4 marked processed; indexes 5..9 routed through onBatchFailure (5,6 with the
    // real failures, 7,8,9 with the synthetic IllegalStateException explaining the violation).
    assertEquals(5, callbacks.processed.size(), "indexes 0..4 should be marked processed");
    for (int i = 0; i < 5; i++) assertSame(records.get(i), callbacks.processed.get(i));

    assertEquals(5, callbacks.failed.size(), "indexes 5..9 should be in onBatchFailure");
    assertSame(records.get(5), callbacks.failed.get(0));
    assertSame(records.get(6), callbacks.failed.get(1));
    assertSame(records.get(7), callbacks.failed.get(2));
    assertSame(records.get(8), callbacks.failed.get(3));
    assertSame(records.get(9), callbacks.failed.get(4));

    // Real per-record exceptions are preserved; the unaccounted indexes get the synthetic
    // IllegalStateException so operators see the contract violation in the DLQ payload.
    assertSame(fail5, callbacks.failureCauses.get(0));
    assertSame(fail6, callbacks.failureCauses.get(1));
    for (int i = 2; i < 5; i++) {
      final var cause = callbacks.failureCauses.get(i);
      assertNotNull(cause, "synthetic cause should not be null");
      assertTrue(
        cause instanceof IllegalStateException,
        "uncovered indexes should fail with IllegalStateException, got " + cause.getClass()
      );
      assertTrue(
        cause.getMessage().contains("did not account for indexes"),
        "synthetic message should mention uncovered indexes; got: " + cause.getMessage()
      );
    }
  }

  @Test
  void allSucceededFactoryProducesFullCoverage() {
    final var result = BatchResult.<byte[]>allSucceeded(4);
    assertEquals(List.of(0, 1, 2, 3), result.succeededIndexes());
    assertEquals(Map.of(), result.failedByIndex());
  }

  @Test
  void allSucceededZeroBatchSizeIsAllowed() {
    final var result = BatchResult.<byte[]>allSucceeded(0);
    assertTrue(result.succeededIndexes().isEmpty());
    assertTrue(result.failedByIndex().isEmpty());
  }

  @Test
  void allFailedFactoryProducesFullCoverageWithSharedCause() {
    final var cause = new RuntimeException("transaction aborted");
    final var result = BatchResult.<byte[]>allFailed(3, cause);
    assertEquals(List.of(), result.succeededIndexes());
    assertEquals(3, result.failedByIndex().size());
    for (int i = 0; i < 3; i++) assertSame(cause, result.failedByIndex().get(i));
  }

  @Test
  void allFailedRejectsNullCause() {
    assertThrows(NullPointerException.class, () -> BatchResult.<byte[]>allFailed(3, null));
  }

  @Test
  void factoriesRejectNegativeBatchSize() {
    assertThrows(IllegalArgumentException.class, () -> BatchResult.<byte[]>allSucceeded(-1));
    assertThrows(
      IllegalArgumentException.class,
      () -> BatchResult.<byte[]>allFailed(-1, new RuntimeException("cause"))
    );
  }
}
