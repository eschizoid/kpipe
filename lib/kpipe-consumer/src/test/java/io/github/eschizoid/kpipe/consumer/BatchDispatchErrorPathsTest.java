package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchResult;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/// The failure paths through a batch flush's outcome dispatch.
///
/// These run in the dispatch that [BatchPipelineWrapper] now hands back to its caller to run after
/// the flush lock is released, so they are worth pinning independently of where that dispatch runs.
///
/// Note that `BatchSink.ofVoid` catches `Exception` and converts it into `BatchResult.allFailed`,
/// so a sink built that way never reaches the wrapper's own catch. Covering that catch takes a sink
/// that implements the interface directly, which is what these do.
class BatchDispatchErrorPathsTest {

  private static final String TOPIC = "dispatch-errors";
  private static final int BATCH = 4;

  /// A sink that throws rather than returning a result routes every record in the batch through the
  /// whole-batch failure path.
  @Test
  void aSinkThatThrowsSendsEveryRecordToTheFailurePath() {
    final var failures = new ArrayList<Long>();
    final BatchSink<byte[]> throwingSink = _ -> {
      throw new IllegalStateException("thrown, not reported");
    };

    run(throwingSink, new RecordingCallbacks(failures, null));

    assertEquals(List.of(0L, 1L, 2L, 3L), failures, "every record in the batch gets a failure outcome");
  }

  /// A sink returning a result that accounts for neither success nor failure at some index must
  /// treat those indexes as failures rather than silently dropping them.
  @Test
  void indexesTheSinkDidNotAccountForAreTreatedAsFailures() {
    final var failures = new ArrayList<Long>();
    // Claims only index 0; 1..3 are unaccounted for.
    final BatchSink<byte[]> partialSink = _ -> new BatchResult(List.of(0), Map.of());

    run(partialSink, new RecordingCallbacks(failures, null));

    assertEquals(List.of(1L, 2L, 3L), failures, "unaccounted indexes become failures, not silent drops");
  }

  /// A callback that throws must not abort the loop and strand the records after it: every record
  /// in the batch gets its callback attempt regardless.
  @Test
  void aThrowingOutcomeCallbackDoesNotStrandTheRemainingRecords() {
    final var failures = new ArrayList<Long>();
    final BatchSink<byte[]> allFail = batch -> BatchResult.allFailed(batch.size(), new IllegalStateException("nope"));

    run(allFail, new RecordingCallbacks(failures, 1L));

    assertEquals(
      List.of(0L, 2L, 3L),
      failures,
      "the throwing record's own outcome is lost, but the ones after it are still attempted"
    );
  }

  /// The same guarantee on the whole-batch failure path, which walks the snapshot in its own loop.
  @Test
  void aThrowingCallbackOnTheWholeBatchFailurePathAlsoContinues() {
    final var failures = new ArrayList<Long>();
    final BatchSink<byte[]> throwingSink = _ -> {
      throw new IllegalStateException("thrown, not reported");
    };

    run(throwingSink, new RecordingCallbacks(failures, 1L));

    assertEquals(List.of(0L, 2L, 3L), failures, "a throwing callback does not abort the whole-batch failure loop");
  }

  /// A sink interrupted at shutdown has not delivered its batch, so its records are not failures:
  /// with no DLQ a failure is logged and committed, which would commit records that never reached
  /// the destination. They stay unmarked, to be redelivered, and the interrupt is restored.
  @Test
  void aSinkInterruptedAtShutdownLeavesItsRecordsUnmarked() {
    final var outcomes = new OutcomeCallbacks(true);
    final BatchSink<byte[]> interrupted = _ -> {
      throw new IllegalStateException("write aborted", new InterruptedException());
    };

    run(interrupted, outcomes);

    assertTrue(Thread.interrupted(), "the interrupt must be restored for the caller");
    assertEquals(List.of(), outcomes.marked(), "an interrupted flush must not mark its records");
    assertEquals(List.of(), outcomes.failed(), "an interrupted flush must not fail its records");
  }

  /// The same when the sink reports the interruption as a failed result rather than throwing, which
  /// is what `BatchSink.ofVoid` does with any exception its consumer throws.
  @Test
  void aSinkReportingAnInterruptionAtShutdownAsAFailedResultLeavesItsRecordsUnmarked() {
    final var outcomes = new OutcomeCallbacks(true);
    final BatchSink<byte[]> interrupted = BatchSink.ofVoid(_ -> {
      throw new IllegalStateException("write aborted", new InterruptedException());
    });

    run(interrupted, outcomes);

    assertTrue(Thread.interrupted(), "the interrupt must be restored for the caller");
    assertEquals(List.of(), outcomes.marked());
    assertEquals(List.of(), outcomes.failed());
  }

  /// The common idiom: the sink restores the interrupt flag and throws without chaining the
  /// `InterruptedException`. The set flag is what marks the flush as interrupted.
  @Test
  void aSinkThatRestoresTheFlagAndThrowsLeavesItsRecordsUnmarked() {
    final var outcomes = new OutcomeCallbacks(true);
    final BatchSink<byte[]> interrupted = _ -> {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("aborted");
    };

    run(interrupted, outcomes);

    assertTrue(Thread.interrupted(), "the flag the sink set must still be set");
    assertEquals(List.of(), outcomes.marked());
    assertEquals(List.of(), outcomes.failed());
  }

  /// A record the sink reported as succeeded before the interruption was delivered and is marked;
  /// every record it did not report as succeeded stays unmarked.
  @Test
  void anInterruptedFlushStillMarksTheRecordsItReportedAsSucceeded() {
    final var outcomes = new OutcomeCallbacks(true);
    final BatchSink<byte[]> partlyDelivered = _ ->
      new BatchResult(
        List.of(0),
        Map.of(
          1,
          new IllegalStateException("aborted", new InterruptedException()),
          2,
          new IllegalStateException("bad row")
        )
      );

    run(partlyDelivered, outcomes);

    assertTrue(Thread.interrupted(), "the interrupt must be restored for the caller");
    assertEquals(List.of(0L), outcomes.marked(), "only the succeeded record is marked");
    assertEquals(List.of(), outcomes.failed(), "nothing from an interrupted flush is failed");
  }

  /// While the consumer is running its thread was not interrupted, so an `InterruptedException`
  /// inside the sink's exception came from the sink's own machinery, such as a helper pool shut
  /// down under it. That is an ordinary failure; treating it as an interruption would fake an
  /// interrupt on the consumer thread and leave the batch pending.
  @Test
  void anInterruptionInsideTheSinksExceptionWhileRunningIsAnOrdinaryFailure() {
    final var outcomes = new OutcomeCallbacks(false);
    final BatchSink<byte[]> helperPoolFailed = _ -> {
      throw new IllegalStateException("helper pool shut down", new InterruptedException());
    };

    run(helperPoolFailed, outcomes);

    assertFalse(Thread.interrupted(), "no interrupt may be invented for a running consumer");
    assertEquals(List.of(0L, 1L, 2L, 3L), outcomes.failed(), "every record takes the failure path");
  }

  private void run(final BatchSink<byte[]> sink, final BatchPipelineWrapper.BatchCallbacks callbacks) {
    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    final var wrapper = new BatchPipelineWrapper<byte[]>(
      TOPIC,
      TestPipelines.identity(),
      sink,
      new BatchPolicy(BATCH, Duration.ofMinutes(1)),
      scheduler,
      callbacks
    );
    try {
      wrapper.start();
      IntStream.range(0, BATCH).forEach(i ->
        wrapper.enqueue(new ConsumerRecord<>(TOPIC, 0, i, new byte[0], new byte[0]), new byte[] { 1 })
      );
      assertEquals(0L, wrapper.bufferedCount(), "the gauge comes down once the dispatch has run");
    } finally {
      wrapper.close();
      scheduler.shutdownNow();
    }
  }

  /// Records the offsets handed to `onBatchFailure`, optionally throwing for one of them.
  private record RecordingCallbacks(
    List<Long> failures,
    Long throwForOffset
  ) implements BatchPipelineWrapper.BatchCallbacks {
    @Override
    public void markProcessed(final ConsumerRecord<byte[], byte[]> record) {}

    @Override
    public void onBatchFailure(final ConsumerRecord<byte[], byte[]> record, final Exception cause) {
      if (throwForOffset != null && throwForOffset == record.offset()) {
        throw new IllegalStateException("callback failure for offset " + record.offset());
      }
      failures.add(record.offset());
    }
  }

  /// Records which offsets were marked and which were failed, and reports a fixed closing state.
  private record OutcomeCallbacks(
    List<Long> marked,
    List<Long> failed,
    boolean closing
  ) implements BatchPipelineWrapper.BatchCallbacks {
    OutcomeCallbacks(final boolean closing) {
      this(new ArrayList<>(), new ArrayList<>(), closing);
    }

    @Override
    public void markProcessed(final ConsumerRecord<byte[], byte[]> record) {
      marked.add(record.offset());
    }

    @Override
    public void onBatchFailure(final ConsumerRecord<byte[], byte[]> record, final Exception cause) {
      failed.add(record.offset());
    }

    @Override
    public boolean consumerClosing() {
      return closing;
    }
  }
}
