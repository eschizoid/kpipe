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
}
