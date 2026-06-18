package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchResult;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Pins the coverage-contract guarantees of [BatchPipelineWrapper]. These are the
/// silent-failure guards the wrapper holds against a buggy [BatchSink]:
///
/// * Missing index in the [BatchResult] (neither succeeded nor failed) → record routed to the
///   failure path (DLQ) with a synthetic [IllegalStateException] cause. Without this guard a
///   misreported batch result would silently mark records as processed that the sink never
///   confirmed.
/// * Out-of-range index in the [BatchResult] → WARNING logged so an operator can diagnose a
///   buggy sink emitting garbage indexes.
/// * `null` [BatchResult] → every record routed to the failure path with an
///   `IllegalStateException` blaming the null return. Without this guard a null return would
///   NPE inside the wrapper's coverage check and silently kill the flush thread.
/// * Age-trigger flush — a single buffered record below the size threshold still flushes once
///   the age window expires.
/// * Drain on `close()` — buffered records are flushed and marked processed (via
///   [BatchPipelineWrapper.BatchCallbacks#markProcessed]) before `close()` returns, so the
///   surrounding [KPipeConsumer] shutdown sequence can call `OffsetManager.close()` afterwards
///   with all batch offsets already accounted for.
class BatchPipelineWrapperCoverageContractTest {

  private ScheduledExecutorService scheduler;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  /// Records every callback invocation in order so tests can assert both the *set* of processed
  /// vs failed records and the *relative ordering* between callback events and any other
  /// external close-style events (see [#closeFlushesBufferedRecordsBeforeOffsetManagerClose]).
  private static final class RecordingCallbacks implements BatchPipelineWrapper.BatchCallbacks<String> {

    final CopyOnWriteArrayList<ConsumerRecord<String, byte[]>> processed = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<ConsumerRecord<String, byte[]>> failed = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<Exception> failureCauses = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<String> events = new CopyOnWriteArrayList<>();

    @Override
    public void markProcessed(final ConsumerRecord<String, byte[]> record) {
      processed.add(record);
      events.add("markProcessed:" + record.offset());
    }

    @Override
    public void onBatchFailure(final ConsumerRecord<String, byte[]> record, final Exception cause) {
      failed.add(record);
      failureCauses.add(cause);
      events.add("onBatchFailure:" + record.offset());
    }
  }

  private static ConsumerRecord<String, byte[]> record(final String topic, final long offset) {
    final var bytes = Long.toString(offset).getBytes();
    return new ConsumerRecord<>(topic, 0, offset, "k-" + offset, bytes);
  }

  /// **Coverage-contract guard.** Feeds N=5 records to a wrapper whose [BatchSink] returns a
  /// [BatchResult] that names indexes 0..3 as succeeded — index 4 is left dangling. The wrapper
  /// must (a) mark 0..3 processed, (b) route index 4 to the failure callback, (c) supply a
  /// synthetic [IllegalStateException] as the cause so an operator can see why the record landed
  /// on the DLQ.
  ///
  /// Without this guard, the missing index would silently be treated as "processed" — the
  /// processed counter rises while the sink never confirmed the write.
  @Test
  void incompleteBatchResultRoutesMissingRecordsToDlq() {
    final var topic = "coverage-missing";
    final var policy = new BatchPolicy(5, Duration.ofMinutes(1));
    final var callbacks = new RecordingCallbacks();

    final BatchSink<byte[]> sink = batch -> {
      // Cover 0..size-2, leave the last index unaccounted for.
      final var succeeded = new ArrayList<Integer>();
      for (int i = 0; i < batch.size() - 1; i++) succeeded.add(i);
      return new BatchResult(succeeded, Map.of());
    };

    final var wrapper = new BatchPipelineWrapper<>(topic, TestPipelines.identity(), sink, policy, scheduler, callbacks);
    wrapper.start();
    try {
      for (int i = 0; i < 5; i++) {
        final var rec = record(topic, i);
        wrapper.enqueue(rec, rec.value());
      }

      assertEquals(4, callbacks.processed.size(), "indexes 0..3 should be marked processed");
      assertEquals(1, callbacks.failed.size(), "the unaccounted-for index 4 must hit the failure path");
      assertEquals(4L, callbacks.failed.getFirst().offset(), "failure should be the trailing record");
      final var cause = callbacks.failureCauses.getFirst();
      assertInstanceOf(
        IllegalStateException.class,
        cause,
        "synthetic cause must be an IllegalStateException flagging the contract violation"
      );
      assertNotNull(cause.getMessage(), "synthetic cause must carry a diagnostic message");
      assertTrue(
        cause.getMessage().contains("did not account for"),
        "diagnostic message must name the contract violation; got: " + cause.getMessage()
      );
    } finally {
      wrapper.close();
    }
  }

  /// **Out-of-range index guard.** Feeds N=3 records to a wrapper whose sink returns a
  /// [BatchResult] naming index 99 (way out of range). The wrapper must (a) log a WARNING about
  /// the out-of-range succeeded index, (b) treat the real indexes 0..2 as unaccounted-for and
  /// route them to the failure callback (so the silent-failure guard still fires). The WARN
  /// gives an operator the breadcrumb to diagnose a buggy sink that's emitting garbage indexes.
  @Test
  void outOfRangeIndexInBatchResultLogsWarning() {
    final var topic = "coverage-oor";
    final var policy = new BatchPolicy(3, Duration.ofMinutes(1));
    final var callbacks = new RecordingCallbacks();

    final BatchSink<byte[]> sink = batch -> new BatchResult(List.of(99), Map.of());

    final var julLogger = Logger.getLogger(BatchPipelineWrapper.class.getName());
    final var captured = new CopyOnWriteArrayList<LogRecord>();
    final var handler = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        if (record.getLevel().intValue() >= Level.WARNING.intValue()) captured.add(record);
      }

      @Override
      public void flush() {}

      @Override
      public void close() {}
    };
    final var originalLevel = julLogger.getLevel();
    final var originalUseParent = julLogger.getUseParentHandlers();
    julLogger.addHandler(handler);
    julLogger.setLevel(Level.ALL);
    julLogger.setUseParentHandlers(false);

    final var wrapper = new BatchPipelineWrapper<>(topic, TestPipelines.identity(), sink, policy, scheduler, callbacks);
    wrapper.start();
    try {
      for (int i = 0; i < 3; i++) {
        final var rec = record(topic, i);
        wrapper.enqueue(rec, rec.value());
      }

      final var outOfRangeWarnings = captured
        .stream()
        .filter(r -> r.getMessage() != null && r.getMessage().contains("out-of-range"))
        .toList();
      assertEquals(
        1,
        outOfRangeWarnings.size(),
        () -> "exactly one out-of-range WARNING expected; captured=" + captured.size()
      );
      assertEquals(0, callbacks.processed.size(), "no record should be marked processed for an out-of-range result");
      assertEquals(3, callbacks.failed.size(), "all three real records must land on the failure path");
    } finally {
      julLogger.removeHandler(handler);
      julLogger.setLevel(originalLevel);
      julLogger.setUseParentHandlers(originalUseParent);
      wrapper.close();
    }
  }

  /// **Null result guard.** Feeds N=4 records to a wrapper whose sink returns `null`. The
  /// wrapper must route every record to the failure callback (never to `markProcessed`) and
  /// carry a synthetic [IllegalStateException] explaining the null return. Without this guard,
  /// a null return would NPE inside the wrapper's coverage check and silently kill the flush
  /// thread.
  @Test
  void nullBatchResultTreatedAsWholeBatchFailure() {
    final var topic = "coverage-null";
    final var policy = new BatchPolicy(4, Duration.ofMinutes(1));
    final var callbacks = new RecordingCallbacks();

    final BatchSink<byte[]> sink = batch -> null;

    final var wrapper = new BatchPipelineWrapper<>(topic, TestPipelines.identity(), sink, policy, scheduler, callbacks);
    wrapper.start();
    try {
      for (int i = 0; i < 4; i++) {
        final var rec = record(topic, i);
        wrapper.enqueue(rec, rec.value());
      }

      assertEquals(0, callbacks.processed.size(), "no record should be marked processed when sink returns null");
      assertEquals(4, callbacks.failed.size(), "every record must be routed to the failure callback");

      final var cause = callbacks.failureCauses.getFirst();
      assertInstanceOf(IllegalStateException.class, cause, "null result must surface as IllegalStateException");
      assertTrue(
        cause.getMessage().contains("null BatchResult"),
        "cause message must name the null return; got: " + cause.getMessage()
      );
      // Whole-batch failure should attribute the same cause to every record.
      for (final var c : callbacks.failureCauses) {
        assertSame(cause, c, "every failed record should carry the same synthetic cause for a null result");
      }
    } finally {
      wrapper.close();
    }
  }

  /// **Age-trigger flush.** Feeds a single record (well below the size threshold of 100) into a
  /// wrapper with a very short max-age (100ms) and asserts that the scheduler's age tick fires
  /// the flush. The test uses a [CountDownLatch] so we don't busy-wait on the result.
  ///
  /// This pins the age trigger as the *only* path that can flush a sub-threshold buffer mid-run
  /// (close-on-shutdown is exercised separately).
  @Test
  void ageTickFlushesBeforeSizeThreshold() throws InterruptedException {
    final var topic = "coverage-age";
    // maxSize 100 so size-trigger can't fire; maxAge 100ms so the scheduler ticks at ~50ms and
    // a single record ages out almost immediately.
    final var policy = new BatchPolicy(100, Duration.ofMillis(100));
    final var sinkLatch = new CountDownLatch(1);
    final var capturedBatchSize = new AtomicReference<Integer>();

    final BatchSink<byte[]> sink = BatchSink.ofVoid(batch -> {
      capturedBatchSize.set(batch.size());
      sinkLatch.countDown();
    });

    // markProcessedLatch fires AFTER the wrapper has invoked the callback for every flushed
    // record — the sink-side latch fires earlier (the wrapper walks the snapshot after the sink
    // returns), so waiting on the callback latch is the right pin for asserting downstream state.
    final var markProcessedLatch = new CountDownLatch(1);
    final var callbacks = new BatchPipelineWrapper.BatchCallbacks<String>() {
      @Override
      public void markProcessed(final ConsumerRecord<String, byte[]> record) {
        markProcessedLatch.countDown();
      }

      @Override
      public void onBatchFailure(final ConsumerRecord<String, byte[]> record, final Exception cause) {
        // no-op — the clean path shouldn't reach this
      }
    };

    final var wrapper = new BatchPipelineWrapper<>(topic, TestPipelines.identity(), sink, policy, scheduler, callbacks);
    wrapper.start();
    try {
      final var rec = record(topic, 42L);
      wrapper.enqueue(rec, rec.value());

      assertTrue(
        sinkLatch.await(5, TimeUnit.SECONDS),
        "age tick must flush the single buffered record before the test timeout"
      );
      assertTrue(
        markProcessedLatch.await(5, TimeUnit.SECONDS),
        "the wrapper must invoke markProcessed for the flushed record after the sink returns"
      );
      assertEquals(1, capturedBatchSize.get(), "flushed batch should contain exactly the one buffered record");
      assertEquals(0L, wrapper.bufferedCount(), "bufferedCount must drop to 0 after the age flush");
    } finally {
      wrapper.close();
    }
  }

  /// **Drain on close.** Buffers N=3 records under a size threshold of 100 (so no size flush
  /// can fire) and a long max-age (so no age flush can fire mid-test). Calls `close()` and
  /// asserts:
  ///
  /// 1. Every buffered record is flushed and reaches the sink.
  /// 2. Every record is then marked processed via [BatchPipelineWrapper.BatchCallbacks#markProcessed].
  /// 3. All three markProcessed events are observed BEFORE a subsequent simulated
  ///    `OffsetManager.close()` call — pinning the ordering invariant that batch buffers
  ///    drain into the offset manager while it's still alive.
  ///
  /// This mirrors the actual call ordering in `KPipeConsumer.releaseConstructedResources`
  /// (`dispatcher.close()` → batch wrappers `close()` → `offsetManager.close()`) at the
  /// wrapper level — without simulating the full consumer.
  @Test
  void closeFlushesBufferedRecordsBeforeOffsetManagerClose() {
    final var topic = "coverage-close";
    final var policy = new BatchPolicy(100, Duration.ofMinutes(5));
    final var callbacks = new RecordingCallbacks();
    final var flushedBatchSizes = new CopyOnWriteArrayList<Integer>();

    final BatchSink<byte[]> sink = BatchSink.ofVoid(batch -> flushedBatchSizes.add(batch.size()));

    final var wrapper = new BatchPipelineWrapper<>(topic, TestPipelines.identity(), sink, policy, scheduler, callbacks);
    wrapper.start();
    try {
      for (int i = 0; i < 3; i++) {
        final var rec = record(topic, i);
        wrapper.enqueue(rec, rec.value());
      }

      // No size flush, no age flush — verify the records are still buffered before close().
      assertEquals(3L, wrapper.bufferedCount(), "all 3 records should still be buffered prior to close()");
      assertEquals(0, callbacks.processed.size(), "no flush should have fired before close()");

      // Drive the close + simulated offset-manager close in the same order
      // `KPipeConsumer.releaseConstructedResources` uses.
      wrapper.close();
      callbacks.events.add("offsetManager.close");

      assertEquals(0L, wrapper.bufferedCount(), "bufferedCount must drop to 0 after close()");
      assertEquals(List.of(3), flushedBatchSizes, "close should flush exactly one batch of size 3");
      assertEquals(3, callbacks.processed.size(), "all 3 records must be marked processed via the wrapper's drain");
      assertEquals(0, callbacks.failed.size(), "no record should land on the failure path during a clean close");

      // Confirm ordering: every markProcessed event must precede the simulated OffsetManager.close.
      final var closeIdx = callbacks.events.indexOf("offsetManager.close");
      assertTrue(closeIdx > 0, "offsetManager.close event must be recorded after the wrapper drain");
      for (int i = 0; i < closeIdx; i++) {
        assertTrue(
          callbacks.events.get(i).startsWith("markProcessed:"),
          "events before offsetManager.close must all be markProcessed; got: " + callbacks.events.get(i)
        );
      }
      // And the buffered offsets must all be present in that pre-close prefix.
      final var processedOffsets = new HashMap<Long, Boolean>();
      for (int i = 0; i < closeIdx; i++) {
        final var ev = callbacks.events.get(i);
        processedOffsets.put(Long.parseLong(ev.substring("markProcessed:".length())), true);
      }
      for (long off = 0; off < 3; off++) {
        assertTrue(processedOffsets.containsKey(off), "offset " + off + " must be marked processed before close");
      }
    } finally {
      // Idempotent — `close()` already ran above; this is a safety net in case the asserts fail.
      wrapper.close();
    }
  }
}
