package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Drives [BatchPipelineWrapper] from many virtual-thread workers concurrently to verify the
/// guarantees that batch sinks must hold under parallel processing:
///
/// * No record is delivered twice.
/// * No record is dropped.
/// * `bufferedCount()` returns to 0 after `close()` drains the buffer.
/// * The aggregate of all flushed records equals the aggregate of all enqueued records.
///
/// Uses byte-level [String] ids so tests can match records by identity. The sink intentionally
/// sleeps a few ms per call to widen the window where contention between flushers and
/// enqueueing workers might surface.
class BatchPipelineWrapperConcurrencyTest {

  private ScheduledExecutorService scheduler;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  /// Records every record that the wrapper hands to the sink so the test can assert exact
  /// coverage. Uses a [CopyOnWriteArrayList] of immutable batch snapshots — copying on write is
  /// cheap relative to the test's record count and keeps later assertion code straightforward.
  private static final class RecordingCallbacks implements BatchPipelineWrapper.BatchCallbacks {

    final CopyOnWriteArrayList<ConsumerRecord<byte[], byte[]>> processed = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<ConsumerRecord<byte[], byte[]>> failed = new CopyOnWriteArrayList<>();

    @Override
    public void markProcessed(final ConsumerRecord<byte[], byte[]> record) {
      processed.add(record);
    }

    @Override
    public void onBatchFailure(final ConsumerRecord<byte[], byte[]> record, final Exception cause) {
      failed.add(record);
    }
  }

  @Test
  void concurrentEnqueueFromManyVirtualThreadsLosesNoRecords() throws Exception {
    final var topic = "concurrent-batch";
    final var policy = new BatchPolicy(50, Duration.ofMillis(200));
    final var callbacks = new RecordingCallbacks();
    final var batches = new CopyOnWriteArrayList<List<byte[]>>();

    // Slow sink — sleeps 5ms per call to widen the contention window between flushers and
    // workers still trying to enqueue. Captures every batch so we can assert "no record appears
    // in two batches" at the end.
    final BatchSink<byte[]> sink = BatchSink.ofVoid(batch -> {
      batches.add(List.copyOf(batch));
      try {
        Thread.sleep(5);
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    });

    final var wrapper = new BatchPipelineWrapper<>(topic, TestPipelines.identity(), sink, policy, scheduler, callbacks);
    wrapper.start();

    final var workerCount = 16;
    final var perWorker = 100;
    final var totalExpected = workerCount * perWorker;
    final var startGate = new CountDownLatch(1);
    final var doneGate = new CountDownLatch(workerCount);
    final var globalIdSeq = new AtomicInteger(0);

    for (int w = 0; w < workerCount; w++) {
      Thread.ofVirtual()
        .name("enqueue-worker-" + w)
        .start(() -> {
          try {
            startGate.await();
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            return;
          }
          for (int i = 0; i < perWorker; i++) {
            final var id = globalIdSeq.getAndIncrement();
            final var bytes = Integer.toString(id).getBytes();
            final var record = new ConsumerRecord<>(topic, 0, id, ("id-" + id).getBytes(UTF_8), bytes);
            wrapper.enqueue(record, bytes);
          }
          doneGate.countDown();
        });
    }

    startGate.countDown();
    assertTrue(doneGate.await(30, TimeUnit.SECONDS), "all enqueue workers should finish");

    // Close drains whatever remains in the buffer — gives flushes a window to land. After close,
    // bufferedCount must return to 0 and every enqueued record must be in `processed`.
    wrapper.close();

    assertEquals(0L, wrapper.bufferedCount(), "buffer should be empty after close()");
    assertEquals(
      totalExpected,
      callbacks.processed.size(),
      "every enqueued record should be marked processed exactly once"
    );
    assertEquals(0, callbacks.failed.size(), "no records should land on the failure path");

    // Aggregate flushed bytes across all batches and verify identity coverage.
    final var flushedIds = new HashSet<Integer>();
    int flushedTotal = 0;
    for (final var batch : batches) {
      for (final var bytes : batch) {
        flushedTotal++;
        flushedIds.add(Integer.parseInt(new String(bytes)));
      }
    }
    assertEquals(totalExpected, flushedTotal, "flushed record count must equal enqueued total");
    assertEquals(totalExpected, flushedIds.size(), "no record may appear in more than one batch");
    for (int id = 0; id < totalExpected; id++) {
      assertTrue(flushedIds.contains(id), "missing id " + id + " from flushed batches");
    }
  }
}
