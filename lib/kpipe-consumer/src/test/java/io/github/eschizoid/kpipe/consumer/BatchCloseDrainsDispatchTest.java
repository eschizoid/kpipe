package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchResult;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/// `close()` must not return while an outcome dispatch is still running.
///
/// The size-triggered path is safe by construction: `close()` runs that dispatch itself. The
/// age-tick path is not. `tickFuture.cancel(false)` does not stop a tick that is already running,
/// and the tick releases the flush lock before running its dispatch — so `close()` can acquire a
/// free lock, find an empty buffer, and return while per-record callbacks are still firing. That
/// leaves the offset manager and the DLQ producer being torn down underneath a live dispatch.
class BatchCloseDrainsDispatchTest {

  private static final String TOPIC = "close-drain";

  @Test
  void closeWaitsForADispatchStartedByTheAgeTick() throws Exception {
    final var scheduler = Executors.newSingleThreadScheduledExecutor();
    final var dispatchStarted = new CountDownLatch(1);
    final var dispatchFinished = new AtomicBoolean();

    final BatchSink<byte[]> allFail = batch -> BatchResult.allFailed(batch.size(), new IllegalStateException("fail"));

    final var callbacks = new BatchPipelineWrapper.BatchCallbacks() {
      @Override
      public void markProcessed(final ConsumerRecord<byte[], byte[]> record) {}

      @Override
      public void onBatchFailure(final ConsumerRecord<byte[], byte[]> record, final Exception cause) {
        dispatchStarted.countDown();
        try {
          Thread.sleep(300); // stands in for a DLQ produce waiting on a broker ack
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        dispatchFinished.set(true);
      }
    };

    // A large size cap so the flush can only come from the age tick, and a short age so it fires.
    final var wrapper = new BatchPipelineWrapper<byte[]>(
      TOPIC,
      TestPipelines.identity(),
      allFail,
      new BatchPolicy(1000, Duration.ofMillis(50)),
      scheduler,
      callbacks
    );

    try {
      wrapper.start();
      wrapper.enqueue(new ConsumerRecord<>(TOPIC, 0, 0L, new byte[0], new byte[0]), new byte[] { 1 });

      assertTrue(dispatchStarted.await(5, TimeUnit.SECONDS), "the age tick should start a dispatch");
      wrapper.close();

      assertTrue(
        dispatchFinished.get(),
        "close() returned while the age tick's dispatch was still running; the offset manager and " +
          "DLQ producer are torn down underneath it"
      );
    } finally {
      scheduler.shutdownNow();
    }
  }
}
