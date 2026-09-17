package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/// Guards which phase of a flush holds the per-topic lock.
///
/// `sink.apply` runs under the lock and must keep doing so — `BatchSink` promises implementers that
/// flushes never overlap, so the sink need not be thread-safe. The per-record outcome dispatch
/// carries no such promise and must NOT hold it: `onBatchFailure` reaches a synchronous DLQ produce
/// that waits for a broker ack, and a whole-batch failure performs one per record, serially.
///
/// This measures it directly rather than inferring it from end-to-end throughput, which for this
/// effect is too noisy to resolve. A competing `enqueue` on another thread is timed while a
/// whole-batch failure dispatches. With the dispatch under the lock that enqueue blocked for 221ms
/// against a 200ms dispatch budget — the whole thing. With the dispatch moved out it blocks for 0ms
/// and every record still reaches the DLQ.
class BatchFlushLockHoldTest {

  private static final String TOPIC = "lock-hold";
  private static final int BATCH = 20;
  private static final Duration PER_RECORD_DLQ = Duration.ofMillis(10);

  /// A whole-batch failure against a slow DLQ must not block other workers enqueueing to the same
  /// topic. `PER_RECORD_DLQ` stands in for a broker ack that in production is bounded by
  /// `max.block.ms` plus `delivery.timeout.ms`, which add — so the real hold this prevents is far
  /// longer than what the test simulates.
  @Test
  void competingEnqueueDoesNotWaitForTheOutcomeDispatch() throws Exception {
    final var scheduler = Executors.newSingleThreadScheduledExecutor();
    final var dispatchStarted = new CountDownLatch(1);
    final var dispatched = new AtomicLong();

    // `ofVoid` converts a thrown exception into `BatchResult.allFailed`, so this drives the
    // per-record failure loop rather than the wrapper's own whole-batch catch. Either way every
    // record gets an `onBatchFailure`, which is what the timing here depends on.
    final BatchSink<byte[]> failingSink = BatchSink.ofVoid(batch -> {
      throw new IllegalStateException("every record fails, so every one takes the DLQ path");
    });

    final var callbacks = new BatchPipelineWrapper.BatchCallbacks() {
      @Override
      public void markProcessed(final ConsumerRecord<byte[], byte[]> record) {}

      @Override
      public void onBatchFailure(final ConsumerRecord<byte[], byte[]> record, final Exception cause) {
        dispatchStarted.countDown();
        // Stands in for KPipeProducer.send(...).get() waiting on a broker that will not ack.
        try {
          Thread.sleep(PER_RECORD_DLQ.toMillis());
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        dispatched.incrementAndGet();
      }
    };

    final var wrapper = new BatchPipelineWrapper<byte[]>(
      TOPIC,
      TestPipelines.identity(),
      failingSink,
      new BatchPolicy(BATCH, Duration.ofMinutes(1)),
      scheduler,
      callbacks
    );

    try {
      wrapper.start();

      // Thread A fills the batch; the final enqueue trips the size flush and runs the dispatch.
      final var flusher = Thread.ofVirtual().start(() -> {
        for (int i = 0; i < BATCH; i++) wrapper.enqueue(record(i), new byte[] { (byte) i });
      });

      assertTrue(dispatchStarted.await(5, TimeUnit.SECONDS), "dispatch should begin");

      // Thread B is a competing worker on the same topic. Time how long its enqueue blocks.
      final var blockedNanos = new AtomicLong();
      final var competitor = Thread.ofVirtual().start(() -> {
        final var t0 = System.nanoTime();
        wrapper.enqueue(record(999), new byte[] { 9 });
        blockedNanos.set(System.nanoTime() - t0);
      });

      competitor.join(Duration.ofSeconds(30));
      flusher.join(Duration.ofSeconds(30));

      final var blockedMs = blockedNanos.get() / 1_000_000L;
      final var dispatchBudgetMs = BATCH * PER_RECORD_DLQ.toMillis();
      System.out.printf(
        "[lock-hold] competing enqueue blocked %d ms; whole-dispatch budget %d ms (%d records x %d ms)%n",
        blockedMs,
        dispatchBudgetMs,
        BATCH,
        PER_RECORD_DLQ.toMillis()
      );

      assertEquals(BATCH, dispatched.get(), "every record in the failed batch still reaches the DLQ path");
      // A quarter of the budget is far above any plausible scheduling jitter for an uncontended
      // lock acquisition, and far below what the dispatch costs — so this separates the two states
      // without being sensitive to how fast the box is.
      assertTrue(
        blockedMs < dispatchBudgetMs / 4,
        "competing enqueue blocked " +
          blockedMs +
          " ms, which is within reach of the " +
          dispatchBudgetMs +
          " ms dispatch budget; the outcome dispatch is holding the flush lock again"
      );
    } finally {
      wrapper.close();
      scheduler.shutdownNow();
    }
  }

  /// The in-flight gauge must come down exactly once per flushed batch, including when no dispatch
  /// ever runs for it. The records leave the buffer before the sink is called, so an `Error` out of
  /// the sink — the one throwable `flush` does not catch — would otherwise strand their count in
  /// the gauge forever, and the gauge feeds the consumer's backpressure watermark.
  @Test
  void anErrorFromTheSinkStillClearsTheInFlightGauge() throws Exception {
    final var scheduler = Executors.newSingleThreadScheduledExecutor();
    final BatchSink<byte[]> erroringSink = BatchSink.ofVoid(batch -> {
      throw new StackOverflowError("an Error, which flush() deliberately does not catch");
    });
    final var callbacks = new BatchPipelineWrapper.BatchCallbacks() {
      @Override
      public void markProcessed(final ConsumerRecord<byte[], byte[]> record) {}

      @Override
      public void onBatchFailure(final ConsumerRecord<byte[], byte[]> record, final Exception cause) {}
    };
    final var wrapper = new BatchPipelineWrapper<byte[]>(
      TOPIC,
      TestPipelines.identity(),
      erroringSink,
      new BatchPolicy(BATCH, Duration.ofMinutes(1)),
      scheduler,
      callbacks
    );

    try {
      wrapper.start();
      assertThrows(StackOverflowError.class, () -> {
        for (int i = 0; i < BATCH; i++) wrapper.enqueue(record(i), new byte[] { (byte) i });
      });
      assertEquals(0L, wrapper.bufferedCount(), "the flushed batch must leave the gauge at zero, not stranded");
    } finally {
      scheduler.shutdownNow();
    }
  }

  private static ConsumerRecord<byte[], byte[]> record(final int offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, new byte[0], new byte[0]);
  }
}
