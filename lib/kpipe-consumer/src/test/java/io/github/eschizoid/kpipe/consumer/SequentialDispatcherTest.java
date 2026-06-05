package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/// Unit tests for [SequentialDispatcher]. Focused on the in-flight counter that drives the
/// public `inFlight` metric and `shutdownGracefully(timeout)` drain reporting — without it,
/// both would always read 0 in SEQUENTIAL mode even while a record is actively processing.
class SequentialDispatcherTest {

  private static ConsumerRecord<String, byte[]> record(final long offset) {
    return new ConsumerRecord<>("test-topic", 0, offset, "k", new byte[0]);
  }

  @Test
  void pendingCountIsZeroWhenIdle() {
    final var dispatcher = new SequentialDispatcher<String>();
    assertEquals(0L, dispatcher.pendingCount());
    dispatcher.close();
  }

  @Test
  void pendingCountReadsOneWhileProcessingThenZero() throws InterruptedException {
    final var dispatcher = new SequentialDispatcher<String>();
    final var insideTask = new CountDownLatch(1);
    final var allowFinish = new CountDownLatch(1);
    final var observed = new AtomicLong(-1);

    // SequentialDispatcher.dispatch is synchronous on the calling thread, so we have to run
    // it on a separate thread to observe pendingCount mid-flight.
    final var worker = Thread.ofVirtual().start(() ->
      dispatcher.dispatch(
        record(0),
        () -> {
          insideTask.countDown();
          try {
            allowFinish.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        },
        () -> {}
      )
    );

    assertTrue(insideTask.await(2, TimeUnit.SECONDS), "task should start");
    observed.set(dispatcher.pendingCount());
    allowFinish.countDown();
    worker.join(2_000);

    assertEquals(1L, observed.get(), "pendingCount must report 1 while processing");
    assertEquals(0L, dispatcher.pendingCount(), "pendingCount must drop to 0 after task returns");
    dispatcher.close();
  }

  @Test
  void onCompleteRunsAfterTaskRegardlessOfThrow() {
    final var dispatcher = new SequentialDispatcher<String>();
    final var completed = new AtomicLong(0);

    try {
      dispatcher.dispatch(
        record(0),
        () -> {
          throw new RuntimeException("boom");
        },
        completed::incrementAndGet
      );
    } catch (final RuntimeException ignored) {
      // expected — propagates from inline run
    }

    assertEquals(1L, completed.get(), "onComplete must fire even when task throws");
    assertEquals(0L, dispatcher.pendingCount(), "pendingCount must drop to 0 even when task throws");
    dispatcher.close();
  }
}
