package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/// Unit tests for [ParallelDispatcher]. Focused on the executor lifecycle: the executor is
/// created in the field initializer (at construction), so it must be shut down by [#close()]
/// even if the dispatcher never dispatched anything — otherwise a consumer built-but-never-
/// started leaks it.
class ParallelDispatcherTest {

  private static ConsumerRecord<String, byte[]> record(final long offset) {
    return new ConsumerRecord<>("test-topic", 0, offset, "k", new byte[0]);
  }

  private static ParallelDispatcher<String> newDispatcher(final AtomicInteger rejectCount) {
    return newDispatcher(rejectCount, Duration.ofSeconds(1));
  }

  private static ParallelDispatcher<String> newDispatcher(
    final AtomicInteger rejectCount,
    final Duration terminationTimeout
  ) {
    return new ParallelDispatcher<>((rec, ex) -> rejectCount.incrementAndGet(), terminationTimeout);
  }

  @Test
  void closeShutsDownExecutorEvenWithNoDispatches() {
    // Build-but-never-dispatch, then close. The executor is created in the ctor, so close()
    // must shut it down. We observe shutdown via the reject handler: a dispatch after close()
    // hits a shut-down executor → RejectedExecutionException → rejectHandler fires.
    final var rejectCount = new AtomicInteger(0);
    final var dispatcher = newDispatcher(rejectCount);

    dispatcher.close();

    dispatcher.dispatch(record(0), () -> {}, () -> {});
    assertEquals(1, rejectCount.get(), "dispatch after close must be rejected (executor shut down)");
    assertEquals(0, dispatcher.pendingCount(), "rejected dispatch must roll back the in-flight counter");
  }

  @Test
  void dispatchRunsTaskAndDrainsInFlight() throws InterruptedException {
    final var rejectCount = new AtomicInteger(0);
    final var dispatcher = newDispatcher(rejectCount);
    final var ran = new CountDownLatch(1);
    final var completed = new CountDownLatch(1);

    dispatcher.dispatch(record(0), ran::countDown, completed::countDown);

    assertTrue(ran.await(2, TimeUnit.SECONDS), "task must run on a virtual thread");
    assertTrue(completed.await(2, TimeUnit.SECONDS), "onComplete must fire");
    final var deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (dispatcher.pendingCount() > 0 && System.nanoTime() < deadline) Thread.sleep(5);
    assertEquals(0, dispatcher.pendingCount(), "in-flight must drain to 0");
    assertEquals(0, rejectCount.get(), "no rejection on the happy path");
    dispatcher.close();
  }

  @Test
  void pendingCountDrainsToZeroWhenCloseInterruptsRunningTask() throws InterruptedException {
    // close() calls shutdownNow() when awaitTermination times out. The concern: an interrupted
    // task never runs its finally, leaving inFlight stuck > 0. That's a real risk for a POOLED
    // executor (shutdownNow returns queued-unstarted tasks), but ParallelDispatcher uses
    // newVirtualThreadPerTaskExecutor() — every task starts immediately on its own VT, no work
    // queue, so shutdownNow() returns empty and just interrupts running tasks. Interrupt does
    // NOT skip the finally block, so inFlight is still decremented. This test proves it: a task
    // blocked in Thread.sleep, a short close timeout forcing shutdownNow(), then inFlight must
    // still reach 0.
    final var rejectCount = new AtomicInteger(0);
    final var dispatcher = newDispatcher(rejectCount, Duration.ofMillis(50));
    final var started = new CountDownLatch(1);

    dispatcher.dispatch(
      record(0),
      () -> {
        started.countDown();
        try {
          Thread.sleep(Duration.ofMinutes(10).toMillis()); // far longer than the close timeout
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt(); // task observes interrupt; finally still runs
        }
      },
      () -> {}
    );
    assertTrue(started.await(2, TimeUnit.SECONDS), "task must start");
    assertEquals(1, dispatcher.pendingCount(), "one task in flight before close");

    dispatcher.close(); // awaitTermination(50ms) times out → shutdownNow() interrupts the sleep

    final var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (dispatcher.pendingCount() > 0 && System.nanoTime() < deadline) Thread.sleep(5);
    assertEquals(0, dispatcher.pendingCount(), "interrupted task's finally must still decrement in-flight");
  }
}
