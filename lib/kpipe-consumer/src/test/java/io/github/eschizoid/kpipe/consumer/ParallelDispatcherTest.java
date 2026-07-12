package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/// Unit tests for [ParallelDispatcher]. Focused on the executor lifecycle: the executor is
/// created in the field initializer (at construction), so it must be shut down by
/// [ParallelDispatcher#close()] even if the dispatcher never dispatched anything — otherwise a
/// consumer built-but-never-started leaks it.
class ParallelDispatcherTest {

  private static ConsumerRecord<byte[], byte[]> record(final long offset) {
    return new ConsumerRecord<>("test-topic", 0, offset, "k".getBytes(UTF_8), new byte[0]);
  }

  private static ParallelDispatcher newDispatcher(final AtomicInteger rejectCount) {
    return newDispatcher(rejectCount, Duration.ofSeconds(1));
  }

  private static ParallelDispatcher newDispatcher(final AtomicInteger rejectCount, final Duration terminationTimeout) {
    return new ParallelDispatcher((_, _) -> rejectCount.incrementAndGet(), terminationTimeout);
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
  void pendingCountDecrementedWhenTaskThrows() throws InterruptedException {
    // Guards the criticality-9 race in §20: ParallelDispatcher owns the in-flight counter that
    // drives PARALLEL-mode backpressure (§5). If the task body throws and the finally block did
    // NOT decrement, pendingCount() would climb monotonically per failed record — the watermark
    // would latch at HIGH and the consumer would pause forever (deadlock). Conversely, double
    // decrement would underflow toward negative and the consumer would never pause (false-
    // negative backpressure). The production code wraps processTask.run() in try/finally so
    // interrupt-style and exception-style exits both decrement exactly once; this test pins it.
    final var rejectCount = new AtomicInteger(0);
    final var dispatcher = newDispatcher(rejectCount);
    final var completed = new CountDownLatch(1);

    dispatcher.dispatch(
      record(0),
      () -> {
        throw new RuntimeException("boom");
      },
      completed::countDown
    );

    assertTrue(completed.await(2, TimeUnit.SECONDS), "onComplete must fire even when task throws");
    final var deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (dispatcher.pendingCount() > 0 && System.nanoTime() < deadline) Thread.sleep(5);
    assertEquals(0, dispatcher.pendingCount(), "throwing task's finally must still decrement in-flight");
    assertEquals(0, rejectCount.get(), "a throwing task body is not a rejection");
    dispatcher.close();
  }

  @Test
  void pendingCountDrainsWhenTaskThrowsError() throws InterruptedException {
    // Sibling of pendingCountDecrementedWhenTaskThrows for `Error` (e.g. `AssertionError`),
    // which is a Throwable-not-Exception and would bypass any future narrowing of the production
    // try/finally to `catch (Exception) { ... } finally { ... }` — pendingCount would silently
    // strand on every Error escape.
    final var rejectCount = new AtomicInteger(0);
    final var dispatcher = newDispatcher(rejectCount);
    final var completed = new CountDownLatch(1);

    dispatcher.dispatch(
      record(0),
      () -> {
        throw new AssertionError("simulated invariant violation");
      },
      completed::countDown
    );

    assertTrue(completed.await(2, TimeUnit.SECONDS), "onComplete must fire even when task throws an Error");
    final var deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (dispatcher.pendingCount() > 0 && System.nanoTime() < deadline) Thread.sleep(5);
    assertEquals(0, dispatcher.pendingCount(), "Error-throwing task's finally must still decrement in-flight");
    dispatcher.close();
  }

  @Test
  void onCompleteFiresExactlyOnceWhenTaskThrows() throws InterruptedException {
    // Pair test for pendingCountDecrementedWhenTaskThrows: the consumer's afterRecordComplete()
    // unparks the consumer thread when backpressure is held (§11 LockSupport park/unpark). If
    // onComplete fired twice on the throw path (e.g. once in a catch and once in finally), the
    // consumer would re-evaluate twice per failed record — wasteful but tolerable. If it fired
    // ZERO times, the consumer would never wake and the pipeline would stall on the first
    // exception. Exactly-once is the contract.
    final var rejectCount = new AtomicInteger(0);
    final var dispatcher = newDispatcher(rejectCount);
    final var completeCount = new AtomicInteger(0);
    final var completed = new CountDownLatch(1);

    dispatcher.dispatch(
      record(0),
      () -> {
        throw new IllegalStateException("processing failed");
      },
      () -> {
        completeCount.incrementAndGet();
        completed.countDown();
      }
    );

    assertTrue(completed.await(2, TimeUnit.SECONDS), "onComplete must fire");
    // Give any spurious second invocation a window to land before asserting.
    Thread.sleep(100);
    assertEquals(1, completeCount.get(), "onComplete must fire exactly once when task throws");
    final var deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (dispatcher.pendingCount() > 0 && System.nanoTime() < deadline) Thread.sleep(5);
    assertEquals(0, dispatcher.pendingCount(), "in-flight must drain to 0 after throwing task");
    dispatcher.close();
  }

  @Test
  void dispatchAfterCloseHitsRejectHandler() throws InterruptedException {
    // Explicit pin for the post-close rejection path. closeShutsDownExecutorEvenWithNoDispatches
    // already exercises the reject handler via a never-dispatched dispatcher; this test adds the
    // stronger contract: the task body itself MUST NOT execute on rejection, and pendingCount()
    // must roll back from the speculative increment in dispatch(). If the increment were not
    // rolled back, every shutdown-race rejection would leak a count, and shutdownGracefully()
    // would burn its full timeout waiting on a ghost.
    final var rejectCount = new AtomicInteger(0);
    final var dispatcher = newDispatcher(rejectCount);
    final var taskRan = new AtomicInteger(0);
    final var onCompleteRan = new AtomicInteger(0);

    dispatcher.close();

    dispatcher.dispatch(record(0), taskRan::incrementAndGet, onCompleteRan::incrementAndGet);

    assertEquals(1, rejectCount.get(), "dispatch after close must hit the reject handler");
    assertEquals(0, taskRan.get(), "rejected task body must NOT execute");
    assertEquals(
      0,
      onCompleteRan.get(),
      "rejected dispatch must NOT invoke onComplete (the consumer takes the reject path instead)"
    );
    assertEquals(0, dispatcher.pendingCount(), "rejected dispatch must roll back the speculative increment");
  }

  @Test
  void concurrentDispatchAndCloseDoesNotLeakPending() throws InterruptedException {
    // Stress the race window between dispatch() and close(): close() flips the executor to
    // shut-down mid-flight while N dispatcher threads call dispatch() concurrently. Each
    // dispatch ends in one of two terminal accounting states — task started + finally drained,
    // OR rejected + counter rolled back. There is no third "leaked" state. The invariant: after
    // all dispatcher threads return and close() has completed, pendingCount() must drain to 0,
    // AND (started + rejected) must equal total. A leak shows up either as a non-zero pendingCount
    // (decrement missing) or as a started + rejected != total mismatch (silent drop).
    final var rejectCount = new AtomicInteger(0);
    final var dispatcher = newDispatcher(rejectCount, Duration.ofSeconds(2));
    final var totalDispatches = 200;
    final var started = new AtomicInteger(0);
    final var allDispatchersDone = new CountDownLatch(totalDispatches);
    final var go = new CountDownLatch(1);

    for (int i = 0; i < totalDispatches; i++) {
      final var offset = i;
      Thread.ofVirtual().start(() -> {
        try {
          go.await();
          dispatcher.dispatch(record(offset), started::incrementAndGet, () -> {});
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          allDispatchersDone.countDown();
        }
      });
    }

    // Close from another thread, mid-flight. close() blocks until awaitTermination returns or
    // times out, so spawning it on a VT keeps the test thread free to release `go`.
    final var closeReturned = new CountDownLatch(1);
    Thread.ofVirtual().start(() -> {
      try {
        dispatcher.close();
      } finally {
        closeReturned.countDown();
      }
    });

    go.countDown();

    assertTrue(allDispatchersDone.await(10, TimeUnit.SECONDS), "all dispatcher threads must return");
    assertTrue(closeReturned.await(10, TimeUnit.SECONDS), "close() must return");

    final var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (dispatcher.pendingCount() > 0 && System.nanoTime() < deadline) Thread.sleep(10);
    assertEquals(0, dispatcher.pendingCount(), "pendingCount must drain to 0 after concurrent dispatch+close");
    assertEquals(
      totalDispatches,
      started.get() + rejectCount.get(),
      "every dispatch must terminate as either started-and-finished or rejected; no silent leaks"
    );
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
