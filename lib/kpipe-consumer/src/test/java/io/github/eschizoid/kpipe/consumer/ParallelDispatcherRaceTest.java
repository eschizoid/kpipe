package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/// Race-on-throw correctness tests for [ParallelDispatcher].
///
/// The dispatcher owns the in-flight counter that the parallel-mode in-flight backpressure
/// strategy reads through `KPipeConsumer.totalInFlight()`. The counter is the single source
/// of truth for the high/low watermark, so a leaked or double-counted in-flight on the throw
/// path translates directly into either pause-forever (the watermark latches high and the
/// consumer parks indefinitely) or pause-never (the count underflows and backpressure stops
/// engaging).
///
/// The consumer also relies on the `onComplete` callback firing after every record — that
/// callback is what unparks the consumer thread when it parked under backpressure. If a
/// throwing record swallowed its `onComplete`, a parked consumer thread would never be woken
/// and the pipeline would stall on the first failure.
///
/// These tests drive [ParallelDispatcher] directly (it is package-private) so the throw paths
/// are deterministic and don't depend on a running Kafka consumer loop. They focus on what
/// unit-level per-record tests can't show: many concurrent throws + successes interleaved, and
/// that a parked waiter is actually released by the throw-path `onComplete`.
class ParallelDispatcherRaceTest {

  private static ConsumerRecord<byte[], byte[]> record(final long offset) {
    return new ConsumerRecord<>("test-topic", 0, offset, "k".getBytes(UTF_8), new byte[0]);
  }

  private static ParallelDispatcher newDispatcher(final AtomicInteger rejectCount) {
    return new ParallelDispatcher((_, _) -> rejectCount.incrementAndGet(), Duration.ofSeconds(2));
  }

  private static void awaitInFlightZero(final ParallelDispatcher dispatcher) throws InterruptedException {
    final var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (dispatcher.activeCount() > 0 && System.nanoTime() < deadline) Thread.sleep(5);
  }

  @Test
  void onCompleteOnThrowReleasesAParkedWaiter() throws InterruptedException {
    // Mirrors the consumer-thread interaction: the consumer parks under backpressure and the
    // dispatcher's onComplete callback is the unpark source. Here a real thread parks and the
    // throw-path onComplete must unpark it. If onComplete were skipped when the task threw,
    // the waiter would stay parked until the test's 3s join timeout and fail.
    final var rejectCount = new AtomicInteger(0);
    final var dispatcher = newDispatcher(rejectCount);
    final var waiterParked = new CountDownLatch(1);
    final var waiterReleased = new CountDownLatch(1);

    final var waiter = Thread.ofVirtual().start(() -> {
      waiterParked.countDown();
      // Park until the throw-path onComplete unparks us, the way the consumer thread
      // waits for
      // the in-flight count to drop. A while-loop guards against spurious wakeups.
      while (dispatcher.activeCount() > 0) {
        LockSupport.park();
      }
      waiterReleased.countDown();
    });

    assertTrue(waiterParked.await(2, TimeUnit.SECONDS), "waiter must reach the park loop");

    dispatcher.dispatch(
      record(0),
      () -> {
        throw new RuntimeException("boom");
      },
      () -> LockSupport.unpark(waiter)
    );

    assertTrue(
      waiterReleased.await(3, TimeUnit.SECONDS),
      "throw-path onComplete must unpark the waiter; a swallowed onComplete would strand it"
    );
    awaitInFlightZero(dispatcher);
    assertEquals(0, dispatcher.activeCount(), "in-flight must drain to 0 after the throwing record");
    assertEquals(0, rejectCount.get(), "a throwing task body is not a rejection");
    dispatcher.close();
  }

  @Test
  void interleavedThrowsAndSuccessesDrainInFlightToZero() throws InterruptedException {
    // Many records dispatched concurrently from many virtual threads; roughly half throw,
    // half succeed. Once every record settles, the in-flight counter must be exactly 0 and
    // onComplete must have fired once per record regardless of outcome. A leak on the throw
    // path shows up as a non-zero residual count or a deficit in the onComplete tally.
    final var rejectCount = new AtomicInteger(0);
    final var dispatcher = newDispatcher(rejectCount);
    final var total = 500;
    final var onCompleteCount = new AtomicInteger(0);
    final var allDispatched = new CountDownLatch(total);
    final var allCompleted = new CountDownLatch(total);
    final var go = new CountDownLatch(1);

    for (var i = 0; i < total; i++) {
      final var offset = i;
      final var shouldThrow = (i % 2) == 0;
      Thread.ofVirtual().start(() -> {
        try {
          go.await();
          dispatcher.dispatch(
            record(offset),
            () -> {
              if (shouldThrow) throw new IllegalStateException("boom " + offset);
            },
            () -> {
              onCompleteCount.incrementAndGet();
              allCompleted.countDown();
            }
          );
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          allDispatched.countDown();
        }
      });
    }

    go.countDown();

    assertTrue(allDispatched.await(10, TimeUnit.SECONDS), "all dispatch calls must return");
    assertTrue(allCompleted.await(10, TimeUnit.SECONDS), "onComplete must fire for every record");

    awaitInFlightZero(dispatcher);
    assertEquals(0, dispatcher.activeCount(), "interleaved throws and successes must drain to 0");
    assertEquals(total, onCompleteCount.get(), "onComplete must fire exactly once per record");
    assertEquals(0, rejectCount.get(), "no dispatch was rejected before close");
    dispatcher.close();
  }

  @Test
  void backpressureRecoversAfterABurstOfThrows() throws InterruptedException {
    // The consumer's totalInFlight() feeds the dispatcher's activeCount() into the in-flight
    // backpressure strategy. This test wires a real BackpressureController to the dispatcher's
    // count, the same way the consumer does, and then exercises the pause/resume transition
    // across a burst of records that all throw.
    //
    // A gate holds the records in-flight so the count climbs above the high watermark and the
    // controller decides PAUSE. Releasing the gate lets every record run its body, which
    // throws. If the throw path leaked the in-flight count, the controller would never drop
    // to the resume watermark and would stay paused forever. The contract: after the throwing
    // burst drains, the count settles at 0 and the controller decides RESUME.
    final var rejectCount = new AtomicInteger(0);
    final var dispatcher = newDispatcher(rejectCount);
    // Consumer passes null safely here: the in-flight strategy reads the supplier, not the
    // consumer, so the broker is never queried.
    final var controller = new BackpressureController(
      8,
      4,
      BackpressureController.inFlightStrategy(dispatcher::activeCount)
    );
    final var total = 32;
    final var release = new CountDownLatch(1);
    final var allParked = new CountDownLatch(total);
    final var allCompleted = new CountDownLatch(total);

    for (var i = 0; i < total; i++) {
      final var offset = i;
      dispatcher.dispatch(
        record(offset),
        () -> {
          allParked.countDown();
          try {
            release.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
          throw new RuntimeException("boom " + offset);
        },
        allCompleted::countDown
      );
    }

    assertTrue(allParked.await(10, TimeUnit.SECONDS), "all records must be in-flight before release");
    // The count is above the high watermark while every record is gated, so a not-yet-paused
    // consumer would be told to PAUSE.
    assertEquals(
      BackpressureController.Action.PAUSE,
      controller.check(null, false),
      "in-flight count above the high watermark must request a pause"
    );

    release.countDown();

    assertTrue(allCompleted.await(10, TimeUnit.SECONDS), "every throwing record must complete after release");
    awaitInFlightZero(dispatcher);

    assertEquals(0, dispatcher.activeCount(), "a burst of throws must leave no residual in-flight count");
    // A consumer that paused at the high watermark must now be told to RESUME — the throwing
    // burst did not corrupt the count, so the watermark recovers.
    assertEquals(
      BackpressureController.Action.RESUME,
      controller.check(null, true),
      "once the throwing burst has drained, a paused consumer must be told to resume"
    );
    dispatcher.close();
  }

  @Test
  void inFlightCountNeverGoesNegativeUnderConcurrentThrows() throws InterruptedException {
    // A double-decrement on the throw path would drive the counter negative under load,
    // which would make the in-flight strategy under-report and stop engaging backpressure.
    // Sample the counter from a watcher thread while throwing records churn through, and
    // assert it never dips below zero and settles at exactly zero.
    final var rejectCount = new AtomicInteger(0);
    final var dispatcher = newDispatcher(rejectCount);
    final var total = 400;
    final var allCompleted = new CountDownLatch(total);
    final var minObserved = new AtomicLong(Long.MAX_VALUE);
    final var stopWatching = new CountDownLatch(1);

    final var watcher = Thread.ofVirtual().start(() -> {
      while (stopWatching.getCount() > 0) {
        final var current = dispatcher.activeCount();
        minObserved.updateAndGet(prev -> Math.min(prev, current));
        Thread.onSpinWait();
      }
    });

    final var go = new CountDownLatch(1);
    for (var i = 0; i < total; i++) {
      final var offset = i;
      Thread.ofVirtual().start(() -> {
        try {
          go.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        dispatcher.dispatch(
          record(offset),
          () -> {
            throw new RuntimeException("boom " + offset);
          },
          allCompleted::countDown
        );
      });
    }

    go.countDown();
    assertTrue(allCompleted.await(10, TimeUnit.SECONDS), "every throwing record must complete");
    awaitInFlightZero(dispatcher);
    stopWatching.countDown();
    watcher.join(Duration.ofSeconds(2));

    assertTrue(minObserved.get() >= 0, "in-flight count must never go negative; saw " + minObserved.get());
    assertEquals(0, dispatcher.activeCount(), "in-flight must settle at exactly 0 after the throwing burst");
    dispatcher.close();
  }
}
