package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Contract tests for [ConsumerHealthController]. Covers the pause-arbitration bitmask
/// (formerly tested as `PauseCoordinator`), the CB state machine end-to-end (trip → probe →
/// recover), and the backpressure tick dispatch.
class ConsumerHealthControllerTest {

  private ScheduledExecutorService scheduler;
  private RecordingHook hook;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    hook = new RecordingHook();
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  // ─────────────────────────── Pause arbitration ────────────────────────────

  @Test
  void requestPauseReturnsTrueOnlyOnTransition() {
    final var health = new ConsumerHealthController(null, null, scheduler, hook);

    assertTrue(health.requestPause(ConsumerHealthController.Source.MANUAL), "first request causes transition");
    assertFalse(health.requestPause(ConsumerHealthController.Source.MANUAL), "second request idempotent");
    assertFalse(
      health.requestPause(ConsumerHealthController.Source.BACKPRESSURE),
      "additional source while already paused does not transition"
    );
  }

  @Test
  void releasePauseTransitionsOnlyOnLastRelease() {
    final var health = new ConsumerHealthController(null, null, scheduler, hook);
    health.requestPause(ConsumerHealthController.Source.MANUAL);
    health.requestPause(ConsumerHealthController.Source.BACKPRESSURE);

    assertFalse(
      health.releasePause(ConsumerHealthController.Source.MANUAL),
      "release with another holder pending must not transition"
    );
    assertTrue(health.isPaused(), "still paused because BACKPRESSURE still holds");
    assertTrue(
      health.releasePause(ConsumerHealthController.Source.BACKPRESSURE),
      "release of the last holder causes the resume transition"
    );
    assertFalse(health.isPaused());
  }

  @Test
  void currentSourcesReflectsHeldSet() {
    final var health = new ConsumerHealthController(null, null, scheduler, hook);
    health.requestPause(ConsumerHealthController.Source.MANUAL);
    health.requestPause(ConsumerHealthController.Source.CIRCUIT_BREAKER);

    assertEquals(
      EnumSet.of(ConsumerHealthController.Source.MANUAL, ConsumerHealthController.Source.CIRCUIT_BREAKER),
      health.currentSources()
    );
  }

  @Test
  void hookRejectedAsNull() {
    assertThrows(IllegalArgumentException.class, () -> new ConsumerHealthController(null, null, scheduler, null));
  }

  // ─────────────────────────── Circuit breaker ──────────────────────────────

  @Test
  void cbControllerRequiresSchedulerWhenConfigured() {
    final var cb = new CircuitBreakerController(0.5, 10, Duration.ofMillis(500));
    assertThrows(IllegalArgumentException.class, () -> new ConsumerHealthController(null, cb, null, hook));
  }

  @Test
  void recordOutcomeNoopWhenCircuitBreakerDisabled() {
    final var health = new ConsumerHealthController(null, null, scheduler, hook);
    health.recordOutcome(false);
    assertEquals(CircuitBreakerState.CLOSED, health.circuitBreakerState());
    assertEquals(0, hook.trips);
  }

  @Test
  void breakerTripsAndPausesWhenThresholdCrossed() {
    final var cb = new CircuitBreakerController(0.5, 4, Duration.ofMillis(500));
    final var health = new ConsumerHealthController(null, cb, scheduler, hook);

    health.recordOutcome(false);
    health.recordOutcome(false);
    assertEquals(CircuitBreakerState.CLOSED, health.circuitBreakerState(), "window not full yet");
    health.recordOutcome(false);
    health.recordOutcome(false);
    assertEquals(CircuitBreakerState.OPEN, health.circuitBreakerState(), "100% failures over full window trips");

    assertTrue(health.isHeldBy(ConsumerHealthController.Source.CIRCUIT_BREAKER));
    assertEquals(1, hook.pauseCalls, "trip fires onPause exactly once");
    assertEquals(1, hook.trips);
  }

  @Test
  void openIgnoresIncomingOutcomes() {
    final var cb = new CircuitBreakerController(0.5, 2, Duration.ofMillis(500));
    final var health = new ConsumerHealthController(null, cb, scheduler, hook);
    health.recordOutcome(false);
    health.recordOutcome(false);
    assertEquals(CircuitBreakerState.OPEN, health.circuitBreakerState());

    // In-flight successes arriving after the trip must not unwedge OPEN — only the probe timer
    // does.
    health.recordOutcome(true);
    health.recordOutcome(true);
    assertEquals(CircuitBreakerState.OPEN, health.circuitBreakerState());
  }

  @Test
  void probeFiresAfterOpenDurationAndHalfOpens() throws InterruptedException {
    final var cb = new CircuitBreakerController(0.5, 2, Duration.ofMillis(150));
    final var health = new ConsumerHealthController(null, cb, scheduler, hook);

    health.recordOutcome(false);
    health.recordOutcome(false);
    assertEquals(CircuitBreakerState.OPEN, health.circuitBreakerState());

    // Probe is scheduled 150ms out; wait for it to fire by latching on the resume hook signal.
    // A bare sleep budget can be exceeded under loaded CI by GC pauses or scheduling jitter.
    assertTrue(hook.awaitResumeCall(Duration.ofSeconds(5)), "probe should have fired and called onResume");
    assertEquals(CircuitBreakerState.HALF_OPEN, health.circuitBreakerState(), "probe should have fired");
    assertEquals(1, hook.resumeCalls, "probe fires onResume exactly once");
  }

  @Test
  void halfOpenSuccessClosesBreaker() throws InterruptedException {
    final var cb = new CircuitBreakerController(0.5, 2, Duration.ofMillis(100));
    final var health = new ConsumerHealthController(null, cb, scheduler, hook);

    health.recordOutcome(false);
    health.recordOutcome(false);
    assertTrue(hook.awaitResumeCall(Duration.ofSeconds(5)), "probe should have fired and called onResume");
    assertEquals(CircuitBreakerState.HALF_OPEN, health.circuitBreakerState());

    health.recordOutcome(true);
    assertEquals(CircuitBreakerState.CLOSED, health.circuitBreakerState());
  }

  @Test
  void halfOpenFailureTripsBreakerAgain() throws InterruptedException {
    final var cb = new CircuitBreakerController(0.5, 2, Duration.ofMillis(100));
    final var health = new ConsumerHealthController(null, cb, scheduler, hook);

    health.recordOutcome(false);
    health.recordOutcome(false);
    assertTrue(hook.awaitResumeCall(Duration.ofSeconds(5)), "probe should have fired and called onResume");
    assertEquals(CircuitBreakerState.HALF_OPEN, health.circuitBreakerState());

    health.recordOutcome(false);
    assertEquals(CircuitBreakerState.OPEN, health.circuitBreakerState());
    assertEquals(2, hook.trips, "the half-open failure should fire another trip");
  }

  @Test
  void shutdownCancelsPendingProbe() throws InterruptedException {
    final var cb = new CircuitBreakerController(0.5, 2, Duration.ofMillis(200));
    final var health = new ConsumerHealthController(null, cb, scheduler, hook);
    health.recordOutcome(false);
    health.recordOutcome(false);
    assertEquals(CircuitBreakerState.OPEN, health.circuitBreakerState());

    health.shutdown();
    Thread.sleep(400);
    assertEquals(CircuitBreakerState.OPEN, health.circuitBreakerState(), "probe should have been cancelled");
  }

  // ─────────────────────────── Backpressure ─────────────────────────────────

  @Test
  void tickBackpressureNoopWhenDisabled() {
    final var health = new ConsumerHealthController(null, null, scheduler, hook);
    final var consumer = new MockConsumer<byte[], byte[]>("earliest");
    health.tickBackpressure(consumer);
    assertEquals(0, hook.pauseCalls);
  }

  @Test
  void tickBackpressureDispatchesPauseAndResumeViaInFlightStrategy() {
    final var inflight = new AtomicInteger(0);
    final var bp = new BackpressureController(10, 3, BackpressureController.inFlightStrategy(inflight::get));
    final var health = new ConsumerHealthController(bp, null, scheduler, hook);
    final var consumer = new MockConsumer<byte[], byte[]>("earliest");

    // Below high watermark — no action.
    inflight.set(5);
    health.tickBackpressure(consumer);
    assertEquals(0, hook.pauseCalls);

    // Crosses the high watermark — pause fired.
    inflight.set(15);
    health.tickBackpressure(consumer);
    assertEquals(1, hook.pauseCalls);
    assertTrue(health.isPaused());
    assertEquals(1, hook.backpressurePauses);

    // Still over the low watermark — no resume.
    inflight.set(8);
    health.tickBackpressure(consumer);
    assertEquals(0, hook.resumeCalls);
    assertTrue(health.isPaused());

    // Drops at or below the low watermark — resume fires.
    inflight.set(2);
    health.tickBackpressure(consumer);
    assertEquals(1, hook.resumeCalls);
    assertFalse(health.isPaused());
  }

  @Test
  void tickBackpressureResumesWhenInFlightDrainsInsidePauseWindow() {
    final var inflight = new AtomicInteger(15);
    final var bp = new BackpressureController(10, 3, BackpressureController.inFlightStrategy(inflight::get));
    final var health = new ConsumerHealthController(bp, null, scheduler, hook);
    final var consumer = new MockConsumer<byte[], byte[]>("earliest");
    // Simulate every in-flight record completing between the PAUSE decision and the pause bit
    // becoming visible: those completions all read the pause mask before the BACKPRESSURE bit
    // was set, so none of them unparks the consumer thread. Without the post-pause re-check the
    // controller stays paused and the isPaused assertion below fails fast (this test asserts the
    // guard's outcome — it does not itself park a thread); in a real consumer that state is a
    // parked consumer thread with no remaining wake-up source.
    hook.backpressurePauseAction = () -> inflight.set(0);

    health.tickBackpressure(consumer);

    assertFalse(health.isPaused(), "tick must not leave the consumer paused with zero in-flight records");
    assertEquals(1, hook.pauseCalls);
    assertEquals(1, hook.resumeCalls);
    assertEquals(1, hook.backpressurePauses, "the pause edge itself must still be counted");
    assertEquals(1, hook.backpressureTimeCalls, "the immediate release must record a paused duration");
    assertTrue(hook.lastBackpressureTimeMs >= 1, "paused duration is clamped to >= 1 ms");
  }

  @Test
  void tickBackpressureResumeIgnoredWhenBackpressureDoesNotHoldThePause() {
    final var inflight = new AtomicInteger(0);
    final var bp = new BackpressureController(10, 3, BackpressureController.inFlightStrategy(inflight::get));
    final var health = new ConsumerHealthController(bp, null, scheduler, hook);
    final var consumer = new MockConsumer<byte[], byte[]>("earliest");
    // MANUAL holds the pause; backpressure never paused, so its pause-start timestamp was never
    // set. The metric sits at/below the low watermark, so the raw check() answer is RESUME —
    // releasing here would feed a garbage duration (nanoTime origin) into the backpressure-time
    // counter and log a misleading "backpressure resolved" line on every tick.
    health.requestPause(ConsumerHealthController.Source.MANUAL);

    health.tickBackpressure(consumer);

    assertTrue(health.isPaused(), "manual pause must survive the tick");
    assertTrue(health.isHeldBy(ConsumerHealthController.Source.MANUAL));
    assertEquals(0, hook.resumeCalls, "no resume side effects when backpressure does not hold the pause");
    assertEquals(0, hook.backpressureTimeCalls, "backpressure-time counter must stay untouched");
  }

  @Test
  void backpressureMetricNameReflectsConfiguredStrategy() {
    final var bp = new BackpressureController(10, 3, BackpressureController.inFlightStrategy(() -> 0L));
    final var health = new ConsumerHealthController(bp, null, scheduler, hook);
    assertEquals("in-flight", health.backpressureMetricName());
    assertTrue(health.backpressureEnabled());
  }

  // ─────────────────────────── Concurrency ──────────────────────────────────

  @Test
  void recordOutcomeUnderContention() throws InterruptedException {
    // Verify the internal window keeps totals consistent under heavy concurrent writes — the
    // sliding-window invariant we used to test directly on the deleted CircuitBreakerStats class.
    final var cb = new CircuitBreakerController(0.99, 1000, Duration.ofSeconds(30));
    final var health = new ConsumerHealthController(null, cb, scheduler, hook);
    final var totalWrites = 50_000;
    final var done = new CountDownLatch(totalWrites);

    for (int i = 0; i < totalWrites; i++) {
      final var idx = i;
      Thread.ofVirtual().start(() -> {
        try {
          health.recordOutcome(idx % 3 != 0);
        } finally {
          done.countDown();
        }
      });
    }

    assertTrue(done.await(20, TimeUnit.SECONDS));
    // Threshold is 0.99 with mostly successes (~67%) — must not trip.
    assertEquals(CircuitBreakerState.CLOSED, health.circuitBreakerState());
  }

  private static final class RecordingHook implements ConsumerHealthController.Hook {

    int pauseCalls;
    int resumeCalls;
    int backpressurePauses;
    int backpressureTimeCalls;
    long lastBackpressureTimeMs;
    int trips;
    Runnable backpressurePauseAction = () -> {};
    private final CountDownLatch resumeLatch = new CountDownLatch(1);

    /// Blocks until `onResume` has been invoked at least once, or `timeout` elapses. Used by
    /// circuit-breaker probe tests to fence on the probe actually firing rather than guessing a
    /// sleep budget — the latter flakes on a loaded CI runner when GC or VT-scheduling jitter
    /// pushes the probe past a fixed sleep.
    ///
    /// @param timeout maximum time to block
    /// @return true if `onResume` was observed within the timeout
    boolean awaitResumeCall(final Duration timeout) throws InterruptedException {
      return resumeLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void onPause() {
      pauseCalls++;
    }

    @Override
    public void onResume() {
      resumeCalls++;
      resumeLatch.countDown();
    }

    @Override
    public void onBackpressurePause() {
      backpressurePauses++;
      backpressurePauseAction.run();
    }

    @Override
    public void onBackpressureTimeMs(final long ms) {
      backpressureTimeCalls++;
      lastBackpressureTimeMs = ms;
    }

    @Override
    public void onCircuitBreakerTrip() {
      trips++;
    }

    @Override
    public void onCircuitBreakerStateChange(final CircuitBreakerState state) {}

    @Override
    public void onCircuitBreakerTimeOpenMs(final long ms) {}
  }
}
