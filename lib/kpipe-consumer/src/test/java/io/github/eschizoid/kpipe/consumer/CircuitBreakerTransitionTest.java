package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/// Deterministic state-machine transition tests for the circuit breaker, driven directly through
/// [ConsumerHealthController] with a recording [ConsumerHealthController.Hook] and a
// hand-controlled
/// scheduler. No Kafka, no `MockConsumer`, no timing-based `awaitCondition` — every transition is
/// driven by an explicit `recordOutcome(...)` call or by firing the captured probe task by hand, so
/// these assertions are race-free.
///
/// `CircuitBreakerControllerTest` pins the pure `shouldTrip` predicate;
// `KPipeCircuitBreakerIntegrationTest`
/// drives the happy-path CLOSED → OPEN → HALF_OPEN → CLOSED cycle end-to-end through a live
// consumer.
/// This file fills the edges neither pins deterministically:
///
///   * HALF_OPEN → OPEN on a failed probe (window restarts, a fresh probe is scheduled).
///   * HALF_OPEN → CLOSED on a successful probe (the window is genuinely reset, not merely
// re-read).
///   * the trip-rate boundary at the state-machine level (exactly at vs just below threshold).
///   * window-not-full: no trip before `windowSize` samples regardless of failure rate.
///   * the one-shot OPEN → HALF_OPEN probe is a single scheduled task, fired exactly once.
class CircuitBreakerTransitionTest {

  /// Records every Hook callback so a test can assert on the exact transition sequence. The
  // CB-relevant
  /// counters are the ones the state machine drives; pause/resume bookkeeping is recorded too
  // because a
  /// trip must pause and a successful probe must resume.
  private static final class RecordingHook implements ConsumerHealthController.Hook {

    final AtomicInteger pauses = new AtomicInteger();
    final AtomicInteger resumes = new AtomicInteger();
    final AtomicInteger trips = new AtomicInteger();
    final AtomicInteger timeOpenReports = new AtomicInteger();
    final List<CircuitBreakerState> stateChanges = new ArrayList<>();

    @Override
    public void onPause() {
      pauses.incrementAndGet();
    }

    @Override
    public void onResume() {
      resumes.incrementAndGet();
    }

    @Override
    public void onBackpressurePause() {}

    @Override
    public void onBackpressureTimeMs(final long ms) {}

    @Override
    public void onCircuitBreakerTrip() {
      trips.incrementAndGet();
    }

    @Override
    public void onCircuitBreakerStateChange(final CircuitBreakerState state) {
      stateChanges.add(state);
    }

    @Override
    public void onCircuitBreakerTimeOpenMs(final long ms) {
      timeOpenReports.incrementAndGet();
    }
  }

  /// A scheduler that does NOT run anything on its own. It captures each submitted task plus the
  // delay
  /// so a test can assert how many probe timers were armed and fire them by hand,
  // deterministically.
  private static final class CapturingScheduler implements ScheduledExecutorService {

    final List<Runnable> scheduled = new ArrayList<>();
    final List<Long> delaysMs = new ArrayList<>();
    int cancelCount = 0;

    /// Fires the most-recently-scheduled task (the live probe) exactly once.
    void fireLatest() {
      scheduled.getLast().run();
    }

    @Override
    public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
      scheduled.add(command);
      delaysMs.add(unit.toMillis(delay));
      return new NoopFuture();
    }

    private final class NoopFuture implements ScheduledFuture<Object> {

      @Override
      public long getDelay(final TimeUnit unit) {
        return 0;
      }

      @Override
      public int compareTo(final Delayed o) {
        return 0;
      }

      @Override
      public boolean cancel(final boolean mayInterruptIfRunning) {
        cancelCount++;
        return true;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public Object get() {
        return null;
      }

      @Override
      public Object get(final long timeout, final TimeUnit unit) {
        return null;
      }
    }

    // ── Unused ScheduledExecutorService surface ─────────────────────────────
    @Override
    public <V> ScheduledFuture<V> schedule(final Callable<V> c, final long d, final TimeUnit u) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable c, final long i, final long p, final TimeUnit u) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable c, final long i, final long d, final TimeUnit u) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {}

    @Override
    public List<Runnable> shutdownNow() {
      return List.of();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) {
      return true;
    }

    @Override
    public <T> Future<T> submit(final Callable<T> task) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<T> submit(final Runnable task, final T result) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Future<?> submit(final Runnable task) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(
      final Collection<? extends Callable<T>> tasks,
      final long timeout,
      final TimeUnit unit
    ) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void execute(final Runnable command) {
      throw new UnsupportedOperationException();
    }
  }

  /// Builds a controller wired to the supplied hook + scheduler. windowSize=4, threshold=0.5 keeps
  // the
  /// arithmetic obvious: a full window of 4 needs 2 failures to hit exactly 50%.
  private static ConsumerHealthController newController(final RecordingHook hook, final CapturingScheduler scheduler) {
    final var cb = new CircuitBreakerController(0.5, 4, Duration.ofMillis(300));
    return new ConsumerHealthController(null, cb, scheduler, hook);
  }

  /// Drives the breaker CLOSED → OPEN by feeding a full window of failures, then asserts the trip
  /// happened. Shared setup for the HALF_OPEN tests.
  private static void tripToOpen(final ConsumerHealthController hc) {
    for (int i = 0; i < 4; i++) hc.recordOutcome(false);
    assertSame(CircuitBreakerState.OPEN, hc.circuitBreakerState(), "full window of failures must trip to OPEN");
  }

  @Test
  void halfOpenProbeFailureReturnsToOpenAndReschedules() {
    final var hook = new RecordingHook();
    final var scheduler = new CapturingScheduler();
    final var hc = newController(hook, scheduler);

    tripToOpen(hc);
    assertEquals(1, scheduler.scheduled.size(), "trip arms exactly one probe timer");
    assertEquals(1, hook.trips.get());

    // Fire the probe timer: OPEN → HALF_OPEN, consumer resumes.
    scheduler.fireLatest();
    assertSame(CircuitBreakerState.HALF_OPEN, hc.circuitBreakerState());

    // The probe record fails → HALF_OPEN → OPEN. Window restarts and a fresh probe is scheduled.
    hc.recordOutcome(false);
    assertSame(CircuitBreakerState.OPEN, hc.circuitBreakerState(), "failed probe must drop back to OPEN");
    assertEquals(2, hook.trips.get(), "a re-trip from HALF_OPEN counts as a trip");
    assertEquals(2, scheduler.scheduled.size(), "a re-trip arms a second probe timer");
  }

  @Test
  void halfOpenProbeSuccessReturnsToClosedAndResetsWindow() {
    final var hook = new RecordingHook();
    final var scheduler = new CapturingScheduler();
    final var hc = newController(hook, scheduler);

    tripToOpen(hc);
    scheduler.fireLatest();
    assertSame(CircuitBreakerState.HALF_OPEN, hc.circuitBreakerState());

    // The probe record succeeds → HALF_OPEN → CLOSED.
    hc.recordOutcome(true);
    assertSame(CircuitBreakerState.CLOSED, hc.circuitBreakerState(), "successful probe must close the breaker");

    // Prove the window was genuinely reset, not merely re-read: a single fresh failure must NOT
    // re-trip
    // even though the pre-reset window was saturated with failures. Three failures fill only 3 of
    // the
    // 4 reset slots, so shouldTrip's full-window guard holds and no second trip fires.
    hc.recordOutcome(false);
    hc.recordOutcome(false);
    hc.recordOutcome(false);
    assertSame(CircuitBreakerState.CLOSED, hc.circuitBreakerState(), "window must be reset after close, not stale");
    assertEquals(1, hook.trips.get(), "no re-trip until the fresh window fills");
  }

  @Test
  void tripFiresExactlyAtThresholdWithFullWindow() {
    final var hook = new RecordingHook();
    final var scheduler = new CapturingScheduler();
    final var hc = newController(hook, scheduler);

    // Window of 4: 2 successes then 2 failures → failureRate exactly 0.5 on the 4th (full) sample.
    hc.recordOutcome(true);
    hc.recordOutcome(true);
    hc.recordOutcome(false);
    assertSame(CircuitBreakerState.CLOSED, hc.circuitBreakerState(), "1/4 failure rate must not trip");
    hc.recordOutcome(false);
    assertSame(CircuitBreakerState.OPEN, hc.circuitBreakerState(), "exactly-at-threshold full window must trip");
    assertEquals(1, hook.trips.get());
  }

  @Test
  void noTripJustBelowThresholdWithFullWindow() {
    final var hook = new RecordingHook();
    final var scheduler = new CapturingScheduler();
    final var hc = newController(hook, scheduler);

    // Window of 4: 3 successes, 1 failure → 0.25 failure rate, below the 0.5 threshold. Even with
    // the
    // window full, the breaker must stay CLOSED.
    hc.recordOutcome(true);
    hc.recordOutcome(true);
    hc.recordOutcome(true);
    hc.recordOutcome(false);
    assertSame(CircuitBreakerState.CLOSED, hc.circuitBreakerState(), "0.25 failure rate must not trip a 0.5 breaker");
    assertEquals(0, hook.trips.get());
    assertTrue(scheduler.scheduled.isEmpty(), "no trip → no probe timer armed");
  }

  @Test
  void neverTripsBeforeWindowFillsEvenAtFullFailureRate() {
    final var hook = new RecordingHook();
    final var scheduler = new CapturingScheduler();
    final var hc = newController(hook, scheduler);

    // 3 failures into a window of 4 — 100% failure rate, but the window is not yet full.
    hc.recordOutcome(false);
    hc.recordOutcome(false);
    hc.recordOutcome(false);
    assertSame(CircuitBreakerState.CLOSED, hc.circuitBreakerState(), "must not trip before windowSize samples");
    assertEquals(0, hook.trips.get());

    // The 4th failure fills the window and crosses the threshold → trip.
    hc.recordOutcome(false);
    assertSame(CircuitBreakerState.OPEN, hc.circuitBreakerState(), "trip fires only once the window is full");
    assertEquals(1, hook.trips.get());
  }

  @Test
  void openToHalfOpenProbeIsOneShotAndFiresExactlyOnce() {
    final var hook = new RecordingHook();
    final var scheduler = new CapturingScheduler();
    final var hc = newController(hook, scheduler);

    tripToOpen(hc);

    // A single probe timer is armed at exactly openDuration; not a repeating tick.
    assertEquals(1, scheduler.scheduled.size(), "trip schedules exactly one probe task");
    assertEquals(300L, scheduler.delaysMs.getLast(), "probe delay equals the configured openDuration");
    assertNotNull(scheduler.scheduled.getLast());

    // While OPEN, in-flight outcomes are ignored — only the probe timer leaves OPEN.
    hc.recordOutcome(true);
    hc.recordOutcome(false);
    assertSame(CircuitBreakerState.OPEN, hc.circuitBreakerState(), "outcomes while OPEN must not move the state");
    assertEquals(1, scheduler.scheduled.size(), "no extra probe armed by OPEN-state outcomes");

    // Firing the single probe task transitions OPEN → HALF_OPEN and resumes exactly once.
    final var resumesBefore = hook.resumes.get();
    scheduler.fireLatest();
    assertSame(CircuitBreakerState.HALF_OPEN, hc.circuitBreakerState());
    assertEquals(resumesBefore + 1, hook.resumes.get(), "probe fire resumes the consumer once");
    assertEquals(1, hook.timeOpenReports.get(), "probe reports the open duration exactly once");

    // Re-firing the same one-shot task is a clean no-op (lost CAS): no second HALF_OPEN entry, no
    // second resume — the breaker is no longer OPEN.
    scheduler.fireLatest();
    assertSame(CircuitBreakerState.HALF_OPEN, hc.circuitBreakerState(), "re-firing the one-shot probe is a no-op");
    assertEquals(resumesBefore + 1, hook.resumes.get(), "one-shot probe must not resume twice");
  }

  @Test
  void disabledBreakerIgnoresOutcomes() {
    final var hook = new RecordingHook();
    final var scheduler = new CapturingScheduler();
    // No CircuitBreakerController → breaker disabled. recordOutcome must be a clean no-op.
    final var hc = new ConsumerHealthController(null, null, scheduler, hook);

    for (int i = 0; i < 50; i++) hc.recordOutcome(false);

    assertSame(CircuitBreakerState.CLOSED, hc.circuitBreakerState(), "disabled breaker stays CLOSED");
    assertEquals(0, hook.trips.get());
    assertTrue(scheduler.scheduled.isEmpty(), "disabled breaker never schedules a probe");
  }

  @Test
  void fullCycleClosedOpenHalfOpenClosedRecordsStateSequence() {
    final var hook = new RecordingHook();
    final var scheduler = new CapturingScheduler();
    final var hc = newController(hook, scheduler);

    tripToOpen(hc); // CLOSED → OPEN
    scheduler.fireLatest(); // OPEN → HALF_OPEN
    hc.recordOutcome(true); // HALF_OPEN → CLOSED

    assertEquals(
      List.of(CircuitBreakerState.OPEN, CircuitBreakerState.HALF_OPEN, CircuitBreakerState.CLOSED),
      hook.stateChanges,
      "the recovery cycle must emit OPEN, HALF_OPEN, CLOSED in order"
    );
    // shutdown() cancels the probe future; after a successful close the future was nulled, so
    // shutdown
    // is a clean no-op rather than cancelling a stale handle.
    final var cancelsBefore = scheduler.cancelCount;
    hc.shutdown();
    assertEquals(cancelsBefore, scheduler.cancelCount, "no pending probe to cancel after a clean close");
  }
}
