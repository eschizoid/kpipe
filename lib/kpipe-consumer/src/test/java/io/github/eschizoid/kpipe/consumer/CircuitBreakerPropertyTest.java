package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;

/// Property-based coverage for the circuit-breaker state machine hosted by
/// [ConsumerHealthController]. Where `CircuitBreakerTransitionTest` pins specific transition
/// scenarios example-by-example, this suite generates randomized event sequences — arbitrary
/// success/failure outcomes interleaved with probe-timer firings, over arbitrary window sizes
/// and failure thresholds — and asserts the machine invariants hold for EVERY generated stream:
///
///   - only legal edges are ever taken: CLOSED → OPEN, OPEN → HALF_OPEN,
///     HALF_OPEN → CLOSED, HALF_OPEN → OPEN — never CLOSED → HALF_OPEN or OPEN → CLOSED;
///   - the breaker never trips from CLOSED before `windowSize` outcomes fill a fresh window;
///   - outcomes recorded while OPEN are ignored — state, transition log, and probe count all
///     stay untouched until the probe timer fires.
///
/// No wall clock is involved: the OPEN → HALF_OPEN probe is captured by a hand-fired scheduler
/// (the same seam `CircuitBreakerTransitionTest` uses), so "open-duration elapses" is an
/// explicit generated event, and the ignored-while-OPEN invariant is checked across
/// arbitrarily long outcome runs with the breaker deterministically held OPEN.
class CircuitBreakerPropertyTest {

  /// One generated step: a per-record outcome, or the probe timer elapsing.
  private enum Event {
    SUCCESS,
    FAILURE,
    FIRE_PROBE,
  }

  private static final Set<List<CircuitBreakerState>> LEGAL_EDGES = Set.of(
    List.of(CircuitBreakerState.CLOSED, CircuitBreakerState.OPEN),
    List.of(CircuitBreakerState.OPEN, CircuitBreakerState.HALF_OPEN),
    List.of(CircuitBreakerState.HALF_OPEN, CircuitBreakerState.CLOSED),
    List.of(CircuitBreakerState.HALF_OPEN, CircuitBreakerState.OPEN)
  );

  @Provide
  Arbitrary<List<Event>> eventSequences() {
    return Arbitraries.of(Event.SUCCESS, Event.FAILURE, Event.FIRE_PROBE).list().ofMinSize(1).ofMaxSize(80);
  }

  @Property(tries = 200)
  void onlyLegalEdgesEverObserved(
    @ForAll("eventSequences") final List<Event> events,
    @ForAll @IntRange(min = 2, max = 10) final int windowSize,
    @ForAll @DoubleRange(min = 0.3, max = 0.9) final double threshold
  ) {
    final var observer = new RecordingObserver();
    final var scheduler = new CapturingScheduler();
    final var hc = newController(threshold, windowSize, observer, scheduler);

    for (final var event : events) apply(hc, scheduler, event);

    // Prepend the initial state; the observer log then yields every consecutive edge taken.
    final var states = new ArrayList<CircuitBreakerState>();
    states.add(CircuitBreakerState.CLOSED);
    states.addAll(observer.stateChanges);
    for (var i = 1; i < states.size(); i++) {
      final var edge = List.of(states.get(i - 1), states.get(i));
      assertTrue(LEGAL_EDGES.contains(edge), "illegal transition %s in %s".formatted(edge, states));
    }
  }

  @Property(tries = 200)
  void neverTripsBeforeWindowFillsFromAFreshWindow(
    @ForAll("eventSequences") final List<Event> events,
    @ForAll @IntRange(min = 2, max = 10) final int windowSize,
    @ForAll @DoubleRange(min = 0.3, max = 0.9) final double threshold
  ) {
    final var observer = new RecordingObserver();
    final var scheduler = new CapturingScheduler();
    final var hc = newController(threshold, windowSize, observer, scheduler);

    // Model of the rolling window's fill level: outcomes accepted (state != OPEN) since the
    // last reset. HALF_OPEN → CLOSED resets the window; a trip does not.
    var samplesSinceReset = 0;
    for (final var event : events) {
      final var before = hc.circuitBreakerState();
      if (event != Event.FIRE_PROBE && before != CircuitBreakerState.OPEN) samplesSinceReset++;
      apply(hc, scheduler, event);
      final var after = hc.circuitBreakerState();
      if (before == CircuitBreakerState.CLOSED && after == CircuitBreakerState.OPEN) {
        assertTrue(
          samplesSinceReset >= windowSize,
          "tripped after only %d of %d window samples".formatted(samplesSinceReset, windowSize)
        );
      }
      if (before == CircuitBreakerState.HALF_OPEN && after == CircuitBreakerState.CLOSED) samplesSinceReset = 0;
    }
  }

  @Property(tries = 200)
  void outcomesWhileOpenAreIgnored(
    @ForAll("eventSequences") final List<Event> events,
    @ForAll @IntRange(min = 2, max = 10) final int windowSize,
    @ForAll @DoubleRange(min = 0.3, max = 0.9) final double threshold
  ) {
    final var observer = new RecordingObserver();
    final var scheduler = new CapturingScheduler();
    final var hc = newController(threshold, windowSize, observer, scheduler);

    for (final var event : events) {
      final var before = hc.circuitBreakerState();
      final var transitionsBefore = observer.stateChanges.size();
      final var probesBefore = scheduler.scheduled.size();
      apply(hc, scheduler, event);
      if (event != Event.FIRE_PROBE && before == CircuitBreakerState.OPEN) {
        assertSame(CircuitBreakerState.OPEN, hc.circuitBreakerState(), "an outcome while OPEN must not move the state");
        assertEquals(transitionsBefore, observer.stateChanges.size(), "no transition may fire from an OPEN outcome");
        assertEquals(probesBefore, scheduler.scheduled.size(), "no extra probe may be armed by an OPEN outcome");
      }
    }
  }

  // ─────────────────────────── Drive helpers ────────────────────────────────

  private static ConsumerHealthController newController(
    final double threshold,
    final int windowSize,
    final RecordingObserver observer,
    final CapturingScheduler scheduler
  ) {
    final var cb = new CircuitBreakerController(threshold, windowSize, Duration.ofMillis(300));
    return new ConsumerHealthController(null, cb, scheduler, observer, observer);
  }

  private static void apply(final ConsumerHealthController hc, final CapturingScheduler scheduler, final Event event) {
    switch (event) {
      case SUCCESS -> hc.recordOutcome(true);
      case FAILURE -> hc.recordOutcome(false);
      // Firing an already-fired one-shot is a clean lost-CAS no-op, so no state guard is needed
      // beyond "a probe was armed at some point."
      case FIRE_PROBE -> {
        if (!scheduler.scheduled.isEmpty()) scheduler.fireLatest();
      }
    }
  }

  /// Records pause/resume and every circuit-breaker state change so properties can assert on the
  /// exact transition sequence. Same recording pattern as `CircuitBreakerTransitionTest`.
  private static final class RecordingObserver
    implements ConsumerHealthController.PauseLifecycleHook, ConsumerHealthController.HealthMetricsObserver
  {

    final List<CircuitBreakerState> stateChanges = new CopyOnWriteArrayList<>();

    @Override
    public void onPause() {}

    @Override
    public void onResume() {}

    @Override
    public void onBackpressurePause() {}

    @Override
    public void onBackpressureTimeMs(final long ms) {}

    @Override
    public void onCircuitBreakerTrip() {}

    @Override
    public void onCircuitBreakerStateChange(final CircuitBreakerState state) {
      stateChanges.add(state);
    }

    @Override
    public void onCircuitBreakerTimeOpenMs(final long ms) {}
  }

  /// A scheduler that never runs anything on its own: each submitted probe task is captured so
  /// the property fires it by hand, making "open-duration elapsed" a deterministic event.
  private static final class CapturingScheduler implements ScheduledExecutorService {

    final List<Runnable> scheduled = new ArrayList<>();

    void fireLatest() {
      scheduled.getLast().run();
    }

    @Override
    public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
      scheduled.add(command);
      return new NoopFuture();
    }

    private static final class NoopFuture implements ScheduledFuture<Object> {

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
}
