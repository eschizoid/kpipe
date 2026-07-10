package io.github.eschizoid.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;

/// Host-level health controller. Composes pause arbitration, backpressure watermarks, and
/// circuit-breaker orchestration behind a single façade.
///
/// [BackpressureController] and [CircuitBreakerController] remain the user-facing decision
/// modules — stateless records with pure boolean methods, easy to unit-test in isolation. This
/// controller is package-private; callers configure behaviour through the builder.
///
/// Owns the pause bitmask, the CB state machine (`AtomicReference<CircuitBreakerState>`), the
/// trip timestamp, the probe-timer `ScheduledFuture`, and the rolling success/failure window.
/// Side effects (`kafkaConsumer.pause`, `LockSupport.unpark`, command-queue inserts) stay on
/// [KPipeConsumer]; transitions are signalled through the [Hook] supplied at construction so the
/// host owns the single site that drives the command queue.
///
/// Surface used by [KPipeConsumer]:
///
///   * [#requestPause] / [#releasePause] for MANUAL pause; returns the "you caused the transition"
///     boolean so the caller can decide whether to call `internalPause`.
///   * [#recordOutcome] for per-record success/failure; drives the CB state machine and fires the
///     pause callbacks itself when CB transitions cross.
///   * [#tickBackpressure] runs after each record (or each poll, sequential mode); drives the BP
///     bitmask and fires the pause callbacks.
///   * [#shutdown] cancels any pending probe timer on consumer close.
final class ConsumerHealthController {

  private static final Logger LOGGER = System.getLogger(ConsumerHealthController.class.getName());

  /// Pause sources arbitrated by the internal bitmask. The consumer is paused iff at least one
  /// source holds it; resume fires only on the *last* release.
  enum Source {
    MANUAL(1),
    BACKPRESSURE(2),
    CIRCUIT_BREAKER(4);

    final int bit;

    Source(final int bit) {
      this.bit = bit;
    }
  }

  /// Side-effect surface the controller delegates to. KPipeConsumer wires this to its
  /// `internalPause` / `internalResume` choreography and to the AtomicLong metrics map plus the
  /// OTel `ConsumerMetrics`. Kept narrow so unit tests can substitute a recording fake.
  interface Hook {
    /// Invoked when an arbitrary source transitions the consumer into the paused state.
    void onPause();

    /// Invoked when the *last* held source releases the pause and the consumer resumes.
    void onResume();

    /// Invoked when backpressure first crosses the high watermark and requests a pause.
    void onBackpressurePause();

    /// Invoked when backpressure releases its hold, with the elapsed paused duration.
    ///
    /// @param ms milliseconds the backpressure pause was held (always `>= 1`)
    void onBackpressureTimeMs(long ms);

    /// Invoked on CLOSED → OPEN and HALF_OPEN → OPEN circuit-breaker transitions.
    void onCircuitBreakerTrip();

    /// Invoked on every circuit-breaker state transition.
    ///
    /// @param state the new state
    void onCircuitBreakerStateChange(CircuitBreakerState state);

    /// Invoked when the breaker leaves OPEN, with the elapsed duration spent open.
    ///
    /// @param ms milliseconds the breaker stayed in OPEN before probing
    void onCircuitBreakerTimeOpenMs(long ms);
  }

  // ── Pause arbitration ────────────────────────────────────────────────────
  private final AtomicInteger pauseMask = new AtomicInteger(0);

  // ── Backpressure (decision module, nullable = disabled) ──────────────────
  private final BackpressureController backpressure;
  private volatile long backpressurePauseStartNanos;

  // ── Circuit breaker (decision module + state machine, nullable = disabled) ─
  private final CircuitBreakerController cbController;
  private final SlidingWindow cbWindow;
  private final AtomicReference<CircuitBreakerState> cbState = new AtomicReference<>(CircuitBreakerState.CLOSED);
  private final AtomicLong cbOpenedAtNanos = new AtomicLong(0L);
  private volatile ScheduledFuture<?> cbProbeFuture;
  private final ScheduledExecutorService scheduler;

  private final Hook hook;

  /// Constructs the controller. `scheduler` is only required when `cbController` is non-null —
  /// it is used to schedule the OPEN → HALF_OPEN probe timer.
  ///
  /// @param backpressure the backpressure decision module, or `null` to disable backpressure
  /// @param cbController the circuit-breaker decision module, or `null` to disable the breaker
  /// @param scheduler    a scheduled executor used to fire the OPEN → HALF_OPEN probe; required
  ///                     iff `cbController` is non-null
  /// @param hook         side-effect callbacks (pause/resume + metric counters); must be non-null
  /// @throws IllegalArgumentException if `cbController` is non-null and `scheduler` is null, or
  ///         if `hook` is null
  ConsumerHealthController(
    final BackpressureController backpressure,
    final CircuitBreakerController cbController,
    final ScheduledExecutorService scheduler,
    final Hook hook
  ) {
    this.backpressure = backpressure;
    this.cbController = cbController;
    this.cbWindow = cbController != null ? new SlidingWindow(cbController.windowSize()) : null;
    this.scheduler = scheduler;
    this.hook = hook;
    if (cbController != null && scheduler == null) {
      throw new IllegalArgumentException("scheduler is required when a CircuitBreakerController is configured");
    }
    if (hook == null) throw new IllegalArgumentException("hook cannot be null");
  }

  // ─────────────────────────── Pause API ────────────────────────────────────

  /// Adds `source` to the held set.
  ///
  /// @param source the pause source acquiring the hold
  /// @return `true` iff this call caused the transition from "no sources holding" to "at least
  ///     one source holding" — the caller should now invoke `internalPause()`. Idempotent.
  boolean requestPause(final Source source) {
    return pauseMask.getAndUpdate(m -> m | source.bit) == 0;
  }

  /// Removes `source` from the held set.
  ///
  /// @param source the pause source releasing its hold
  /// @return `true` iff this call caused the transition from "at least one source holding" to
  ///     "no sources holding" — the caller should now invoke `internalResume()`. Idempotent.
  boolean releasePause(final Source source) {
    final var prev = pauseMask.getAndUpdate(m -> m & ~source.bit);
    return prev != 0 && (prev & ~source.bit) == 0;
  }

  boolean isPaused() {
    return pauseMask.get() != 0;
  }

  boolean isHeldBy(final Source source) {
    return (pauseMask.get() & source.bit) != 0;
  }

  /// Snapshot of which sources currently hold the pause. Allocates — use only for logging.
  Set<Source> currentSources() {
    final var m = pauseMask.get();
    final var set = EnumSet.noneOf(Source.class);
    for (final var s : Source.values()) if ((m & s.bit) != 0) set.add(s);
    return set;
  }

  // ─────────────────────────── Backpressure API ─────────────────────────────

  /// Runs the backpressure decision against the current metric and dispatches the resulting action.
  /// On PAUSE this records the pause-start timestamp, fires the trip + onPause callbacks. On RESUME
  /// this computes the pause duration, fires the duration + onResume callbacks.
  ///
  /// @param kafkaConsumer the Kafka consumer whose metric is read by the configured strategy
  void tickBackpressure(final Consumer<?, ?> kafkaConsumer) {
    if (backpressure == null) return;
    switch (backpressure.check(kafkaConsumer, isPaused())) {
      case PAUSE -> {
        final long value = backpressure.getMetric(kafkaConsumer);
        LOGGER.log(
          Level.WARNING,
          "Backpressure triggered: pausing consumer ({0}={1})",
          backpressure.getMetricName(),
          value
        );
        hook.onBackpressurePause();
        backpressurePauseStartNanos = System.nanoTime();
        if (requestPause(Source.BACKPRESSURE)) hook.onPause();
        // Lost-wakeup guard. The PAUSE decision above came from a metric read taken BEFORE
        // `requestPause` published the BACKPRESSURE bit, but record completions only unpark the
        // consumer thread when they observe that bit after decrementing the in-flight count.
        // Every record that completed inside that window skipped the unpark — and when the
        // count drains all the way down in there (routine for PARALLEL mode with fast records:
        // 10k in-flight at ~100µs each finish in a few milliseconds, faster than the log + hook
        // calls above), no completion ever arrives to wake the consumer, which then parks
        // forever with nothing in flight. Re-checking with the bit visible closes the window:
        // either the metric is already at/below the low watermark (release right now), or at
        // least one record was still in flight at this read and its completion is guaranteed to
        // see the bit and unpark. Both reads and the bit are volatile, so the total order of
        // the flag/count accesses makes this Dekker-style handshake sound
        // (BackpressureHandshakeJCStressTest exercises the pair).
        //
        // Gated to the in-flight strategy: the lag metric only moves when the consumer thread
        // itself advances positions, so it cannot drop concurrently inside this window (there
        // is no race to close), and re-checking it would issue a second `endOffsets` broker
        // round-trip on every pause edge.
        if (
          BackpressureController.IN_FLIGHT_METRIC_NAME.equals(backpressure.getMetricName()) &&
          backpressure.check(kafkaConsumer, true) == BackpressureController.Action.RESUME
        ) {
          releaseBackpressure();
        }
      }
      case RESUME -> {
        // Only release when backpressure actually holds the pause. `check` answers RESUME for
        // "paused and metric at/below the low watermark" regardless of WHICH source paused the
        // consumer — during a MANUAL or circuit-breaker pause with an idle pipeline this arm
        // would otherwise fire on every tick, feeding a garbage duration (nanoTime origin minus
        // a never-set pause start) into the backpressure-time counter and logging a misleading
        // "backpressure resolved" line each time.
        if (isHeldBy(Source.BACKPRESSURE)) releaseBackpressure();
      }
      case NONE -> {
      }
    }
  }

  /// Releases the backpressure hold: fires the paused-duration callback and — when backpressure
  /// was the last holder — the resume callback. Shared by the regular RESUME tick and the
  /// post-pause lost-wakeup guard above.
  private void releaseBackpressure() {
    final long duration = Math.max(1, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - backpressurePauseStartNanos));
    hook.onBackpressureTimeMs(duration);
    if (releasePause(Source.BACKPRESSURE)) {
      LOGGER.log(Level.INFO, "Backpressure resolved: resuming consumer (paused for {0} ms)", duration);
      hook.onResume();
    } else {
      LOGGER.log(
        Level.INFO,
        "Backpressure resolved (paused for {0} ms), other sources still hold the pause: {1}",
        duration,
        currentSources()
      );
    }
  }

  String backpressureMetricName() {
    return backpressure != null ? backpressure.getMetricName() : "none";
  }

  boolean backpressureEnabled() {
    return backpressure != null;
  }

  // ─────────────────────────── Circuit breaker API ──────────────────────────

  /// Records a per-record outcome and drives the CB state machine:
  ///
  ///   * `CLOSED + failure` (threshold crossed) → `OPEN` (CAS, pause, schedule probe)
  ///   * `HALF_OPEN + success` → `CLOSED` (CAS, window reset)
  ///   * `HALF_OPEN + failure` → `OPEN` (CAS, pause, restart probe)
  ///
  /// Outcomes that arrive while `OPEN` (in-flight remnants from before the trip) are ignored —
  /// only the probe timer leaves `OPEN`, and feeding them into stats would skew the window the
  /// breaker uses to re-evaluate later.
  ///
  /// @param success `true` for a successful record, `false` for a failure
  void recordOutcome(final boolean success) {
    if (cbController == null) return;
    final var current = cbState.get();
    if (current == CircuitBreakerState.OPEN) return;
    cbWindow.record(success);

    if (current == CircuitBreakerState.CLOSED) {
      if (!success && cbController.shouldTrip(cbWindow.totalSamples(), cbWindow.failureRate())) {
        if (cbState.compareAndSet(CircuitBreakerState.CLOSED, CircuitBreakerState.OPEN)) trip();
      }
    } else {
      // HALF_OPEN — the only remaining state after the OPEN early-return.
      final var target = success ? CircuitBreakerState.CLOSED : CircuitBreakerState.OPEN;
      if (cbState.compareAndSet(CircuitBreakerState.HALF_OPEN, target)) {
        if (success) close();
        else trip();
      }
    }
  }

  CircuitBreakerState circuitBreakerState() {
    return cbState.get();
  }

  /// Cancels the in-flight probe timer if any. Called from KPipeConsumer.close() so a pending
  /// probe doesn't fire after the consumer thread has exited.
  void shutdown() {
    final var f = cbProbeFuture;
    if (f != null) f.cancel(false);
  }

  // ─────────────────────────── Internal CB choreography ─────────────────────

  /// Transitions the breaker into OPEN: stamp the trip timestamp, fire the trip + pause callbacks,
  /// schedule the probe timer. Called on CLOSED → OPEN and HALF_OPEN → OPEN transitions.
  private void trip() {
    cbOpenedAtNanos.set(System.nanoTime());
    LOGGER.log(
      Level.WARNING,
      "Circuit breaker tripped: pausing consumer (failureRate={0}, openDuration={1})",
      cbWindow.failureRate(),
      cbController.openDuration()
    );
    hook.onCircuitBreakerTrip();
    hook.onCircuitBreakerStateChange(CircuitBreakerState.OPEN);
    if (requestPause(Source.CIRCUIT_BREAKER)) hook.onPause();
    final var existing = cbProbeFuture;
    if (existing != null) existing.cancel(false);
    cbProbeFuture = scheduler.schedule(
      this::tryHalfOpen,
      cbController.openDuration().toMillis(),
      TimeUnit.MILLISECONDS
    );
  }

  /// One-shot probe-timer callback. CAS OPEN → HALF_OPEN and fire the resume callback; the next
  /// record's outcome drives the final transition. Lost CAS = already moved on (manual resume /
  /// shutdown), no-op.
  private void tryHalfOpen() {
    if (!cbState.compareAndSet(CircuitBreakerState.OPEN, CircuitBreakerState.HALF_OPEN)) return;
    // The probe future fired (this method IS the firing callback). Null the field so a subsequent
    // `trip` cancel doesn't hold a stale completed handle.
    cbProbeFuture = null;
    final var openedAt = cbOpenedAtNanos.get();
    final var openMs = openedAt == 0L ? 0L : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - openedAt);
    LOGGER.log(Level.INFO, "Circuit breaker probing: open for {0} ms, resuming consumer", openMs);
    hook.onCircuitBreakerTimeOpenMs(openMs);
    hook.onCircuitBreakerStateChange(CircuitBreakerState.HALF_OPEN);
    if (releasePause(Source.CIRCUIT_BREAKER)) hook.onResume();
  }

  /// Transitions the breaker back to CLOSED after a successful probe. Resets stats so the next trip
  /// cycle starts with a fresh window.
  private void close() {
    LOGGER.log(Level.INFO, "Circuit breaker closed: probe succeeded, resuming normal operation");
    cbWindow.reset();
    cbOpenedAtNanos.set(0L);
    hook.onCircuitBreakerStateChange(CircuitBreakerState.CLOSED);
  }

  // ─────────────────────────── Sliding window ───────────────────────────────

  /// Count-based sliding window of success/failure outcomes.
  ///
  /// Each `record` claims a unique slot via `head.getAndIncrement`, then atomically swaps the
  /// slot. Counters are kept in sync by decrementing the evicted outcome and incrementing the new
  /// one, so `failureRate()` is `O(1)` regardless of window size.
  // Package-private (not private) so the jcstress harness can drive the real window directly
  // rather than a replica — same approach as the other package-private internals under test.
  static final class SlidingWindow {

    private static final long EMPTY = 0L;
    private static final long SUCCESS = 1L;
    private static final long FAILURE = 2L;

    private final AtomicLongArray slots;
    private final AtomicLong head = new AtomicLong(0);
    private final AtomicLong successes = new AtomicLong(0);
    private final AtomicLong failures = new AtomicLong(0);

    SlidingWindow(final int windowSize) {
      if (windowSize <= 0) throw new IllegalArgumentException("windowSize must be positive, got " + windowSize);
      this.slots = new AtomicLongArray(windowSize);
    }

    void record(final boolean success) {
      final long outcome = success ? SUCCESS : FAILURE;
      final var idx = (int) (head.getAndIncrement() % slots.length());
      final var prev = slots.getAndSet(idx, outcome);
      if (prev == SUCCESS) successes.decrementAndGet();
      else if (prev == FAILURE) failures.decrementAndGet();
      if (outcome == SUCCESS) successes.incrementAndGet();
      else failures.incrementAndGet();
    }

    long totalSamples() {
      return successes.get() + failures.get();
    }

    double failureRate() {
      // Snapshot both counters once — re-reading `failures` here after `totalSamples()` could
      // produce a ratio > 1.0 when a failure arrived between the two reads.
      final var f = failures.get();
      final var s = successes.get();
      final var total = f + s;
      return total == 0 ? 0.0 : (double) f / total;
    }

    void reset() {
      for (int i = 0; i < slots.length(); i++) slots.set(i, EMPTY);
      head.set(0);
      successes.set(0);
      failures.set(0);
    }
  }
}
