package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// Health-controller races: the pause mask and the circuit
/// breaker's rolling window.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class ConsumerHealthFrayTest {

  /// Two subsystems request a pause at once, each owning a different bit of the mask.
  ///
  /// The mask is one `AtomicInteger` mutated by read-modify-write, and `requestPause` returns
  /// whether the caller was the one that moved the consumer from running to paused. Two things
  /// must hold under every interleaving: both bits survive, and exactly one caller claims the
  /// transition. A non-atomic get-then-set drops whichever bit lost the race, so the consumer
  /// resumes while a subsystem still believes it is holding the pause; two callers both claiming
  /// the transition would fire the pause hook twice.
  ///
  /// The mutually-blind outcome of the pause handshake is not testable here and is deliberately
  /// not attempted: reaching it needs both reads to precede both writes, which is a cycle in any
  /// total order, so a scheduling explorer cannot produce it. The mask race is the property on
  /// this class that a scheduler *can* break, because the atomic is a scheduling point.
  @FrayTest(iterations = 500)
  void concurrentPauseSourcesBothLandAndExactlyOneOwnsTheTransition() {
    final var health = new ConsumerHealthController(null, null, null, NoopHook.INSTANCE, NoopHook.INSTANCE);
    final var manualClaimed = new AtomicBoolean();
    final var backpressureClaimed = new AtomicBoolean();

    FrayScenarios.runConcurrently(
      () -> manualClaimed.set(health.requestPause(ConsumerHealthController.Source.MANUAL)),
      () -> backpressureClaimed.set(health.requestPause(ConsumerHealthController.Source.BACKPRESSURE))
    );

    assertTrue(health.isHeldBy(ConsumerHealthController.Source.MANUAL), "the MANUAL pause bit was lost");
    assertTrue(
      health.isHeldBy(ConsumerHealthController.Source.BACKPRESSURE),
      "the BACKPRESSURE pause bit was lost"
    );
    assertTrue(
      manualClaimed.get() ^ backpressureClaimed.get(),
      () ->
        "exactly one caller must observe the running-to-paused transition, but manual="
          + manualClaimed.get()
          + " and backpressure="
          + backpressureClaimed.get()
    );
  }

  /// A success and a failure land in the breaker's rolling window at once. Both must be counted:
  /// the window is what the trip decision reads, so a lost sample moves the failure rate and can
  /// either trip the breaker early or fail to trip it when the threshold was genuinely crossed.
  @FrayTest(iterations = 500)
  void concurrentOutcomesBothLandInTheWindow() {
    final var window = new ConsumerHealthController.SlidingWindow(2);

    FrayScenarios.runConcurrently(() -> window.record(true), () -> window.record(false));

    assertEquals(2L, window.totalSamples(), "both outcomes should have been recorded");
    assertEquals(0.5d, window.failureRate(), 0.0d, "one of the two samples was a failure");
  }

  /// Side-effect-free hook: these scenarios exercise the pause mask and the sample window, never
  /// the pause / resume choreography, so every callback does nothing.
  private static final class NoopHook
    implements ConsumerHealthController.PauseLifecycleHook, ConsumerHealthController.HealthMetricsObserver
  {

    private static final NoopHook INSTANCE = new NoopHook();

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
    public void onCircuitBreakerStateChange(final CircuitBreakerState state) {}

    @Override
    public void onCircuitBreakerTimeOpenMs(final long ms) {}
  }
}
