package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// Fray ports of the health-controller races: the backpressure pause handshake and the circuit
/// breaker's rolling window.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class ConsumerHealthFrayTest {

  /// The lost-wakeup handshake between the consumer thread and the last worker to finish.
  ///
  /// The consumer publishes the pause bit and then re-reads the in-flight count; the worker
  /// decrements the in-flight count and then reads the pause bit to decide whether to nudge the
  /// consumer. Three outcomes are fine — either side may observe the other, or both may. The one
  /// forbidden state is neither side seeing the other: the consumer would hold with nothing in
  /// flight and no completion left to release it.
  ///
  /// The handshake stopped being a liveness invariant when the paused consumer moved to
  /// polling on a fixed cadence instead of parking indefinitely, so today this bounds resume
  /// latency rather than preventing a permanent stall. It is still the property the ordering is
  /// written to provide.
  @FrayTest(iterations = 500)
  void pauseHandshakeIsNeverMutuallyBlind() {
    final var health = new ConsumerHealthController(null, null, null, NoopHook.INSTANCE, NoopHook.INSTANCE);
    final var inFlight = new AtomicLong(1);
    final var consumerSawDrain = new AtomicBoolean();
    final var workerSawPause = new AtomicBoolean();

    FrayScenarios.runConcurrently(
      () -> {
        health.requestPause(ConsumerHealthController.Source.BACKPRESSURE);
        consumerSawDrain.set(inFlight.get() == 0);
      },
      () -> {
        inFlight.decrementAndGet();
        workerSawPause.set(health.isHeldBy(ConsumerHealthController.Source.BACKPRESSURE));
      }
    );

    assertFalse(
      !consumerSawDrain.get() && !workerSawPause.get(),
      "neither side observed the other: the consumer holds with nothing in flight and no " +
        "completion left to release it"
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
