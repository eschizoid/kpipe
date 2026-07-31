package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.consumer.BackpressureController.Action;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.LongRange;

/// Property-based coverage for the backpressure watermark hysteresis. Where
/// `WatermarkHysteresisTest` pins the exact watermark edges with hand-picked values, this suite
/// generates randomized load walks — arbitrary in-flight values over arbitrary watermark pairs
/// (low fixed at 70% of high, the shape of the production defaults) — and asserts the decision
/// invariants hold for EVERY generated walk:
///
///   - PAUSE never fires while the metric is below the high watermark;
///   - RESUME never fires while the metric is above the low watermark;
///   - PAUSE and RESUME strictly alternate — no pause-pause or resume-resume without the
///     opposite edge between them, so the band between the watermarks is a true dead zone.
///
/// Each walk drives [BackpressureController#check] through the same paused-flag feedback loop
/// the consumer runs: every decision is applied to the flag that feeds the next check, exactly
/// like `tickBackpressure`.
class BackpressureHysteresisPropertyTest {

  @Provide
  Arbitrary<List<Long>> loadWalks() {
    return Arbitraries.longs().between(0L, 20_000L).list().ofMinSize(1).ofMaxSize(200);
  }

  @Property(tries = 200)
  void pauseOnlyAtOrAboveHighAndResumeOnlyAtOrBelowLow(
    @ForAll("loadWalks") final List<Long> loads,
    @ForAll @LongRange(min = 100, max = 10_000) final long high
  ) {
    final var low = Math.round(high * 0.7);
    final var metric = new AtomicLong();
    final var controller = new BackpressureController(high, low, BackpressureController.inFlightStrategy(metric::get));

    var paused = false;
    for (final var load : loads) {
      metric.set(load);
      switch (controller.check(null, paused)) {
        case PAUSE -> {
          assertTrue(load >= high, "PAUSE at %d below high watermark %d".formatted(load, high));
          assertFalse(paused, "PAUSE must never fire while already paused");
          paused = true;
        }
        case RESUME -> {
          assertTrue(load <= low, "RESUME at %d above low watermark %d".formatted(load, low));
          assertTrue(paused, "RESUME must never fire while running");
          paused = false;
        }
        case NONE -> {
        }
      }
    }
  }

  @Property(tries = 200)
  void pauseAndResumeStrictlyAlternate(
    @ForAll("loadWalks") final List<Long> loads,
    @ForAll @LongRange(min = 100, max = 10_000) final long high
  ) {
    final var low = Math.round(high * 0.7);
    final var metric = new AtomicLong();
    final var controller = new BackpressureController(high, low, BackpressureController.inFlightStrategy(metric::get));

    var paused = false;
    Action lastEdge = null;
    for (final var load : loads) {
      metric.set(load);
      final var action = controller.check(null, paused);
      switch (action) {
        case PAUSE -> {
          assertNotEquals(Action.PAUSE, lastEdge, "pause-pause without a resume between is flapping");
          paused = true;
          lastEdge = action;
        }
        case RESUME -> {
          assertEquals(Action.PAUSE, lastEdge, "the first RESUME edge must follow a PAUSE edge");
          paused = false;
          lastEdge = action;
        }
        case NONE -> {
        }
      }
    }
  }
}
