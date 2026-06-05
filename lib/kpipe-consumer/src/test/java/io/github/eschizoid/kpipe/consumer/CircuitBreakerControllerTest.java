package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/// Contract tests for [CircuitBreakerController]. Verifies the validation invariants and the two
/// pure-function predicates (`shouldTrip`, `shouldProbe`) that drive [KPipeConsumer]'s state
/// transitions.
class CircuitBreakerControllerTest {

  @Test
  void validControllerConstructs() {
    final var c = new CircuitBreakerController(0.5, 100, Duration.ofSeconds(30));
    assertEquals(0.5, c.failureThreshold());
    assertEquals(100, c.windowSize());
    assertEquals(Duration.ofSeconds(30), c.openDuration());
  }

  @Test
  void invalidThresholdRejected() {
    assertThrows(IllegalArgumentException.class, () -> new CircuitBreakerController(0.0, 100, Duration.ofSeconds(1)));
    assertThrows(IllegalArgumentException.class, () -> new CircuitBreakerController(-0.1, 100, Duration.ofSeconds(1)));
    assertThrows(IllegalArgumentException.class, () -> new CircuitBreakerController(1.1, 100, Duration.ofSeconds(1)));
  }

  @Test
  void invalidWindowSizeRejected() {
    assertThrows(IllegalArgumentException.class, () -> new CircuitBreakerController(0.5, 0, Duration.ofSeconds(1)));
    assertThrows(IllegalArgumentException.class, () -> new CircuitBreakerController(0.5, -10, Duration.ofSeconds(1)));
  }

  @Test
  void invalidOpenDurationRejected() {
    assertThrows(NullPointerException.class, () -> new CircuitBreakerController(0.5, 100, null));
    assertThrows(IllegalArgumentException.class, () -> new CircuitBreakerController(0.5, 100, Duration.ZERO));
    assertThrows(IllegalArgumentException.class, () -> new CircuitBreakerController(0.5, 100, Duration.ofSeconds(-1)));
  }

  @Test
  void shouldTripRequiresFullWindow() {
    final var controller = new CircuitBreakerController(0.5, 10, Duration.ofSeconds(30));
    // 9 failures out of 9 — 100% rate, but the window isn't full yet.
    assertFalse(controller.shouldTrip(9, 1.0), "should not trip until window has windowSize samples");
    assertTrue(controller.shouldTrip(10, 1.0), "should trip once the full window is over threshold");
  }

  @Test
  void shouldTripExactlyAtThreshold() {
    final var controller = new CircuitBreakerController(0.5, 10, Duration.ofSeconds(30));
    assertTrue(controller.shouldTrip(10, 0.5), "rate exactly at threshold should trip (>= comparison)");
  }

  @Test
  void shouldTripFalseBelowThreshold() {
    final var controller = new CircuitBreakerController(0.5, 10, Duration.ofSeconds(30));
    assertFalse(controller.shouldTrip(10, 0.4), "40% failure rate should not trip a 50% threshold");
  }

  @Test
  void shouldProbeFalseUntilDurationElapsed() {
    final var controller = new CircuitBreakerController(0.5, 100, Duration.ofMillis(500));
    final var openedAt = System.nanoTime();

    assertFalse(controller.shouldProbe(openedAt, openedAt + 1_000_000L), "1ms after open shouldn't probe");
    assertTrue(
      controller.shouldProbe(openedAt, openedAt + Duration.ofMillis(500).toNanos()),
      "exactly at duration should probe"
    );
    assertTrue(controller.shouldProbe(openedAt, openedAt + Duration.ofSeconds(1).toNanos()), "after duration: probe");
  }

  @Test
  void shouldProbeFalseForSentinelOpenedAt() {
    final var controller = new CircuitBreakerController(0.5, 100, Duration.ofSeconds(1));
    assertFalse(controller.shouldProbe(0L, System.nanoTime()), "never-tripped sentinel must return false");
  }
}
