package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.kpipe.consumer.BackpressureController.Action;

class BackpressureControllerTest {

  @Test
  void shouldRejectInvalidWatermarks() {
    assertThrows(IllegalArgumentException.class, () -> new BackpressureController(0, 0));
    assertThrows(IllegalArgumentException.class, () -> new BackpressureController(1000, -1));
    assertThrows(IllegalArgumentException.class, () -> new BackpressureController(1000, 1000));
    assertThrows(IllegalArgumentException.class, () -> new BackpressureController(500, 1000));
  }

  @ParameterizedTest(name = "in-flight={0}, paused={1} → {2}")
  @CsvSource(
    {
      // Pause: not paused, in-flight reaches or exceeds high watermark
      "1000, false, PAUSE",
      "1500, false, PAUSE",
      // Resume: paused, in-flight at or below low watermark
      "700,  true,  RESUME",
      "0,    true,  RESUME",
      // Hold: below high watermark when not paused
      "999,  false, NONE",
      // Hold: between watermarks when paused
      "701,  true,  NONE",
      "850,  true,  NONE",
      // Hold: already paused, in-flight still above high watermark (no double-pause)
      "1500, true,  NONE",
    }
  )
  void checkReturnsCorrectAction(final long inFlight, final boolean paused, final Action expected) {
    final var controller = new BackpressureController(1000, 700);
    assertEquals(expected, controller.check(inFlight, paused));
  }
}
