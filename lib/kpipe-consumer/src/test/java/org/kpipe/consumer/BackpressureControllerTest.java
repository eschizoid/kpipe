package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.kpipe.consumer.BackpressureController.Action;
import org.mockito.Mockito;

class BackpressureControllerTest {

  private static final BackpressureController.Strategy DUMMY_STRATEGY = new BackpressureController.Strategy() {
    @Override
    public long getMetric(Consumer<?, ?> consumer) {
      return 0;
    }

    @Override
    public String getName() {
      return "dummy";
    }
  };

  @Test
  void shouldRejectInvalidWatermarks() {
    assertThrows(IllegalArgumentException.class, () -> new BackpressureController(0, 0, DUMMY_STRATEGY));
    assertThrows(IllegalArgumentException.class, () -> new BackpressureController(1000, -1, DUMMY_STRATEGY));
    assertThrows(IllegalArgumentException.class, () -> new BackpressureController(1000, 1000, DUMMY_STRATEGY));
    assertThrows(IllegalArgumentException.class, () -> new BackpressureController(500, 1000, DUMMY_STRATEGY));
  }

  @ParameterizedTest(name = "value={0}, paused={1} → {2}")
  @CsvSource(
    {
      // Pause: not paused, value reaches or exceeds high watermark
      "1000, false, PAUSE",
      "1500, false, PAUSE",
      // Resume: paused, value at or below low watermark
      "700,  true,  RESUME",
      "0,    true,  RESUME",
      // Hold: below high watermark when not paused
      "999,  false, NONE",
      // Hold: between watermarks when paused
      "701,  true,  NONE",
      "850,  true,  NONE",
      // Hold: already paused, value still above high watermark (no double-pause)
      "1500, true,  NONE",
    }
  )
  void checkReturnsCorrectAction(final long metricValue, final boolean paused, final Action expected) {
    final var strategy = Mockito.mock(BackpressureController.Strategy.class);
    final var consumer = Mockito.mock(Consumer.class);
    when(strategy.getMetric(consumer)).thenReturn(metricValue);

    final var controller = new BackpressureController(1000, 700, strategy);
    assertEquals(expected, controller.check(consumer, paused));
  }

  @Test
  void calculateTotalLagShouldReturnZeroWhenNoAssignment() {
    final var consumer = Mockito.mock(Consumer.class);
    when(consumer.assignment()).thenReturn(Collections.emptySet());

    assertEquals(0, BackpressureController.calculateTotalLag(consumer));
  }

  @Test
  void calculateTotalLagShouldReturnCorrectLag() {
    final var consumer = Mockito.mock(Consumer.class);
    final var tp1 = new TopicPartition("test", 0);
    final var tp2 = new TopicPartition("test", 1);
    final var assignment = Set.of(tp1, tp2);

    when(consumer.assignment()).thenReturn(assignment);
    when(consumer.endOffsets(assignment)).thenReturn(Map.of(tp1, 100L, tp2, 200L));
    when(consumer.position(tp1)).thenReturn(90L); // lag 10
    when(consumer.position(tp2)).thenReturn(150L); // lag 50

    assertEquals(60, BackpressureController.calculateTotalLag(consumer));
  }

  @Test
  void calculateTotalLagShouldHandleErrors() {
    final var consumer = Mockito.mock(Consumer.class);
    when(consumer.assignment()).thenThrow(new RuntimeException("Kafka error"));

    assertEquals(0, BackpressureController.calculateTotalLag(consumer));
  }

  @Test
  void lagStrategyShouldReturnCorrectNameAndMetric() {
    final var strategy = BackpressureController.lagStrategy();
    assertEquals("lag", strategy.getName());

    final var consumer = Mockito.mock(Consumer.class);
    final var tp = new TopicPartition("test", 0);
    final var assignment = Set.of(tp);

    when(consumer.assignment()).thenReturn(assignment);
    when(consumer.endOffsets(assignment)).thenReturn(Map.of(tp, 100L));
    when(consumer.position(tp)).thenReturn(80L);

    assertEquals(20, strategy.getMetric(consumer));
  }

  @Test
  void inFlightStrategyShouldReturnCorrectNameAndMetric() {
    final var inFlightValue = new java.util.concurrent.atomic.AtomicLong(42);
    final var strategy = BackpressureController.inFlightStrategy(inFlightValue::get);
    assertEquals("in-flight", strategy.getName());

    assertEquals(42, strategy.getMetric(null));
    inFlightValue.set(100);
    assertEquals(100, strategy.getMetric(null));
  }

  @Test
  void inFlightStrategyShouldRejectNullSupplier() {
    assertThrows(IllegalArgumentException.class, () -> BackpressureController.inFlightStrategy(null));
  }
}
