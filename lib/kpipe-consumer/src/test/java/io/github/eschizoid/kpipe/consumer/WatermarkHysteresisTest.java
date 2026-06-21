package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import io.github.eschizoid.kpipe.consumer.BackpressureController.Action;
import io.github.eschizoid.kpipe.consumer.BackpressureController.Strategy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/// Deterministic coverage for the backpressure watermark state machine.
///
/// `BackpressureController.check(...)` is a pure threshold function:
///
/// ```java
/// if (!paused && value >= high) return PAUSE;
/// if (paused  && value <= low)  return RESUME;
/// return NONE;
/// ```
///
/// The interesting correctness properties live at the watermark *edges* and in the *band*
/// between them — the hysteresis gap that exists to stop pause/resume thrashing. This suite
/// pins those edges with exact-value assertions (no randomness): pause fires at exactly the
/// high watermark and not one below it, resume fires at exactly the low watermark and not one
/// above it, and a value oscillating inside the band never flips the decision. Both the
/// in-flight (PARALLEL) and lag (SEQUENTIAL) strategies are exercised through the same
/// decision path, and the in-flight reading is verified to include buffered batch records the
/// way the consumer's `totalInFlight()` composes the metric.
///
/// Watermarks of `100` (high) / `50` (low) are used throughout so the boundary indices read
/// cleanly.
class WatermarkHysteresisTest {

  private static final long HIGH = 100L;
  private static final long LOW = 50L;

  /// Builds a controller whose metric is whatever `value` is supplied, so each test drives the
  /// decision purely off the watermark math.
  private static BackpressureController controllerReturning(final long value) {
    final var strategy = Mockito.mock(Strategy.class);
    when(strategy.getMetric(Mockito.any())).thenReturn(value);
    return new BackpressureController(HIGH, LOW, strategy);
  }

  @Nested
  class PauseEdge {

    /// PAUSE must fire at exactly the high watermark — `>=`, not strictly greater.
    @Test
    void pauseFiresAtExactlyHighWatermark() {
      final var controller = controllerReturning(HIGH);
      assertEquals(Action.PAUSE, controller.check(null, false));
    }

    /// One below the high watermark must hold (NONE), proving pause does not fire early.
    @Test
    void pauseDoesNotFireOneBelowHighWatermark() {
      final var controller = controllerReturning(HIGH - 1);
      assertEquals(Action.NONE, controller.check(null, false));
    }

    /// Above the high watermark still pauses (the `>=` upper region).
    @Test
    void pauseFiresAboveHighWatermark() {
      final var controller = controllerReturning(HIGH + 1);
      assertEquals(Action.PAUSE, controller.check(null, false));
    }

    /// Already paused and above high is a hold, not a redundant second PAUSE.
    @Test
    void noDoublePauseWhenAlreadyPausedAboveHigh() {
      final var controller = controllerReturning(HIGH + 1000);
      assertEquals(Action.NONE, controller.check(null, true));
    }
  }

  @Nested
  class ResumeEdge {

    /// RESUME must fire at exactly the low watermark — `<=`, not strictly less.
    @Test
    void resumeFiresAtExactlyLowWatermark() {
      final var controller = controllerReturning(LOW);
      assertEquals(Action.RESUME, controller.check(null, true));
    }

    /// One above the low watermark must hold (NONE) while paused, proving resume does not
    /// fire early — this is the lower lip of the hysteresis band.
    @Test
    void resumeDoesNotFireOneAboveLowWatermark() {
      final var controller = controllerReturning(LOW + 1);
      assertEquals(Action.NONE, controller.check(null, true));
    }

    /// Below the low watermark still resumes (the `<=` lower region).
    @Test
    void resumeFiresBelowLowWatermark() {
      final var controller = controllerReturning(LOW - 1);
      assertEquals(Action.RESUME, controller.check(null, true));
    }

    /// A value in the band while NOT paused is a hold — resume only applies once paused.
    @Test
    void noResumeWhenNotPausedInBand() {
      final var controller = controllerReturning((HIGH + LOW) / 2);
      assertEquals(Action.NONE, controller.check(null, false));
    }
  }

  @Nested
  class NoThrashInBand {

    /// Once paused at the high watermark, an in-flight count that drains down through the band
    /// but never reaches the low watermark must hold PAUSE the entire way — a single resume
    /// would defeat the hysteresis floor and cause flapping. We model the metric with a live
    /// counter and walk it down `high → low+1` step by step, asserting every step is NONE.
    @Test
    void drainingThroughBandWithoutReachingLowNeverResumes() {
      final var inFlight = new AtomicLong(HIGH);
      final var controller = new BackpressureController(
        HIGH, LOW, BackpressureController.inFlightStrategy(inFlight::get));

      // Enter the paused state at the high watermark.
      assertEquals(Action.PAUSE, controller.check(null, false));

      // Drain down to one above the low watermark; paused stays held the whole time.
      for (var value = HIGH; value > LOW; value--) {
        inFlight.set(value);
        assertEquals(
          Action.NONE,
          controller.check(null, true),
          () -> "Expected hold while paused at value above low watermark");
      }

      // Only when it finally touches the low watermark does RESUME fire.
      inFlight.set(LOW);
      assertEquals(Action.RESUME, controller.check(null, true));
    }

    /// The mirror case: while running, a value that climbs through the band but never reaches
    /// the high watermark must never pause. Walk `low → high-1` and assert every step holds.
    @Test
    void climbingThroughBandWithoutReachingHighNeverPauses() {
      final var inFlight = new AtomicLong(LOW);
      final var controller = new BackpressureController(
        HIGH, LOW, BackpressureController.inFlightStrategy(inFlight::get));

      for (var value = LOW; value < HIGH; value++) {
        inFlight.set(value);
        assertEquals(
          Action.NONE,
          controller.check(null, false),
          () -> "Expected hold while running at value below high watermark");
      }

      inFlight.set(HIGH);
      assertEquals(Action.PAUSE, controller.check(null, false));
    }

    /// Full oscillation: simulate the controller driving a stateful paused flag while the
    /// metric bounces inside the band (low+1 .. high-1) for many cycles. The paused flag must
    /// never flip — exactly one PAUSE at entry and zero RESUMEs until the metric actually
    /// crosses a watermark. This is the property that proves the band is a true dead-zone.
    @Test
    void oscillationInsideBandProducesNoStateChanges() {
      final var inFlight = new AtomicLong(HIGH); // start saturated → first check pauses
      final var controller = new BackpressureController(
        HIGH, LOW, BackpressureController.inFlightStrategy(inFlight::get));

      var paused = false;
      var pauseCount = 0;
      var resumeCount = 0;

      // First check at the high watermark: pauses once.
      switch (controller.check(null, paused)) {
        case PAUSE -> {
          paused = true;
          pauseCount++;
        }
        case RESUME -> {
          paused = false;
          resumeCount++;
        }
        case NONE -> {}
      }
      assertEquals(1, pauseCount, "First saturated check must pause exactly once");

      // Now bounce the metric strictly inside the band for many cycles. Apply each decision to
      // the running paused flag, just like the consumer loop would.
      final long[] bandWalk = {LOW + 1, HIGH - 1, (HIGH + LOW) / 2, HIGH - 1, LOW + 1, LOW + 2};
      for (var cycle = 0; cycle < 100; cycle++) {
        inFlight.set(bandWalk[cycle % bandWalk.length]);
        switch (controller.check(null, paused)) {
          case PAUSE -> {
            paused = true;
            pauseCount++;
          }
          case RESUME -> {
            paused = false;
            resumeCount++;
          }
          case NONE -> {}
        }
      }

      assertEquals(1, pauseCount, "No additional pauses should fire while bouncing in the band");
      assertEquals(0, resumeCount, "No resume should fire while the metric stays inside the band");
      assertTrue(paused, "Consumer should remain paused throughout band oscillation");
    }
  }

  @Nested
  class BothStrategies {

    /// The in-flight (PARALLEL) strategy reads its supplier and the decision lands at the
    /// watermark edges identically to the abstract math above.
    @Test
    void inFlightStrategyDecidesAtEdges() {
      final var inFlight = new AtomicLong(HIGH);
      final var controller = new BackpressureController(
        HIGH, LOW, BackpressureController.inFlightStrategy(inFlight::get));

      assertEquals(Action.PAUSE, controller.check(null, false));

      inFlight.set(HIGH - 1);
      assertEquals(Action.NONE, controller.check(null, false));

      inFlight.set(LOW);
      assertEquals(Action.RESUME, controller.check(null, true));

      inFlight.set(LOW + 1);
      assertEquals(Action.NONE, controller.check(null, true));
    }

    /// The lag (SEQUENTIAL) strategy computes `Σ (endOffset - position)` over the assignment
    /// and the decision lands at the same edges. We craft lag exactly equal to the high
    /// watermark, one below it, and exactly the low watermark.
    @Test
    void lagStrategyDecidesAtEdges() {
      final var strategy = BackpressureController.lagStrategy();
      final var controller = new BackpressureController(HIGH, LOW, strategy);

      // Lag exactly == HIGH → pause when running.
      assertEquals(Action.PAUSE, controller.check(consumerWithLag(HIGH), false));

      // Lag one below HIGH → hold.
      assertEquals(Action.NONE, controller.check(consumerWithLag(HIGH - 1), false));

      // Lag exactly == LOW → resume when paused.
      assertEquals(Action.RESUME, controller.check(consumerWithLag(LOW), true));

      // Lag one above LOW → hold while paused (band).
      assertEquals(Action.NONE, controller.check(consumerWithLag(LOW + 1), true));
    }

    /// Builds a mock consumer whose single-partition lag equals `targetLag` (endOffset -
    /// position). Keeps the lag-strategy edge tests free of arithmetic noise.
    private static Consumer<?, ?> consumerWithLag(final long targetLag) {
      final var consumer = Mockito.mock(Consumer.class);
      final var tp = new TopicPartition("topic", 0);
      final var assignment = Set.of(tp);
      final long position = 1_000L;
      when(consumer.assignment()).thenReturn(assignment);
      when(consumer.endOffsets(Mockito.eq(assignment), Mockito.any()))
        .thenReturn(Map.of(tp, position + targetLag));
      when(consumer.position(tp)).thenReturn(position);
      return consumer;
    }
  }

  @Nested
  class InFlightIncludesBufferedBatchRecords {

    /// The consumer composes its in-flight metric as `dispatcher.pendingCount() + Σ buffered
    /// batch records`. A controller wired to that composite supplier must see buffered records
    /// as real pressure: a dispatcher that has drained to zero can still keep the consumer
    /// paused if enough records are sitting in batch buffers. This models that composite supplier
    /// and proves the buffered count alone can hold (and trip) the watermark.
    @Test
    void bufferedBatchRecordsAloneCanTripAndHoldThePauseDecision() {
      final var dispatcherPending = new AtomicLong(0);
      final var bufferedBatch = new AtomicLong(0);
      // Mirrors KPipeConsumer.totalInFlight(): pending workers + buffered batch records.
      final var controller = new BackpressureController(
        HIGH, LOW, BackpressureController.inFlightStrategy(() -> dispatcherPending.get() + bufferedBatch.get()));

      // No workers active, but the batch buffer is saturated at the high watermark → PAUSE.
      bufferedBatch.set(HIGH);
      assertEquals(
        Action.PAUSE,
        controller.check(null, false),
        "Buffered batch records alone must be able to trip the pause watermark");

      // The dispatcher fully drains, yet buffered records still sit above the low watermark →
      // stay paused. If totalInFlight ignored the buffer this would wrongly resume.
      dispatcherPending.set(0);
      bufferedBatch.set(LOW + 1);
      assertEquals(
        Action.NONE,
        controller.check(null, true),
        "Buffered records above low watermark must keep the consumer paused");

      // Buffer drains to the low watermark → resume.
      bufferedBatch.set(LOW);
      assertEquals(Action.RESUME, controller.check(null, true));
    }

    /// The composite is a true sum: neither dimension alone reaches the high watermark, but
    /// their sum does, and the controller pauses. This pins the additive contract — buffered
    /// records and active workers are not tracked in isolation.
    @Test
    void pendingAndBufferedSumToTripWatermark() {
      final var dispatcherPending = new AtomicLong(HIGH - 1); // just under high on its own
      final var bufferedBatch = new AtomicLong(0);
      final var controller = new BackpressureController(
        HIGH, LOW, BackpressureController.inFlightStrategy(() -> dispatcherPending.get() + bufferedBatch.get()));

      // Each dimension below high; sum below high → hold.
      assertEquals(Action.NONE, controller.check(null, false));

      // One buffered record tips the sum to exactly the high watermark → pause.
      bufferedBatch.set(1);
      assertEquals(Action.PAUSE, controller.check(null, false));
    }
  }

  @Nested
  class ParallelCompletionMetricCorrectness {

    /// Under parallel completion the in-flight supplier reads a live counter that workers
    /// decrement as they finish. The controller must read the value *at the moment of the
    /// check* — not a stale snapshot. We drive completions on a separate virtual thread and
    /// observe the decision transition from PAUSE-eligible to RESUME-eligible as the counter
    /// crosses the watermarks, proving the read is current each call.
    @Test
    void controllerReadsCurrentCountAsParallelWorkersComplete() throws InterruptedException {
      final var inFlight = new AtomicLong(HIGH + 5); // start saturated
      final var controller = new BackpressureController(
        HIGH, LOW, BackpressureController.inFlightStrategy(inFlight::get));

      assertEquals(Action.PAUSE, controller.check(null, false));

      // A virtual thread completes records one at a time; after each completion we re-check.
      final var observed = new ArrayList<Action>();
      final var done = new CountDownLatch(1);
      final var worker = Thread.ofVirtual().unstarted(() -> {
        // Drain from (HIGH+5) down to (LOW-1): crosses high then low.
        for (var remaining = HIGH + 4; remaining >= LOW - 1; remaining--) {
          inFlight.set(remaining);
          synchronized (observed) {
            observed.add(controller.check(null, true));
          }
        }
        done.countDown();
      });
      worker.start();
      done.await();

      // While remaining stayed above the low watermark, every check held (NONE). RESUME only
      // appears once the live counter reaches or drops below the low watermark.
      final List<Action> snapshot;
      synchronized (observed) {
        snapshot = List.copyOf(observed);
      }
      assertEquals(
        Action.RESUME,
        snapshot.getLast(),
        "Final completion drops below low watermark → controller must read it and resume");
      // No premature resume while the counter was still inside the band.
      final var firstResumeIndex = snapshot.indexOf(Action.RESUME);
      for (var i = 0; i < firstResumeIndex; i++) {
        assertEquals(
          Action.NONE,
          snapshot.get(i),
          "No resume may fire while the live in-flight count is still above the low watermark");
      }
    }
  }
}
