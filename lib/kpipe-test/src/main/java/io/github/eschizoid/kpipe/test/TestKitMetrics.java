package io.github.eschizoid.kpipe.test;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.metrics.ConsumerMetricKeys;
import java.time.Duration;
import java.util.Map;

/// Shared quiescence + accounting over [KPipeConsumer#getMetrics] for the Docker-free test kit.
///
/// Both harnesses need the same "everything sent has been polled, terminally accounted for, and
/// drained" check: [TestStream] (live drive) drives it in its `flush()` loop, and
/// [CrashRestartHarness] drives it between its crash and restart phases. Holding the metric-key
/// names and the accounting formula here stops the two from drifting — before this, both hand-rolled
/// identical constants + `accountedFor` + `quiescent`.
final class TestKitMetrics {

  // Aliases of the shared key set (ConsumerMetricKeys) — never re-declare the literals here.
  private static final String METRIC_RECEIVED = ConsumerMetricKeys.MESSAGES_RECEIVED;
  private static final String METRIC_PROCESSED = ConsumerMetricKeys.MESSAGES_PROCESSED;
  private static final String METRIC_ERRORS = ConsumerMetricKeys.PROCESSING_ERRORS;
  private static final String METRIC_IN_FLIGHT = ConsumerMetricKeys.IN_FLIGHT;

  private TestKitMetrics() {}

  /// True once every sent record is (1) polled (`messagesReceived` ≥ target), (2) terminally
  /// accounted for, and (3) drained (`waitForInFlightDrain`, then a re-read of (1)+(2)).
  ///
  /// Records move strictly forward (unpolled → in-flight → buffered/terminal), so observing "all
  /// accounted for" *before* observing "in-flight drained" cannot yield a false positive — a record
  /// still unpolled keeps the sum below target, and a record still processing fails the drain check.
  /// The final re-read confirms the accounting after the drain.
  ///
  /// @param consumer the consumer to inspect
  /// @param target   the number of records expected to have been sent
  /// @return true if the consumer has quiesced at or above `target`
  static boolean quiescent(final KPipeConsumer consumer, final long target) {
    final var snapshot = consumer.getMetrics();
    if (snapshot.get(METRIC_RECEIVED) < target) return false;
    if (accountedFor(snapshot) < target) return false;
    if (!consumer.waitForInFlightDrain(Duration.ofMillis(1))) return false;
    final var settled = consumer.getMetrics();
    return settled.get(METRIC_RECEIVED) >= target && accountedFor(settled) >= target;
  }

  /// Terminal records plus in-flight ones (`inFlight` = dispatcher pending + batch-buffered; after a
  /// drain, pending is zero so the in-flight component is exactly the buffered count).
  ///
  /// @param snapshot a [KPipeConsumer#getMetrics] snapshot
  /// @return processed + errored + in-flight
  private static long accountedFor(final Map<String, Long> snapshot) {
    return snapshot.get(METRIC_PROCESSED) + snapshot.get(METRIC_ERRORS) + snapshot.get(METRIC_IN_FLIGHT);
  }
}
