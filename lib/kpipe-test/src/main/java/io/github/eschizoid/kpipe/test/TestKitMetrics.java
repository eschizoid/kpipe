package io.github.eschizoid.kpipe.test;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.MetricsSnapshot;
import java.time.Duration;

/// Shared quiescence and accounting over [KPipeConsumer#metrics] for the Docker-free test kit.
///
/// Both harnesses need the same "everything sent has been polled, terminally accounted for, and
/// drained" check: [TestStream] runs it in its `flush()` loop, and [CrashRestartHarness] between
/// its crash and restart phases. Keeping the accounting formula here stops the two from drifting.
final class TestKitMetrics {

  private TestKitMetrics() {}

  /// True once every sent record is (1) polled (`received` ≥ target), (2) terminally
  /// accounted for, and (3) drained (`waitForInFlightDrain`, then a re-read of (1)+(2)).
  ///
  /// Records move strictly forward (unpolled → in-flight → buffered/terminal), so observing "all
  /// accounted for" before observing "in-flight drained" cannot yield a false positive: a record
  /// still unpolled keeps the sum below target, and a record still processing fails the drain
  /// check. The final re-read confirms the accounting after the drain.
  ///
  /// @param consumer the consumer to inspect
  /// @param target   the number of records expected to have been sent
  /// @return true if the consumer has quiesced at or above `target`
  static boolean quiescent(final KPipeConsumer consumer, final long target) {
    final var snapshot = consumer.metrics();
    if (snapshot.records().received() < target) return false;
    if (accountedFor(snapshot) < target) return false;
    if (!consumer.waitForInFlightDrain(Duration.ofMillis(1))) return false;
    final var settled = consumer.metrics();
    return settled.records().received() >= target && accountedFor(settled) >= target;
  }

  /// Terminal records plus in-flight ones. In-flight is the dispatcher's pending records plus
  /// batch-buffered ones; after a drain, pending is zero, so it is exactly the buffered count.
  ///
  /// @param snapshot a [KPipeConsumer#metrics] snapshot
  /// @return processed + filtered + failed + in-flight
  private static long accountedFor(final MetricsSnapshot snapshot) {
    final var records = snapshot.records();
    return records.processed() + records.filtered() + records.failed() + snapshot.inFlight();
  }
}
