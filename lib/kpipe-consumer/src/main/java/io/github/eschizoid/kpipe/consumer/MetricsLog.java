package io.github.eschizoid.kpipe.consumer;

import java.lang.System.Logger;
import java.time.Duration;
import java.util.Collection;
import java.util.Locale;

/// Formats the line `withMetricsLog` writes: one `key=value` pair per figure. The keys follow the
/// OpenTelemetry instrument names without the `kpipe.consumer.` and `records.` prefixes; times are
/// in milliseconds, `processing.avg` is the average processing time, and `circuit_breaker.state`
/// is the current state.
final class MetricsLog {

  /// The logger the line goes to, so it can be routed apart from the consumer's own logs.
  static final Logger LOGGER = System.getLogger("io.github.eschizoid.kpipe.metrics.log");

  private MetricsLog() {}

  /// @param topics the consumer's topics
  /// @param group the consumer's `group.id`, or null
  /// @param m the consumer's metrics
  /// @param breaker the circuit breaker's state
  /// @return the line
  static String line(
    final Collection<String> topics,
    final String group,
    final MetricsSnapshot m,
    final CircuitBreakerState breaker
  ) {
    final var r = m.records();
    final var label = String.join(",", topics) + (group == null ? "" : " group=" + group);
    return (
      "kpipe metrics [" +
      label +
      "] received=" +
      r.received() +
      " processed=" +
      r.processed() +
      " filtered=" +
      r.filtered() +
      " failed=" +
      r.failed() +
      " retries=" +
      r.retries() +
      " dlq.sent=" +
      r.dlqSent() +
      " dlq.failed=" +
      r.dlqFailed() +
      " in_flight=" +
      m.inFlight() +
      " lag=" +
      m.lag() +
      " processing.avg=" +
      millis(r.averageProcessingTime()) +
      " backpressure.pauses=" +
      m.backpressure().pauses() +
      " backpressure.time=" +
      millis(m.backpressure().time()) +
      " circuit_breaker.state=" +
      breaker.name().toLowerCase(Locale.ROOT) +
      " circuit_breaker.trips=" +
      m.circuitBreaker().trips() +
      " circuit_breaker.open_time=" +
      millis(m.circuitBreaker().openTime())
    );
  }

  private static String millis(final Duration time) {
    return String.format(Locale.ROOT, "%.1fms", time.toNanos() / 1e6);
  }
}
