package io.github.eschizoid.kpipe.consumer;

/// What a [MetricsExporter] reads from a running consumer. Exporters may read it while holding
/// their own lock, so an implementation must answer quickly and must not block on a lock that a
/// thread starting or closing a consumer could hold.
public interface MetricsSource {
  /// @return the consumer's `group.id`, or null when it has none
  String consumerGroup();

  /// @return the consumer's counters now
  MetricsSnapshot metrics();

  /// @return the consumer's health now, including the circuit breaker's state
  HealthSnapshot health();
}
