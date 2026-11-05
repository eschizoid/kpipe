package io.github.eschizoid.kpipe.consumer;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/// The consumer's counters at one moment, from `KPipeConsumer.metrics()` or `Handle.metrics()`.
///
/// ```java
/// final var m = handle.metrics();
/// m.records().processed();          // across every topic
/// m.records("orders").failed();     // one topic
/// m.inFlight();
/// m.lag("orders");                  // records behind the end of the log
/// m.circuitBreaker().trips();
/// ```
///
/// Counters only grow; [#inFlight] and the lag are current values. Each figure is read once per
/// snapshot, and the outcome counts are read before [Records#received], so
/// `received >= processed + filtered + failed` holds in every snapshot. The difference is the
/// records still being processed, plus any a shutdown abandoned for redelivery.
///
/// @param records counts across every topic
/// @param topics counts per topic, for each topic that has received a record
/// @param inFlight records the consumer holds: being processed, queued behind their key, or
///     waiting in a batch buffer. Queued ones are not yet counted in [Records#received].
/// @param lagByTopic per topic, the offsets between this consumer's position and the end of the
///     log, summed over its assigned partitions whose lag is known; read at most once a second
/// @param backpressure how often and how long backpressure paused consumption
/// @param circuitBreaker how often and how long the circuit breaker stopped consumption
public record MetricsSnapshot(
  Records records,
  Map<String, Records> topics,
  long inFlight,
  Map<String, Long> lagByTopic,
  Backpressure backpressure,
  CircuitBreaker circuitBreaker
) {
  /// Copies `topics` and rejects null parts.
  public MetricsSnapshot {
    Objects.requireNonNull(records, "records");
    topics = Map.copyOf(topics);
    lagByTopic = Map.copyOf(lagByTopic);
    Objects.requireNonNull(backpressure, "backpressure");
    Objects.requireNonNull(circuitBreaker, "circuitBreaker");
  }

  /// A snapshot of a consumer that has received nothing, for test doubles.
  ///
  /// @return a snapshot with every counter at zero
  public static MetricsSnapshot empty() {
    return new MetricsSnapshot(
      Records.ZERO,
      Map.of(),
      0,
      Map.of(),
      new Backpressure(0, Duration.ZERO),
      new CircuitBreaker(0, Duration.ZERO)
    );
  }

  /// The counts for one topic.
  ///
  /// @param topic a topic name
  /// @return the counts for `topic`, all zero if it has received no record
  public Records records(final String topic) {
    return topics.getOrDefault(topic, Records.ZERO);
  }

  /// @return the lag summed over every topic
  public long lag() {
    long total = 0;
    for (final var lag : lagByTopic.values()) total += lag;
    return total;
  }

  /// @param topic a topic name
  /// @return the lag for `topic`, zero when this consumer holds none of its partitions or the
  ///     client does not yet know their lag
  public long lag(final String topic) {
    return lagByTopic.getOrDefault(topic, 0L);
  }

  /// Record counts for one topic or for all of them.
  ///
  /// @param received records the consumer started processing
  /// @param processed records that passed every operator and were handed to their sink, if the
  ///     pipeline has one
  /// @param filtered records dropped on purpose: by an operator that filtered them, or because no
  ///     pipeline is routed for their topic
  /// @param failed records that failed after their retries, or at once for a
  ///     `NonRetryableException`; they went to the error handler and, when configured, the DLQ
  /// @param retries retry attempts, not counting first attempts
  /// @param dlqSent failed records written to the dead-letter topic
  /// @param dlqFailed failed records whose dead-letter write failed; their offsets stay pending
  /// @param processingTime wall-clock time from receiving a record to its sink, retries included,
  ///     summed over processed records on per-record routes; batch routes do not measure it
  public record Records(
    long received,
    long processed,
    long filtered,
    long failed,
    long retries,
    long dlqSent,
    long dlqFailed,
    Duration processingTime
  ) {
    static final Records ZERO = new Records(0, 0, 0, 0, 0, 0, 0, Duration.ZERO);

    /// Rejects a null processing time.
    public Records {
      Objects.requireNonNull(processingTime, "processingTime");
    }

    /// A topic is either a batch route or a per-record one. Batch routes do not measure processing
    /// time, so a batch topic's average reads zero, and the average over all topics reads low
    /// when some of them are batch routes; read it per topic in that case.
    ///
    /// @return [#processingTime] divided by [#processed], or zero before the first record
    public Duration averageProcessingTime() {
      return processed == 0 ? Duration.ZERO : processingTime.dividedBy(processed);
    }
  }

  /// How often and how long backpressure paused consumption.
  ///
  /// @param pauses times backpressure paused consumption
  /// @param time total time paused, counting a pause still in progress
  public record Backpressure(long pauses, Duration time) {
    /// Rejects a null time.
    public Backpressure {
      Objects.requireNonNull(time, "time");
    }
  }

  /// How often and how long the circuit breaker stopped consumption.
  ///
  /// @param trips times the breaker opened
  /// @param openTime total time open, counting an open period still in progress
  public record CircuitBreaker(long trips, Duration openTime) {
    /// Rejects a null open time.
    public CircuitBreaker {
      Objects.requireNonNull(openTime, "openTime");
    }
  }
}
