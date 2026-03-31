package org.kpipe.consumer;

import java.util.Optional;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.Consumer;

/// Controls backpressure for a Kafka consumer by monitoring a metric (in-flight or lag)
/// and deciding when to pause or resume consumption.
///
/// This is a pure decision module — it has no side effects and holds no mutable state. It
/// implements hysteresis via two configurable thresholds:
///
/// * **High watermark** — when the metric reaches this value, the consumer should pause
/// * **Low watermark** — when the metric drops to this value (while paused), the consumer
///   should resume
///
/// @param highWatermark the metric value at or above which the consumer should pause
/// @param lowWatermark  the metric value at or below which the consumer should resume (must
///                      be less than highWatermark)
/// @param strategy      the strategy to use for calculating the metric
public record BackpressureController(long highWatermark, long lowWatermark, Strategy strategy) {
  /// Creates a new BackpressureController with the same watermarks but a different strategy.
  ///
  /// @param strategy the new strategy to use
  /// @return a new {@link BackpressureController} instance
  public BackpressureController withStrategy(final Strategy strategy) {
    return new BackpressureController(highWatermark, lowWatermark, strategy);
  }

  /// The action that the consumer should take based on the current metric value.
  public enum Action {
    /// Indicates the consumer should pause fetching new messages.
    PAUSE,
    /// Indicates the consumer should resume fetching messages.
    RESUME,
    /// Indicates no action is needed.
    NONE,
  }

  /// Strategy for measuring the backpressure metric.
  public interface Strategy {
    /// Returns the current value of the metric.
    long getMetric(final Consumer<?, ?> consumer);

    /// Returns a human-readable name for the metric (e.g., "in-flight", "lag").
    String getName();
  }

  /// Creates a strategy that monitors Kafka consumer lag.
  ///
  /// @return a new {@link Strategy} instance
  public static Strategy lagStrategy() {
    return new Strategy() {
      @Override
      public long getMetric(final Consumer<?, ?> consumer) {
        return calculateTotalLag(consumer);
      }

      @Override
      public String getName() {
        return "lag";
      }
    };
  }

  /// Creates a strategy that monitors an in-flight message count.
  ///
  /// @param inFlightSupplier the supplier of the current in-flight count
  /// @return a new {@link Strategy} instance
  public static Strategy inFlightStrategy(final LongSupplier inFlightSupplier) {
    if (inFlightSupplier == null) throw new IllegalArgumentException("inFlightSupplier cannot be null");
    return new Strategy() {
      @Override
      public long getMetric(final Consumer<?, ?> consumer) {
        return inFlightSupplier.getAsLong();
      }

      @Override
      public String getName() {
        return "in-flight";
      }
    };
  }

  /// Constructs a BackpressureController with the specified high and low watermarks.
  ///
  /// @param highWatermark the metric value at or above which the consumer should pause
  /// @param lowWatermark the metric value at or below which the consumer should resume
  /// @param strategy the strategy to use for calculating the metric
  public BackpressureController {
    if (highWatermark <= 0) throw new IllegalArgumentException("highWatermark must be positive");
    if (lowWatermark < 0) throw new IllegalArgumentException("lowWatermark cannot be negative");
    if (lowWatermark >= highWatermark) throw new IllegalArgumentException(
      "lowWatermark (%d) must be less than highWatermark (%d)".formatted(lowWatermark, highWatermark)
    );
  }

  /// Determines the action to take based on the current state of the consumer.
  ///
  /// @param consumer the Kafka consumer to monitor
  /// @param currentlyPaused whether the consumer is currently paused
  /// @return the action to take: {@link Action#PAUSE}, {@link Action#RESUME}, or
  ///     {@link Action#NONE}
  public Action check(final Consumer<?, ?> consumer, final boolean currentlyPaused) {
    if (strategy == null) return Action.NONE;
    final long currentValue = strategy.getMetric(consumer);
    if (!currentlyPaused && currentValue >= highWatermark) return Action.PAUSE;
    if (currentlyPaused && currentValue <= lowWatermark) return Action.RESUME;
    return Action.NONE;
  }

  /// Returns the current metric value.
  ///
  /// @param consumer the Kafka consumer to monitor
  /// @return the current metric value
  public long getMetric(final Consumer<?, ?> consumer) {
    return strategy != null ? strategy.getMetric(consumer) : 0L;
  }

  /// Returns the name of the metric being monitored.
  ///
  /// @return the name of the metric being monitored
  public String getMetricName() {
    return strategy != null ? strategy.getName() : "none";
  }

  /// Calculates the total lag across all assigned partitions for the given consumer.
  ///
  /// The formula for total lag is:
  /// ```
  /// lag = Σ (endOffset - position)
  /// ```
  /// where `endOffset` is the highest available offset in the partition and `position` is the
  /// offset of the next record to be fetched by the consumer.
  ///
  /// @param consumer the Kafka consumer to calculate lag for
  /// @return the total lag, or 0 if no partitions are assigned or an error occurs
  public static long calculateTotalLag(final Consumer<?, ?> consumer) {
    return Optional.ofNullable(consumer)
      .map(c -> {
        try {
          final var assignment = c.assignment();
          if (assignment.isEmpty()) return 0L;

          final var endOffsets = c.endOffsets(assignment);
          long totalLag = 0L;
          for (final var tp : assignment) {
            final long position = c.position(tp);
            final long endOffset = endOffsets.getOrDefault(tp, position);
            totalLag += Math.max(0, endOffset - position);
          }
          return totalLag;
        } catch (final Exception e) {
          return 0L;
        }
      })
      .orElse(0L);
  }
}
