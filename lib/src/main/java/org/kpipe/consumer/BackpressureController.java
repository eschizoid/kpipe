package org.kpipe.consumer;

/// Controls backpressure for a Kafka consumer by monitoring in-flight message counts and
/// deciding when to pause or resume consumption.
///
/// This is a pure decision module — it has no side effects and holds no mutable state. It
/// implements hysteresis via two configurable thresholds:
///
/// * **High watermark** — when in-flight count reaches this value, the consumer should pause
/// * **Low watermark** — when in-flight count drops to this value (while paused), the consumer
///   should resume
///
/// Example usage:
///
/// ```java
/// final var controller = new BackpressureController(10_000, 7_000);
/// switch (controller.check(inFlightCount, isPaused)) {
///   case PAUSE  -> consumer.pause();
///   case RESUME -> consumer.resume();
///   case NONE   -> {}
/// }
/// ```
///
/// @param highWatermark the in-flight count at or above which the consumer should pause
/// @param lowWatermark  the in-flight count at or below which the consumer should resume (must
///                      be less than highWatermark)
public record BackpressureController(long highWatermark, long lowWatermark) {
  /// The action that the consumer should take based on the current in-flight count.
  public enum Action {
    /// Indicates the consumer should pause fetching new messages due to high in-flight count.
    PAUSE,
    /// Indicates the consumer should resume fetching messages as in-flight count is low.
    RESUME,
    /// Indicates no action is needed; the consumer should maintain its current state.
    NONE,
  }

  /// Constructs a BackpressureController with the specified high and low watermarks.
  ///
  /// @param highWatermark the in-flight count at or above which the consumer should pause
  /// @param lowWatermark the in-flight count at or below which the consumer should resume
  public BackpressureController {
    if (highWatermark <= 0) throw new IllegalArgumentException("highWatermark must be positive");
    if (lowWatermark < 0) throw new IllegalArgumentException("lowWatermark cannot be negative");
    if (lowWatermark >= highWatermark) throw new IllegalArgumentException(
      "lowWatermark (%d) must be less than highWatermark (%d)".formatted(lowWatermark, highWatermark)
    );
  }

  /// Determines the action to take based on the current in-flight count and pause state.
  ///
  /// @param inFlightCount  the current number of in-flight messages
  /// @param currentlyPaused whether the consumer is currently paused
  /// @return the action to take: {@link Action#PAUSE}, {@link Action#RESUME}, or
  ///     {@link Action#NONE}
  public Action check(final long inFlightCount, final boolean currentlyPaused) {
    if (!currentlyPaused && inFlightCount >= highWatermark) return Action.PAUSE;
    if (currentlyPaused && inFlightCount <= lowWatermark) return Action.RESUME;
    return Action.NONE;
  }
}
