package org.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Tracks message processing state in message-based systems by monitoring metrics counts.
 *
 * <p>MessageTracker provides tools to calculate in-flight message counts and wait for message
 * processing completion, particularly useful during graceful shutdown procedures.
 *
 * <p>The tracker operates by comparing received, processed, and error message counts to determine
 * how many messages are currently being processed. This enables:
 *
 * <ul>
 *   <li>Monitoring in-flight messages during normal operation
 *   <li>Tracking processing backlog
 *   <li>Implementing graceful shutdown patterns that wait for in-flight messages to complete
 *   <li>Supporting high-availability scenarios requiring processing guarantees
 * </ul>
 *
 * <p>Example usage for graceful shutdown:
 *
 * <pre>{@code
 * // Create a tracker for consumer metrics
 * MessageTracker tracker = MessageTracker.builder()
 *     .withMetricsSupplier(consumer::getMetrics)
 *     .withReceivedMetricKey("messagesReceived")
 *     .withProcessedMetricKey("messagesProcessed")
 *     .withErrorsMetricKey("processingErrors")
 *     .build();
 *
 * // During shutdown
 * consumer.pause();  // Stop consuming new messages
 * boolean allProcessed = tracker.waitForCompletion(5000)
 *     .orElse(false);
 *
 * if (allProcessed) {
 *     logger.info("All messages processed successfully");
 * } else {
 *     logger.warning("Shutdown timeout with {} messages unprocessed",
 *         tracker.getInFlightMessageCount());
 * }
 * }</pre>
 *
 * <p>This class follows a functional design with clear separation between pure calculation logic
 * and side-effecting operations.
 */
public class MessageTracker {

  private static final Logger LOGGER = System.getLogger(MessageTracker.class.getName());

  private final Supplier<Map<String, Long>> metricsSupplier;
  private final String receivedMetricKey;
  private final String processedMetricKey;
  private final String errorsMetricKey;

  private MessageTracker(final Builder builder) {
    this.metricsSupplier = Objects.requireNonNull(builder.metricsSupplier, "metricsSupplier must not be null");
    this.receivedMetricKey = Objects.requireNonNull(builder.receivedMetricKey, "receivedMetricKey must not be null");
    this.processedMetricKey = Objects.requireNonNull(builder.processedMetricKey, "processedMetricKey must not be null");
    this.errorsMetricKey = Objects.requireNonNull(builder.errorsMetricKey, "errorsMetricKey must not be null");
  }

  /**
   * Function to calculate in-flight message count from metrics.
   *
   * @param metrics the metrics map
   * @return number of in-flight messages
   */
  public long calculateInFlightCount(final Map<String, Long> metrics) {
    long received = metrics.getOrDefault(receivedMetricKey, 0L);
    long processed = metrics.getOrDefault(processedMetricKey, 0L);
    long errors = metrics.getOrDefault(errorsMetricKey, 0L);

    return Math.max(0, received - processed - errors);
  }

  /**
   * Retrieves current metrics and calculates in-flight message count.
   *
   * @return number of in-flight messages
   */
  public long getInFlightMessageCount() {
    return calculateInFlightCount(metricsSupplier.get());
  }

  /**
   * Checks if there are currently any in-flight messages.
   *
   * @return true if there are in-flight messages, false otherwise
   */
  public boolean hasInFlightMessages() {
    return calculateInFlightCount(metricsSupplier.get()) > 0;
  }

  /**
   * Checks if all messages have been processed (no in-flight messages).
   *
   * @return true if all messages are processed, false if there are in-flight messages
   */
  public boolean isProcessingComplete() {
    return !hasInFlightMessages();
  }

  /**
   * Function for waiting for in-flight messages to complete.
   *
   * @param timeoutMs maximum time to wait in milliseconds
   * @return Optional containing success state, empty if interrupted
   */
  public Optional<Boolean> waitForCompletion(final long timeoutMs) {
    // Early exit if no messages in flight
    if (isProcessingComplete()) {
      LOGGER.log(Level.INFO, "No in-flight messages detected");
      return Optional.of(true);
    }

    LOGGER.log(Level.INFO, "Waiting for %s in-flight messages to complete".formatted(getInFlightMessageCount()));

    final Instant deadline = Instant.now().plusMillis(timeoutMs);

    try {
      // Use an atomic reference to track the completion state
      return waitUntil(deadline, Duration.ofMillis(Math.min(500, timeoutMs / 10)), this::isProcessingComplete)
        .map(completed -> {
          if (completed) {
            LOGGER.log(Level.INFO, "All in-flight messages completed");
          } else {
            LOGGER.log(
              Level.WARNING,
              "Timeout reached with %s messages still in-flight".formatted(getInFlightMessageCount())
            );
          }
          return completed;
        });
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Wait for in-flight messages was interrupted");
      return Optional.empty();
    }
  }

  /**
   * Wait pattern that polls a predicate until it returns true or deadline is reached.
   *
   * @param deadline when to stop waiting
   * @param checkInterval how often to check the predicate
   * @param completionPredicate function that determines if waiting should stop
   * @return Optional with true if predicate was satisfied, false if deadline reached, empty if
   *     interrupted
   */
  private Optional<Boolean> waitUntil(
    final Instant deadline,
    final Duration checkInterval,
    final Supplier<Boolean> completionPredicate
  ) throws InterruptedException {
    while (Instant.now().isBefore(deadline)) {
      if (completionPredicate.get()) {
        return Optional.of(true);
      }
      Thread.sleep(checkInterval.toMillis());
    }
    return Optional.of(false);
  }

  /**
   * Creates a new Builder instance for constructing MessageTracker objects.
   *
   * @return a new Builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Constructs a new Builder object. */
  public static class Builder {

    private Builder() {}

    private Supplier<Map<String, Long>> metricsSupplier;
    private String receivedMetricKey;
    private String processedMetricKey;
    private String errorsMetricKey;

    /**
     * Sets the metrics supplier function.
     *
     * @param metricsSupplier the supplier that provides the metrics map
     * @return this Builder instance for method chaining
     */
    public Builder withMetrics(final Supplier<Map<String, Long>> metricsSupplier) {
      this.metricsSupplier = metricsSupplier;
      return this;
    }

    /**
     * Sets the key for received messages metric.
     *
     * @param receivedMetricKey the key for received messages metric
     * @return this Builder instance for method chaining
     */
    public Builder withReceivedMetricKey(final String receivedMetricKey) {
      this.receivedMetricKey = receivedMetricKey;
      return this;
    }

    /**
     * Sets the key for processed messages metric.
     *
     * @param processedMetricKey the key for processed messages metric
     * @return this Builder instance for method chaining
     */
    public Builder withProcessedMetricKey(final String processedMetricKey) {
      this.processedMetricKey = processedMetricKey;
      return this;
    }

    /**
     * Sets the key for error messages metric.
     *
     * @param errorsMetricKey the key for error messages metric
     * @return this Builder instance for method chaining
     */
    public Builder withErrorsMetricKey(final String errorsMetricKey) {
      this.errorsMetricKey = errorsMetricKey;
      return this;
    }

    /**
     * Builds a new MessageTracker instance with the configured parameters.
     *
     * @return a new MessageTracker instance
     */
    public MessageTracker build() {
      return new MessageTracker(this);
    }
  }
}
