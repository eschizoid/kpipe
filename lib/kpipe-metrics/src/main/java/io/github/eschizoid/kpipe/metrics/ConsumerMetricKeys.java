package io.github.eschizoid.kpipe.metrics;

/// The map keys of the consumer's metrics snapshot (`KPipeConsumer.getMetrics()`), in one place.
///
/// These strings are a public contract: the consumer publishes them, `ConsumerMetricsReporter`
/// formats them, and the kpipe-test kit's quiescence check reads them. Before this holder each of
/// those sites carried its own private copies of the same literals — a rename in one place would
/// silently desynchronize the others (the reporter would log zeros; the test kit would hang).
/// Reference these constants instead of re-declaring the strings.
public final class ConsumerMetricKeys {

  /// Records polled from Kafka.
  public static final String MESSAGES_RECEIVED = "messagesReceived";

  /// Records successfully processed (includes intentional filters).
  public static final String MESSAGES_PROCESSED = "messagesProcessed";

  /// Records that failed processing.
  public static final String PROCESSING_ERRORS = "processingErrors";

  /// Total per-record processing time, milliseconds.
  public static final String PROCESSING_DURATION_TOTAL_MS = "processingDurationTotalMs";

  /// Retry attempts made.
  public static final String RETRIES = "retries";

  /// Times backpressure paused the consumer.
  public static final String BACKPRESSURE_PAUSE_COUNT = "backpressurePauseCount";

  /// Total time paused under backpressure, milliseconds.
  public static final String BACKPRESSURE_TIME_MS = "backpressureTimeMs";

  /// Circuit-breaker CLOSED→OPEN (and HALF_OPEN→OPEN) transitions.
  public static final String CIRCUIT_BREAKER_TRIPS = "circuitBreakerTrips";

  /// Total time the circuit breaker spent OPEN, milliseconds.
  public static final String CIRCUIT_BREAKER_TIME_OPEN_MS = "circuitBreakerTimeOpenMs";

  /// Records durably parked in the dead-letter topic.
  public static final String DLQ_SENT = "dlqSent";

  /// Failed dead-letter sends (offset held pending; record reprocessed on restart).
  public static final String DLQ_FAILED = "dlqFailed";

  /// Live count of records currently held by the consumer (dispatcher pending + batch-buffered).
  public static final String IN_FLIGHT = "inFlight";

  private ConsumerMetricKeys() {}
}
