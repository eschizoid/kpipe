package org.kpipe.metrics;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/// Service for reporting Kafka consumer metrics.
///
/// **Example 1:** Basic usage with default logging:
///
/// ```java
/// // Create with default logging
/// final var reporter = ConsumerMetricsReporter.forConsumer(consumer::getMetrics);
/// reporter.reportMetrics(); // Will log to System logger
/// ```
///
/// **Example 2:** Custom metrics reporting:
///
/// ```java
/// // Report metrics to Prometheus
/// final var reporter = ConsumerMetricsReporter.forConsumer(consumer::getMetrics)
///     .toConsumer(metric -> PrometheusClient.pushGauge("consumer_metrics", parseValues(metric)));
/// ```
///
/// **Example 3:** Fluent usage with custom reporter:
///
/// ```java
/// final var reporter = ConsumerMetricsReporter.forConsumer(consumer::getMetrics)
///     .toConsumer(System.out::println);
/// reporter.reportMetrics();
/// ```
///
/// @param metricsSupplier supplier of consumer metrics
/// @param uptimeSupplier supplier of application uptime in ms
/// @param reporter consumer for reporting metrics (defaults to logger if null)
public record ConsumerMetricsReporter(
  Supplier<Map<String, Long>> metricsSupplier,
  Supplier<Long> uptimeSupplier,
  Consumer<String> reporter
) implements KPipeMetricsReporter {
  private static final Logger LOGGER = System.getLogger(ConsumerMetricsReporter.class.getName());
  private static final long APP_START_TIME = System.currentTimeMillis();
  private static final String METRIC_MESSAGES_RECEIVED = "messagesReceived";
  private static final String METRIC_MESSAGES_PROCESSED = "messagesProcessed";
  private static final String METRIC_PROCESSING_ERRORS = "processingErrors";
  private static final String METRIC_BACKPRESSURE_PAUSE_COUNT = "backpressurePauseCount";
  private static final String METRIC_BACKPRESSURE_TIME_MS = "backpressureTimeMs";

  /// Creates a new ConsumerMetricsReporter, defaulting to log-based reporting when reporter is null.
  ///
  /// @param metricsSupplier supplier of consumer metrics
  /// @param uptimeSupplier supplier of application uptime in ms
  /// @param reporter consumer for reporting metrics (defaults to logger if null)
  public ConsumerMetricsReporter(
    final Supplier<Map<String, Long>> metricsSupplier,
    final Supplier<Long> uptimeSupplier,
    final Consumer<String> reporter
  ) {
    this.metricsSupplier = metricsSupplier;
    this.uptimeSupplier = uptimeSupplier;
    this.reporter = reporter != null ? reporter : this::logMetrics;
  }

  @Override
  public void reportMetrics() {
    try {
      final var metrics = metricsSupplier.get();
      if (metrics != null && !metrics.isEmpty()) {
        final var sb = new StringBuilder(
          "Consumer metrics: messages received: %d, messages processed: %d, errors: %d, uptime: %d ms".formatted(
            metrics.getOrDefault(METRIC_MESSAGES_RECEIVED, 0L),
            metrics.getOrDefault(METRIC_MESSAGES_PROCESSED, 0L),
            metrics.getOrDefault(METRIC_PROCESSING_ERRORS, 0L),
            uptimeSupplier.get()
          )
        );

        if (metrics.containsKey(METRIC_BACKPRESSURE_PAUSE_COUNT)) {
          sb.append(
            ", backpressure pauses: %d, backpressure time: %d ms".formatted(
              metrics.getOrDefault(METRIC_BACKPRESSURE_PAUSE_COUNT, 0L),
              metrics.getOrDefault(METRIC_BACKPRESSURE_TIME_MS, 0L)
            )
          );
        }

        reporter.accept(sb.toString());
      }
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error reporting consumer metrics", e);
    }
  }

  private void logMetrics(final String metrics) {
    LOGGER.log(Level.INFO, metrics);
  }

  /// Creates a consumer metrics reporter with default logging.
  ///
  /// @param metricsSupplier supplier of consumer metrics
  /// @return a new reporter that can be further customized
  public static ConsumerMetricsReporter forConsumer(final Supplier<Map<String, Long>> metricsSupplier) {
    return new ConsumerMetricsReporter(metricsSupplier, () -> System.currentTimeMillis() - APP_START_TIME, null);
  }

  /// Creates a consumer metrics reporter with custom uptime supplier.
  ///
  /// @param metricsSupplier supplier of consumer metrics
  /// @param uptimeSupplier supplier of application uptime in ms
  /// @return a new reporter that can be further customized
  public static ConsumerMetricsReporter forConsumer(
    final Supplier<Map<String, Long>> metricsSupplier,
    final Supplier<Long> uptimeSupplier
  ) {
    return new ConsumerMetricsReporter(metricsSupplier, uptimeSupplier, null);
  }

  /// Creates a new reporter with the specified output consumer.
  ///
  /// @param reporter the consumer for reporting metrics
  /// @return a new ConsumerMetricsReporter instance
  public ConsumerMetricsReporter toConsumer(final Consumer<String> reporter) {
    return new ConsumerMetricsReporter(this.metricsSupplier, this.uptimeSupplier, reporter);
  }
}
