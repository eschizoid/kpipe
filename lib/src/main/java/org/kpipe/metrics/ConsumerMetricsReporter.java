package org.kpipe.metrics;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Functional service for reporting Kafka consumer metrics.
 *
 * <p><strong>Example 1:</strong> Basic usage with default logging:
 *
 * <pre>{@code
 * // Create with default logging
 * ConsumerMetricsReporter reporter = new ConsumerMetricsReporter(
 *     consumer::getMetrics,
 *     () -> System.currentTimeMillis() - startTime
 * );
 * reporter.reportMetrics(); // Will log to System logger
 * }</pre>
 *
 * <p><strong>Example 2:</strong> Custom metrics reporting:
 *
 * <pre>{@code
 * // Report metrics to Prometheus
 * Consumer<String> prometheusReporter = metric ->
 *     PrometheusClient.pushGauge("consumer_metrics", parseValues(metric));
 *
 * ConsumerMetricsReporter reporter = new ConsumerMetricsReporter(
 *     consumer::getMetrics,
 *     () -> System.currentTimeMillis() - startTime,
 *     prometheusReporter
 * );
 * }</pre>
 *
 * <p><strong>Example 3:</strong> Advanced usage with alerting:
 *
 * <pre>{@code
 * // Report and check thresholds
 * ConsumerMetricsReporter reporter = new ConsumerMetricsReporter(
 *     consumer::getMetrics,
 *     () -> System.currentTimeMillis() - startTime,
 *     metric -> {
 *         logger.info(metric);
 *
 *         // Extract error count and check threshold
 *         int errors = extractErrorCount(metric);
 *         if (errors > ERROR_THRESHOLD) {
 *             alertSystem.sendAlert("High error rate detected: " + errors);
 *         }
 *     }
 * );
 * }</pre>
 *
 * @param metricsSupplier supplier of consumer metrics
 * @param uptimeSupplier supplier of application uptime in ms
 * @param reporter consumer for reporting metrics (defaults to logger if null)
 */
public record ConsumerMetricsReporter(
  Supplier<Map<String, Long>> metricsSupplier,
  Supplier<Long> uptimeSupplier,
  Consumer<String> reporter
)
  implements MetricsReporter {
  private static final Logger LOGGER = System.getLogger(ConsumerMetricsReporter.class.getName());
  private static final String METRIC_MESSAGES_RECEIVED = "messagesReceived";
  private static final String METRIC_MESSAGES_PROCESSED = "messagesProcessed";
  private static final String METRIC_PROCESSING_ERRORS = "processingErrors";

  /**
   * Creates a consumer metrics reporter with the specified metrics supplier and reporter.
   *
   * <p>Example with custom reporter:
   *
   * <pre>{@code
   * // Send metrics to Slack
   * Consumer<String> slackReporter = metric ->
   *     slackClient.sendMessage("#metrics-channel", metric);
   *
   * var reporter = new ConsumerMetricsReporter(
   *     consumer::getMetrics,
   *     () -> System.currentTimeMillis() - startTime,
   *     slackReporter
   * );
   * }</pre>
   *
   * @param metricsSupplier supplier of consumer metrics
   * @param uptimeSupplier supplier of application uptime in ms
   * @param reporter consumer for reporting metrics (defaults to logger if null)
   */
  public ConsumerMetricsReporter(
    final Supplier<Map<String, Long>> metricsSupplier,
    final Supplier<Long> uptimeSupplier,
    final Consumer<String> reporter
  ) {
    this.metricsSupplier = metricsSupplier;
    this.uptimeSupplier = uptimeSupplier;
    this.reporter = reporter != null ? reporter : this::logMetrics;
  }

  /**
   * Reports consumer metrics.
   *
   * <p>The reporting process:
   *
   * <ol>
   *   <li>Retrieves current metrics from the metrics supplier
   *   <li>Formats key metrics (messages received/processed/errors) and uptime
   *   <li>Passes the formatted report to the configured reporter
   * </ol>
   *
   * <p>If metrics retrieval fails, the error is logged without interrupting application flow.
   */
  @Override
  public void reportMetrics() {
    try {
      final var metrics = metricsSupplier.get();
      if (metrics != null && !metrics.isEmpty()) {
        final var report =
          "Consumer metrics: messages received: %d, messages processed: %d, errors: %d, uptime: %d ms".formatted(
              metrics.getOrDefault(METRIC_MESSAGES_RECEIVED, 0L),
              metrics.getOrDefault(METRIC_MESSAGES_PROCESSED, 0L),
              metrics.getOrDefault(METRIC_PROCESSING_ERRORS, 0L),
              uptimeSupplier.get()
            );

        reporter.accept(report);
      }
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error reporting consumer metrics", e);
    }
  }

  private void logMetrics(final String metrics) {
    LOGGER.log(Level.INFO, metrics);
  }
}
