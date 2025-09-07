package org.kpipe.metrics;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.kpipe.registry.MessageSinkRegistry;

/**
 * Reports metrics for message sinks, allowing flexible composition of:
 *
 * <ul>
 *   <li>How sink names are retrieved (via a {@link Supplier})
 *   <li>How metrics are fetched for each sink (via a {@link Function})
 *   <li>How metrics are reported (via a {@link Consumer})
 * </ul>
 *
 * <p><strong>Example 1:</strong> Basic usage with default logging:
 *
 * <pre>{@code
 * // Creates reporter with default logging behavior
 * SinkMetricsReporter reporter = new SinkMetricsReporter(registry);
 * reporter.reportMetrics();
 * }</pre>
 *
 * <p><strong>Example 2:</strong> Custom metrics reporting:
 *
 * <pre>{@code
 * // Report metrics to a monitoring system
 * Consumer<String> prometheusReporter = metric ->
 *     PrometheusClient.pushMetric("sink_stats", metric);
 *
 * SinkMetricsReporter reporter = new SinkMetricsReporter(
 *     registry, prometheusReporter);
 * reporter.reportMetrics();
 * }</pre>
 *
 * <p><strong>Example 3:</strong> Customizing all components:
 *
 * <pre>{@code
 * // Custom suppliers and reporting
 * SinkMetricsReporter reporter = new SinkMetricsReporter(
 *     () -> Set.of("sink1", "sink2"),
 *     name -> metricsService.fetchMetricsFor(name),
 *     metric -> slackNotifier.send("METRICS", metric));
 * reporter.reportMetrics();
 * }</pre>
 *
 * @param sinkNamesSupplier supplier of sink names
 * @param metricsFetcher function to fetch metrics for a sink name
 * @param reporter consumer for reporting metrics (defaults to logger if null)
 */
public record SinkMetricsReporter(
  Supplier<Set<String>> sinkNamesSupplier,
  Function<String, Map<String, Object>> metricsFetcher,
  Consumer<String> reporter
)
  implements MetricsReporter {
  private static final Logger LOGGER = System.getLogger(SinkMetricsReporter.class.getName());

  /**
   * Creates a sink metrics reporter with the specified registry and default logging.
   *
   * @param registry the message sink registry
   */
  public SinkMetricsReporter(final MessageSinkRegistry registry) {
    this(registry, null);
  }

  /**
   * Creates a sink metrics reporter with the specified registry and custom reporter.
   *
   * <p>Example with a custom reporter:
   *
   * <pre>{@code
   * // Send metrics to database
   * Consumer<String> dbReporter = metric ->
   *     jdbcTemplate.update("INSERT INTO metrics VALUES(?)", metric);
   *
   * var reporter = new SinkMetricsReporter(registry, dbReporter);
   * }</pre>
   *
   * @param registry the message sink registry
   * @param reporter consumer for reporting metrics (defaults to logger if null)
   */
  public SinkMetricsReporter(final MessageSinkRegistry registry, final Consumer<String> reporter) {
    this(() -> registry.getAll().keySet(), registry::getMetrics, reporter);
  }

  /**
   * Creates a sink metrics reporter with custom suppliers and reporter.
   *
   * <p>This constructor offers maximum flexibility for customizing behavior.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Custom implementation with filtered sinks
   * var reporter = new SinkMetricsReporter(
   *     () -> registry.getAll().keySet().stream()
   *              .filter(name -> name.startsWith("critical-"))
   *              .collect(Collectors.toSet()),
   *     name -> registry.getMetrics(name),
   *     metric -> {
   *         logger.info(metric);
   *         alertSystem.checkThresholds(metric);
   *     }
   * );
   * }</pre>
   *
   * @param sinkNamesSupplier supplier of sink names
   * @param metricsFetcher function to fetch metrics for a sink name
   * @param reporter consumer for reporting metrics (defaults to logger if null)
   */
  public SinkMetricsReporter(
    final Supplier<Set<String>> sinkNamesSupplier,
    final Function<String, Map<String, Object>> metricsFetcher,
    final Consumer<String> reporter
  ) {
    this.sinkNamesSupplier = sinkNamesSupplier;
    this.metricsFetcher = metricsFetcher;
    this.reporter = reporter != null ? reporter : this::logMetrics;
  }

  /**
   * Reports metrics for all sinks.
   *
   * <p>The reporting process:
   *
   * <ol>
   *   <li>Retrieves all sink names from the supplier
   *   <li>For each name, fetches its metrics using the metrics fetcher
   *   <li>Reports non-empty metrics using the configured reporter
   * </ol>
   *
   * <p>Exceptions during processing are caught and logged but don't interrupt the reporting flow.
   */
  public void reportMetrics() {
    try {
      sinkNamesSupplier
        .get()
        .forEach(sinkName -> {
          try {
            final var metrics = metricsFetcher.apply(sinkName);
            if (!metrics.isEmpty()) reporter.accept("Sink '%s' metrics: %s".formatted(sinkName, metrics));
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error retrieving metrics for sink: %s".formatted(sinkName), e);
          }
        });
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error retrieving sink registry", e);
    }
  }

  private void logMetrics(String metrics) {
    LOGGER.log(Level.INFO, metrics);
  }
}
