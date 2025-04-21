package org.kpipe.metrics;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.kpipe.registry.MessageProcessorRegistry;

/**
 * Reports metrics for message processors.
 *
 * <p>This class follows a functional approach, allowing flexible composition of:
 *
 * <ul>
 *   <li>How processor names are retrieved (via a {@link Supplier})
 *   <li>How metrics are fetched for each processor (via a {@link Function})
 *   <li>How metrics are reported (via a {@link Consumer})
 * </ul>
 *
 * <p><strong>Example 1:</strong> Basic usage with default logging:
 *
 * <pre>{@code
 * // Creates reporter with default logging behavior
 * ProcessorMetricsReporter reporter = new ProcessorMetricsReporter(registry);
 * reporter.reportMetrics();
 * }</pre>
 *
 * <p><strong>Example 2:</strong> Custom metrics reporting:
 *
 * <pre>{@code
 * // Report metrics to a monitoring system
 * Consumer<String> prometheusReporter = metric ->
 *     PrometheusClient.pushMetric("processor_stats", metric);
 *
 * ProcessorMetricsReporter reporter = new ProcessorMetricsReporter(
 *     registry, prometheusReporter);
 * reporter.reportMetrics();
 * }</pre>
 *
 * <p><strong>Example 3:</strong> Customizing all components:
 *
 * <pre>{@code
 * // Custom suppliers and reporting
 * ProcessorMetricsReporter reporter = new ProcessorMetricsReporter(
 *     () -> Set.of("processor1", "processor2"),
 *     name -> metricsService.fetchMetricsFor(name),
 *     metric -> slackNotifier.send("METRICS", metric));
 * reporter.reportMetrics();
 * }</pre>
 */
public class ProcessorMetricsReporter implements MetricsReporter {

  private static final Logger LOGGER = System.getLogger(ProcessorMetricsReporter.class.getName());

  private final Supplier<Set<String>> processorNamesSupplier;
  private final Function<String, Map<String, Object>> metricsFetcher;
  private final Consumer<String> reporter;

  /**
   * Creates a processor metrics reporter with the specified registry and default logging.
   *
   * @param registry the message processor registry
   */
  public ProcessorMetricsReporter(final MessageProcessorRegistry registry) {
    this(registry, null);
  }

  /**
   * Creates a processor metrics reporter with the specified registry and custom reporter.
   *
   * <p>Example with custom reporter:
   *
   * <pre>{@code
   * // Send metrics to database
   * Consumer<String> dbReporter = metric ->
   *     jdbcTemplate.update("INSERT INTO metrics VALUES(?)", metric);
   *
   * var reporter = new ProcessorMetricsReporter(registry, dbReporter);
   * }</pre>
   *
   * @param registry the message processor registry
   * @param reporter consumer for reporting metrics (defaults to logger if null)
   */
  public ProcessorMetricsReporter(final MessageProcessorRegistry registry, final Consumer<String> reporter) {
    this(() -> registry.getAll().keySet(), registry::getMetrics, reporter);
  }

  /**
   * Creates a processor metrics reporter with custom suppliers and reporter.
   *
   * <p>This constructor offers maximum flexibility for customizing behavior.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Custom implementation with filtered processors
   * var reporter = new ProcessorMetricsReporter(
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
   * @param processorNamesSupplier supplier of processor names
   * @param metricsFetcher function to fetch metrics for a processor name
   * @param reporter consumer for reporting metrics (defaults to logger if null)
   */
  public ProcessorMetricsReporter(
    final Supplier<Set<String>> processorNamesSupplier,
    final Function<String, Map<String, Object>> metricsFetcher,
    final Consumer<String> reporter
  ) {
    this.processorNamesSupplier = processorNamesSupplier;
    this.metricsFetcher = metricsFetcher;
    this.reporter = reporter != null ? reporter : this::logMetrics;
  }

  /**
   * Reports metrics for all processors.
   *
   * <p>The reporting process:
   *
   * <ol>
   *   <li>Retrieves all processor names from the supplier
   *   <li>For each name, fetches its metrics using the metrics fetcher
   *   <li>Reports non-empty metrics using the configured reporter
   * </ol>
   *
   * <p>Exceptions during processing are caught and logged but don't interrupt the reporting flow.
   */
  public void reportMetrics() {
    try {
      processorNamesSupplier
        .get()
        .forEach(processorName -> {
          try {
            final var metrics = metricsFetcher.apply(processorName);
            if (!metrics.isEmpty()) {
              reporter.accept("Processor '%s' metrics: %s".formatted(processorName, metrics));
            }
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error retrieving metrics for processor: %s".formatted(processorName), e);
          }
        });
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error retrieving processor registry", e);
    }
  }

  private void logMetrics(String metrics) {
    LOGGER.log(Level.INFO, metrics);
  }
}
