package org.kpipe.metrics;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.RegistryKey;

/// Reports metrics for message processors, allowing flexible composition of:
///
/// * How processor names are retrieved (via a {@link Supplier})
/// * How metrics are fetched for each processor (via a {@link Function})
/// * How metrics are reported (via a {@link Consumer})
///
/// **Example 1:** Basic usage with default logging:
///
/// ```java
/// // Creates reporter with default logging behavior
/// ProcessorMetricsReporter reporter = new ProcessorMetricsReporter(registry);
/// reporter.reportMetrics();
/// ```
///
/// **Example 2:** Custom metrics reporting:
///
/// ```java
/// // Report metrics to a monitoring system
/// Consumer<String> prometheusReporter = metric ->
///     PrometheusClient.pushMetric("processor_stats", metric);
///
/// final var reporter = new ProcessorMetricsReporter(
///     registry, prometheusReporter);
/// reporter.reportMetrics();
/// ```
///
/// **Example 3:** Customizing all components:
///
/// ```java
/// // Custom suppliers and reporting
/// final var reporter = new ProcessorMetricsReporter(
///     () -> Set.of("processor1", "processor2"),
///     name -> metricsService.fetchMetricsFor(name),
///     metric -> slackNotifier.send("METRICS", metric));
/// reporter.reportMetrics();
/// ```
///
/// @param processorNamesSupplier supplier of processor names
/// @param metricsFetcher function to fetch metrics for a processor name
/// @param reporter consumer for reporting metrics (defaults to logger if null)
public record ProcessorMetricsReporter(
  Supplier<Set<RegistryKey<?>>> processorNamesSupplier,
  Function<RegistryKey<?>, Map<String, Object>> metricsFetcher,
  Consumer<String> reporter
)
  implements MetricsReporter {
  private static final Logger LOGGER = System.getLogger(ProcessorMetricsReporter.class.getName());

  /// Creates a processor metrics reporter with the specified registry and default logging.
  ///
  /// @param registry the message processor registry
  public ProcessorMetricsReporter(final MessageProcessorRegistry registry) {
    this(registry, null);
  }

  /// Creates a processor metrics reporter with the specified registry and custom reporter.
  ///
  /// Example with a custom reporter:
  ///
  /// ```java
  /// // Send metrics to database
  /// Consumer<String> dbReporter = metric ->
  ///     jdbcTemplate.update("INSERT INTO metrics VALUES(?)", metric);
  ///
  /// final var reporter = new ProcessorMetricsReporter(registry, dbReporter);
  /// ```
  ///
  /// @param registry the message processor registry
  /// @param reporter consumer for reporting metrics (defaults to logger if null)
  public ProcessorMetricsReporter(final MessageProcessorRegistry registry, final Consumer<String> reporter) {
    this(() -> registry.getAll().keySet(), registry::getMetrics, reporter);
  }

  /// Creates a processor metrics reporter with custom suppliers and reporter.
  ///
  /// This constructor offers maximum flexibility for customizing behavior.
  ///
  /// Example:
  ///
  /// ```java
  /// // Custom implementation with filtered processors
  /// final var reporter = new ProcessorMetricsReporter(
  ///     () -> registry.getAll().keySet().stream()
  ///              .filter(name -> name.startsWith("critical-"))
  ///              .collect(Collectors.toSet()),
  ///     name -> registry.getMetrics(name),
  ///     metric -> {
  ///         logger.info(metric);
  ///         alertSystem.checkThresholds(metric);
  ///     }
  /// );
  /// ```
  ///
  /// @param processorNamesSupplier supplier of processor names
  /// @param metricsFetcher function to fetch metrics for a processor name
  /// @param reporter consumer for reporting metrics (defaults to logger if null)
  public ProcessorMetricsReporter(
    final Supplier<Set<RegistryKey<?>>> processorNamesSupplier,
    final Function<RegistryKey<?>, Map<String, Object>> metricsFetcher,
    final Consumer<String> reporter
  ) {
    this.processorNamesSupplier = processorNamesSupplier;
    this.metricsFetcher = metricsFetcher;
    this.reporter = reporter != null ? reporter : this::logMetrics;
  }

  /// Reports metrics for all processors.
  ///
  /// The reporting process:
  ///
  /// 1. Retrieves all processor names from the supplier
  /// 2. For each name, fetches its metrics using the metrics fetcher
  /// 3. Reports non-empty metrics using the configured reporter
  ///
  /// Exceptions during processing are caught and logged but don't interrupt the reporting flow.
  public void reportMetrics() {
    try {
      processorNamesSupplier
        .get()
        .forEach(key -> {
          try {
            final var metrics = metricsFetcher.apply(key);
            if (!metrics.isEmpty()) reporter.accept("Processor '%s' metrics: %s".formatted(key.name(), metrics));
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error retrieving metrics for processor: %s".formatted(key.name()), e);
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
