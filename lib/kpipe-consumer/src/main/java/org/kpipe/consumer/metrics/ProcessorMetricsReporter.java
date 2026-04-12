package org.kpipe.consumer.metrics;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.kpipe.metrics.KPipeMetricsReporter;
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
/// final var reporter = ProcessorMetricsReporter.forRegistry(registry);
/// reporter.reportMetrics();
/// ```
///
/// **Example 2:** Custom metrics reporting:
///
/// ```java
/// // Report metrics to a monitoring system
/// final var reporter = ProcessorMetricsReporter.forRegistry(registry)
///     .toConsumer(metric -> PrometheusClient.pushMetric("processor_stats", metric));
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
) implements KPipeMetricsReporter {
  private static final Logger LOGGER = System.getLogger(ProcessorMetricsReporter.class.getName());

  public ProcessorMetricsReporter(
    final Supplier<Set<RegistryKey<?>>> processorNamesSupplier,
    final Function<RegistryKey<?>, Map<String, Object>> metricsFetcher,
    final Consumer<String> reporter
  ) {
    this.processorNamesSupplier = processorNamesSupplier;
    this.metricsFetcher = metricsFetcher;
    this.reporter = reporter != null ? reporter : this::logMetrics;
  }

  @Override
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

  private void logMetrics(final String metrics) {
    LOGGER.log(Level.INFO, metrics);
  }

  /// Creates a reporter for all processors in the registry.
  ///
  /// @param registry the message processor registry
  /// @return a new reporter that can be further customized
  public static ProcessorMetricsReporter forRegistry(final MessageProcessorRegistry registry) {
    return new ProcessorMetricsReporter(registry::getKeys, registry::getMetrics, null);
  }

  /// Creates a reporter for a specific subset of processors.
  ///
  /// @param registry the message processor registry
  /// @param keys the specific processor keys to report on
  /// @return a new reporter that can be further customized
  public static ProcessorMetricsReporter forRegistry(
    final MessageProcessorRegistry registry,
    final Set<RegistryKey<?>> keys
  ) {
    return new ProcessorMetricsReporter(() -> keys, registry::getMetrics, null);
  }

  /// Creates a new reporter with the specified output consumer.
  ///
  /// @param reporter the consumer for reporting metrics
  /// @return a new ProcessorMetricsReporter instance
  public ProcessorMetricsReporter toConsumer(final Consumer<String> reporter) {
    return new ProcessorMetricsReporter(this.processorNamesSupplier, this.metricsFetcher, reporter);
  }
}
