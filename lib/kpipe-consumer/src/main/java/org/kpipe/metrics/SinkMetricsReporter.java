package org.kpipe.metrics;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.kpipe.registry.MessageSinkRegistry;
import org.kpipe.registry.RegistryKey;

/// Reports metrics for the message sinks, allowing flexible composition of:
///
/// * How sink names are retrieved (via a {@link Supplier})
/// * How metrics are fetched for each sink (via a {@link Function})
/// * How metrics are reported (via a {@link Consumer})
///
/// **Example 1:** Basic usage with default logging:
///
/// ```java
/// // Creates reporter with default logging behavior
/// final var reporter = SinkMetricsReporter.forRegistry(registry);
/// reporter.reportMetrics();
/// ```
///
/// **Example 2:** Custom metrics reporting:
///
/// ```java
/// // Report metrics to a monitoring system
/// final var reporter = SinkMetricsReporter.forRegistry(registry)
///     .toConsumer(metric -> PrometheusClient.pushMetric("sink_stats", metric));
/// reporter.reportMetrics();
/// ```
///
/// **Example 3:** Fluent usage with custom reporter:
///
/// ```java
/// final var reporter = SinkMetricsReporter.forRegistry(registry)
///     .toConsumer(System.out::println);
/// reporter.reportMetrics();
/// ```
///
/// @param sinkNamesSupplier supplier of sink names
/// @param metricsFetcher function to fetch metrics for a sink name
/// @param reporter consumer for reporting metrics (defaults to logger if null)
public record SinkMetricsReporter(
  Supplier<Set<RegistryKey<?>>> sinkNamesSupplier,
  Function<RegistryKey<?>, Map<String, Object>> metricsFetcher,
  Consumer<String> reporter
) implements KPipeMetricsReporter {
  private static final Logger LOGGER = System.getLogger(SinkMetricsReporter.class.getName());

  /// Creates a sink metrics reporter with custom components.
  ///
  /// This constructor is intended for internal use or advanced customization.
  /// Use {@link #forRegistry} for a more ergonomic API.
  ///
  /// @param sinkNamesSupplier supplier of sink names
  /// @param metricsFetcher function to fetch metrics for a sink name
  /// @param reporter consumer for reporting metrics (defaults to logger if null)
  public SinkMetricsReporter(
    final Supplier<Set<RegistryKey<?>>> sinkNamesSupplier,
    final Function<RegistryKey<?>, Map<String, Object>> metricsFetcher,
    final Consumer<String> reporter
  ) {
    this.sinkNamesSupplier = sinkNamesSupplier;
    this.metricsFetcher = metricsFetcher;
    this.reporter = reporter != null ? reporter : this::logMetrics;
  }

  /// Reports metrics for all sinks.
  ///
  /// The reporting process:
  ///
  /// 1. Retrieves all sink names from the supplier
  /// 2. For each name, fetches its metrics using the metrics fetcher
  /// 3. Reports non-empty metrics using the configured reporter
  ///
  /// Exceptions during processing are caught and logged but don't interrupt the reporting flow.
  public void reportMetrics() {
    try {
      sinkNamesSupplier
        .get()
        .forEach(key -> {
          try {
            final var metrics = metricsFetcher.apply(key);
            if (!metrics.isEmpty()) reporter.accept("Sink '%s' metrics: %s".formatted(key.name(), metrics));
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error retrieving metrics for sink: %s".formatted(key.name()), e);
          }
        });
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error retrieving sink registry", e);
    }
  }

  private void logMetrics(String metrics) {
    LOGGER.log(Level.INFO, metrics);
  }

  /// Creates a fluent builder-like starting point for a sink metrics reporter.
  ///
  /// @param registry the message sink registry
  /// @return a new reporter that can be further customized
  public static SinkMetricsReporter forRegistry(final MessageSinkRegistry registry) {
    return new SinkMetricsReporter(() -> registry.getAll().keySet(), registry::getMetrics, null);
  }

  /// Creates a fluent builder-like starting point for selective sink metrics reporting.
  ///
  /// @param registry the message sink registry
  /// @param keys the specific sink keys to report on
  /// @return a new reporter that can be further customized
  public static SinkMetricsReporter forRegistry(final MessageSinkRegistry registry, final Set<RegistryKey<?>> keys) {
    return new SinkMetricsReporter(() -> keys, registry::getMetrics, null);
  }

  /// Creates a new reporter with the specified consumer for output.
  ///
  /// @param reporter the consumer for reporting metrics
  /// @return a new SinkMetricsReporter instance
  public SinkMetricsReporter toConsumer(final Consumer<String> reporter) {
    return new SinkMetricsReporter(this.sinkNamesSupplier, this.metricsFetcher, reporter);
  }
}
