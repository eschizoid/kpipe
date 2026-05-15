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

/// Reports per-entry metrics from a [MessageProcessorRegistry] namespace — either operators or
/// sinks. Replaces the two structurally-identical reporters that existed in 1.12
/// (`ProcessorMetricsReporter` / `SinkMetricsReporter`); they differed only in the log-message
/// prefix.
///
/// Factory methods select the namespace:
///
/// ```java
/// // Report operator metrics
/// final var processorReporter = EntryMetricsReporter.forProcessors(registry);
/// processorReporter.reportMetrics();
///
/// // Report sink metrics
/// final var sinkReporter = EntryMetricsReporter.forSinks(registry);
/// sinkReporter.reportMetrics();
///
/// // Custom output sink
/// EntryMetricsReporter.forProcessors(registry)
///   .toConsumer(metric -> PrometheusClient.pushMetric("processor_stats", metric))
///   .reportMetrics();
/// ```
///
/// @param entryKind         short label for the entry kind ("Processor" or "Sink") — used as the
///                          log-message prefix and the reporter's identity in mixed pipelines
/// @param namesSupplier     supplier of the set of [RegistryKey]s to report on
/// @param metricsFetcher    function from a key to its metric snapshot
/// @param reporter          consumer for each formatted report line (defaults to a logger when
///                          constructed with `null`)
public record EntryMetricsReporter(
  String entryKind,
  Supplier<Set<RegistryKey<?>>> namesSupplier,
  Function<RegistryKey<?>, Map<String, Object>> metricsFetcher,
  Consumer<String> reporter
) implements KPipeMetricsReporter {

  private static final Logger LOGGER = System.getLogger(EntryMetricsReporter.class.getName());

  /// Canonical constructor; defaults `reporter` to a logger when null.
  public EntryMetricsReporter(
    final String entryKind,
    final Supplier<Set<RegistryKey<?>>> namesSupplier,
    final Function<RegistryKey<?>, Map<String, Object>> metricsFetcher,
    final Consumer<String> reporter
  ) {
    this.entryKind = entryKind;
    this.namesSupplier = namesSupplier;
    this.metricsFetcher = metricsFetcher;
    this.reporter = reporter != null ? reporter : this::logMetrics;
  }

  @Override
  public void reportMetrics() {
    try {
      namesSupplier
        .get()
        .forEach(key -> {
          try {
            final var metrics = metricsFetcher.apply(key);
            if (!metrics.isEmpty()) {
              reporter.accept("%s '%s' metrics: %s".formatted(entryKind, key.name(), metrics));
            }
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error retrieving metrics for %s: %s".formatted(entryKind.toLowerCase(), key.name()), e);
          }
        });
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error retrieving %s registry".formatted(entryKind.toLowerCase()), e);
    }
  }

  private void logMetrics(final String metrics) {
    LOGGER.log(Level.INFO, metrics);
  }

  /// Creates a reporter for every operator registered in `registry`.
  public static EntryMetricsReporter forProcessors(final MessageProcessorRegistry registry) {
    return new EntryMetricsReporter("Processor", registry::getKeys, registry::getMetrics, null);
  }

  /// Creates a reporter for a specific subset of operators.
  public static EntryMetricsReporter forProcessors(
    final MessageProcessorRegistry registry,
    final Set<RegistryKey<?>> keys
  ) {
    return new EntryMetricsReporter("Processor", () -> keys, registry::getMetrics, null);
  }

  /// Creates a reporter for every sink registered in `registry`.
  public static EntryMetricsReporter forSinks(final MessageProcessorRegistry registry) {
    return new EntryMetricsReporter("Sink", registry::getSinkKeys, registry::getSinkMetrics, null);
  }

  /// Creates a reporter for a specific subset of sinks.
  public static EntryMetricsReporter forSinks(
    final MessageProcessorRegistry registry,
    final Set<RegistryKey<?>> keys
  ) {
    return new EntryMetricsReporter("Sink", () -> keys, registry::getSinkMetrics, null);
  }

  /// Returns a new reporter with the specified output consumer.
  public EntryMetricsReporter toConsumer(final Consumer<String> reporter) {
    return new EntryMetricsReporter(this.entryKind, this.namesSupplier, this.metricsFetcher, reporter);
  }
}
