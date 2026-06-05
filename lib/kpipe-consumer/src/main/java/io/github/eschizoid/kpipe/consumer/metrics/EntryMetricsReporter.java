package io.github.eschizoid.kpipe.consumer.metrics;

import io.github.eschizoid.kpipe.metrics.KPipeMetricsReporter;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.RegistryKey;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/// Reports per-entry metrics from a [MessageProcessorRegistry] namespace — either operators or
/// sinks. Replaces the two structurally-identical reporters that existed in 1.12
/// (`ProcessorMetricsReporter` / `SinkMetricsReporter`); they differed only in the log-message
/// prefix.
///
/// Factory methods select the namespace:
///
/// ```java
/// // Report operator metrics
/// EntryMetricsReporter.forProcessors(registry).reportMetrics();
///
/// // Report sink metrics, with a subset of keys and a Prometheus consumer
/// EntryMetricsReporter.forSinks(registry, Set.of(dbSinkKey))
///   .toConsumer(metric -> prometheus.push("sink_stats", metric))
///   .reportMetrics();
/// ```
///
/// @param entryKind         short label for the entry kind ("Processor" or "Sink") — used as the
///                          log-message prefix and the reporter's identity in mixed pipelines
/// @param namesSupplier     supplier of the set of [RegistryKey]s to report on
/// @param metricsFetcher    function from a key to its metric snapshot
/// @param reporter          consumer for each formatted report line. Must be non-null — use
///                          [#LOGGING] for the default log-based behaviour, or supply your own
///                          via [#toConsumer(Consumer)]
public record EntryMetricsReporter(
  String entryKind,
  Supplier<Set<RegistryKey<?>>> namesSupplier,
  Function<RegistryKey<?>, Map<String, Object>> metricsFetcher,
  Consumer<String> reporter
) implements KPipeMetricsReporter {
  private static final Logger LOGGER = System.getLogger(EntryMetricsReporter.class.getName());

  /// Default reporter that logs each line at INFO via the package logger. Use as the `reporter`
  /// argument to the canonical constructor when you want log-based output.
  public static final Consumer<String> LOGGING = line -> LOGGER.log(Level.INFO, line);

  /// Canonical constructor; rejects null fields so the record is transparent (`reporter()` always
  /// returns the value the caller supplied).
  public EntryMetricsReporter {
    Objects.requireNonNull(entryKind, "entryKind cannot be null");
    Objects.requireNonNull(namesSupplier, "namesSupplier cannot be null");
    Objects.requireNonNull(metricsFetcher, "metricsFetcher cannot be null");
    Objects.requireNonNull(reporter, "reporter cannot be null — use EntryMetricsReporter.LOGGING for the default");
  }

  @Override
  public void reportMetrics() {
    try {
      namesSupplier
        .get()
        .forEach(key -> {
          try {
            final var metrics = metricsFetcher.apply(key);
            if (!metrics.isEmpty()) reporter.accept("%s '%s' metrics: %s".formatted(entryKind, key.name(), metrics));
          } catch (final Exception e) {
            LOGGER.log(
              Level.WARNING,
              "Error retrieving metrics for %s: %s".formatted(entryKind.toLowerCase(), key.name()),
              e
            );
          }
        });
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error retrieving %s registry".formatted(entryKind.toLowerCase()), e);
    }
  }

  /// Creates a reporter for every operator registered in `registry`, with default log-based
  /// output. Use [#forProcessors(MessageProcessorRegistry, Set)] for a specific subset.
  ///
  /// @param registry the processor registry whose operator namespace to report on
  /// @return a new reporter wired with the [#LOGGING] default output
  public static EntryMetricsReporter forProcessors(final MessageProcessorRegistry registry) {
    Objects.requireNonNull(registry, "registry cannot be null");
    return new EntryMetricsReporter("Processor", registry::getKeys, registry::getMetrics, LOGGING);
  }

  /// Creates a reporter for the given subset of operator keys, with default log-based output.
  ///
  /// @param registry the processor registry whose operator namespace to report on
  /// @param keys     the specific operator keys to include in each report
  /// @return a new reporter wired with the [#LOGGING] default output
  public static EntryMetricsReporter forProcessors(
    final MessageProcessorRegistry registry,
    final Set<RegistryKey<?>> keys
  ) {
    Objects.requireNonNull(registry, "registry cannot be null");
    Objects.requireNonNull(keys, "keys cannot be null");
    return new EntryMetricsReporter("Processor", () -> keys, registry::getMetrics, LOGGING);
  }

  /// Creates a reporter for every sink registered in `registry`, with default log-based output.
  ///
  /// @param registry the processor registry whose sink namespace to report on
  /// @return a new reporter wired with the [#LOGGING] default output
  public static EntryMetricsReporter forSinks(final MessageProcessorRegistry registry) {
    Objects.requireNonNull(registry, "registry cannot be null");
    return new EntryMetricsReporter("Sink", registry::getSinkKeys, registry::getSinkMetrics, LOGGING);
  }

  /// Creates a reporter for the given subset of sink keys, with default log-based output.
  ///
  /// @param registry the processor registry whose sink namespace to report on
  /// @param keys     the specific sink keys to include in each report
  /// @return a new reporter wired with the [#LOGGING] default output
  public static EntryMetricsReporter forSinks(final MessageProcessorRegistry registry, final Set<RegistryKey<?>> keys) {
    Objects.requireNonNull(registry, "registry cannot be null");
    Objects.requireNonNull(keys, "keys cannot be null");
    return new EntryMetricsReporter("Sink", () -> keys, registry::getSinkMetrics, LOGGING);
  }

  /// Returns a new reporter with the specified output consumer. The other fields are preserved.
  ///
  /// @param reporter the consumer to receive each formatted report line
  /// @return a new reporter routing output through `reporter`
  public EntryMetricsReporter toConsumer(final Consumer<String> reporter) {
    Objects.requireNonNull(reporter, "reporter cannot be null");
    return new EntryMetricsReporter(this.entryKind, this.namesSupplier, this.metricsFetcher, reporter);
  }
}
