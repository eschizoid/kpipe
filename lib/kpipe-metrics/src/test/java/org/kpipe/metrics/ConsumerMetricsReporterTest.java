package org.kpipe.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConsumerMetricsReporterTest {

  @Test
  void shouldReportMetricsToCustomConsumer() {
    final List<String> output = new ArrayList<>();
    final var reporter = ConsumerMetricsReporter.forConsumer(() ->
      Map.of("messagesReceived", 10L, "messagesProcessed", 8L, "processingErrors", 2L)
    ).toConsumer(output::add);

    reporter.reportMetrics();

    assertTrue(output.size() == 1);
    assertTrue(output.get(0).contains("messages received: 10"));
    assertTrue(output.get(0).contains("messages processed: 8"));
    assertTrue(output.get(0).contains("errors: 2"));
  }

  @Test
  void shouldIncludeBackpressureMetricsWhenPresent() {
    final List<String> output = new ArrayList<>();
    final var reporter = ConsumerMetricsReporter.forConsumer(() ->
      Map.of(
        "messagesReceived",
        5L,
        "messagesProcessed",
        5L,
        "processingErrors",
        0L,
        "backpressurePauseCount",
        3L,
        "backpressureTimeMs",
        150L
      )
    ).toConsumer(output::add);

    reporter.reportMetrics();

    assertTrue(output.get(0).contains("backpressure pauses: 3"));
    assertTrue(output.get(0).contains("backpressure time: 150 ms"));
  }

  @Test
  void shouldNotReportWhenMetricsAreEmpty() {
    final List<String> output = new ArrayList<>();
    final var reporter = ConsumerMetricsReporter.forConsumer(Map::of).toConsumer(output::add);

    reporter.reportMetrics();

    assertTrue(output.isEmpty());
  }

  @Test
  void shouldNotThrowWhenMetricsSupplierReturnsNull() {
    final var reporter = ConsumerMetricsReporter.forConsumer(() -> null);
    assertDoesNotThrow(reporter::reportMetrics);
  }

  @Test
  void shouldNotThrowWhenMetricsSupplierThrows() {
    final var reporter = ConsumerMetricsReporter.forConsumer(() -> {
      throw new RuntimeException("supplier failure");
    });
    assertDoesNotThrow(reporter::reportMetrics);
  }

  @Test
  void shouldDefaultToLoggerWhenNoConsumerProvided() {
    final var reporter = ConsumerMetricsReporter.forConsumer(() ->
      Map.of("messagesReceived", 1L, "messagesProcessed", 1L, "processingErrors", 0L)
    );
    assertDoesNotThrow(reporter::reportMetrics);
  }

  @Test
  void shouldUseCustomUptimeSupplier() {
    final List<String> output = new ArrayList<>();
    final var reporter = ConsumerMetricsReporter.forConsumer(
      () -> Map.of("messagesReceived", 1L, "messagesProcessed", 1L, "processingErrors", 0L),
      () -> 9999L
    ).toConsumer(output::add);

    reporter.reportMetrics();

    assertTrue(output.get(0).contains("uptime: 9999 ms"));
  }
}
