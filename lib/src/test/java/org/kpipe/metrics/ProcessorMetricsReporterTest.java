package org.kpipe.metrics;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kpipe.registry.MessageProcessorRegistry;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessorMetricsReporterTest {

  @Mock
  private MessageProcessorRegistry registry;

  @Mock
  private Supplier<Set<String>> processorNamesSupplier;

  @Mock
  private Function<String, Map<String, Object>> metricsFetcher;

  @Mock
  private Consumer<String> reporter;

  @Captor
  private ArgumentCaptor<String> reportCaptor;

  private ProcessorMetricsReporter metricsReporter;
  private Set<String> processorNames;
  private Map<String, Object> testMetrics;

  @BeforeEach
  void setUp() {
    // Setup processor names
    processorNames = new HashSet<>(Arrays.asList("processor1", "processor2"));

    // Setup test metrics
    testMetrics = new HashMap<>();
    testMetrics.put("throughput", 100);
    testMetrics.put("errors", 5);
  }

  @Test
  void shouldCreateFromRegistryWithDefaultReporter() {
    // Arrange
    final var processorMap = new HashMap<>();
    for (String name : processorNames) {
      processorMap.put(name, new Object());
    }
    doReturn(processorMap).when(registry).getAll();
    doReturn(testMetrics).when(registry).getMetrics(anyString());

    metricsReporter = new ProcessorMetricsReporter(registry);

    // Assert
    assertDoesNotThrow(() -> metricsReporter.reportMetrics());
  }

  @Test
  void shouldCreateFromRegistryWithCustomReporter() {
    // Arrange
    final var processorMap = new HashMap<>();
    for (final var name : processorNames) {
      processorMap.put(name, new Object());
    }
    doReturn(processorMap).when(registry).getAll();
    doReturn(testMetrics).when(registry).getMetrics(anyString());

    metricsReporter = new ProcessorMetricsReporter(registry, reporter);

    // Act
    metricsReporter.reportMetrics();

    // Assert
    verify(reporter, times(processorNames.size())).accept(reportCaptor.capture());
    final var reports = reportCaptor.getAllValues();
    assertEquals(processorNames.size(), reports.size());
    for (final var report : reports) {
      assertTrue(report.contains("Processor '"));
      assertTrue(report.contains("metrics: {"));
    }
  }

  @Test
  void shouldCreateWithFullCustomization() {
    // Arrange
    when(processorNamesSupplier.get()).thenReturn(processorNames);
    when(metricsFetcher.apply(anyString())).thenReturn(testMetrics);

    metricsReporter = new ProcessorMetricsReporter(processorNamesSupplier, metricsFetcher, reporter);

    // Act
    metricsReporter.reportMetrics();

    // Assert
    verify(processorNamesSupplier).get();
    verify(metricsFetcher, times(processorNames.size())).apply(anyString());
    verify(reporter, times(processorNames.size())).accept(anyString());
  }

  @Test
  void shouldHandleEmptyMetricsGracefully() {
    // Arrange
    when(processorNamesSupplier.get()).thenReturn(processorNames);
    when(metricsFetcher.apply(anyString())).thenReturn(Collections.emptyMap());

    metricsReporter = new ProcessorMetricsReporter(processorNamesSupplier, metricsFetcher, reporter);

    // Act
    metricsReporter.reportMetrics();

    // Assert
    verify(reporter, never()).accept(anyString());
  }

  @Test
  void shouldHandleExceptionInProcessorNamesSupplier() {
    // Arrange
    when(processorNamesSupplier.get()).thenThrow(new RuntimeException("Test exception"));

    metricsReporter = new ProcessorMetricsReporter(processorNamesSupplier, metricsFetcher, reporter);

    // Act
    assertDoesNotThrow(() -> metricsReporter.reportMetrics());

    // Assert
    verifyNoInteractions(reporter);
  }

  @Test
  void shouldHandleExceptionInMetricsFetcher() {
    // Arrange
    when(processorNamesSupplier.get()).thenReturn(processorNames);
    when(metricsFetcher.apply("processor1")).thenThrow(new RuntimeException("Test exception"));
    when(metricsFetcher.apply("processor2")).thenReturn(testMetrics);

    metricsReporter = new ProcessorMetricsReporter(processorNamesSupplier, metricsFetcher, reporter);

    // Act
    assertDoesNotThrow(() -> metricsReporter.reportMetrics());

    // Assert
    verify(reporter, times(1)).accept(anyString());
  }
}
