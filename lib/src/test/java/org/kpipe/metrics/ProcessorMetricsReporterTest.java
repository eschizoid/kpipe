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
import org.kpipe.registry.RegistryKey;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessorMetricsReporterTest {

  @Mock
  private MessageProcessorRegistry registry;

  @Mock
  private Supplier<Set<RegistryKey<?>>> processorNamesSupplier;

  @Mock
  private Function<RegistryKey<?>, Map<String, Object>> metricsFetcher;

  @Mock
  private Consumer<String> reporter;

  @Captor
  private ArgumentCaptor<String> reportCaptor;

  private ProcessorMetricsReporter metricsReporter;
  private Set<RegistryKey<?>> processorNames;
  private Map<String, Object> testMetrics;

  @BeforeEach
  void setUp() {
    // Setup processor names
    processorNames = new HashSet<>(
      Arrays.asList(RegistryKey.of("processor1", Object.class), RegistryKey.of("processor2", Object.class))
    );

    // Setup test metrics
    testMetrics = new HashMap<>();
    testMetrics.put("throughput", 100);
    testMetrics.put("errors", 5);
  }

  @Test
  void shouldCreateFromRegistryWithDefaultReporter() {
    // Arrange
    doReturn(processorNames).when(registry).getKeys();
    doReturn(testMetrics).when(registry).getMetrics(any(RegistryKey.class));

    metricsReporter = ProcessorMetricsReporter.forRegistry(registry);

    // Assert
    assertDoesNotThrow(() -> metricsReporter.reportMetrics());
  }

  @Test
  void shouldCreateFromRegistryWithCustomReporter() {
    // Arrange
    doReturn(processorNames).when(registry).getKeys();
    doReturn(testMetrics).when(registry).getMetrics(any(RegistryKey.class));

    metricsReporter = ProcessorMetricsReporter.forRegistry(registry).toConsumer(reporter);

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
    when(metricsFetcher.apply(any(RegistryKey.class))).thenReturn(testMetrics);

    metricsReporter = new ProcessorMetricsReporter(processorNamesSupplier, metricsFetcher, reporter);

    // Act
    metricsReporter.reportMetrics();

    // Assert
    verify(processorNamesSupplier).get();
    verify(metricsFetcher, times(processorNames.size())).apply(any(RegistryKey.class));
    verify(reporter, times(processorNames.size())).accept(anyString());
  }

  @Test
  void shouldHandleEmptyMetricsGracefully() {
    // Arrange
    when(processorNamesSupplier.get()).thenReturn(processorNames);
    when(metricsFetcher.apply(any(RegistryKey.class))).thenReturn(Collections.emptyMap());

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
    final var it = processorNames.iterator();
    final var p1 = it.next();
    final var p2 = it.next();

    when(metricsFetcher.apply(p1)).thenThrow(new RuntimeException("Test exception"));
    when(metricsFetcher.apply(p2)).thenReturn(testMetrics);

    metricsReporter = new ProcessorMetricsReporter(processorNamesSupplier, metricsFetcher, reporter);

    // Act
    assertDoesNotThrow(() -> metricsReporter.reportMetrics());

    // Assert
    verify(reporter, times(1)).accept(anyString());
  }

  @Test
  void shouldWorkWithFluentApi() {
    // Arrange
    doReturn(processorNames).when(registry).getKeys();
    doReturn(testMetrics).when(registry).getMetrics(any(RegistryKey.class));

    // Act
    ProcessorMetricsReporter.forRegistry(registry).toConsumer(reporter).reportMetrics();

    // Assert
    verify(reporter, times(processorNames.size())).accept(anyString());
  }

  @Test
  void shouldSupportSelectiveReporting() {
    // Arrange
    final RegistryKey<?> selectedKey = RegistryKey.of("selected", Object.class);
    final Set<RegistryKey<?>> selectedKeys = Collections.singleton(selectedKey);
    doReturn(testMetrics).when(registry).getMetrics(selectedKey);

    // Act
    final var reporterInstance = ProcessorMetricsReporter.forRegistry(registry, selectedKeys).toConsumer(reporter);
    reporterInstance.reportMetrics();

    // Assert
    verify(reporter, times(1)).accept(contains("selected"));
    verify(registry, never()).getKeys();
  }
}
