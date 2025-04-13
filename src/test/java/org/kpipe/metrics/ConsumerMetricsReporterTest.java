package org.kpipe.metrics;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConsumerMetricsReporterTest {

  @Mock
  private Supplier<Map<String, Long>> metricsSupplier;

  @Mock
  private Supplier<Long> uptimeSupplier;

  @Mock
  private Consumer<String> reporter;

  @Captor
  private ArgumentCaptor<String> reportCaptor;

  private ConsumerMetricsReporter metricsReporter;
  private Map<String, Long> testMetrics;

  @BeforeEach
  void setUp() {
    testMetrics = new HashMap<>();
    testMetrics.put("messagesReceived", 100L);
    testMetrics.put("messagesProcessed", 95L);
    testMetrics.put("processingErrors", 5L);
  }

  @Test
  void shouldUseProvidedReporter() {
    // Arrange
    when(metricsSupplier.get()).thenReturn(testMetrics);
    when(uptimeSupplier.get()).thenReturn(60000L);
    metricsReporter = new ConsumerMetricsReporter(metricsSupplier, uptimeSupplier, reporter);

    // Act
    metricsReporter.reportMetrics();

    // Assert
    verify(reporter).accept(reportCaptor.capture());
    final var report = reportCaptor.getValue();
    assertTrue(report.contains("messages received: 100"));
    assertTrue(report.contains("messages processed: 95"));
    assertTrue(report.contains("errors: 5"));
    assertTrue(report.contains("uptime: 60000"));
  }

  @Test
  void shouldUseDefaultReporterWhenNull() {
    // Arrange
    when(metricsSupplier.get()).thenReturn(testMetrics);
    when(uptimeSupplier.get()).thenReturn(60000L);

    // Act
    metricsReporter = new ConsumerMetricsReporter(metricsSupplier, uptimeSupplier, null);

    // Assert
    assertDoesNotThrow(() -> metricsReporter.reportMetrics());
  }

  @Test
  void shouldHandleNoMetricsGracefully() {
    // Arrange
    when(metricsSupplier.get()).thenReturn(Collections.emptyMap());
    metricsReporter = new ConsumerMetricsReporter(metricsSupplier, uptimeSupplier, reporter);

    // Act
    metricsReporter.reportMetrics();

    // Assert
    verifyNoInteractions(reporter);
  }

  @Test
  void shouldHandleNullMetricsGracefully() {
    // Arrange
    when(metricsSupplier.get()).thenReturn(null);

    metricsReporter = new ConsumerMetricsReporter(metricsSupplier, uptimeSupplier, reporter);

    // Act
    metricsReporter.reportMetrics();

    // Assert
    verifyNoInteractions(reporter);
  }

  @Test
  void shouldHandleExceptionInMetricsSupplier() {
    // Arrange
    when(metricsSupplier.get()).thenThrow(new RuntimeException("Test exception"));

    // Act
    metricsReporter = new ConsumerMetricsReporter(metricsSupplier, uptimeSupplier, reporter);

    // Assert
    assertDoesNotThrow(() -> metricsReporter.reportMetrics());
    verifyNoInteractions(reporter);
  }
}
