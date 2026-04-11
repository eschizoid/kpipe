package org.kpipe.metrics;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kpipe.registry.MessageSinkRegistry;
import org.kpipe.registry.RegistryKey;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SinkMetricsReporterTest {

  @Mock
  private MessageSinkRegistry registry;

  @Mock
  private Supplier<Set<RegistryKey<?>>> sinkNamesSupplier;

  @Mock
  private Function<RegistryKey<?>, Map<String, Object>> metricsFetcher;

  @Mock
  private Consumer<String> reporter;

  @Captor
  private ArgumentCaptor<String> reportCaptor;

  private SinkMetricsReporter metricsReporter;
  private Set<RegistryKey<?>> sinkKeys;
  private Map<String, Object> testMetrics;

  @BeforeEach
  void setUp() {
    sinkKeys = new HashSet<>(
      Arrays.asList(RegistryKey.of("sink1", byte[].class), RegistryKey.of("sink2", byte[].class))
    );

    testMetrics = new HashMap<>();
    testMetrics.put("delivered", 100);
    testMetrics.put("errors", 0);
  }

  @Test
  void shouldWorkWithFluentApi() {
    // Arrange
    final var sinkMap = new HashMap<RegistryKey<?>, Object>();
    for (final var key : sinkKeys) {
      sinkMap.put(key, new Object());
    }
    doReturn(sinkMap).when(registry).getAll();
    doReturn(testMetrics).when(registry).getMetrics(any(RegistryKey.class));

    // Act
    SinkMetricsReporter.forRegistry(registry).toConsumer(reporter).reportMetrics();

    // Assert
    verify(reporter, times(sinkKeys.size())).accept(anyString());
  }

  @Test
  void shouldSupportSelectiveReporting() {
    // Arrange
    final RegistryKey<?> selectedKey = RegistryKey.of("selected", byte[].class);
    final Set<RegistryKey<?>> selectedKeys = Collections.singleton(selectedKey);
    doReturn(testMetrics).when(registry).getMetrics(selectedKey);

    // Act
    final var reporterInstance = SinkMetricsReporter.forRegistry(registry, selectedKeys).toConsumer(reporter);
    reporterInstance.reportMetrics();

    // Assert
    verify(reporter, times(1)).accept(contains("selected"));
    verify(registry, never()).getAll();
  }
}
