package org.kpipe.consumer.metrics;

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
class EntryMetricsReporterTest {

  @Mock
  private MessageProcessorRegistry registry;

  @Mock
  private Supplier<Set<RegistryKey<?>>> namesSupplier;

  @Mock
  private Function<RegistryKey<?>, Map<String, Object>> metricsFetcher;

  @Mock
  private Consumer<String> reporter;

  @Captor
  private ArgumentCaptor<String> reportCaptor;

  private Set<RegistryKey<?>> keys;
  private Map<String, Object> testMetrics;

  @BeforeEach
  void setUp() {
    keys = new HashSet<>(
      Arrays.asList(RegistryKey.of("entry1", Object.class), RegistryKey.of("entry2", Object.class))
    );

    testMetrics = new HashMap<>();
    testMetrics.put("throughput", 100);
    testMetrics.put("errors", 5);
  }

  // ───────────────────────────── forProcessors ─────────────────────────────

  @Test
  void forProcessors_emitsOneLinePerKeyUsingProcessorPrefix() {
    doReturn(keys).when(registry).getKeys();
    doReturn(testMetrics).when(registry).getMetrics(any(RegistryKey.class));

    EntryMetricsReporter.forProcessors(registry).toConsumer(reporter).reportMetrics();

    verify(reporter, times(keys.size())).accept(reportCaptor.capture());
    for (final var line : reportCaptor.getAllValues()) {
      assertTrue(line.startsWith("Processor '"), "expected processor-prefix log line, got: " + line);
      assertTrue(line.contains("metrics: {"));
    }
  }

  @Test
  void forProcessors_subset_doesNotQueryAllKeys() {
    final RegistryKey<?> selectedKey = RegistryKey.of("selected", Object.class);
    final Set<RegistryKey<?>> selected = Collections.singleton(selectedKey);
    doReturn(testMetrics).when(registry).getMetrics(selectedKey);

    EntryMetricsReporter.forProcessors(registry, selected).toConsumer(reporter).reportMetrics();

    verify(reporter, times(1)).accept(contains("selected"));
    verify(registry, never()).getKeys();
  }

  @Test
  void forProcessors_defaultsToLoggerWhenConsumerIsNull() {
    doReturn(keys).when(registry).getKeys();
    doReturn(testMetrics).when(registry).getMetrics(any(RegistryKey.class));

    assertDoesNotThrow(() -> EntryMetricsReporter.forProcessors(registry).reportMetrics());
  }

  // ─────────────────────────────── forSinks ────────────────────────────────

  @Test
  void forSinks_emitsOneLinePerKeyUsingSinkPrefix() {
    doReturn(keys).when(registry).getSinkKeys();
    doReturn(testMetrics).when(registry).getSinkMetrics(any(RegistryKey.class));

    EntryMetricsReporter.forSinks(registry).toConsumer(reporter).reportMetrics();

    verify(reporter, times(keys.size())).accept(reportCaptor.capture());
    for (final var line : reportCaptor.getAllValues()) {
      assertTrue(line.startsWith("Sink '"), "expected sink-prefix log line, got: " + line);
      assertTrue(line.contains("metrics: {"));
    }
  }

  @Test
  void forSinks_subset_doesNotQueryAllKeys() {
    final RegistryKey<?> selectedKey = RegistryKey.of("selectedSink", Object.class);
    final Set<RegistryKey<?>> selected = Collections.singleton(selectedKey);
    doReturn(testMetrics).when(registry).getSinkMetrics(selectedKey);

    EntryMetricsReporter.forSinks(registry, selected).toConsumer(reporter).reportMetrics();

    verify(reporter, times(1)).accept(contains("selectedSink"));
    verify(registry, never()).getSinkKeys();
  }

  // ─────────────────────────── Shared behaviour ────────────────────────────

  @Test
  void emptyMetricsAreNotReported() {
    when(namesSupplier.get()).thenReturn(keys);
    when(metricsFetcher.apply(any(RegistryKey.class))).thenReturn(Collections.emptyMap());

    final var rep = new EntryMetricsReporter("Processor", namesSupplier, metricsFetcher, reporter);
    rep.reportMetrics();

    verify(reporter, never()).accept(anyString());
  }

  @Test
  void exceptionInNamesSupplierIsSwallowed() {
    when(namesSupplier.get()).thenThrow(new RuntimeException("boom"));

    final var rep = new EntryMetricsReporter("Processor", namesSupplier, metricsFetcher, reporter);

    assertDoesNotThrow(rep::reportMetrics);
    verifyNoInteractions(reporter);
  }

  @Test
  void exceptionFetchingOneEntryDoesNotStopTheOthers() {
    when(namesSupplier.get()).thenReturn(keys);
    final var it = keys.iterator();
    final var k1 = it.next();
    final var k2 = it.next();
    when(metricsFetcher.apply(k1)).thenThrow(new RuntimeException("boom"));
    when(metricsFetcher.apply(k2)).thenReturn(testMetrics);

    final var rep = new EntryMetricsReporter("Sink", namesSupplier, metricsFetcher, reporter);

    assertDoesNotThrow(rep::reportMetrics);
    verify(reporter, times(1)).accept(anyString());
  }

  @Test
  void toConsumer_swapsOutputWithoutMutatingOriginal() {
    doReturn(keys).when(registry).getKeys();
    doReturn(testMetrics).when(registry).getMetrics(any(RegistryKey.class));

    final var original = EntryMetricsReporter.forProcessors(registry);
    final var withCustom = original.toConsumer(reporter);

    withCustom.reportMetrics();
    verify(reporter, times(keys.size())).accept(anyString());
    // original is independent — using its own (logger) consumer
    assertNotSame(original.reporter(), withCustom.reporter());
  }
}
