package io.github.eschizoid.kpipe;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.consumer.CircuitBreakerController;
import io.github.eschizoid.kpipe.consumer.ConsumerSettings;
import io.github.eschizoid.kpipe.consumer.ConsumerSettings.Setting;
import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.KPipeConsumerBuilder;
import io.github.eschizoid.kpipe.consumer.MetricsExporter;
import io.github.eschizoid.kpipe.consumer.OffsetStore;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import io.github.eschizoid.kpipe.tracing.Tracer;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Every consumer-wide setting is declared once, in [ConsumerSettings], and set the same way from
/// `Stream`, `MultiBuilder` and `KPipeConsumerBuilder`. The table names one call per surface for
/// each setting, and [#everySettingHasARow()] fails when a setting is added without one.
class ConsumerSettingsParityTest {

  private record Calls(
    UnaryOperator<Stream<byte[]>> stream,
    UnaryOperator<MultiBuilder> multi,
    UnaryOperator<KPipeConsumerBuilder> builder
  ) {}

  private static final MetricsExporter EXPORTER = _ -> MetricsExporter.Export.NONE;
  private static final CircuitBreakerController BREAKER = new CircuitBreakerController(0.5, 10, ofSeconds(1));
  private static final KPipeConsumer.ErrorHandler HANDLER = _ -> {};
  private static final Tracer TRACER = (_, _) -> Tracer.SpanScope.noop();
  private static final OffsetStore STORE = new OffsetStore() {
    @Override
    public Map<TopicPartition, Long> load(final Collection<TopicPartition> partitions) {
      return Map.of();
    }

    @Override
    public void commit(final Map<TopicPartition, Long> offsets) {}
  };

  private static final Map<Setting, Calls> CALLS = Map.ofEntries(
    entry(
      Setting.RETRY,
      new Calls(s -> s.withRetry(2, ofMillis(5)), m -> m.withRetry(2, ofMillis(5)), b -> b.withRetry(2, ofMillis(5)))
    ),
    entry(
      Setting.BACKPRESSURE,
      new Calls(s -> s.withBackpressure(20, 10), m -> m.withBackpressure(20, 10), b -> b.withBackpressure(20, 10))
    ),
    entry(
      Setting.PROCESSING_MODE,
      new Calls(
        s -> s.withProcessingMode(ProcessingMode.SEQUENTIAL),
        m -> m.withProcessingMode(ProcessingMode.SEQUENTIAL),
        b -> b.withProcessingMode(ProcessingMode.SEQUENTIAL)
      )
    ),
    entry(
      Setting.KEY_ORDERED_MAX_KEYS,
      new Calls(s -> s.withKeyOrderedMaxKeys(5), m -> m.withKeyOrderedMaxKeys(5), b -> b.withKeyOrderedMaxKeys(5))
    ),
    entry(
      Setting.METRICS,
      new Calls(s -> s.withMetrics(EXPORTER), m -> m.withMetrics(EXPORTER), b -> b.withMetrics(EXPORTER))
    ),
    entry(
      Setting.ATTEMPT_FAILED,
      new Calls(s -> s.onAttemptFailed(HANDLER), m -> m.onAttemptFailed(HANDLER), b -> b.onAttemptFailed(HANDLER))
    ),
    entry(
      Setting.ERROR_HANDLER,
      new Calls(s -> s.withErrorHandler(HANDLER), m -> m.withErrorHandler(HANDLER), b -> b.withErrorHandler(HANDLER))
    ),
    entry(
      Setting.DEAD_LETTER_TOPIC,
      new Calls(s -> s.withDeadLetterTopic("dlq"), m -> m.withDeadLetterTopic("dlq"), b -> b.withDeadLetterTopic("dlq"))
    ),
    entry(
      Setting.POLL_TIMEOUT,
      new Calls(
        s -> s.withPollTimeout(ofMillis(20)),
        m -> m.withPollTimeout(ofMillis(20)),
        b -> b.withPollTimeout(ofMillis(20))
      )
    ),
    entry(Setting.TRACER, new Calls(s -> s.withTracer(TRACER), m -> m.withTracer(TRACER), b -> b.withTracer(TRACER))),
    entry(
      Setting.CIRCUIT_BREAKER,
      new Calls(
        s -> s.withCircuitBreaker(BREAKER),
        m -> m.withCircuitBreaker(BREAKER),
        b -> b.withCircuitBreaker(BREAKER)
      )
    ),
    entry(
      Setting.METRICS_LOG,
      new Calls(
        s -> s.withMetricsLog(ofSeconds(1)),
        m -> m.withMetricsLog(ofSeconds(1)),
        b -> b.withMetricsLog(ofSeconds(1))
      )
    ),
    entry(
      Setting.SHUTDOWN_HOOK,
      new Calls(s -> s.withShutdownHook(true), m -> m.withShutdownHook(true), b -> b.withShutdownHook(true))
    ),
    entry(
      Setting.THREAD_TERMINATION_TIMEOUT,
      new Calls(
        s -> s.withThreadTerminationTimeout(ofSeconds(1)),
        m -> m.withThreadTerminationTimeout(ofSeconds(1)),
        b -> b.withThreadTerminationTimeout(ofSeconds(1))
      )
    ),
    entry(
      Setting.WAIT_FOR_MESSAGES_TIMEOUT,
      new Calls(
        s -> s.withWaitForMessagesTimeout(ofSeconds(1)),
        m -> m.withWaitForMessagesTimeout(ofSeconds(1)),
        b -> b.withWaitForMessagesTimeout(ofSeconds(1))
      )
    ),
    entry(
      Setting.COMMIT_INTERVAL,
      new Calls(
        s -> s.withCommitInterval(ofSeconds(1)),
        m -> m.withCommitInterval(ofSeconds(1)),
        b -> b.withCommitInterval(ofSeconds(1))
      )
    ),
    entry(
      Setting.OFFSET_STORE,
      new Calls(s -> s.withOffsetStore(STORE), m -> m.withOffsetStore(STORE), b -> b.withOffsetStore(STORE))
    )
  );

  @Test
  void everySettingHasARow() {
    assertEquals(EnumSet.allOf(Setting.class), CALLS.keySet());
  }

  /// One call records exactly its own setting, on every surface, with the same value, and that
  /// value differs from the default, so a surface that ignores its argument fails.
  @ParameterizedTest
  @EnumSource(Setting.class)
  void eachSurfaceRecordsTheSameSetting(final Setting setting) {
    final var calls = CALLS.get(setting);
    final var fromStream = ((DefaultStream<?>) calls.stream().apply(KPipe.bytes("orders", props()))).settings();
    final var fromMulti = calls.multi().apply(KPipe.multi(props())).settings();
    final var fromBuilder = calls.builder().apply(KPipeConsumer.builder()).settings();

    for (final var settings : List.of(fromStream, fromMulti, fromBuilder)) {
      assertEquals(Set.of(setting), settings.explicitlySet());
    }
    assertNotEquals(describe(ConsumerSettings.defaults()), describe(fromStream), "the call changes a value");
    assertEquals(describe(fromStream), describe(fromMulti));
    assertEquals(describe(fromStream), describe(fromBuilder));
  }

  /// A `MultiBuilder` route that sets a consumer-wide setting on its own stream is rejected, and
  /// the message names the call.
  @ParameterizedTest
  @EnumSource(Setting.class)
  void aRouteThatSetsItIsRejectedByName(final Setting setting) {
    final var calls = CALLS.get(setting);
    assertRejected(
      setting,
      KPipe.multi(props()).bytes("orders", s ->
        calls
          .stream()
          .apply(s)
          .toCustom(_ -> {})
      )
    );
  }

  /// A batch route is checked the same way.
  @ParameterizedTest
  @EnumSource(Setting.class)
  void aBatchRouteThatSetsItIsRejectedByName(final Setting setting) {
    final var calls = CALLS.get(setting);
    assertRejected(
      setting,
      KPipe.multi(props()).bytes("orders", s ->
        calls.stream().apply(s).toBatch(BatchSink.ofVoid(_ -> {}), BatchPolicy.ofSize(10))
      )
    );
  }

  private static void assertRejected(final Setting setting, final MultiBuilder multi) {
    final var thrown = assertThrows(IllegalArgumentException.class, multi::start);
    assertTrue(thrown.getMessage().contains("sets " + setting.method() + " on its Stream"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("MultiBuilder." + setting.method()), thrown.getMessage());
  }

  private static String describe(final ConsumerSettings s) {
    return String.join(
      "|",
      String.valueOf(s.maxRetries()),
      String.valueOf(s.retryBackoff()),
      String.valueOf(s.backpressureHigh()),
      String.valueOf(s.backpressureLow()),
      String.valueOf(s.processingMode()),
      String.valueOf(s.keyOrderedMaxKeys()),
      String.valueOf(s.metricsExporter()),
      String.valueOf(s.attemptFailedObserver()),
      String.valueOf(s.errorHandler()),
      String.valueOf(s.deadLetterTopic()),
      String.valueOf(s.pollTimeout()),
      String.valueOf(s.tracer()),
      String.valueOf(s.circuitBreaker()),
      String.valueOf(s.metricsLogInterval()),
      String.valueOf(s.shutdownHook()),
      String.valueOf(s.threadTerminationTimeout()),
      String.valueOf(s.waitForMessagesTimeout()),
      String.valueOf(s.commitInterval()),
      String.valueOf(s.offsetStore())
    );
  }

  private static Properties props() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "parity");
    return p;
  }
}
