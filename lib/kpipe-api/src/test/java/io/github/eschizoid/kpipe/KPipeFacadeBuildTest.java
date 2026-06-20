package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.consumer.BackpressureController;
import io.github.eschizoid.kpipe.consumer.CircuitBreakerController;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.format.json.JsonFormat;
import io.github.eschizoid.kpipe.metrics.ConsumerMetrics;
import io.github.eschizoid.kpipe.producer.tracing.Tracer;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/// Verifies that each top-level [KPipe] factory returns a non-null [Stream] and that the chain
/// of fluent operations accumulates state correctly without requiring a real Kafka connection.
class KPipeFacadeBuildTest {

  private static Properties props() {
    final var props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "test-group");
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return props;
  }

  @Test
  void chainingProducesNewStreamPerStepAndAccumulatesOperators() {
    final var root = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props());

    final var chained = (DefaultStream<Map<String, Object>>) root
      .pipe(m -> {
        m.put("ts", 1L);
        return m;
      })
      .filter(m -> m.containsKey("ts"))
      .peek(_ -> {})
      .when(_ -> true, m -> m, m -> m);

    assertNotSame(root, chained, "immutable fluent chain should return a new instance per step");
    assertEquals(0, root.operators().size(), "root stream remains untouched");
    assertEquals(4, chained.operators().size());
  }

  @Test
  void withRetryAndBackpressureAndSequentialAreCaptured() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props())
      .withRetry(5, java.time.Duration.ofMillis(100))
      .withBackpressure(2_000, 1_000)
      .withProcessingMode(ProcessingMode.SEQUENTIAL);

    assertEquals(5, stream.maxRetries());
    assertEquals(java.time.Duration.ofMillis(100), stream.retryBackoff());
    assertEquals(2_000L, stream.backpressureHigh());
    assertEquals(1_000L, stream.backpressureLow());
    assertEquals(ProcessingMode.SEQUENTIAL, stream.processingMode());
  }

  @Test
  void defaultBackpressureUsesStandardWatermarks() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props()).withBackpressure();
    // Stream.withBackpressure() must produce the same defaults as the BackpressureController
    // constants. Hardcoded literals here would drift if the constants change; assert against the
    // single source of truth and pin the literal values as a separate smoke check.
    assertEquals(BackpressureController.DEFAULT_HIGH_WATERMARK, stream.backpressureHigh());
    assertEquals(BackpressureController.DEFAULT_LOW_WATERMARK, stream.backpressureLow());
    assertEquals(10_000L, stream.backpressureHigh());
    assertEquals(7_000L, stream.backpressureLow());
  }

  @Test
  void invalidWatermarksAreRejected() {
    final var stream = KPipe.json("topic", props());
    assertThrows(IllegalArgumentException.class, () -> stream.withBackpressure(100, 100));
    assertThrows(IllegalArgumentException.class, () -> stream.withBackpressure(0, -1));
    assertThrows(IllegalArgumentException.class, () -> stream.withBackpressure(50, 100));
  }

  @Test
  void negativeRetriesAreRejected() {
    final var stream = KPipe.json("topic", props());
    assertThrows(IllegalArgumentException.class, () -> stream.withRetry(-1, java.time.Duration.ZERO));
  }

  @Test
  void terminalSinkProducesNonNullSink() {
    final var ref = new AtomicReference<Map<String, Object>>();
    final Sink<Map<String, Object>> sink = KPipe.json("topic", props())
      .pipe(m -> m)
      .toCustom(ref::set);
    assertNotNull(sink);
    assertInstanceOf(DefaultSink.class, sink);
  }

  @Test
  void multiSinkRejectsEmpty() {
    final var stream = KPipe.json("topic", props());
    assertThrows(IllegalArgumentException.class, stream::toMulti);
  }

  @Test
  void facadeUsesJsonFormatInstance() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props());
    assertSame(JsonFormat.INSTANCE, stream.format());
  }

  @Test
  void chainingWithoutStartingDoesNotThrow() {
    assertDoesNotThrow(() ->
      KPipe.json("topic", props())
        .pipe(m -> m)
        .filter(_ -> true)
        .withRetry(2, java.time.Duration.ofMillis(50))
        .withBackpressure()
        .withProcessingMode(ProcessingMode.PARALLEL)
        .toCustom(_ -> {})
    );
  }

  @Test
  void multiBuilderRejectsPerRouteProcessingMode() {
    // Processing mode is a consumer-wide setting. If a route configurator sets KEY_ORDERED or
    // SEQUENTIAL, MultiBuilder.start() would silently drop it because only one mode wins on
    // the underlying consumer. We detect and fail loud at start() time.
    final var multi = KPipe.multi(props()).json("topic-a", s ->
      s.withProcessingMode(ProcessingMode.KEY_ORDERED).toCustom(_ -> {})
    );
    final var ex = assertThrows(IllegalArgumentException.class, multi::start);
    assertTrue(
      ex.getMessage().contains("KEY_ORDERED"),
      () -> "message should name the offending mode: " + ex.getMessage()
    );
    assertTrue(
      ex.getMessage().contains("MultiBuilder.withProcessingMode"),
      () -> "message should point users at the consumer-level setter: " + ex.getMessage()
    );
  }

  @Test
  void multiBuilderRejectsPerRouteKeyOrderedMaxKeys() {
    // Same footgun as withProcessingMode — withKeyOrderedMaxKeys on a route would be silently
    // dropped because the LRU cap is a consumer-wide setting. Fail loud at start().
    final var multi = KPipe.multi(props()).json("topic-a", s -> s.withKeyOrderedMaxKeys(50_000).toCustom(_ -> {}));
    final var ex = assertThrows(IllegalArgumentException.class, multi::start);
    assertTrue(ex.getMessage().contains("50000"), () -> "message should name the offending value: " + ex.getMessage());
    assertTrue(
      ex.getMessage().contains("MultiBuilder.withKeyOrderedMaxKeys"),
      () -> "message should point users at the consumer-level setter: " + ex.getMessage()
    );
  }

  @Test
  void multiBuilderAcceptsConsumerWideProcessingMode() {
    // The legitimate path: set processing mode on the MultiBuilder itself, leave routes default.
    // We're not calling .start() here because that would try to open a real Kafka connection;
    // chaining-without-start is sufficient to prove the API surface compiles and configures.
    assertDoesNotThrow(() ->
      KPipe.multi(props())
        .withProcessingMode(ProcessingMode.KEY_ORDERED)
        .withKeyOrderedMaxKeys(5_000)
        .json("topic-a", s -> s.toCustom(_ -> {}))
        .json("topic-b", s -> s.toCustom(_ -> {}))
    );
  }

  // --- per-route consumer-wide-setting rejection ---
  // Every public consumer-wide setting reachable from the per-route Stream is silently dropped
  // by the original MultiBuilder.start() — it only invokes ds.buildPipeline() / addBatchRoute()
  // and never replays the route's consumer-level config onto the underlying single
  // KPipeConsumer.Builder. The tests below pin each setting to fail loud rather than disappear.

  @Test
  void multiBuilderRejectsPerRouteWithRetry() {
    final var multi = KPipe.multi(props()).json("topic-a", s ->
      s.withRetry(3, Duration.ofSeconds(1)).toCustom(_ -> {})
    );
    final var ex = assertThrows(IllegalArgumentException.class, multi::start);
    assertTrue(ex.getMessage().contains("withRetry"), () -> "message should name withRetry: " + ex.getMessage());
    assertTrue(
      ex.getMessage().contains("'topic-a'"),
      () -> "message should name the offending route: " + ex.getMessage()
    );
    assertTrue(
      ex.getMessage().contains("KPipeConsumer.Builder"),
      () -> "message should point users at the explicit-builder escape hatch: " + ex.getMessage()
    );
  }

  @Test
  void multiBuilderRejectsPerRouteWithBackpressure() {
    final var multi = KPipe.multi(props()).json("topic-a", s -> s.withBackpressure(5_000, 1_000).toCustom(_ -> {}));
    final var ex = assertThrows(IllegalArgumentException.class, multi::start);
    assertTrue(
      ex.getMessage().contains("withBackpressure"),
      () -> "message should name withBackpressure: " + ex.getMessage()
    );
    assertTrue(ex.getMessage().contains("'topic-a'"), () -> "message should name the route: " + ex.getMessage());
  }

  @Test
  void multiBuilderRejectsPerRouteWithDeadLetterTopic() {
    final var multi = KPipe.multi(props()).json("topic-a", s -> s.withDeadLetterTopic("dlq").toCustom(_ -> {}));
    final var ex = assertThrows(IllegalArgumentException.class, multi::start);
    assertTrue(
      ex.getMessage().contains("withDeadLetterTopic"),
      () -> "message should name withDeadLetterTopic: " + ex.getMessage()
    );
    assertTrue(ex.getMessage().contains("'topic-a'"), () -> "message should name the route: " + ex.getMessage());
  }

  @Test
  void multiBuilderRejectsPerRouteWithErrorHandler() {
    final var multi = KPipe.multi(props()).json("topic-a", s -> s.withErrorHandler(_ -> {}).toCustom(_ -> {}));
    final var ex = assertThrows(IllegalArgumentException.class, multi::start);
    assertTrue(
      ex.getMessage().contains("withErrorHandler"),
      () -> "message should name withErrorHandler: " + ex.getMessage()
    );
    assertTrue(ex.getMessage().contains("'topic-a'"), () -> "message should name the route: " + ex.getMessage());
  }

  @Test
  void multiBuilderRejectsPerRouteWithPollTimeout() {
    final var multi = KPipe.multi(props()).json("topic-a", s ->
      s.withPollTimeout(Duration.ofMillis(250)).toCustom(_ -> {})
    );
    final var ex = assertThrows(IllegalArgumentException.class, multi::start);
    assertTrue(
      ex.getMessage().contains("withPollTimeout"),
      () -> "message should name withPollTimeout: " + ex.getMessage()
    );
    assertTrue(ex.getMessage().contains("'topic-a'"), () -> "message should name the route: " + ex.getMessage());
  }

  @Test
  void multiBuilderRejectsPerRouteWithMetrics() {
    final var multi = KPipe.multi(props()).json("topic-a", s -> s.withMetrics(ConsumerMetrics.noop()).toCustom(_ -> {}));
    final var ex = assertThrows(IllegalArgumentException.class, multi::start);
    assertTrue(ex.getMessage().contains("withMetrics"), () -> "message should name withMetrics: " + ex.getMessage());
    assertTrue(
      ex.getMessage().contains("MultiBuilder.withMetrics"),
      () -> "message should point users at the consumer-level setter: " + ex.getMessage()
    );
    assertTrue(ex.getMessage().contains("'topic-a'"), () -> "message should name the route: " + ex.getMessage());
  }

  @Test
  void multiBuilderRejectsPerRouteWithTracer() {
    final var multi = KPipe.multi(props()).json("topic-a", s -> s.withTracer(Tracer.noop()).toCustom(_ -> {}));
    final var ex = assertThrows(IllegalArgumentException.class, multi::start);
    assertTrue(ex.getMessage().contains("withTracer"), () -> "message should name withTracer: " + ex.getMessage());
    assertTrue(
      ex.getMessage().contains("MultiBuilder.withTracer"),
      () -> "message should point users at the consumer-level setter: " + ex.getMessage()
    );
    assertTrue(ex.getMessage().contains("'topic-a'"), () -> "message should name the route: " + ex.getMessage());
  }

  @Test
  void multiBuilderRejectsPerRouteWithCircuitBreaker() {
    final var breaker = new CircuitBreakerController(0.5, 100, Duration.ofSeconds(1));
    final var multi = KPipe.multi(props()).json("topic-a", s -> s.withCircuitBreaker(breaker).toCustom(_ -> {}));
    final var ex = assertThrows(IllegalArgumentException.class, multi::start);
    assertTrue(
      ex.getMessage().contains("withCircuitBreaker"),
      () -> "message should name withCircuitBreaker: " + ex.getMessage()
    );
    assertTrue(
      ex.getMessage().contains("MultiBuilder.withCircuitBreaker"),
      () -> "message should point users at the consumer-level setter: " + ex.getMessage()
    );
    assertTrue(ex.getMessage().contains("'topic-a'"), () -> "message should name the route: " + ex.getMessage());
  }

  @Test
  void multiBuilderAcceptsConsumerWideMetricsTracerAndCircuitBreaker() {
    // Smoke: the consumer-wide MultiBuilder setters still work and don't trip the per-route guard.
    final var breaker = new CircuitBreakerController(0.5, 100, Duration.ofSeconds(1));
    assertDoesNotThrow(() ->
      KPipe.multi(props())
        .withMetrics(ConsumerMetrics.noop())
        .withTracer(Tracer.noop())
        .withCircuitBreaker(breaker)
        .json("topic-a", s -> s.toCustom(_ -> {}))
        .json("topic-b", s -> s.toCustom(_ -> {}))
    );
  }
}
