package org.kpipe;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.kpipe.consumer.ProcessingMode;
import org.kpipe.format.json.JsonFormat;

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

  // Note: tests for "MultiBuilder rejects per-route withProcessingMode / withKeyOrderedMaxKeys"
  // used to live here. Those settings are no longer reachable on a route configurator's
  // RouteStream surface (compile-time impossible), so there's nothing left to test at runtime.

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
}
