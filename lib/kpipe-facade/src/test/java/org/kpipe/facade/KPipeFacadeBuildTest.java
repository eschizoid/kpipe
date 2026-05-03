package org.kpipe.facade;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Message;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.kpipe.Sink;
import org.kpipe.Stream;
import org.kpipe.format.json.JsonFormat;
import org.kpipe.registry.MessageFormat;

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
  void jsonFactoryReturnsStream() {
    final Stream<Map<String, Object>> stream = KPipe.json("events", props());
    assertNotNull(stream);
    assertTrue(stream instanceof DefaultStream<?>);
  }

  @Test
  void avroFactoryReturnsStream() {
    final Stream<GenericRecord> stream = KPipe.avro("events", props());
    assertNotNull(stream);
  }

  @Test
  void protobufFactoryReturnsStream() {
    final Stream<Message> stream = KPipe.protobuf("events", props());
    assertNotNull(stream);
  }

  @Test
  void bytesFactoryReturnsStream() {
    final Stream<byte[]> stream = KPipe.bytes("events", props());
    assertNotNull(stream);
  }

  @Test
  void customFactoryReturnsStream() {
    final Stream<byte[]> stream = KPipe.custom("events", props(), MessageFormat.bytes());
    assertNotNull(stream);
  }

  @Test
  void chainingPreservesStreamInstanceAndAccumulatesOperators() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props());

    final var chained = stream
      .pipe(m -> {
        m.put("ts", 1L);
        return m;
      })
      .filter(m -> m.containsKey("ts"))
      .peek(_ -> {})
      .when(_ -> true, m -> m, m -> m);

    assertSame(stream, chained, "fluent chain should return the same DefaultStream instance");
    assertEquals(4, stream.operators().size());
  }

  @Test
  void operatorsAreInvokedInAdditionOrder() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props());
    final var trace = new java.util.ArrayList<Integer>();

    stream
      .pipe(m -> {
        trace.add(1);
        return m;
      })
      .pipe(m -> {
        trace.add(2);
        return m;
      })
      .pipe(m -> {
        trace.add(3);
        return m;
      });

    final var ops = stream.operators();
    assertEquals(3, ops.size());

    final Map<String, Object> seed = new HashMap<>();
    Map<String, Object> v = seed;
    for (final var op : ops) v = op.apply(v);
    assertEquals(java.util.List.of(1, 2, 3), trace);
  }

  @Test
  void withRetryAndBackpressureAndSequentialAreCaptured() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props());

    stream
      .withRetry(5, java.time.Duration.ofMillis(100))
      .withBackpressure(2_000, 1_000)
      .withSequentialProcessing(true);

    assertEquals(5, stream.maxRetries());
    assertEquals(java.time.Duration.ofMillis(100), stream.retryBackoff());
    assertEquals(2_000L, stream.backpressureHigh());
    assertEquals(1_000L, stream.backpressureLow());
    assertTrue(stream.sequentialProcessing());
  }

  @Test
  void defaultBackpressureUsesStandardWatermarks() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props());
    stream.withBackpressure();
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
    assertTrue(sink instanceof DefaultSink<?>);
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
        .withSequentialProcessing(false)
        .toCustom(_ -> {})
    );
  }
}
