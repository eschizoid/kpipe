package org.kpipe;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.Message;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
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
    assertInstanceOf(DefaultStream.class, stream);
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
  void operatorsAreInvokedInAdditionOrder() {
    final var trace = new java.util.ArrayList<Integer>();
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props())
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

    Map<String, Object> v = new HashMap<>();
    for (final var op : ops) v = op.apply(v);
    assertEquals(List.of(1, 2, 3), trace);
  }

  @Test
  void withRetryAndBackpressureAndSequentialAreCaptured() {
    final var stream = (DefaultStream<Map<String, Object>>) KPipe.json("topic", props())
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
        .withSequentialProcessing(false)
        .toCustom(_ -> {})
    );
  }
}
