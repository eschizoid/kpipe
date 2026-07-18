package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.eschizoid.kpipe.metrics.ConsumerMetrics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/// Pins the pre-release hardening guards: a throwing user [ConsumerMetrics] must never crash a
/// record's processing, and the DLQ record must carry the original record's headers plus the
/// source-timestamp envelope header so operators can correlate and replay.
class HardeningGuardsTest {

  private static final String TOPIC = "hardening-topic";

  /// A user metrics implementation where EVERY callback throws. The guard wraps it at
  /// construction, so records must still process, mark, and count normally.
  private static final class ThrowingMetrics implements ConsumerMetrics {

    @Override
    public void recordMessageReceived() {
      throw new RuntimeException("metrics bug");
    }

    @Override
    public void recordMessageProcessed() {
      throw new RuntimeException("metrics bug");
    }

    @Override
    public void recordProcessingError() {
      throw new RuntimeException("metrics bug");
    }

    @Override
    public void recordProcessingDuration(final long millis) {
      throw new RuntimeException("metrics bug");
    }

    @Override
    public void recordBackpressurePause() {
      throw new RuntimeException("metrics bug");
    }

    @Override
    public void recordBackpressureTime(final long millis) {
      throw new RuntimeException("metrics bug");
    }
  }

  @Test
  void throwingUserMetricsMustNotCrashProcessingOrSkipOffsetMarking() {
    final var marked = new CopyOnWriteArrayList<Long>();
    final var offsetManager = new OffsetManager() {
      @Override
      public OffsetManager start() {
        return this;
      }

      @Override
      public OffsetManager stop() {
        return this;
      }

      @Override
      public void trackOffset(final ConsumerRecord<byte[], byte[]> record) {}

      @Override
      public void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record) {
        marked.add(record.offset());
      }

      @Override
      public void notifyCommitComplete(final String commitId, final boolean success) {}

      @Override
      public OffsetState getState() {
        return OffsetState.RUNNING;
      }

      @Override
      public boolean isRunning() {
        return true;
      }

      @Override
      public OffsetStatistics getStatistics() {
        return OffsetStatistics.empty();
      }

      @Override
      public void close() {}
    };

    try (
      final var consumer = KPipeConsumer.builder()
        .withProperties(byteProperties())
        .withTopic(TOPIC)
        .withPipeline(TestPipelines.sideEffect(v -> v))
        .withMetrics(new ThrowingMetrics())
        .withOffsetManager(offsetManager)
        .build()
    ) {
      final var record = new ConsumerRecord<>(TOPIC, 0, 5L, "k".getBytes(UTF_8), "v".getBytes(UTF_8));
      assertDoesNotThrow(() -> consumer.processRecord(record), "a throwing metrics impl must be swallowed");
      assertTrue(marked.contains(5L), "the record must still be marked processed");
      assertEquals(1L, consumer.getMetrics().get("messagesProcessed"), "internal counters unaffected");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void buildDoesNotMutateTheCallersProperties() {
    final var callerProps = byteProperties();
    // Seed a DIVERGENT key.deserializer: the pin would overwrite it, so if build() still mutated
    // the caller's object this assertion flips. (With the default ByteArray value the mutation
    // writes an identical value and the test can't see it.)
    callerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    final var before = (Properties) callerProps.clone();
    try (
      final var consumer = KPipeConsumer.builder()
        .withProperties(callerProps)
        .withTopic(TOPIC)
        .withPipeline(TestPipelines.sideEffect(v -> v))
        .build()
    ) {
      assertEquals(
        before,
        callerProps,
        "build() must not mutate the caller's Properties (the key.deserializer pin lands on an internal clone)"
      );
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void dlqRecordCarriesOriginalHeadersAndSourceTimestamp() throws Exception {
    final var producer = (Producer<byte[], byte[]>) mock(Producer.class);
    final var captor = ArgumentCaptor.forClass(ProducerRecord.class);
    when(producer.send(captor.capture())).thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));

    final var originalHeaders = new RecordHeaders(
      List.of(new RecordHeader("correlationId", "req-123".getBytes(UTF_8)))
    );
    final var record = new ConsumerRecord<>(
      TOPIC,
      0,
      7L,
      1234567890L,
      TimestampType.CREATE_TIME,
      -1,
      -1,
      "k".getBytes(UTF_8),
      "v".getBytes(StandardCharsets.UTF_8),
      originalHeaders,
      java.util.Optional.empty()
    );

    try (
      final var consumer = KPipeConsumer.builder()
        .withProperties(byteProperties())
        .withTopic(TOPIC)
        .withPipeline(
          TestPipelines.sideEffect(_ -> {
            throw new RuntimeException("always fails");
          })
        )
        .withRetry(0, Duration.ofMillis(1))
        .withDeadLetterTopic("dlq")
        .withKafkaProducer(producer)
        .build()
    ) {
      consumer.processRecord(record);
    }

    final var dlqRecord = captor.getValue();
    assertEquals(
      "req-123",
      new String(dlqRecord.headers().lastHeader("correlationId").value(), UTF_8),
      "original headers must survive into the DLQ record"
    );
    assertEquals(
      "1234567890",
      new String(dlqRecord.headers().lastHeader("x-dlq-source-timestamp").value(), UTF_8),
      "the original event timestamp must travel in the envelope"
    );
    assertEquals(
      String.valueOf(7L),
      new String(dlqRecord.headers().lastHeader("x-dlq-source-offset").value(), UTF_8)
    );
  }

  private static Properties byteProperties() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "hardening-group");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("enable.auto.commit", "false");
    return p;
  }
}
