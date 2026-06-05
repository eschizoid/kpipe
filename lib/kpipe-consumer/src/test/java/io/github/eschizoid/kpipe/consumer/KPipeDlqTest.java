package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.github.eschizoid.kpipe.producer.KPipeProducer;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KPipeDlqTest {

  private static final String TOPIC = "test-topic";
  private static final String DLQ_TOPIC = "test-dlq-topic";

  @Mock
  private Producer<String, byte[]> mockProducer;

  @Mock
  private Producer<byte[], byte[]> mockByteProducer;

  @Test
  @SuppressWarnings("unchecked")
  void shouldSendToDlqAfterMaxRetries() throws Exception {
    final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value".getBytes(StandardCharsets.UTF_8));
    when(mockProducer.send(any(ProducerRecord.class))).thenReturn(
      CompletableFuture.completedFuture(mock(RecordMetadata.class))
    );

    try (
      final var consumer = KPipeConsumer.<String>builder()
        .withProperties(byteProperties())
        .withTopic(TOPIC)
        .withPipeline(
          TestPipelines.sideEffect(v -> {
            throw new RuntimeException("fail");
          })
        )
        .withRetry(1, Duration.ofMillis(1))
        .withDeadLetterTopic(DLQ_TOPIC)
        .withKafkaProducer(mockProducer)
        .build()
    ) {
      consumer.processRecord(record);

      verify(mockProducer).send(
        argThat(r -> {
          assertEquals(DLQ_TOPIC, r.topic());
          assertEquals("key", r.key());
          assertEquals("value", new String(r.value(), StandardCharsets.UTF_8));
          return true;
        })
      );
    }
  }

  @Test
  void shouldNotSendToDlqIfProcessingSucceedsAfterRetry() throws Exception {
    final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value".getBytes(StandardCharsets.UTF_8));
    final var attempts = new AtomicInteger(0);

    try (
      final var consumer = KPipeConsumer.<String>builder()
        .withProperties(byteProperties())
        .withTopic(TOPIC)
        .withPipeline(
          TestPipelines.sideEffect(v -> {
            if (attempts.getAndIncrement() == 0) throw new RuntimeException("fail first time");
            return v;
          })
        )
        .withRetry(1, Duration.ofMillis(1))
        .withDeadLetterTopic(DLQ_TOPIC)
        .withKafkaProducer(mockProducer)
        .build()
    ) {
      consumer.processRecord(record);

      verify(mockProducer, never()).send(any(ProducerRecord.class));
      assertEquals(2, attempts.get());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldSendToDlqWhenPipelineReturnsNull() {
    final var record = new ConsumerRecord<>(
      TOPIC,
      0,
      200L,
      "key".getBytes(StandardCharsets.UTF_8),
      "value".getBytes(StandardCharsets.UTF_8)
    );
    when(mockByteProducer.send(any(ProducerRecord.class))).thenReturn(
      CompletableFuture.completedFuture(mock(RecordMetadata.class))
    );

    try (
      final var consumer = KPipeConsumer.<byte[]>builder()
        .withProperties(byteProperties())
        .withTopic(TOPIC)
        .withPipeline(nullDeserializePipeline())
        .withDeadLetterTopic(DLQ_TOPIC)
        .withKafkaProducer(mockByteProducer)
        .build()
    ) {
      consumer.processRecord(record);

      verify(mockByteProducer).send(
        argThat(r -> {
          assertEquals(DLQ_TOPIC, r.topic());
          return true;
        })
      );
    }
  }

  /// Verifies that `KPipeConsumer.handleProcessingError` invokes `KPipeProducer.send` (the DLQ
  /// path) exactly once per failed record, and that even when the DLQ send throws, the consumer
  /// continues to process subsequent records without deadlocking. The `sendToDlq` call is
  /// synchronous on the failure path, so we verify both: (1) the call is made, and (2) the
  /// pipeline keeps draining after a throwing send.
  @Test
  @SuppressWarnings("unchecked")
  void dlqSendIsSynchronousAndBlocksShutdownPath() throws Exception {
    final var record1 = new ConsumerRecord<>(TOPIC, 0, 100L, "k1", "v1".getBytes(StandardCharsets.UTF_8));
    final var record2 = new ConsumerRecord<>(TOPIC, 0, 101L, "k2", "v2".getBytes(StandardCharsets.UTF_8));

    // First send (record1's DLQ) throws; second send (record2's DLQ) succeeds. We exercise both
    // failure modes within the same consumer, ensuring no deadlock between them.
    when(mockProducer.send(any(ProducerRecord.class)))
      .thenThrow(new RuntimeException("dlq send boom"))
      .thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));

    try (
      final var consumer = KPipeConsumer.<String>builder()
        .withProperties(byteProperties())
        .withTopic(TOPIC)
        .withPipeline(
          TestPipelines.sideEffect(v -> {
            throw new RuntimeException("processor always fails");
          })
        )
        .withRetry(0, Duration.ofMillis(1))
        .withDeadLetterTopic(DLQ_TOPIC)
        .withKafkaProducer(mockProducer)
        .build()
    ) {
      // First failure → DLQ send throws inside KPipeProducer.sendToDlq, but the exception is
      // swallowed (returns false). The consumer must not propagate the exception.
      assertDoesNotThrow(() -> consumer.processRecord(record1));

      // Second failure → DLQ send succeeds. Proves the consumer is still operational and not
      // deadlocked or in a broken state after the previous throwing send.
      assertDoesNotThrow(() -> consumer.processRecord(record2));

      // Both records went to DLQ exactly once.
      verify(mockProducer, times(2)).send(any(ProducerRecord.class));
    }
  }

  /// Exercises the atomic [Builder#withDeadLetterQueue(String, KPipeProducer)] setter: a single
  /// call pairs the DLQ topic with a pre-built [KPipeProducer], and the consumer routes failures
  /// through that producer to that topic. The two-call alternative (`withDeadLetterTopic` +
  /// `withKafkaProducer`) is covered by the tests above; this one specifically pins the bundled
  /// shape so the convenience setter can't silently regress.
  @Test
  @SuppressWarnings("unchecked")
  void shouldSendToDlqViaBundledWithDeadLetterQueueSetter() throws Exception {
    final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value".getBytes(StandardCharsets.UTF_8));
    when(mockProducer.send(any(ProducerRecord.class))).thenReturn(
      CompletableFuture.completedFuture(mock(RecordMetadata.class))
    );

    // Wrap the mock raw Producer in a KPipeProducer so we can hand it to the bundled setter.
    // try-with-resources because KPipeProducer implements AutoCloseable — keeps the test honest
    // even though the mock doesn't own real resources today.
    try (
      final var kpipeProducer = KPipeProducer.<String, byte[]>builder().withProducer(mockProducer).build();
      final var consumer = KPipeConsumer.<String>builder()
        .withProperties(byteProperties())
        .withTopic(TOPIC)
        .withPipeline(
          TestPipelines.sideEffect(v -> {
            throw new RuntimeException("fail");
          })
        )
        .withRetry(0, Duration.ofMillis(1))
        .withDeadLetterQueue(DLQ_TOPIC, kpipeProducer)
        .build()
    ) {
      consumer.processRecord(record);

      // Same DLQ contract as the two-call form: topic + key + value preserved verbatim.
      // Compare bytes directly rather than decoding — the value is already in `record.value()`
      // and decoding through `new String(byte[])` would use the platform default charset.
      verify(mockProducer).send(
        argThat(r -> {
          assertEquals(DLQ_TOPIC, r.topic());
          assertEquals("key", r.key());
          assertArrayEquals(record.value(), r.value());
          return true;
        })
      );
    }
  }

  /// Negative test for the atomic setter: null topic must be rejected up front (not silently
  /// accepted and then explode at first failure).
  @Test
  void withDeadLetterQueueRejectsNullArgs() {
    try (final var kpipeProducer = KPipeProducer.<String, byte[]>builder().withProducer(mockProducer).build()) {
      final var builder = KPipeConsumer.<String>builder().withProperties(byteProperties()).withTopic(TOPIC);
      assertThrows(NullPointerException.class, () -> builder.withDeadLetterQueue(null, kpipeProducer));
      assertThrows(NullPointerException.class, () -> builder.withDeadLetterQueue(DLQ_TOPIC, null));
    }
  }

  private static MessagePipeline<String> nullDeserializePipeline() {
    return new MessagePipeline<>() {
      @Override
      public String deserialize(final byte[] data) {
        return null;
      }

      @Override
      public byte[] serialize(final String data) {
        return data.getBytes(StandardCharsets.UTF_8);
      }

      @Override
      public io.github.eschizoid.kpipe.registry.Result<String> process(final String data) {
        return io.github.eschizoid.kpipe.registry.Result.passed(data);
      }
    };
  }

  private static Properties stringProperties() {
    return buildProperties(
      "org.apache.kafka.common.serialization.StringDeserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    );
  }

  private static Properties byteProperties() {
    return buildProperties(
      "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    );
  }

  private static Properties buildProperties(final String keyDeserializer, final String valueDeserializer) {
    final var props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "test-group");
    props.put("key.deserializer", keyDeserializer);
    props.put("value.deserializer", valueDeserializer);
    return props;
  }
}
