package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
import org.kpipe.registry.MessagePipeline;
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
    final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value".getBytes());
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
          assertEquals("value", new String(r.value()));
          return true;
        })
      );
    }
  }

  @Test
  void shouldNotSendToDlqIfProcessingSucceedsAfterRetry() throws Exception {
    final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value".getBytes());
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
    final var record = new ConsumerRecord<>(TOPIC, 0, 200L, "key".getBytes(), "value".getBytes());
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
    final var record1 = new ConsumerRecord<>(TOPIC, 0, 100L, "k1", "v1".getBytes());
    final var record2 = new ConsumerRecord<>(TOPIC, 0, 101L, "k2", "v2".getBytes());

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

  private static MessagePipeline<String> nullDeserializePipeline() {
    return new MessagePipeline<>() {
      @Override
      public String deserialize(final byte[] data) {
        return null;
      }

      @Override
      public byte[] serialize(final String data) {
        return data.getBytes();
      }

      @Override
      public org.kpipe.registry.Result<String> process(final String data) {
        return org.kpipe.registry.Result.passed(data);
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
