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
  private Producer<String, String> mockProducer;

  @Mock
  private Producer<byte[], byte[]> mockByteProducer;

  @Test
  @SuppressWarnings("unchecked")
  void shouldSendToDlqAfterMaxRetries() throws Exception {
    final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value");
    when(mockProducer.send(any(ProducerRecord.class))).thenReturn(
      CompletableFuture.completedFuture(mock(RecordMetadata.class))
    );

    try (
      final var consumer = KPipeConsumer.<String, String>builder()
        .withProperties(stringProperties())
        .withTopic(TOPIC)
        .withProcessor(v -> {
          throw new RuntimeException("fail");
        })
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
          assertEquals("value", r.value());
          return true;
        })
      );
    }
  }

  @Test
  void shouldNotSendToDlqIfProcessingSucceedsAfterRetry() throws Exception {
    final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value");
    final var attempts = new AtomicInteger(0);

    try (
      final var consumer = KPipeConsumer.<String, String>builder()
        .withProperties(stringProperties())
        .withTopic(TOPIC)
        .withProcessor(v -> {
          if (attempts.getAndIncrement() == 0) throw new RuntimeException("fail first time");
          return v;
        })
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
      final var consumer = KPipeConsumer.<byte[], byte[]>builder()
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
      public String process(final String data) {
        return data;
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
