package org.kpipe.sink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaMessageSinkTest {

  private static final String TOPIC = "sink-topic";

  @Mock
  private Producer<byte[], byte[]> mockProducer;

  @Test
  @SuppressWarnings("unchecked")
  void shouldSendToKafka() {
    // Arrange
    final var future = CompletableFuture.completedFuture(mock(RecordMetadata.class));
    when(mockProducer.send(any(ProducerRecord.class))).thenReturn(future);

    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes);

    // Act
    sink.accept("hello");

    // Assert
    verify(mockProducer, times(1)).send(
      argThat(record -> {
        assertEquals(TOPIC, record.topic());
        assertArrayEquals("hello".getBytes(), record.value());
        assertNull(record.key());
        return true;
      })
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldSendWithKey() {
    // Arrange
    final var future = CompletableFuture.completedFuture(mock(RecordMetadata.class));
    when(mockProducer.send(any(ProducerRecord.class))).thenReturn(future);

    final KafkaMessageSink<String> sink = new KafkaMessageSink<>(
      mockProducer,
      TOPIC,
      s -> "key".getBytes(),
      String::getBytes
    );

    // Act
    sink.accept("value");

    // Assert
    verify(mockProducer, times(1)).send(
      argThat(record -> {
        assertEquals(TOPIC, record.topic());
        assertArrayEquals("key".getBytes(), record.key());
        assertArrayEquals("value".getBytes(), record.value());
        return true;
      })
    );
  }
}
