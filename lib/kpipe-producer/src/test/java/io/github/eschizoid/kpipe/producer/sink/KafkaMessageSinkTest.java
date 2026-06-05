package io.github.eschizoid.kpipe.producer.sink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
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
    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    sink.accept("hello");

    verify(mockProducer, times(1)).send(
      argThat(record -> {
        assertEquals(TOPIC, record.topic());
        assertArrayEquals("hello".getBytes(), record.value());
        assertNull(record.key());
        return true;
      }),
      any(Callback.class)
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldSendWithKey() {
    final KafkaMessageSink<String> sink = new KafkaMessageSink<>(
      mockProducer,
      TOPIC,
      s -> "key".getBytes(),
      String::getBytes,
      null
    );

    sink.accept("value");

    verify(mockProducer, times(1)).send(
      argThat(record -> {
        assertEquals(TOPIC, record.topic());
        assertArrayEquals("key".getBytes(), record.key());
        assertArrayEquals("value".getBytes(), record.value());
        return true;
      }),
      any(Callback.class)
    );
  }

  @Test
  void shouldNotSendWhenValueIsNull() {
    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    sink.accept(null);

    verifyNoInteractions(mockProducer);
  }
}
