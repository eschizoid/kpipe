package io.github.eschizoid.kpipe.producer.sink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
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
  void shouldSendWithKey() {
    final KafkaMessageSink<String> sink = new KafkaMessageSink<>(
      mockProducer,
      TOPIC,
      _ -> "key".getBytes(),
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

  /// When the underlying Kafka `Producer.send` throws a `KafkaException` synchronously (e.g.
  /// serialization failure, authorization failure, buffer exhaustion with `max.block.ms=0`), the
  /// sink must propagate the exception unwrapped rather than catch-and-log. The async-callback
  /// failure path is the only "log and swallow" path the sink documents — synchronous `send`
  /// throws are surfaced so upstream pipeline error handling (`withSinkErrorHandling`) can see
  /// them.
  @Test
  void acceptPropagatesProducerExceptionUnwrapped() {
    final var failure = new KafkaException("synchronous send failure");
    when(mockProducer.send(any(), any(Callback.class))).thenThrow(failure);

    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    final var thrown = assertThrows(KafkaException.class, () -> sink.accept("payload"));
    assertSame(failure, thrown, "the original KafkaException must propagate unwrapped");
  }

  /// Pins the closed-producer behaviour: per Kafka's contract, `Producer.send` on a closed producer
  /// throws `IllegalStateException("Cannot perform operation after producer has been closed")`.
  ///
  /// The sink does not pre-check producer state — it relies on Kafka's own exception. The
  /// invariant being pinned here is that the sink does NOT swallow this exception; it propagates so
  /// the pipeline's error handler can route or fail loudly.
  @Test
  void acceptOnClosedProducerThrows() {
    final var closed = new IllegalStateException("Cannot perform operation after producer has been closed");
    when(mockProducer.send(any(), any(Callback.class))).thenThrow(closed);

    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    final var thrown = assertThrows(IllegalStateException.class, () -> sink.accept("payload"));
    assertSame(closed, thrown, "the closed-producer ISE must propagate unwrapped");
  }

  /// Verifies the static factory `KafkaMessageSink.of(...)` builds a sink whose keyMapper is null,
  /// which produces a `ProducerRecord` with a null key. Kafka accepts null keys (partition
  /// assignment falls back to the configured partitioner — round-robin / sticky for the default).
  @Test
  void ofStaticFactoryWithNullKeyAccepted() {
    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    sink.accept("payload");

    verify(mockProducer, times(1)).send(
      argThat(record -> {
        assertEquals(TOPIC, record.topic());
        assertNull(record.key(), "of(...) must produce a null-keyed record");
        assertArrayEquals("payload".getBytes(), record.value());
        return true;
      }),
      any(Callback.class)
    );
  }
}
