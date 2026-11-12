package io.github.eschizoid.kpipe.producer.sink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.github.eschizoid.kpipe.producer.KPipeProducer;
import io.github.eschizoid.kpipe.record.RecordContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaMessageSinkTest {

  private static final String TOPIC = "sink-topic";

  @Mock
  private Producer<byte[], byte[]> mockProducer;

  private void acknowledgeSends() {
    when(mockProducer.send(any())).thenReturn(
      CompletableFuture.completedFuture(new RecordMetadata(new TopicPartition(TOPIC, 0), 0, 0, 0, 0, 0))
    );
  }

  @Test
  void shouldSendToKafka() {
    acknowledgeSends();
    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    sink.accept("hello");

    verify(mockProducer, times(1)).send(
      argThat(record -> {
        assertEquals(TOPIC, record.topic());
        assertArrayEquals("hello".getBytes(), record.value());
        assertNull(record.key());
        return true;
      })
    );
  }

  /// Inside a pipeline the sink writes the record's key and every input header, in order.
  @Test
  void forwardsTheCurrentRecordsKeyAndHeaders() {
    acknowledgeSends();
    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);
    final var context = new FixedContext(
      "key".getBytes(),
      List.of(Map.entry("tenant", "acme".getBytes()), Map.entry("tenant", "beta".getBytes()))
    );

    RecordContext.callWith(context, () -> {
      sink.accept("value");
      return null;
    });

    verify(mockProducer, times(1)).send(
      argThat(record -> {
        assertArrayEquals("key".getBytes(), record.key());
        assertArrayEquals("value".getBytes(), record.value());
        final var tenants = new ArrayList<String>();
        record
          .headers()
          .headers("tenant")
          .forEach(h -> tenants.add(new String(h.value())));
        assertEquals(List.of("acme", "beta"), tenants);
        return true;
      })
    );
  }

  /// Outside a pipeline no record is bound, so the key is null and no header is copied.
  @Test
  void withNoCurrentRecordTheKeyIsNullAndNoHeadersAreCopied() {
    acknowledgeSends();
    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    sink.accept("value");

    verify(mockProducer, times(1)).send(
      argThat(record -> {
        assertNull(record.key());
        assertEquals(0, record.headers().toArray().length);
        return true;
      })
    );
  }

  /// A sink over a [KPipeProducer] sends through it and waits for the ack the same way.
  @Test
  void sendsThroughAKPipeProducer() {
    acknowledgeSends();
    final var producer = KPipeProducer.<byte[], byte[]>builder().withProducer(mockProducer).build();
    final KafkaMessageSink<String> sink = KafkaMessageSink.of(producer, TOPIC, String::getBytes, null);

    RecordContext.callWith(new FixedContext("key".getBytes(), List.of()), () -> {
      sink.accept("value");
      return null;
    });

    verify(mockProducer, times(1)).send(
      argThat(record -> {
        assertEquals(TOPIC, record.topic());
        assertArrayEquals("key".getBytes(), record.key());
        return true;
      })
    );
  }

  @Test
  void shouldNotSendWhenValueIsNull() {
    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    sink.accept(null);

    verifyNoInteractions(mockProducer);
  }

  /// A send the broker rejects fails `accept`, so the consumer retries the record and then routes
  /// it to the error handler and the dead-letter topic instead of committing its offset.
  @Test
  void aRejectedSendFailsAccept() {
    final var rejected = new KafkaException("broker rejected the send");
    when(mockProducer.send(any())).thenReturn(CompletableFuture.failedFuture(rejected));
    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    final var thrown = assertThrows(IllegalStateException.class, () -> sink.accept("payload"));
    assertSame(rejected, thrown.getCause(), "the broker's exception is the cause");
    assertTrue(thrown.getMessage().contains(TOPIC), thrown.getMessage());
  }

  /// `accept` returns only once the broker has acknowledged the send.
  @Test
  void acceptWaitsForTheAck() throws Exception {
    final var ack = new CompletableFuture<RecordMetadata>();
    when(mockProducer.send(any())).thenReturn(ack);
    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    final var accepted = new CompletableFuture<Void>();
    Thread.ofVirtual().start(() -> {
      sink.accept("payload");
      accepted.complete(null);
    });
    verify(mockProducer, timeout(5_000)).send(any());
    Thread.sleep(50);
    assertFalse(accepted.isDone(), "accept must not return before the ack");

    ack.complete(new RecordMetadata(new TopicPartition(TOPIC, 0), 0, 0, 0, 0, 0));
    accepted.get();
  }

  /// An interrupt while waiting for the ack fails `accept` and keeps the thread's interrupt flag.
  @Test
  void anInterruptWhileWaitingFailsAcceptAndKeepsTheFlag() {
    when(mockProducer.send(any())).thenReturn(new CompletableFuture<>());
    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    Thread.currentThread().interrupt();
    try {
      assertThrows(IllegalStateException.class, () -> sink.accept("payload"));
      assertTrue(Thread.currentThread().isInterrupted(), "the interrupt flag is restored");
    } finally {
      Thread.interrupted();
    }
  }

  /// A synchronous `Producer.send` failure (serialization, authorization, buffer exhaustion with
  /// `max.block.ms=0`) propagates unwrapped, so the pipeline's error handling sees it.
  @Test
  void acceptPropagatesProducerExceptionUnwrapped() {
    final var failure = new KafkaException("synchronous send failure");
    when(mockProducer.send(any())).thenThrow(failure);

    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    final var thrown = assertThrows(KafkaException.class, () -> sink.accept("payload"));
    assertSame(failure, thrown, "the original KafkaException must propagate unwrapped");
  }

  /// `Producer.send` on a closed producer throws `IllegalStateException`; the sink does not
  /// pre-check producer state and does not swallow it.
  @Test
  void acceptOnClosedProducerThrows() {
    final var closed = new IllegalStateException("Cannot perform operation after producer has been closed");
    when(mockProducer.send(any())).thenThrow(closed);

    final KafkaMessageSink<String> sink = KafkaMessageSink.of(mockProducer, TOPIC, String::getBytes, null);

    final var thrown = assertThrows(IllegalStateException.class, () -> sink.accept("payload"));
    assertSame(closed, thrown, "the closed-producer ISE must propagate unwrapped");
  }

  private static final class FixedContext extends RecordContext {

    private final byte[] key;
    private final List<Map.Entry<String, byte[]>> headers;

    FixedContext(final byte[] key, final List<Map.Entry<String, byte[]>> headers) {
      this.key = key;
      this.headers = headers;
    }

    @Override
    public String topic() {
      return "in";
    }

    @Override
    public int partition() {
      return 0;
    }

    @Override
    public long offset() {
      return 0;
    }

    @Override
    public long timestamp() {
      return 0;
    }

    @Override
    public byte[] key() {
      return key;
    }

    @Override
    public byte[] originalKey() {
      return key;
    }

    @Override
    public Optional<byte[]> header(final String name) {
      return headers
        .reversed()
        .stream()
        .filter(h -> h.getKey().equals(name))
        .map(Map.Entry::getValue)
        .findFirst();
    }

    @Override
    public void forEachHeader(final BiConsumer<String, byte[]> action) {
      headers.forEach(h -> action.accept(h.getKey(), h.getValue()));
    }
  }
}
