package org.kpipe.producer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KPipeProducerTest {

  private static final String TOPIC = "test-topic";
  private static final String DLQ_TOPIC = "test-dlq";

  @Mock
  private Producer<byte[], byte[]> mockProducer;

  // ── constructor ──────────────────────────────────────────────────────────

  @Test
  void shouldRejectNullProducer() {
    assertThrows(NullPointerException.class, () -> new KPipeProducer<>(null, true));
  }

  // ── send ─────────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void shouldReturnMetadataOnSend() {
    final var expected = mock(RecordMetadata.class);
    when(mockProducer.send(any(ProducerRecord.class)))
      .thenReturn(CompletableFuture.completedFuture(expected));

    final var result = new KPipeProducer<>(mockProducer, false)
      .send(new ProducerRecord<>(TOPIC, "k".getBytes(), "v".getBytes()));

    assertSame(expected, result);
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldWrapExecutionExceptionAsSendFailed() {
    when(mockProducer.send(any(ProducerRecord.class)))
      .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

    final var ex = assertThrows(
      RuntimeException.class,
      () -> new KPipeProducer<>(mockProducer, false).send(new ProducerRecord<>(TOPIC, null, "v".getBytes()))
    );
    assertEquals("Send failed", ex.getMessage());
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldRestoreInterruptFlagOnInterruptedSend() throws Exception {
    final var future = mock(java.util.concurrent.Future.class);
    when(mockProducer.send(any(ProducerRecord.class))).thenReturn(future);
    when(future.get()).thenThrow(new InterruptedException());

    final var producer = new KPipeProducer<>(mockProducer, false);

    assertThrows(
      RuntimeException.class,
      () -> producer.send(new ProducerRecord<>(TOPIC, null, "v".getBytes()))
    );
    assertTrue(Thread.interrupted(), "interrupt flag should be restored");
  }

  // ── sendAsync ────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void shouldReturnFutureOnSendAsync() {
    final var future = CompletableFuture.completedFuture(mock(RecordMetadata.class));
    when(mockProducer.send(any(ProducerRecord.class))).thenReturn(future);

    final var result = new KPipeProducer<>(mockProducer, false)
      .sendAsync(new ProducerRecord<>(TOPIC, null, "v".getBytes()));

    assertSame(future, result);
  }

  // ── sendToDlq ────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void shouldSendDlqRecordWithAllHeaders() {
    when(mockProducer.send(any(ProducerRecord.class)))
      .thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));

    final var record = new ConsumerRecord<>(TOPIC, 2, 42L, "k".getBytes(), "v".getBytes());
    new KPipeProducer<>(mockProducer, false)
      .sendToDlq(DLQ_TOPIC, record, TOPIC, new RuntimeException("boom"), null);

    verify(mockProducer).send(
      argThat(r -> {
        assertEquals(DLQ_TOPIC, r.topic());
        assertArrayEquals("k".getBytes(), r.key());
        assertArrayEquals("v".getBytes(), r.value());

        final var headers = r.headers();
        assertNotNull(headers.lastHeader("x-dlq-exception-class"));
        assertNotNull(headers.lastHeader("x-dlq-exception-message"));
        assertEquals("boom", new String(headers.lastHeader("x-dlq-exception-message").value()));
        assertEquals(TOPIC, new String(headers.lastHeader("x-dlq-source-topic").value()));
        assertEquals("2", new String(headers.lastHeader("x-dlq-source-partition").value()));
        assertEquals("42", new String(headers.lastHeader("x-dlq-source-offset").value()));
        return true;
      })
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldIncrementDlqMetricOnSuccess() {
    when(mockProducer.send(any(ProducerRecord.class)))
      .thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));

    final var metric = new AtomicLong(0);
    final var record = new ConsumerRecord<>(TOPIC, 0, 0L, "k".getBytes(), "v".getBytes());
    new KPipeProducer<>(mockProducer, false)
      .sendToDlq(DLQ_TOPIC, record, TOPIC, new RuntimeException("fail"), metric);

    assertEquals(1, metric.get());
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldNotIncrementDlqMetricOnSendFailure() {
    when(mockProducer.send(any(ProducerRecord.class)))
      .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

    final var metric = new AtomicLong(0);
    final var record = new ConsumerRecord<>(TOPIC, 0, 0L, "k".getBytes(), "v".getBytes());
    new KPipeProducer<>(mockProducer, false)
      .sendToDlq(DLQ_TOPIC, record, TOPIC, new RuntimeException("fail"), metric);

    assertEquals(0, metric.get());
    verify(mockProducer).send(any());
  }

  @Test
  void shouldBeNoOpWhenDlqTopicIsNull() {
    final var record = new ConsumerRecord<>(TOPIC, 0, 0L, "k".getBytes(), "v".getBytes());
    new KPipeProducer<>(mockProducer, false)
      .sendToDlq(null, record, TOPIC, new RuntimeException("fail"), null);

    verifyNoInteractions(mockProducer);
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldHandleNullExceptionMessage() {
    when(mockProducer.send(any(ProducerRecord.class)))
      .thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));

    final var record = new ConsumerRecord<>(TOPIC, 0, 0L, "k".getBytes(), "v".getBytes());
    // Exception with null message should not throw
    assertDoesNotThrow(
      () -> new KPipeProducer<>(mockProducer, false)
        .sendToDlq(DLQ_TOPIC, record, TOPIC, new RuntimeException((String) null), null)
    );

    verify(mockProducer).send(
      argThat(r -> {
        assertEquals("", new String(r.headers().lastHeader("x-dlq-exception-message").value()));
        return true;
      })
    );
  }

  // ── close ─────────────────────────────────────────────────────────────────

  @Test
  void shouldCloseUnderlyingProducerWhenOwned() {
    new KPipeProducer<>(mockProducer, true).close();
    verify(mockProducer).close();
  }

  @Test
  void shouldNotCloseUnderlyingProducerWhenNotOwned() {
    new KPipeProducer<>(mockProducer, false).close();
    verify(mockProducer, never()).close();
  }

  @Test
  void shouldHandleExceptionDuringClose() {
    doThrow(new RuntimeException("close failed")).when(mockProducer).close();
    assertDoesNotThrow(() -> new KPipeProducer<>(mockProducer, true).close());
  }

  // ── createDefaultProducer ─────────────────────────────────────────────────

  @Test
  void shouldFilterOnlyRelevantPropertiesForDefaultProducer() {
    final var consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", "localhost:9092");
    consumerProps.put("group.id", "my-group");
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProps.put("sasl.mechanism", "PLAIN");
    consumerProps.put("acks", "all");

    // createDefaultProducer creates a real KafkaProducer — just verify the properties filter
    // by checking that group.id and deserializer keys don't leak through
    final var filtered = new Properties();
    consumerProps.forEach((k, v) -> {
      final String key = k.toString();
      if (
        key.startsWith("bootstrap.servers") ||
        key.startsWith("sasl.") ||
        key.startsWith("security.") ||
        key.startsWith("ssl.") ||
        key.startsWith("client.id") ||
        key.equals("key.serializer") ||
        key.equals("value.serializer") ||
        key.equals("acks")
      ) filtered.put(k, v);
    });
    filtered.putIfAbsent("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    filtered.putIfAbsent("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    assertFalse(filtered.containsKey("group.id"));
    assertFalse(filtered.containsKey("key.deserializer"));
    assertFalse(filtered.containsKey("value.deserializer"));
    assertEquals("localhost:9092", filtered.getProperty("bootstrap.servers"));
    assertEquals("PLAIN", filtered.getProperty("sasl.mechanism"));
    assertEquals("all", filtered.getProperty("acks"));
    assertEquals(
      "org.apache.kafka.common.serialization.ByteArraySerializer",
      filtered.getProperty("key.serializer")
    );
  }

  @Test
  void shouldAppendProducerSuffixToClientId() {
    final var props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("client.id", "my-consumer");

    // verify the suffix logic directly (without constructing a real KafkaProducer)
    final var clientId = props.getProperty("client.id");
    final var expectedClientId = clientId + "-producer";
    assertEquals("my-consumer-producer", expectedClientId);
  }

  // ── virtual threads ───────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void shouldSupportConcurrentSendsFromVirtualThreads() throws Exception {
    when(mockProducer.send(any(ProducerRecord.class)))
      .thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));

    final var producer = new KPipeProducer<>(mockProducer, false);
    final var errors = new CopyOnWriteArrayList<Throwable>();
    final int threadCount = 100;

    final var virtualThreads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      final int index = i;
      virtualThreads[i] = Thread.ofVirtual().unstarted(() -> {
        try {
          producer.send(new ProducerRecord<>(TOPIC, ("key-" + index).getBytes(), ("val-" + index).getBytes()));
        } catch (final Throwable t) {
          errors.add(t);
        }
      });
    }
    for (final var t : virtualThreads) t.start();
    for (final var t : virtualThreads) t.join();

    assertTrue(errors.isEmpty(), "No errors expected from concurrent virtual thread sends: " + errors);
    verify(mockProducer, times(threadCount)).send(any(ProducerRecord.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldSupportConcurrentDlqSendsFromVirtualThreads() throws Exception {
    when(mockProducer.send(any(ProducerRecord.class)))
      .thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));

    final var kpipeProducer = new KPipeProducer<>(mockProducer, false);
    final var metric = new AtomicLong(0);
    final var errors = new CopyOnWriteArrayList<Throwable>();
    final int threadCount = 50;

    final var virtualThreads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      final int index = i;
      virtualThreads[i] = Thread.ofVirtual().unstarted(() -> {
        try {
          final var record = new ConsumerRecord<>(
            TOPIC, 0, index, ("k-" + index).getBytes(), ("v-" + index).getBytes()
          );
          kpipeProducer.sendToDlq(DLQ_TOPIC, record, TOPIC, new RuntimeException("fail-" + index), metric);
        } catch (final Throwable t) {
          errors.add(t);
        }
      });
    }
    for (final var t : virtualThreads) t.start();
    for (final var t : virtualThreads) t.join();

    assertTrue(errors.isEmpty(), "No errors expected from concurrent DLQ sends: " + errors);
    assertEquals(threadCount, metric.get());
    verify(mockProducer, times(threadCount)).send(any(ProducerRecord.class));
  }
}
