package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/// Pins the DLQ-send-failure offset behavior on the per-record error path.
///
/// The at-least-once contract: the offset advances only once the record reaches a durable terminal
/// state — either the sink processed it, or it is safely parked in the DLQ. When a record fails
/// processing AND the dead-letter send itself fails (broker down, producer closed, serializer
/// blowup), the record is in neither place, so its offset must NOT advance. `handleProcessingError`
/// therefore marks the offset only on a successful DLQ send. A failed send leaves the offset
/// pending (commit point holds), so the record is reprocessed — and the DLQ retried — on the
/// next poll/restart. A down DLQ applies backpressure, it never silently drops.
///
/// These tests use a recording [OffsetManager] double so the marked/tracked offsets can be
/// inspected directly without a live Kafka broker. The processor always throws, and the injected
/// DLQ producer's `send` throws, so `KPipeProducer.sendToDlq` returns false.
@ExtendWith(MockitoExtension.class)
class DlqSendFailureTest {

  private static final String TOPIC = "test-topic";
  private static final String DLQ_TOPIC = "test-dlq-topic";

  @Mock
  private Producer<String, byte[]> mockProducer;

  /// A failing-DLQ-send record on the single-record error path. The record is tracked, the DLQ
  /// send is attempted and fails, so the offset must NOT be marked. It stays pending and is
  /// reprocessed on restart; the failure surfaces via the `dlqFailed` counter and an ERROR log.
  @Test
  @SuppressWarnings("unchecked")
  void failedDlqSendDoesNotMarkOffset() throws Exception {
    final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value".getBytes(StandardCharsets.UTF_8));

    // The DLQ producer's send throws synchronously -> sendToDlq catches it and returns false.
    when(mockProducer.send(any(ProducerRecord.class))).thenThrow(new RuntimeException("dlq broker down"));

    final var recorder = new RecordingOffsetManager<String>();

    try (
      final var consumer = KPipeConsumer.<String>builder()
        .withProperties(byteProperties())
        .withTopic(TOPIC)
        .withPipeline(
          TestPipelines.sideEffect(_ -> {
            throw new RuntimeException("processor always fails");
          })
        )
        .withRetry(0, Duration.ofMillis(1))
        .withDeadLetterTopic(DLQ_TOPIC)
        .withKafkaProducer(mockProducer)
        .withOffsetManager(recorder)
        .build()
    ) {
      // Mirror the real consumer loop: trackOffset happens before dispatch in processRecords.
      recorder.trackOffset(record);

      // A throwing DLQ send must not propagate out of the consumer.
      assertDoesNotThrow(() -> consumer.processRecord(record));

      // The DLQ send was attempted exactly once and failed (sendToDlq returned false).
      verify(mockProducer, times(1)).send(any(ProducerRecord.class));
      assertTrue(recorder.tracked.contains(100L), "record should have been tracked");

      // Core at-least-once invariant: a record that is neither processed nor durably in the DLQ
      // must NOT have its offset advanced, so it is reprocessed on restart rather than lost.
      assertFalse(
        recorder.marked.contains(100L),
        "offset must NOT be marked when the DLQ send failed (record is neither processed nor parked)"
      );

      // The failure is observable, not swallowed.
      assertEquals(1L, consumer.getMetrics().get("dlqFailed"), "failed DLQ send must increment the dlqFailed counter");
      assertEquals(0L, consumer.getMetrics().get("dlqSent"), "no successful DLQ send occurred");
    }
  }

  /// Control case: when the DLQ send SUCCEEDS, marking the offset is correct -- the record is
  /// durably parked in the DLQ, so the commit point may legitimately advance to 101. This pins the
  /// happy path so a future fix for the failure case cannot regress it.
  @Test
  @SuppressWarnings("unchecked")
  void successfulDlqSendMarksOffsetProcessed() throws Exception {
    final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value".getBytes(StandardCharsets.UTF_8));

    when(mockProducer.send(any(ProducerRecord.class))).thenReturn(
      CompletableFuture.completedFuture(mock(RecordMetadata.class))
    );

    final var recorder = new RecordingOffsetManager<String>();

    try (
      final var consumer = KPipeConsumer.<String>builder()
        .withProperties(byteProperties())
        .withTopic(TOPIC)
        .withPipeline(
          TestPipelines.sideEffect(_ -> {
            throw new RuntimeException("processor always fails");
          })
        )
        .withRetry(0, Duration.ofMillis(1))
        .withDeadLetterTopic(DLQ_TOPIC)
        .withKafkaProducer(mockProducer)
        .withOffsetManager(recorder)
        .build()
    ) {
      recorder.trackOffset(record);
      consumer.processRecord(record);

      verify(mockProducer, times(1)).send(any(ProducerRecord.class));
      assertTrue(recorder.marked.contains(100L), "offset should be marked after a successful DLQ send");
      assertEquals(1L, consumer.getMetrics().get("dlqSent"), "successful DLQ send must increment dlqSent");
      assertEquals(0L, consumer.getMetrics().get("dlqFailed"), "no DLQ failure occurred");
    }
  }

  /// The batch error path (`BatchCallbacks.onBatchFailure`) must obey the same durable-terminal
  /// rule as the per-record path: a failed DLQ send leaves the offset pending so the record is
  /// reprocessed, never silently dropped. A `BatchPolicy.ofSize(1)` flushes inline on enqueue, so
  /// the single record's whole-batch failure (the sink throws) reaches `onBatchFailure`
  /// synchronously; we then await the observable `dlqFailed` counter and assert the offset stayed
  /// unmarked.
  @Test
  @SuppressWarnings("unchecked")
  void failedDlqSendOnBatchPathDoesNotMarkOffset() throws Exception {
    when(mockProducer.send(any(ProducerRecord.class))).thenThrow(new RuntimeException("dlq broker down"));

    final var recorder = new RecordingOffsetManager<String>();
    final var mock = new MockConsumer<String, byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    mock.addRecord(new ConsumerRecord<>(TOPIC, 0, 5L, "key", "value".getBytes(StandardCharsets.UTF_8)));

    try (
      final var consumer = KPipeConsumer.<String>builder()
        .withProperties(byteProperties())
        .withConsumer(() -> mock)
        .withOffsetManager(recorder)
        .withBatchPipeline(
          TOPIC,
          TestPipelines.sideEffect(v -> v),
          BatchSink.ofVoid(_ -> {
            throw new RuntimeException("batch sink always fails");
          }),
          BatchPolicy.ofSize(1)
        )
        .withDeadLetterTopic(DLQ_TOPIC)
        .withKafkaProducer(mockProducer)
        .withPollTimeout(Duration.ofMillis(10))
        .build()
    ) {
      consumer.start();

      // Await the synchronous flush → failed DLQ send. The dlqFailed counter is the observable
      // signal that onBatchFailure ran the failure branch.
      final var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
      while (consumer.getMetrics().getOrDefault("dlqFailed", 0L) < 1 && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }

      assertEquals(1L, consumer.getMetrics().get("dlqFailed"), "batch DLQ send failure must increment dlqFailed");
      assertEquals(0L, consumer.getMetrics().get("dlqSent"), "no successful DLQ send occurred");
      assertFalse(
        recorder.marked.contains(5L),
        "batch path: offset must NOT be marked when the DLQ send failed (record stays eligible for reprocessing)"
      );
    }
  }

  private static Properties byteProperties() {
    final var props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "test-group");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return props;
  }

  /// Minimal [OffsetManager] test double that records which offsets were tracked and which were
  /// marked processed, so the failure path's effect on the commit point can be asserted directly.
  private static final class RecordingOffsetManager<K> implements OffsetManager<K> {

    private final List<Long> tracked = new CopyOnWriteArrayList<>();
    private final List<Long> marked = new CopyOnWriteArrayList<>();

    @Override
    public OffsetManager<K> start() {
      return this;
    }

    @Override
    public OffsetManager<K> stop() {
      return this;
    }

    @Override
    public void trackOffset(final ConsumerRecord<K, byte[]> record) {
      tracked.add(record.offset());
    }

    @Override
    public void markOffsetProcessed(final ConsumerRecord<K, byte[]> record) {
      marked.add(record.offset());
    }

    @Override
    public void notifyCommitComplete(final String commitId, final boolean success) {
      // no-op
    }

    @Override
    public ConsumerRebalanceListener createRebalanceListener() {
      return new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
          // no-op
        }

        @Override
        public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
          // no-op
        }
      };
    }

    @Override
    public OffsetState getState() {
      return OffsetState.RUNNING;
    }

    @Override
    public boolean isRunning() {
      return true;
    }

    @Override
    public Map<String, Object> getStatistics() {
      return Map.of();
    }

    @Override
    public void close() {
      // no-op
    }
  }
}
