package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
/// Question under test: when a record fails processing AND the dead-letter send itself fails
/// (broker down, producer closed, serializer blowup), is the record's offset marked processed?
/// The at-least-once contract says a record that is neither successfully processed nor safely
/// parked in the DLQ must NOT have its offset advanced — otherwise it is silently lost (never
/// reprocessed because the commit point moved past it, never landed in the DLQ).
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

  /// A failing-DLQ-send record on the single-record error path. Asserts the offset-lifecycle
  /// outcome: the record was tracked, the DLQ send was attempted and failed, and we observe
  /// whether the offset was marked processed despite that failure.
  ///
  /// CORRECTNESS FINDING (silent loss). `handleProcessingError` calls `sendToDlq` and then calls
  /// `markOffsetProcessed` UNCONDITIONALLY -- it ignores the boolean `sendToDlq` returns.
  /// `KPipeProducer.sendToDlq` swallows any send failure (logs at ERROR) and returns false, so a
  /// record whose DLQ send fails (broker down, producer closed, serializer blowup) still has its
  /// offset marked processed. The commit point advances past it: the record is neither durably in
  /// the DLQ nor eligible for reprocessing on restart. It is silently lost. The only signal is one
  /// ERROR log line and `kpipe.consumer.dlq.sent` failing to increment.
  ///
  /// At-least-once correctness requires that a failed DLQ send NOT mark the offset (so the record
  /// is reprocessed) or surface the failure loudly (throw / route somewhere durable). This test
  /// PINS THE CURRENT (buggy) BEHAVIOR so the regression is documented and any future fix flips this
  /// assertion deliberately. The same shape exists on the batch failure path
  /// (`BatchCallbacks.onBatchFailure`). Do not "fix" by making this green silently -- it is a
  /// design decision for review.
  @Test
  @SuppressWarnings("unchecked")
  void failedDlqSendStillMarksOffset_documentedSilentLoss() throws Exception {
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

      // The record is NOT in the DLQ (the send failed), yet the offset IS marked processed. The
      // correct at-least-once behavior would be the opposite -- this assertion encodes the bug.
      assertTrue(
        recorder.marked.contains(100L),
        "documents current silent-loss behavior: offset marked despite failed DLQ send"
      );
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
