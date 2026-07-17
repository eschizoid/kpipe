package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/// Lockstep contract for the two failure paths that own the durable-terminal-state rule.
///
/// The rule: a failed record's offset advances only once the record reaches a durable terminal
/// state — parked in the DLQ on a successful send, or explicitly opted into log-and-advance when
/// no DLQ is configured. A failed DLQ send leaves the offset pending (reprocess on restart, never
/// silently drop). That rule is implemented TWICE — in `handleProcessingError` (per-record) and in
/// the batch wrapper's `onBatchFailure` callback — and was deliberately left duplicated rather
/// than extracted (the paths carry deliberate asymmetries around span handling, retry counts, and
/// circuit-breaker ordering that a shared helper would blur).
///
/// This table is the drift guard: every cell of {path} × {DLQ scenario} asserts the identical
/// terminal outcome on both paths. If a future change touches one path's DLQ-or-mark block and
/// not the other, a cell here fails.
class DlqTerminalContractTest {

  private static final String TOPIC = "contract-topic";
  private static final String DLQ_TOPIC = "contract-dlq";
  private static final long OFFSET = 7L;

  enum Path {
    PER_RECORD,
    BATCH,
  }

  enum DlqScenario {
    SEND_OK,
    SEND_FAIL,
    NOT_CONFIGURED,
  }

  static Stream<Arguments> cells() {
    return Stream.of(
      // marked?, dlqSent, dlqFailed — identical per scenario across BOTH paths, by contract.
      Arguments.of(Path.PER_RECORD, DlqScenario.SEND_OK, true, 1L, 0L),
      Arguments.of(Path.PER_RECORD, DlqScenario.SEND_FAIL, false, 0L, 1L),
      Arguments.of(Path.PER_RECORD, DlqScenario.NOT_CONFIGURED, true, 0L, 0L),
      Arguments.of(Path.BATCH, DlqScenario.SEND_OK, true, 1L, 0L),
      Arguments.of(Path.BATCH, DlqScenario.SEND_FAIL, false, 0L, 1L),
      Arguments.of(Path.BATCH, DlqScenario.NOT_CONFIGURED, true, 0L, 0L)
    );
  }

  @ParameterizedTest(name = "{0} × {1} → marked={2} dlqSent={3} dlqFailed={4}")
  @MethodSource("cells")
  void bothFailurePathsShareTheDurableTerminalContract(
    final Path path,
    final DlqScenario scenario,
    final boolean expectMarked,
    final long expectDlqSent,
    final long expectDlqFailed
  ) throws Exception {
    final var recorder = new RecordingOffsetManager();
    final var errorsSeen = new AtomicInteger();
    final var producer = producerFor(scenario);

    final var builder = KPipeConsumer.builder()
      .withProperties(byteProperties())
      .withOffsetManager(recorder)
      .withErrorHandler(_ -> errorsSeen.incrementAndGet())
      .withPollTimeout(Duration.ofMillis(10));

    if (scenario != DlqScenario.NOT_CONFIGURED) {
      builder.withDeadLetterTopic(DLQ_TOPIC).withKafkaProducer(producer);
    }

    if (path == Path.PER_RECORD) {
      builder
        .withTopic(TOPIC)
        .withPipeline(
          TestPipelines.sideEffect(_ -> {
            throw new RuntimeException("processor always fails");
          })
        )
        .withRetry(0, Duration.ofMillis(1));

      try (final var consumer = builder.build()) {
        final var record = new ConsumerRecord<>(TOPIC, 0, OFFSET, "k".getBytes(UTF_8), "v".getBytes(UTF_8));
        recorder.trackOffset(record);
        consumer.processRecord(record);
        assertCell(consumer, recorder, errorsSeen, expectMarked, expectDlqSent, expectDlqFailed);
      }
    } else {
      // Batch: the pipeline passes, the batch sink throws, so the single record's whole-batch
      // failure reaches onBatchFailure via the size-1 inline flush.
      builder
        .withConsumer(DlqTerminalContractTest::seededMockConsumer)
        .withBatchPipeline(
          TOPIC,
          TestPipelines.sideEffect(v -> v),
          BatchSink.ofVoid(_ -> {
            throw new RuntimeException("batch sink always fails");
          }),
          BatchPolicy.ofSize(1)
        );

      try (final var consumer = builder.build()) {
        consumer.start();
        // Terminal signal per cell: the mark itself when marking is expected, else the dlqFailed
        // counter (the only unmarked cell is SEND_FAIL, whose observable is the failed send).
        if (expectMarked) {
          awaitCondition(() -> recorder.marked.contains(OFFSET), 5_000);
        } else {
          awaitCondition(() -> consumer.getMetrics().getOrDefault("dlqFailed", 0L) >= 1, 5_000);
        }
        awaitCondition(() -> errorsSeen.get() >= 1, 5_000);
        assertCell(consumer, recorder, errorsSeen, expectMarked, expectDlqSent, expectDlqFailed);
      }
    }
  }

  private static void assertCell(
    final KPipeConsumer consumer,
    final RecordingOffsetManager recorder,
    final AtomicInteger errorsSeen,
    final boolean expectMarked,
    final long expectDlqSent,
    final long expectDlqFailed
  ) {
    assertTrue(recorder.tracked.contains(OFFSET), "record must have been tracked (guards the negative assertions)");
    if (expectMarked) {
      assertTrue(recorder.marked.contains(OFFSET), "offset must be marked (durably terminal: DLQ'd or opted-in)");
    } else {
      assertFalse(recorder.marked.contains(OFFSET), "offset must stay pending when the record is not durably parked");
    }
    assertEquals(expectDlqSent, consumer.getMetrics().getOrDefault("dlqSent", 0L), "dlqSent");
    assertEquals(expectDlqFailed, consumer.getMetrics().getOrDefault("dlqFailed", 0L), "dlqFailed");
    assertEquals(1, errorsSeen.get(), "error handler must observe the failure exactly once");
  }

  @SuppressWarnings("unchecked")
  private static Producer<byte[], byte[]> producerFor(final DlqScenario scenario) {
    if (scenario == DlqScenario.NOT_CONFIGURED) return null;
    final var producer = (Producer<byte[], byte[]>) mock(Producer.class);
    if (scenario == DlqScenario.SEND_OK) {
      when(producer.send(any(ProducerRecord.class))).thenReturn(
        CompletableFuture.completedFuture(mock(RecordMetadata.class))
      );
    } else {
      when(producer.send(any(ProducerRecord.class))).thenThrow(new RuntimeException("dlq broker down"));
    }
    return producer;
  }

  private static MockConsumer<byte[], byte[]> seededMockConsumer() {
    final var mock = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    mock.addRecord(new ConsumerRecord<>(TOPIC, 0, OFFSET, "k".getBytes(UTF_8), "v".getBytes(UTF_8)));
    return mock;
  }

  private static Properties byteProperties() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "dlq-contract");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("enable.auto.commit", "false");
    return p;
  }

  private static void awaitCondition(final BooleanSupplier cond, final long timeoutMs) throws InterruptedException {
    final var deadline = System.currentTimeMillis() + timeoutMs;
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() >= deadline) throw new AssertionError("timed out awaiting cell terminal state");
      Thread.sleep(10);
    }
  }

  private static final class RecordingOffsetManager implements OffsetManager {

    private final List<Long> tracked = new CopyOnWriteArrayList<>();
    private final List<Long> marked = new CopyOnWriteArrayList<>();

    @Override
    public OffsetManager start() {
      return this;
    }

    @Override
    public OffsetManager stop() {
      return this;
    }

    @Override
    public void trackOffset(final ConsumerRecord<byte[], byte[]> record) {
      tracked.add(record.offset());
    }

    @Override
    public void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record) {
      marked.add(record.offset());
    }

    @Override
    public void notifyCommitComplete(final String commitId, final boolean success) {}

    @Override
    public OffsetState getState() {
      return OffsetState.RUNNING;
    }

    @Override
    public boolean isRunning() {
      return true;
    }

    @Override
    public OffsetStatistics getStatistics() {
      return OffsetStatistics.empty();
    }

    @Override
    public void close() {}
  }
}
