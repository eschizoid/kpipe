package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/// Pins the honor-retries contract on batch routes: `withRetry(n)` applies to a batch-route
/// record's deserialize + operator step before buffering, exactly as it does on the regular
/// per-record path. Historically the batch enqueue path ran a single attempt with a hardcoded
/// retry count of 0 — `withRetry(3)` + `withBatchPipeline` silently applied zero retries and
/// reported `retryCount=0` to the error handler. These tests fail on that regression.
///
/// The batch sink flush itself is deliberately NOT retried (its failure handling is the
/// `BatchResult` / DLQ path, pinned by `DlqTerminalContractTest` and the batch matrix); only
/// the pre-buffer step gains attempts, which is what both cells below exercise.
class BatchRetryIntegrationTest {

  private static final String TOPIC = "batch-retry-topic";
  private static final String DLQ_TOPIC = "batch-retry-dlq";
  private static final long OFFSET = 0L;

  private static Properties props() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "batch-retry-group");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("enable.auto.commit", "false");
    return p;
  }

  private static MockConsumer<byte[], byte[]> seededSingleRecord() {
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

  @SuppressWarnings("unchecked")
  private static Producer<byte[], byte[]> recordingDlqProducer(final List<Long> dlqOffsets) {
    final Producer<byte[], byte[]> producer = Mockito.mock(Producer.class);
    Mockito.lenient()
      .when(producer.send(Mockito.any(ProducerRecord.class)))
      .thenAnswer(_ -> {
        dlqOffsets.add(OFFSET);
        return CompletableFuture.completedFuture(Mockito.mock(RecordMetadata.class));
      });
    return producer;
  }

  private static void awaitCondition(final BooleanSupplier cond, final long timeoutMs) throws InterruptedException {
    final var deadline = System.currentTimeMillis() + timeoutMs;
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() >= deadline) throw new AssertionError(
        "awaitCondition timed out after " + timeoutMs + "ms"
      );
      Thread.sleep(10);
    }
  }

  /// Cell (a): withRetry(2) + a pipeline that fails twice then succeeds. The record must end up
  /// buffered and flushed to the batch sink — zero DLQ traffic, zero terminal errors, no error
  /// handler invocation — and the retries counter must show exactly the two real retries.
  @Test
  void batchRouteHonorsRetriesAndRecoversWithoutDlq() throws Exception {
    final var attempts = new AtomicInteger();
    final var flushed = new CopyOnWriteArrayList<byte[]>();
    final var dlqOffsets = new CopyOnWriteArrayList<Long>();
    final var errors = new CopyOnWriteArrayList<KPipeConsumer.ProcessingError>();

    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withBatchPipeline(
        TOPIC,
        TestPipelines.sideEffect(v -> {
          if (attempts.incrementAndGet() <= 2) throw new RuntimeException("transient, recovers on attempt 3");
          return v;
        }),
        BatchSink.ofVoid(flushed::addAll),
        BatchPolicy.ofSize(1)
      )
      .withRetry(2, Duration.ofMillis(1))
      .withDeadLetterTopic(DLQ_TOPIC)
      .withKafkaProducer(recordingDlqProducer(dlqOffsets))
      .withErrorHandler(errors::add)
      .withConsumer(BatchRetryIntegrationTest::seededSingleRecord)
      .withPollTimeout(Duration.ofMillis(5))
      .build();

    try {
      consumer.start();
      awaitCondition(() -> !flushed.isEmpty(), 10_000);

      assertEquals(3, attempts.get(), "the pipeline must be driven once per attempt (1 initial + 2 retries)");
      assertEquals(2L, consumer.getMetrics().get("retries"), "retries metric must count the two real retries");
      assertEquals(1, flushed.size(), "the recovered record must reach the batch sink exactly once");
      assertTrue(dlqOffsets.isEmpty(), "a record that recovers on retry must never touch the DLQ");
      assertEquals(0L, consumer.getMetrics().get("processingErrors"), "no terminal error for a recovered record");
      assertTrue(errors.isEmpty(), "the error handler must not fire for a recovered record");
    } finally {
      consumer.close();
    }
  }

  /// Cell (b): withRetry(1) + a persistently failing pipeline. The record must reach the error
  /// handler with `retryCount=1` (the retries actually performed, not the historical hardcoded
  /// 0) and park in the DLQ; the batch sink must never see it.
  @Test
  void batchRouteExhaustedRetriesReportRealCountAndDlq() throws Exception {
    final var attempts = new AtomicInteger();
    final var flushed = new CopyOnWriteArrayList<byte[]>();
    final var dlqOffsets = new CopyOnWriteArrayList<Long>();
    final var errors = new CopyOnWriteArrayList<KPipeConsumer.ProcessingError>();

    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withBatchPipeline(
        TOPIC,
        TestPipelines.sideEffect(v -> {
          attempts.incrementAndGet();
          throw new RuntimeException("permanent failure");
        }),
        BatchSink.ofVoid(flushed::addAll),
        BatchPolicy.ofSize(1)
      )
      .withRetry(1, Duration.ofMillis(1))
      .withDeadLetterTopic(DLQ_TOPIC)
      .withKafkaProducer(recordingDlqProducer(dlqOffsets))
      .withErrorHandler(errors::add)
      .withConsumer(BatchRetryIntegrationTest::seededSingleRecord)
      .withPollTimeout(Duration.ofMillis(5))
      .build();

    try {
      consumer.start();
      awaitCondition(() -> !errors.isEmpty() && !dlqOffsets.isEmpty(), 10_000);

      assertEquals(2, attempts.get(), "the pipeline must be driven twice (1 initial + 1 retry)");
      assertEquals(1L, consumer.getMetrics().get("retries"), "retries metric must count the single real retry");
      assertEquals(1, errors.size(), "the error handler must observe the exhausted failure exactly once");
      assertEquals(1, errors.getFirst().retryCount(), "retryCount must reflect the retries actually performed");
      assertEquals(List.of(OFFSET), dlqOffsets, "the exhausted record must park in the DLQ");
      assertTrue(flushed.isEmpty(), "a never-passing record must not reach the batch sink");
      assertEquals(1L, consumer.getMetrics().get("processingErrors"), "exactly one terminal error");
      assertEquals(1L, consumer.getMetrics().get("dlqSent"), "exactly one DLQ send");
    } finally {
      consumer.close();
    }
  }
}
