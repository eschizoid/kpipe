package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.tracing.Tracer;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// While the consumer runs, an `InterruptedException` inside a pipeline's exception came from the
/// pipeline's own machinery, such as a helper pool shut down under it; the thread processing the
/// record was not interrupted. The record is an ordinary failure: with no DLQ it is logged and
/// committed past, and the consumer keeps processing later records. Treating it as a shutdown
/// interruption would leave it unmarked, pinning the commit, and set a fake interrupt that stalls
/// a SEQUENTIAL consumer's thread.
class PipelineInterruptionWhileRunningTest {

  private static final String TOPIC = "pipeline-interrupt";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
  private static final int RECORDS = 6;
  private static final int FAILING_OFFSET = 1;

  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void anInterruptionInsideAPipelineExceptionWhileRunningIsAnOrdinaryFailure(final ProcessingMode mode)
    throws InterruptedException {
    final var commits = new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>();
    final var mockConsumer = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void commitSync(
        final Map<TopicPartition, OffsetAndMetadata> offsets,
        final Duration timeout
      ) {
        super.commitSync(offsets, timeout);
        commits.putAll(offsets);
      }

      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}
    };
    mockConsumer.assign(List.of(PARTITION));
    mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
    for (int i = 0; i < RECORDS; i++) {
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, ("k" + i).getBytes(UTF_8), ("v" + i).getBytes(UTF_8)));
    }

    final var processed = new AtomicInteger();
    final var errors = new AtomicInteger();
    final var properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "pipeline-interrupt-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessingMode(mode)
      .withErrorHandler(_ -> errors.incrementAndGet())
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          if (new String(value, UTF_8).equals("v" + FAILING_OFFSET)) {
            throw new IllegalStateException("helper pool shut down", new InterruptedException());
          }
          processed.incrementAndGet();
          return value;
        })
      )
      .withConsumer(() -> mockConsumer)
      .build();

    try {
      consumer.start();
      TestAwaits.pollUntil(
        () -> processed.get() == RECORDS - 1 && errors.get() == 1,
        Duration.ofSeconds(10),
        "every other record is processed and the failing one reaches the error handler"
      );
      assertTrue(consumer.isRunning(), "the consumer keeps running in " + mode);
    } finally {
      consumer.close();
    }

    assertEquals(
      RECORDS,
      commits.get(PARTITION).offset(),
      "the failed record is logged and committed past, as with no DLQ, in " + mode
    );
  }

  /// At shutdown the same exception does mean the record was cut short, even when the pipeline
  /// wraps the `InterruptedException` without restoring the interrupt flag. The record stays
  /// unmarked, so the commit stops at it and it is redelivered.
  @org.junit.jupiter.api.Test
  void anInterruptionAtShutdownStillLeavesTheRecordUnmarkedWithoutTheFlag() throws InterruptedException {
    final var commits = new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>();
    final var mockConsumer = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void commitSync(
        final Map<TopicPartition, OffsetAndMetadata> offsets,
        final Duration timeout
      ) {
        super.commitSync(offsets, timeout);
        commits.putAll(offsets);
      }

      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}
    };
    mockConsumer.assign(List.of(PARTITION));
    mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
    for (int i = 0; i < RECORDS; i++) {
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, ("k" + i).getBytes(UTF_8), ("v" + i).getBytes(UTF_8)));
    }

    final var processed = new AtomicInteger();
    final var errors = new AtomicInteger();
    final var aborted = new CountDownLatch(1);
    // The record's span closes after its whole outcome path, error handler included, has run, so
    // waiting on it is a complete signal rather than a guess at how long that path takes.
    final var stuckRecordDone = new CountDownLatch(1);
    final var tracer = new Tracer() {
      @Override
      public SpanScope startConsumerSpan(final ConsumerRecord<?, byte[]> record) {
        return new SpanScope() {
          @Override
          public void recordException(final Throwable t) {}

          @Override
          public void close() {
            if (record.offset() == FAILING_OFFSET) stuckRecordDone.countDown();
          }
        };
      }

      @Override
      public void injectContextInto(final Headers headers) {}
    };
    final var neverReleased = new CountDownLatch(1);
    final var properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "pipeline-interrupt-shutdown-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessingMode(ProcessingMode.PARALLEL)
      .withWaitForMessagesTimeout(Duration.ofMillis(200))
      .withErrorHandler(_ -> errors.incrementAndGet())
      .withTracer(tracer)
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          if (new String(value, UTF_8).equals("v" + FAILING_OFFSET)) {
            try {
              neverReleased.await();
            } catch (final InterruptedException e) {
              // Deliberately does not restore the flag.
              aborted.countDown();
              throw new IllegalStateException("aborted", e);
            }
          }
          processed.incrementAndGet();
          return value;
        })
      )
      .withConsumer(() -> mockConsumer)
      .build();

    try {
      consumer.start();
      TestAwaits.pollUntil(() -> processed.get() == RECORDS - 1, Duration.ofSeconds(10), "every other record finishes");
      consumer.close();
      assertTrue(aborted.await(5, TimeUnit.SECONDS), "shutdown interrupts the stuck record");
      assertTrue(stuckRecordDone.await(5, TimeUnit.SECONDS), "the stuck record's outcome path finishes");
    } finally {
      neverReleased.countDown();
    }

    assertEquals(0, errors.get(), "a record cut short at shutdown is abandoned, not reported as a failure");
    assertEquals(
      FAILING_OFFSET,
      commits.get(PARTITION).offset(),
      "the record cut short at shutdown stays unmarked, so the commit stops at it"
    );
  }
}
