package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.github.eschizoid.kpipe.record.RecordContext;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchResult;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

/// The consumer binds each record as [RecordContext#current()] for every processing attempt, on
/// both route shapes, and binds no record while a batch sink runs.
class RecordContextBindingTest {

  private static final String TOPIC = "record-context-topic";
  private static final long TIMESTAMP = 1_700_000_000_000L;

  /// What an operator or sink saw as the current record.
  private record Seen(String topic, int partition, long offset, long timestamp, String key, String tenant) {
    static Seen current() {
      final var ctx = RecordContext.current();
      return new Seen(
        ctx.topic(),
        ctx.partition(),
        ctx.offset(),
        ctx.timestamp(),
        ctx.keyString(),
        ctx.headerString("tenant").orElse(null)
      );
    }
  }

  private static final Seen EXPECTED = new Seen(TOPIC, 2, 7L, TIMESTAMP, "order-1", "acme");

  @Test
  void everyAttemptAndTheSinkReadTheConsumedRecord() throws Exception {
    final var attempts = new AtomicInteger();
    final var seen = new CopyOnWriteArrayList<Seen>();
    final var pipeline = new MessageProcessorRegistry()
      .pipeline(MessageFormat.bytes())
      .pipe(value -> {
        seen.add(Seen.current());
        if (attempts.incrementAndGet() == 1) throw new IllegalStateException("first attempt fails");
        return value;
      })
      .toSink(_ -> seen.add(Seen.current()))
      .build();

    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withPipeline(pipeline)
      .withRetry(1, Duration.ofMillis(1))
      .withConsumer(RecordContextBindingTest::seeded)
      .withPollTimeout(Duration.ofMillis(5))
      .build();
    try {
      consumer.start();
      awaitCondition(() -> seen.size() == 3, 10_000);
    } finally {
      consumer.close();
    }

    assertEquals(List.of(EXPECTED, EXPECTED, EXPECTED), List.copyOf(seen), "two attempts, then the sink");
    assertFalse(RecordContext.isBound(), "nothing is bound on the test thread");
  }

  @Test
  void aBatchRouteBindsTheRecordBeforeBufferingAndNoneWhileTheSinkRuns() throws Exception {
    final var beforeBuffer = new CopyOnWriteArrayList<Seen>();
    final var inSink = new CopyOnWriteArrayList<String>();
    final var pipeline = new MessageProcessorRegistry()
      .pipeline(MessageFormat.bytes())
      .pipe(value -> {
        beforeBuffer.add(Seen.current());
        return value;
      })
      .build();

    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withBatchPipeline(
        TOPIC,
        pipeline,
        values -> {
          inSink.add(RecordContext.isBound() ? "bound" : "unbound");
          return BatchResult.allSucceeded(values.size());
        },
        BatchPolicy.ofSize(1)
      )
      .withConsumer(RecordContextBindingTest::seeded)
      .withPollTimeout(Duration.ofMillis(5))
      .build();
    try {
      consumer.start();
      awaitCondition(() -> !inSink.isEmpty(), 10_000);
    } finally {
      consumer.close();
    }

    assertEquals(List.of(EXPECTED), List.copyOf(beforeBuffer));
    assertEquals(List.of("unbound"), List.copyOf(inSink), "a size-triggered flush runs on the record's thread");
  }

  /// The error handler gets its record in the `ProcessingError` and never has a current record:
  /// not after a per-record failure, and not after a failed size-triggered flush, which runs on
  /// the thread of the record that filled the buffer.
  @Test
  void theErrorHandlerNeverHasACurrentRecord() throws Exception {
    final var bound = new CopyOnWriteArrayList<String>();
    final KPipeConsumer.ErrorHandler handler = error ->
      bound.add(error.record().offset() + ":" + (RecordContext.isBound() ? "bound" : "unbound"));

    final var failing = new MessageProcessorRegistry()
      .pipeline(MessageFormat.bytes())
      .pipe(_ -> {
        throw new IllegalStateException("operator fails");
      })
      .toSink(_ -> {})
      .build();
    final var perRecord = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withPipeline(failing)
      .withErrorHandler(handler)
      .withConsumer(() -> seeded(1))
      .withPollTimeout(Duration.ofMillis(5))
      .build();
    try {
      perRecord.start();
      awaitCondition(() -> bound.size() == 1, 10_000);
    } finally {
      perRecord.close();
    }

    final var batch = KPipeConsumer.builder()
      .withProperties(props())
      .withBatchPipeline(
        TOPIC,
        new MessageProcessorRegistry().pipeline(MessageFormat.bytes()).build(),
        values -> BatchResult.allFailed(values.size(), new IllegalStateException("sink fails")),
        BatchPolicy.ofSize(2)
      )
      .withErrorHandler(handler)
      .withConsumer(() -> seeded(2))
      .withPollTimeout(Duration.ofMillis(5))
      .build();
    try {
      batch.start();
      awaitCondition(() -> bound.size() == 3, 10_000);
    } finally {
      batch.close();
    }

    assertEquals(List.of("7:unbound", "7:unbound", "8:unbound"), bound.stream().sorted().toList());
  }

  /// A batch sink reads each record through `currentBatch()`, index-aligned with the values and as
  /// the record stood when it was buffered, after a `rekey`; on a size-triggered flush and on an
  /// age-triggered one.
  @Test
  void aBatchSinkReadsItsRecordsThroughCurrentBatch() throws Exception {
    assertEquals(List.of("v7|key-7|order-1", "v8|key-8|order-2"), flushOf(BatchPolicy.ofSize(2)));
    assertEquals(
      List.of("v7|key-7|order-1", "v8|key-8|order-2"),
      flushOf(new BatchPolicy(1_000, Duration.ofMillis(100)))
    );
  }

  private static List<String> flushOf(final BatchPolicy policy) throws Exception {
    final var flushed = new CopyOnWriteArrayList<String>();
    final var pipeline = new MessageProcessorRegistry()
      .pipeline(MessageFormat.bytes())
      .rekeyString(_ -> "key-" + RecordContext.current().offset())
      .build();
    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withBatchPipeline(
        TOPIC,
        pipeline,
        values -> {
          final var contexts = RecordContext.currentBatch();
          for (int i = 0; i < values.size(); i++) flushed.add(
            new String(values.get(i), UTF_8) +
              "|" +
              contexts.get(i).keyString() +
              "|" +
              new String(contexts.get(i).originalKey(), UTF_8)
          );
          return BatchResult.allSucceeded(values.size());
        },
        policy
      )
      .withConsumer(() -> seeded(2))
      .withPollTimeout(Duration.ofMillis(5))
      .build();
    try {
      consumer.start();
      awaitCondition(() -> flushed.size() == 2, 10_000);
    } finally {
      consumer.close();
    }
    // Buffer order follows arrival, which PARALLEL mode does not fix; each value must sit with its
    // own record.
    return flushed.stream().sorted().toList();
  }

  private static MockConsumer<byte[], byte[]> seeded() {
    return seeded(1);
  }

  /// `count` records from offset 7 on partition 2; the first is keyed `order-1` with a `tenant`
  /// header.
  private static MockConsumer<byte[], byte[]> seeded(final int count) {
    final var mock = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var tp = new TopicPartition(TOPIC, 2);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 7L));
    final var headers = new RecordHeaders();
    headers.add("tenant", "acme".getBytes(UTF_8));
    for (int i = 0; i < count; i++) {
      mock.addRecord(
        new ConsumerRecord<>(
          TOPIC,
          2,
          7L + i,
          TIMESTAMP,
          TimestampType.CREATE_TIME,
          0,
          1,
          ("order-" + (i + 1)).getBytes(UTF_8),
          ("v" + (7 + i)).getBytes(UTF_8),
          headers,
          Optional.empty()
        )
      );
    }
    return mock;
  }

  private static Properties props() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "record-context-group");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("enable.auto.commit", "false");
    return p;
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
}
