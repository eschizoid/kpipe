package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.format.json.JsonFormat;
import io.github.eschizoid.kpipe.pipeline.MalformedRecordException;
import io.github.eschizoid.kpipe.pipeline.MessageFormat;
import io.github.eschizoid.kpipe.pipeline.NonRetryableException;
import io.github.eschizoid.kpipe.pipeline.Pipeline;
import io.github.eschizoid.kpipe.record.RecordContext;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchResult;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// The attempt observer hears every failed processing attempt, at every stage, before the error
/// handler hears the record's final failure.
class AttemptFailedObserverTest {

  private static final String TOPIC = "attempt-observer-topic";

  /// What the observer and the error handler heard, in order: `attempt:<retryCount>:<cause>` and
  /// `error:<retryCount>:<cause>`.
  private final List<String> heard = new CopyOnWriteArrayList<>();

  private final KPipeConsumer.ErrorHandler observer = error ->
    heard.add("attempt:" + error.retryCount() + ":" + error.exception().getMessage());

  private final KPipeConsumer.ErrorHandler errorHandler = error ->
    heard.add("error:" + error.retryCount() + ":" + error.exception().getMessage());

  @Test
  void aRecordThatRecoversIsHeardOnlyByTheObserver() throws Exception {
    final var calls = new AtomicInteger();
    run(
      operator(value -> {
        if (calls.incrementAndGet() <= 2) throw new IllegalStateException("transient");
        return value;
      }),
      2,
      2
    );
    assertEquals(List.of("attempt:0:transient", "attempt:1:transient"), List.copyOf(heard));
  }

  @Test
  void everyAttemptOfAFailingRecordIsHeardThenTheErrorHandler() throws Exception {
    run(
      operator(_ -> {
        throw new IllegalStateException("down");
      }),
      1,
      3
    );
    assertEquals(List.of("attempt:0:down", "attempt:1:down", "error:1:down"), List.copyOf(heard));
  }

  @Test
  void aNonRetryableFailureIsHeardOnce() throws Exception {
    run(
      operator(_ -> {
        throw new NonRetryableException("poison");
      }),
      3,
      2
    );
    assertEquals(List.of("attempt:0:poison", "error:0:poison"), List.copyOf(heard));
  }

  @Test
  void aSinkFailureIsHeard() throws Exception {
    final var pipeline = Pipeline.from(MessageFormat.bytes())
      .toSink(_ -> {
        throw new IllegalStateException("sink down");
      })
      .build();
    run(pipeline, 0, 2);
    assertEquals(List.of("attempt:0:sink down", "error:0:sink down"), List.copyOf(heard));
  }

  /// Bytes that do not parse fail the same way on every attempt, so a malformed record gets one
  /// attempt however many retries are configured.
  @Test
  void aMalformedRecordIsHeardOnceAndNotRetried() throws Exception {
    final var errors = new CopyOnWriteArrayList<Exception>();
    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withPipeline(
        Pipeline.from(JsonFormat.INSTANCE)
          .toSink(_ -> {})
          .build()
      )
      .withRetry(3, Duration.ofMillis(1))
      .withAttemptFailedObserver(observer)
      .withErrorHandler(error -> {
        errors.add(error.exception());
        errorHandler.accept(error);
      })
      .withConsumer(AttemptFailedObserverTest::seeded)
      .withPollTimeout(Duration.ofMillis(5))
      .build();
    awaitHeard(consumer, 2);
    assertEquals(2, heard.size(), List.copyOf(heard).toString());
    assertTrue(heard.getFirst().startsWith("attempt:0:"), heard.getFirst());
    assertTrue(heard.getLast().startsWith("error:0:"), heard.getLast());
    assertInstanceOf(MalformedRecordException.class, errors.getFirst());
  }

  /// A format failure that is not about the bytes, such as a schema registry that is down, may pass
  /// on a later attempt, so it is retried.
  @Test
  void aFormatFailureThatMayPassLaterIsRetried() throws Exception {
    final var calls = new AtomicInteger();
    final var registryDown = new MessageFormat<byte[]>() {
      @Override
      public byte[] serialize(final byte[] data) {
        return data;
      }

      @Override
      public byte[] deserialize(final byte[] data) {
        calls.incrementAndGet();
        throw new IllegalStateException("registry down");
      }
    };
    run(
      Pipeline.from(registryDown)
        .toSink(_ -> {})
        .build(),
      2,
      4
    );
    assertEquals(3, calls.get(), "one attempt plus two retries");
    assertTrue(heard.getLast().startsWith("error:2:"), heard.getLast());
  }

  @Test
  void aFailedBatchFlushIsHeardForEachRecord() throws Exception {
    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withBatchPipeline(
        TOPIC,
        Pipeline.from(MessageFormat.bytes()).build(),
        values -> BatchResult.allFailed(values.size(), new IllegalStateException("flush down")),
        BatchPolicy.ofSize(1)
      )
      .withAttemptFailedObserver(observer)
      .withErrorHandler(errorHandler)
      .withConsumer(AttemptFailedObserverTest::seeded)
      .withPollTimeout(Duration.ofMillis(5))
      .build();
    awaitHeard(consumer, 2);
    assertEquals(List.of("attempt:0:flush down", "error:0:flush down"), List.copyOf(heard));
  }

  /// The observer has no current record, and one that throws changes nothing for the record.
  @Test
  void theObserverHasNoCurrentRecordAndCannotChangeTheOutcome() throws Exception {
    final var bound = new CopyOnWriteArrayList<Boolean>();
    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withPipeline(
        operator(_ -> {
          throw new IllegalStateException("down");
        })
      )
      .withRetry(1, Duration.ofMillis(1))
      .withAttemptFailedObserver(_ -> {
        bound.add(RecordContext.isBound());
        throw new IllegalArgumentException("observer bug");
      })
      .withErrorHandler(errorHandler)
      .withConsumer(AttemptFailedObserverTest::seeded)
      .withPollTimeout(Duration.ofMillis(5))
      .build();
    awaitHeard(consumer, 1);
    assertEquals(List.of(false, false), List.copyOf(bound), "both attempts reached the observer, unbound");
    assertEquals(List.of("error:1:down"), List.copyOf(heard), "the record still retried and failed");
  }

  /// An observer that throws an `AssertionError`, as a failed assertion in a test does, still
  /// cannot change what happens to the record.
  @Test
  void anObserverThatThrowsAnErrorCannotChangeTheOutcome() throws Exception {
    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withPipeline(
        operator(_ -> {
          throw new IllegalStateException("down");
        })
      )
      .withRetry(1, Duration.ofMillis(1))
      .withAttemptFailedObserver(_ -> {
        throw new AssertionError("assertion in an observer");
      })
      .withErrorHandler(errorHandler)
      .withConsumer(AttemptFailedObserverTest::seeded)
      .withPollTimeout(Duration.ofMillis(5))
      .build();
    awaitHeard(consumer, 1);
    assertEquals(List.of("error:1:down"), List.copyOf(heard), "the record still retried and failed");
  }

  /// A flush that fails one of two records reports only that record.
  @Test
  void aFlushReportsOnlyTheRecordsItFailed() throws Exception {
    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withBatchPipeline(
        TOPIC,
        Pipeline.from(MessageFormat.bytes()).build(),
        values -> new BatchResult(List.of(0), Map.of(1, new IllegalStateException("second down"))),
        BatchPolicy.ofSize(2)
      )
      .withAttemptFailedObserver(observer)
      .withErrorHandler(errorHandler)
      .withConsumer(() -> seeded(2))
      .withPollTimeout(Duration.ofMillis(5))
      .build();
    awaitHeard(consumer, 2);
    assertEquals(List.of("attempt:0:second down", "error:0:second down"), List.copyOf(heard));
  }

  /// A record retried before it was buffered reports those retries when its flush fails.
  @Test
  void aFlushFailureReportsTheRetriesTakenBeforeBuffering() throws Exception {
    final var calls = new AtomicInteger();
    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withBatchPipeline(
        TOPIC,
        Pipeline.from(MessageFormat.bytes())
          .pipe(value -> {
            if (calls.incrementAndGet() == 1) throw new IllegalStateException("transient");
            return value;
          })
          .build(),
        values -> BatchResult.allFailed(values.size(), new IllegalStateException("flush down")),
        BatchPolicy.ofSize(1)
      )
      .withRetry(1, Duration.ofMillis(1))
      .withAttemptFailedObserver(observer)
      .withErrorHandler(errorHandler)
      .withConsumer(AttemptFailedObserverTest::seeded)
      .withPollTimeout(Duration.ofMillis(5))
      .build();
    awaitHeard(consumer, 3);
    assertEquals(List.of("attempt:0:transient", "attempt:1:flush down", "error:1:flush down"), List.copyOf(heard));
  }

  private static Pipeline<byte[]> operator(final UnaryOperator<byte[]> op) {
    return Pipeline.from(MessageFormat.bytes())
      .pipe(op)
      .toSink(_ -> {})
      .build();
  }

  private void run(final Pipeline<?> pipeline, final int retries, final int expected) throws Exception {
    final var builder = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withPipeline(pipeline)
      .withAttemptFailedObserver(observer)
      .withErrorHandler(errorHandler)
      .withConsumer(AttemptFailedObserverTest::seeded)
      .withPollTimeout(Duration.ofMillis(5));
    if (retries > 0) builder.withRetry(retries, Duration.ofMillis(1));
    awaitHeard(builder.build(), expected);
  }

  private void awaitHeard(final KPipeConsumer consumer, final int expected) throws Exception {
    try {
      consumer.start();
      awaitCondition(() -> heard.size() >= expected, 10_000);
      // Anything heard after the expected count would be a bug; give it a moment to show.
      Thread.sleep(50);
    } finally {
      consumer.close();
    }
  }

  private static MockConsumer<byte[], byte[]> seeded() {
    return seeded(1);
  }

  private static MockConsumer<byte[], byte[]> seeded(final int count) {
    final var mock = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    for (int i = 0; i < count; i++) mock.addRecord(
      new ConsumerRecord<>(TOPIC, 0, i, ("k" + i).getBytes(UTF_8), "not json".getBytes(UTF_8))
    );
    return mock;
  }

  private static Properties props() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "attempt-observer-group");
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
