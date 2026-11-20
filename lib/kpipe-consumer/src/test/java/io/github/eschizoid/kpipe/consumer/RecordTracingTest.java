package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import io.github.eschizoid.kpipe.tracing.TraceContext;
import io.github.eschizoid.kpipe.tracing.Tracer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/// Each record's span follows the record, not the thread: the sink sees it, every failed attempt is
/// recorded on it, a dead-letter record written by a batch flush on another thread carries it, and
/// it is closed exactly once.
class RecordTracingTest {

  private static final String TOPIC = "traced";
  private static final String DLQ = "traced-dlq";

  /// A tracer whose spans stamp their record's offset into the headers they inject, and count what
  /// happens to them.
  private static final class RecordingTracer implements Tracer {

    final Map<Long, RecordingSpan> spans = new ConcurrentHashMap<>();

    @Override
    public SpanScope startConsumerSpan(final ConsumerRecord<?, byte[]> record, final String consumerGroup) {
      final var span = new RecordingSpan(record.offset());
      spans.put(record.offset(), span);
      return span;
    }
  }

  private static final class RecordingSpan implements Tracer.SpanScope {

    final long offset;
    final AtomicInteger attemptsFailed = new AtomicInteger();
    final AtomicInteger failures = new AtomicInteger();
    final AtomicInteger closes = new AtomicInteger();

    RecordingSpan(final long offset) {
      this.offset = offset;
    }

    @Override
    public Activation activate() {
      return Activation.NONE;
    }

    @Override
    public void inject(final Headers headers) {
      headers.remove("x-span");
      headers.add("x-span", Long.toString(offset).getBytes(UTF_8));
    }

    @Override
    public void attemptFailed(final int attempt, final Throwable failure) {
      attemptsFailed.incrementAndGet();
    }

    @Override
    public void recordException(final Throwable failure) {
      failures.incrementAndGet();
    }

    @Override
    public void close() {
      closes.incrementAndGet();
    }
  }

  @Test
  void theSinkRunsUnderItsRecordsSpan() {
    final var tracer = new RecordingTracer();
    final var seen = new CopyOnWriteArrayList<Tracer.SpanScope>();
    try (
      final var consumer = KPipeConsumer.builder()
        .withProperties(props())
        .withTopic(TOPIC)
        .withPipeline(
          TestPipelines.sideEffect(value -> {
            seen.add(TraceContext.current());
            return value;
          })
        )
        .withTracer(tracer)
        .build()
    ) {
      consumer.processRecord(new ConsumerRecord<>(TOPIC, 0, 5L, "k".getBytes(UTF_8), "v".getBytes(UTF_8)));

      assertSame(tracer.spans.get(5L), seen.getFirst());
      assertEquals(1, tracer.spans.get(5L).closes.get());
    }
  }

  @Test
  void eachFailedAttemptIsRecordedAndTheFinalFailureOnce() {
    final var tracer = new RecordingTracer();
    try (
      final var consumer = KPipeConsumer.builder()
        .withProperties(props())
        .withTopic(TOPIC)
        .withPipeline(
          TestPipelines.sideEffect(_ -> {
            throw new IllegalStateException("down");
          })
        )
        .withRetry(2, Duration.ofMillis(1))
        .withTracer(tracer)
        .build()
    ) {
      consumer.processRecord(new ConsumerRecord<>(TOPIC, 0, 1L, "k".getBytes(UTF_8), "v".getBytes(UTF_8)));

      final var span = tracer.spans.get(1L);
      assertEquals(3, span.attemptsFailed.get(), "the first attempt and two retries");
      assertEquals(1, span.failures.get());
      assertEquals(1, span.closes.get());
    }
  }

  /// A failed batch is sent to the dead-letter topic from the flush, on another thread than each
  /// record's own; every dead-letter record still carries its own record's span, through a
  /// producer the consumer did not build.
  @Test
  void aFailedBatchSendsEachRecordToTheDlqWithItsOwnTrace() throws InterruptedException {
    final var tracer = new RecordingTracer();
    final var producer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
    final var handlerSaw = new ConcurrentHashMap<Long, Tracer.SpanScope>();
    try (
      final var consumer = KPipeConsumer.builder()
        .withProperties(props())
        .withBatchPipeline(
          TOPIC,
          TestPipelines.identity(),
          BatchSink.ofVoid(_ -> {
            throw new IllegalStateException("warehouse down");
          }),
          BatchPolicy.ofSize(3)
        )
        .withDeadLetterTopic(DLQ)
        .withKafkaProducer(producer)
        .withErrorHandler(e -> handlerSaw.put(e.record().offset(), TraceContext.current()))
        .withTracer(tracer)
        .withConsumer(() -> seeded(3))
        .withPollTimeout(Duration.ofMillis(5))
        .build()
    ) {
      consumer.start();
      TestAwaits.pollUntil(() -> producer.history().size() == 3, Duration.ofSeconds(5), "three DLQ sends");

      final var carried = new ArrayList<String>();
      final var sourceOffsets = new ArrayList<String>();
      for (final var sent : producer.history()) {
        carried.add(new String(sent.headers().lastHeader("x-span").value(), UTF_8));
        sourceOffsets.add(new String(sent.headers().lastHeader("x-dlq-source-offset").value(), UTF_8));
      }
      assertEquals(sourceOffsets, carried, "each DLQ record carries its own record's span");
      TestAwaits.pollUntil(
        () ->
          tracer.spans
            .values()
            .stream()
            .allMatch(s -> s.closes.get() == 1),
        Duration.ofSeconds(5),
        "every span closed"
      );
      tracer.spans.values().forEach(s -> assertEquals(1, s.failures.get()));
      TestAwaits.pollUntil(() -> handlerSaw.size() == 3, Duration.ofSeconds(5), "three error handler calls");
      tracer.spans.forEach((offset, span) ->
        assertSame(span, handlerSaw.get(offset), "the handler runs under its record's span")
      );
    }
  }

  @Test
  void aFlushedBatchClosesEachRecordsSpanOnce() throws InterruptedException {
    final var tracer = new RecordingTracer();
    try (
      final var consumer = KPipeConsumer.builder()
        .withProperties(props())
        .withBatchPipeline(TOPIC, TestPipelines.identity(), BatchSink.ofVoid(_ -> {}), BatchPolicy.ofSize(3))
        .withTracer(tracer)
        .withConsumer(() -> seeded(3))
        .withPollTimeout(Duration.ofMillis(5))
        .build()
    ) {
      consumer.start();
      TestAwaits.pollUntil(() -> consumer.metrics().records().processed() == 3L, Duration.ofSeconds(5), "flushed");
      TestAwaits.pollUntil(
        () ->
          tracer.spans.size() == 3 &&
          tracer.spans
            .values()
            .stream()
            .allMatch(s -> s.closes.get() == 1),
        Duration.ofSeconds(5),
        "every span closed once, after the flush"
      );
      assertTrue(
        tracer.spans
          .values()
          .stream()
          .allMatch(s -> s.failures.get() == 0)
      );
    }
  }

  /// A tracer that returns null, throws, or hands out an activation whose close throws loses its
  /// span, and the record is still processed.
  @Test
  void aBrokenTracerNeverStopsARecord() {
    final Tracer returnsNull = (_, _) -> null;
    final Tracer throwsOnStart = (_, _) -> {
      throw new IllegalStateException("tracer down");
    };
    final var closes = new AtomicInteger();
    final Tracer throwsOnDeactivate = (_, _) ->
      new Tracer.SpanScope() {
        @Override
        public Activation activate() {
          return () -> {
            throw new AssertionError("activation close");
          };
        }

        @Override
        public void inject(final Headers headers) {}

        @Override
        public void recordException(final Throwable failure) {}

        @Override
        public void close() {
          closes.incrementAndGet();
        }
      };
    for (final var tracer : List.of(returnsNull, throwsOnStart, throwsOnDeactivate)) {
      try (
        final var consumer = KPipeConsumer.builder()
          .withProperties(props())
          .withTopic(TOPIC)
          .withPipeline(TestPipelines.identity())
          .withTracer(tracer)
          .build()
      ) {
        consumer.processRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k".getBytes(UTF_8), "v".getBytes(UTF_8)));
        assertEquals(1L, consumer.metrics().records().processed());
      }
    }
    assertEquals(1, closes.get(), "the span is still closed when its activation's close throws");
  }

  private static MockConsumer<byte[], byte[]> seeded(final int count) {
    final var mock = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var partition = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(partition));
    mock.updateBeginningOffsets(Map.of(partition, 0L));
    for (int i = 0; i < count; i++) mock.addRecord(
      new ConsumerRecord<>(TOPIC, 0, i, ("k" + i).getBytes(UTF_8), "v".getBytes(UTF_8))
    );
    return mock;
  }

  private static Properties props() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "tracing");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return p;
  }
}
