package io.github.eschizoid.kpipe.test;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.Operators;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;

/// A Docker-free test driver for KPipe pipelines. Wraps a real [KPipeConsumer] around a Kafka
/// `MockConsumer`, so tests exercise the production dispatch, offset, and sink machinery without
/// a broker — each test completes in milliseconds instead of the seconds a Testcontainers
/// round-trip costs.
///
/// ```java
/// final var captured = new CapturingSink<Map<String, Object>>();
/// try (final var driver = TestStream.<Map<String, Object>>builder(JsonFormat.INSTANCE)
///     .pipe(addTimestamp)
///     .filter(active)
///     .toCustom(captured)
///     .build()) {
///   driver.send(record1);
///   driver.send(record2);
///   driver.flush();
///   assertEquals(List.of(expected), captured.captured());
/// }
/// ```
///
/// **Drive model.** `send` serializes the typed value through the configured [MessageFormat] and
/// appends it to the mock partition; the running consumer polls it off and drives it through the
/// same pipeline code paths production uses. Processing defaults to
/// [ProcessingMode#SEQUENTIAL] so capture order matches send order; override with
/// [Builder#withProcessingMode] to exercise parallel behaviour.
///
/// **Determinism.** [#flush] blocks until every sent record is accounted for — terminal
/// (processed, filtered, or failed) or, in batch mode, buffered awaiting the size trigger — and
/// the dispatcher's in-flight count has drained. Assertions after `flush()` never race the
/// consumer thread.
///
/// **Batch semantics.** With [Builder#toBatch], full batches flush on the size trigger as records
/// arrive (observable after `flush()`); the trailing partial batch flushes on [#close] — the same
/// shutdown-drain guarantee the production consumer gives. There is no age-based flush: the
/// policy's age cap is set beyond any sane test duration so size and shutdown are the only
/// triggers.
///
/// @param <T> the pipeline value type
public final class TestStream<T> implements AutoCloseable {

  private static final Duration DEFAULT_FLUSH_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration CLOSE_AGE_CAP = Duration.ofDays(1);

  // Public metric-map keys of KPipeConsumer.getMetrics(); see the metrics table in README.
  private static final String METRIC_RECEIVED = "messagesReceived";
  private static final String METRIC_PROCESSED = "messagesProcessed";
  private static final String METRIC_ERRORS = "processingErrors";
  private static final String METRIC_IN_FLIGHT = "inFlight";

  private final String topic;
  private final MessageFormat<T> format;
  private final KPipeConsumer consumer;
  private final MockConsumer<byte[], byte[]> mockConsumer;
  private final AtomicLong nextOffset = new AtomicLong(0);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final CopyOnWriteArrayList<KPipeConsumer.ProcessingError> errors;

  private TestStream(
    final String topic,
    final MessageFormat<T> format,
    final KPipeConsumer consumer,
    final MockConsumer<byte[], byte[]> mockConsumer,
    final CopyOnWriteArrayList<KPipeConsumer.ProcessingError> errors
  ) {
    this.topic = topic;
    this.format = format;
    this.consumer = consumer;
    this.mockConsumer = mockConsumer;
    this.errors = errors;
  }

  /// Creates a builder for a test stream over the given format.
  ///
  /// @param format the SerDe for the pipeline value type
  /// @param <T> the pipeline value type
  /// @return a new builder
  public static <T> Builder<T> builder(final MessageFormat<T> format) {
    return new Builder<>(format);
  }

  /// Serializes `value` through the stream's format and appends it to the mock partition with a
  /// null key.
  ///
  /// @param value the typed record value (must not be null)
  /// @return this stream for chaining
  public TestStream<T> send(final T value) {
    return send(null, value);
  }

  /// Serializes `value` through the stream's format and appends it to the mock partition. The
  /// key is encoded as UTF-8 bytes — the consumer always sees `byte[]` keys, matching
  /// production.
  ///
  /// @param key the record key (nullable)
  /// @param value the typed record value (must not be null)
  /// @return this stream for chaining
  public TestStream<T> send(final String key, final T value) {
    Objects.requireNonNull(value, "value cannot be null — use sendRaw to exercise null-value error paths");
    return sendRaw(key, format.serialize(value));
  }

  /// Appends raw bytes to the mock partition with a null key, bypassing the format's serializer.
  /// Use this to exercise deserialization-failure paths with malformed payloads.
  ///
  /// @param rawValue the raw record value (nullable — a null value exercises the consumer's
  ///                 null-record error path)
  /// @return this stream for chaining
  public TestStream<T> sendRaw(final byte[] rawValue) {
    return sendRaw(null, rawValue);
  }

  /// Appends raw bytes to the mock partition, bypassing the format's serializer.
  ///
  /// @param key the record key (nullable)
  /// @param rawValue the raw record value (nullable)
  /// @return this stream for chaining
  public TestStream<T> sendRaw(final String key, final byte[] rawValue) {
    ensureOpen();
    final var offset = nextOffset.getAndIncrement();
    final var keyBytes = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
    mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, offset, keyBytes, rawValue));
    return this;
  }

  /// Deterministically drains the stream with the default 10-second timeout. See
  /// [#flush(Duration)].
  ///
  /// @return this stream for chaining
  public TestStream<T> flush() {
    return flush(DEFAULT_FLUSH_TIMEOUT);
  }

  /// Blocks until every record sent so far is accounted for: polled off the mock partition,
  /// finished processing (dispatcher in-flight count drained to zero), and terminal — processed,
  /// filtered, or failed. In batch mode records buffered awaiting the size trigger also count as
  /// accounted for; the trailing partial batch flushes on [#close].
  ///
  /// @param timeout the maximum time to wait
  /// @return this stream for chaining
  /// @throws AssertionError if the stream does not quiesce within `timeout`
  public TestStream<T> flush(final Duration timeout) {
    ensureOpen();
    final var target = nextOffset.get();
    final var deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (quiescent(target)) return this;
      try {
        //noinspection BusyWait — deadline-bounded quiescence poll, not a spin
        Thread.sleep(5);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("flush() interrupted while waiting for " + target + " records: " + metrics(), e);
      }
    }
    throw new AssertionError(
      "flush(" + timeout + ") timed out waiting for " + target + " records to settle: " + metrics()
    );
  }

  /// A snapshot of the underlying consumer's metrics (`messagesReceived`, `messagesProcessed`,
  /// `processingErrors`, `inFlight`, ...), for assertions on filter and error paths.
  ///
  /// @return an immutable metrics snapshot
  public Map<String, Long> metrics() {
    return consumer.getMetrics();
  }

  /// Every [KPipeConsumer.ProcessingError] the consumer reported so far, in report order. Failed
  /// operators, deserialization failures, and null-value records all land here — the same
  /// terminal-error channel production error handlers observe.
  ///
  /// @return an immutable snapshot of the reported errors
  public List<KPipeConsumer.ProcessingError> errors() {
    return List.copyOf(errors);
  }

  /// Closes the underlying consumer. In batch mode this drains the trailing partial batch (the
  /// production shutdown-flush guarantee), so batch assertions covering every sent record belong
  /// after `close()`. Idempotent.
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) consumer.close();
  }

  /// All sent records are either terminal (processed / filtered / failed) or buffered in a batch
  /// wrapper, and no dispatcher worker is mid-record. Checked in a deliberate order: records move
  /// strictly forward (unpolled → in-flight → buffered/terminal), so observing "all accounted
  /// for" *before* observing "in-flight drained" cannot yield a false positive — a record that
  /// was still unpolled at the first check keeps the sum below target, and a record still
  /// processing fails the drain check. The final re-read confirms the accounting after the drain.
  private boolean quiescent(final long target) {
    final var snapshot = metrics();
    if (snapshot.get(METRIC_RECEIVED) < target) return false;
    if (accountedFor(snapshot) < target) return false;
    if (!consumer.waitForInFlightDrain(Duration.ofMillis(1))) return false;
    final var settled = metrics();
    return settled.get(METRIC_RECEIVED) >= target && accountedFor(settled) >= target;
  }

  /// Terminal records plus in-flight ones (`inFlight` = dispatcher pending + batch-buffered;
  /// after a drain, pending is zero so the in-flight component is exactly the buffered count).
  private static long accountedFor(final Map<String, Long> snapshot) {
    return snapshot.get(METRIC_PROCESSED) + snapshot.get(METRIC_ERRORS) + snapshot.get(METRIC_IN_FLIGHT);
  }

  private void ensureOpen() {
    if (closed.get()) throw new IllegalStateException("TestStream is closed");
  }

  /// Builder for [TestStream]. Mirrors the production facade's `pipe` / `filter` / `peek` /
  /// `toCustom` vocabulary so a passing TestStream chain translates 1:1 to a `KPipe.X(...)`
  /// chain.
  ///
  /// @param <T> the pipeline value type
  public static final class Builder<T> {

    private final MessageFormat<T> format;
    private final List<UnaryOperator<T>> operators = new ArrayList<>();
    private String topic = "kpipe-test-topic";
    private MessageSink<T> sink;
    private BatchSink<T> batchSink;
    private int batchMaxSize;
    private ProcessingMode processingMode = ProcessingMode.SEQUENTIAL;
    private Consumer<KPipeConsumer.ProcessingError> errorHandler;

    private Builder(final MessageFormat<T> format) {
      this.format = Objects.requireNonNull(format, "format cannot be null");
    }

    /// Overrides the synthetic topic name (default `kpipe-test-topic`). Only matters when an
    /// operator or assertion inspects `ProcessingError.record().topic()`.
    ///
    /// @param topic the topic name
    /// @return this builder
    public Builder<T> withTopic(final String topic) {
      Objects.requireNonNull(topic, "topic cannot be null");
      if (topic.isBlank()) throw new IllegalArgumentException("topic cannot be blank");
      this.topic = topic;
      return this;
    }

    /// Appends a transformation operator to the pipeline.
    ///
    /// @param op the operator
    /// @return this builder
    public Builder<T> pipe(final UnaryOperator<T> op) {
      operators.add(Objects.requireNonNull(op, "operator cannot be null"));
      return this;
    }

    /// Appends a filter: records failing the predicate are dropped as intentional filtering
    /// (counted processed, never delivered to the sink, no error reported).
    ///
    /// @param keep records pass through only when this predicate returns true
    /// @return this builder
    public Builder<T> filter(final Predicate<T> keep) {
      Objects.requireNonNull(keep, "predicate cannot be null");
      operators.add(Operators.filter(keep));
      return this;
    }

    /// Appends a side-effect observer that passes the value through unchanged.
    ///
    /// @param sideEffect the observer
    /// @return this builder
    public Builder<T> peek(final Consumer<T> sideEffect) {
      Objects.requireNonNull(sideEffect, "sideEffect cannot be null");
      operators.add(Operators.peek(sideEffect));
      return this;
    }

    /// Sets the terminal sink — typically a [CapturingSink]. Mutually exclusive with [#toBatch].
    ///
    /// @param sink the terminal sink
    /// @return this builder
    public Builder<T> toCustom(final MessageSink<T> sink) {
      this.sink = Objects.requireNonNull(sink, "sink cannot be null");
      return this;
    }

    /// Routes the stream through a batch sink flushing every `maxSize` records. The trailing
    /// partial batch flushes when the stream closes. There is no age-based flush trigger.
    /// Mutually exclusive with [#toCustom].
    ///
    /// @param sink the batch sink (wrap a void-style consumer with `BatchSink.ofVoid`)
    /// @param maxSize the size-flush threshold (must be ≥ 1)
    /// @return this builder
    public Builder<T> toBatch(final BatchSink<T> sink, final int maxSize) {
      this.batchSink = Objects.requireNonNull(sink, "sink cannot be null");
      if (maxSize < 1) throw new IllegalArgumentException("maxSize must be >= 1, got " + maxSize);
      this.batchMaxSize = maxSize;
      return this;
    }

    /// Overrides the processing mode (default [ProcessingMode#SEQUENTIAL], which keeps capture
    /// order identical to send order). Use [ProcessingMode#PARALLEL] or
    /// [ProcessingMode#KEY_ORDERED] to exercise concurrent dispatch — assertions must then
    /// compare order-insensitively.
    ///
    /// @param mode the processing mode
    /// @return this builder
    public Builder<T> withProcessingMode(final ProcessingMode mode) {
      this.processingMode = Objects.requireNonNull(mode, "mode cannot be null");
      return this;
    }

    /// Registers an additional error observer, invoked after the stream records the error for
    /// [TestStream#errors].
    ///
    /// @param handler the error observer
    /// @return this builder
    public Builder<T> withErrorHandler(final Consumer<KPipeConsumer.ProcessingError> handler) {
      this.errorHandler = Objects.requireNonNull(handler, "handler cannot be null");
      return this;
    }

    /// Builds and starts the stream: constructs the pipeline, seeds a `MockConsumer`, and starts
    /// the [KPipeConsumer] poll loop. The returned stream is immediately ready for
    /// [TestStream#send].
    ///
    /// @return a started test stream
    public TestStream<T> build() {
      if (sink != null && batchSink != null) throw new IllegalArgumentException(
        "toCustom and toBatch are mutually exclusive — a stream terminates in exactly one sink shape"
      );

      final var pipelineBuilder = new MessageProcessorRegistry().pipeline(format);
      for (final var op : operators) pipelineBuilder.add(op);
      if (sink != null) pipelineBuilder.toSink(sink);
      final var pipeline = pipelineBuilder.build();

      final var mock = new MockConsumer<byte[], byte[]>("earliest") {
        @Override
        public synchronized void subscribe(final Collection<String> topics) {}

        @Override
        public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
      };
      final var tp = new TopicPartition(topic, 0);
      mock.assign(List.of(tp));
      mock.updateBeginningOffsets(Map.of(tp, 0L));

      final var errors = new CopyOnWriteArrayList<KPipeConsumer.ProcessingError>();
      final var userHandler = errorHandler;
      final KPipeConsumer.ErrorHandler recordingHandler = error -> {
        errors.add(error);
        if (userHandler != null) userHandler.accept(error);
      };

      final var consumerBuilder = KPipeConsumer.builder()
        .withProperties(props())
        .withProcessingMode(processingMode)
        .withErrorHandler(recordingHandler)
        .withConsumer(() -> mock)
        .withPollTimeout(Duration.ofMillis(10));
      if (batchSink != null) {
        consumerBuilder.withBatchPipeline(topic, pipeline, batchSink, new BatchPolicy(batchMaxSize, CLOSE_AGE_CAP));
      } else {
        consumerBuilder.withTopic(topic).withPipeline(pipeline);
      }

      final var consumer = consumerBuilder.build();
      consumer.start();
      return new TestStream<>(topic, format, consumer, mock, errors);
    }

    /// Minimal, never-contacted config — the `MockConsumer` supplier bypasses the real client, so
    /// only the properties object's presence matters.
    private static Properties props() {
      final var p = new Properties();
      p.put("bootstrap.servers", "localhost:9092");
      p.put("group.id", "kpipe-test");
      p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      p.put("enable.auto.commit", "false");
      return p;
    }
  }
}
