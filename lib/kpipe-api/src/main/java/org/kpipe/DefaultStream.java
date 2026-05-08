package org.kpipe;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.metrics.ConsumerMetrics;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.Operators;
import org.kpipe.sink.CompositeMessageSink;
import org.kpipe.sink.MessageSink;

/// Package-private immutable record-based [Stream]. Each fluent method returns a NEW
/// `DefaultStream` carrying the updated configuration; the original instance is never mutated.
/// This makes branching safe (`s.pipe(a)` and `s.pipe(b)` produce independent streams) and
/// matches the Java Stream API's functional style.
///
/// Adding a new fluent setter is a one-place change: declare the component, add the `with*`
/// fluent method, and reference it in [DefaultSink#start] if it affects consumer construction.
///
/// @param <T> the deserialized message type
record DefaultStream<T>(
  Set<String> topics,
  Properties kafkaProps,
  MessageFormat<T> format,
  Supplier<MessageSink<T>> defaultConsoleSinkFactory,
  List<UnaryOperator<T>> operators,
  int maxRetries,
  Duration retryBackoff,
  Long backpressureHigh,
  Long backpressureLow,
  boolean sequentialProcessing,
  int skipBytes,
  ConsumerMetrics consumerMetrics,
  Consumer<KPipeConsumer.ProcessingError<byte[]>> errorHandler,
  String deadLetterTopic,
  Duration pollTimeout
) implements Stream<T> {
  /// Public constructor used by [KPipe] factories — single topic, empty pipeline, default
  /// retry / backpressure settings.
  DefaultStream(
    final String topic,
    final Properties kafkaProps,
    final MessageFormat<T> format,
    final Supplier<MessageSink<T>> defaultConsoleSinkFactory
  ) {
    this(Set.of(Objects.requireNonNull(topic, "topic cannot be null")), kafkaProps, format, defaultConsoleSinkFactory);
  }

  /// Public constructor used by [KPipe] factories — multiple topics, single shared pipeline.
  /// Defensively copies `kafkaProps` and `topics` so subsequent caller mutations cannot affect
  /// the in-flight stream.
  DefaultStream(
    final Collection<String> topics,
    final Properties kafkaProps,
    final MessageFormat<T> format,
    final Supplier<MessageSink<T>> defaultConsoleSinkFactory
  ) {
    this(
      validateTopics(topics),
      (Properties) Objects.requireNonNull(kafkaProps, "kafkaProps cannot be null").clone(),
      Objects.requireNonNull(format, "format cannot be null"),
      Objects.requireNonNull(defaultConsoleSinkFactory, "defaultConsoleSinkFactory cannot be null"),
      List.of(),
      0,
      Duration.ofMillis(500),
      null,
      null,
      false,
      0,
      null,
      null,
      null,
      null
    );
  }

  private static Set<String> validateTopics(final Collection<String> topics) {
    Objects.requireNonNull(topics, "topics cannot be null");
    if (topics.isEmpty()) throw new IllegalArgumentException("topics cannot be empty");
    for (final var t : topics) {
      if (t == null || t.isBlank()) throw new IllegalArgumentException("topic name cannot be null or blank");
    }
    return Set.copyOf(topics);
  }

  @Override
  public Stream<T> pipe(final UnaryOperator<T> op) {
    return withOperator(Objects.requireNonNull(op, "operator cannot be null"));
  }

  @Override
  public Stream<T> filter(final Predicate<T> keep) {
    Objects.requireNonNull(keep, "predicate cannot be null");
    return withOperator(Operators.filter(keep));
  }

  @Override
  public Stream<T> peek(final Consumer<T> sideEffect) {
    Objects.requireNonNull(sideEffect, "sideEffect cannot be null");
    return withOperator(Operators.peek(sideEffect));
  }

  @Override
  public Stream<T> when(final Predicate<T> cond, final UnaryOperator<T> ifTrue, final UnaryOperator<T> ifFalse) {
    Objects.requireNonNull(cond, "condition cannot be null");
    Objects.requireNonNull(ifTrue, "ifTrue cannot be null");
    Objects.requireNonNull(ifFalse, "ifFalse cannot be null");
    return withOperator(value -> cond.test(value) ? ifTrue.apply(value) : ifFalse.apply(value));
  }

  @Override
  public Stream<T> withRetry(final int maxRetries, final Duration backoff) {
    if (maxRetries < 0) throw new IllegalArgumentException("maxRetries cannot be negative");
    Objects.requireNonNull(backoff, "backoff cannot be null");
    return new DefaultStream<>(
      topics,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      operators,
      maxRetries,
      backoff,
      backpressureHigh,
      backpressureLow,
      sequentialProcessing,
      skipBytes,
      consumerMetrics,
      errorHandler,
      deadLetterTopic,
      pollTimeout
    );
  }

  @Override
  public Stream<T> withBackpressure() {
    return withBackpressure(10_000L, 7_000L);
  }

  @Override
  public Stream<T> withBackpressure(final long high, final long low) {
    if (low < 0 || low >= high) throw new IllegalArgumentException(
      "Invalid watermarks: high must be positive and > low (got high=%d, low=%d)".formatted(high, low)
    );
    return new DefaultStream<>(
      topics,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      operators,
      maxRetries,
      retryBackoff,
      high,
      low,
      sequentialProcessing,
      skipBytes,
      consumerMetrics,
      errorHandler,
      deadLetterTopic,
      pollTimeout
    );
  }

  @Override
  public Stream<T> withSequentialProcessing(final boolean sequential) {
    return new DefaultStream<>(
      topics,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      operators,
      maxRetries,
      retryBackoff,
      backpressureHigh,
      backpressureLow,
      sequential,
      skipBytes,
      consumerMetrics,
      errorHandler,
      deadLetterTopic,
      pollTimeout
    );
  }

  @Override
  public Stream<T> skipBytes(final int n) {
    if (n < 0) throw new IllegalArgumentException("n cannot be negative");
    return new DefaultStream<>(
      topics,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      operators,
      maxRetries,
      retryBackoff,
      backpressureHigh,
      backpressureLow,
      sequentialProcessing,
      n,
      consumerMetrics,
      errorHandler,
      deadLetterTopic,
      pollTimeout
    );
  }

  @Override
  public Stream<T> withMetrics(final ConsumerMetrics metrics) {
    Objects.requireNonNull(metrics, "metrics cannot be null");
    return new DefaultStream<>(
      topics,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      operators,
      maxRetries,
      retryBackoff,
      backpressureHigh,
      backpressureLow,
      sequentialProcessing,
      skipBytes,
      metrics,
      errorHandler,
      deadLetterTopic,
      pollTimeout
    );
  }

  @Override
  public Stream<T> withErrorHandler(final Consumer<KPipeConsumer.ProcessingError<byte[]>> handler) {
    Objects.requireNonNull(handler, "handler cannot be null");
    return new DefaultStream<>(
      topics,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      operators,
      maxRetries,
      retryBackoff,
      backpressureHigh,
      backpressureLow,
      sequentialProcessing,
      skipBytes,
      consumerMetrics,
      handler,
      deadLetterTopic,
      pollTimeout
    );
  }

  @Override
  public Stream<T> withDeadLetterTopic(final String dlqTopic) {
    if (dlqTopic == null || dlqTopic.isBlank()) throw new IllegalArgumentException("dlqTopic cannot be null or blank");
    return new DefaultStream<>(
      topics,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      operators,
      maxRetries,
      retryBackoff,
      backpressureHigh,
      backpressureLow,
      sequentialProcessing,
      skipBytes,
      consumerMetrics,
      errorHandler,
      dlqTopic,
      pollTimeout
    );
  }

  @Override
  public Stream<T> withPollTimeout(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    return new DefaultStream<>(
      topics,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      operators,
      maxRetries,
      retryBackoff,
      backpressureHigh,
      backpressureLow,
      sequentialProcessing,
      skipBytes,
      consumerMetrics,
      errorHandler,
      deadLetterTopic,
      timeout
    );
  }

  @Override
  public Sink<T> toConsole() {
    return toCustom(defaultConsoleSinkFactory.get());
  }

  @Override
  public Sink<T> toCustom(final MessageSink<T> sink) {
    Objects.requireNonNull(sink, "sink cannot be null");
    return new DefaultSink<>(this, sink);
  }

  @Override
  @SafeVarargs
  public final Sink<T> toMulti(final MessageSink<T>... sinks) {
    Objects.requireNonNull(sinks, "sinks cannot be null");
    if (sinks.length == 0) throw new IllegalArgumentException("at least one sink is required");
    return new DefaultSink<>(this, new CompositeMessageSink<>(List.of(sinks)));
  }

  private DefaultStream<T> withOperator(final UnaryOperator<T> op) {
    final var next = new ArrayList<>(operators);
    next.add(op);
    return new DefaultStream<>(
      topics,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      List.copyOf(next),
      maxRetries,
      retryBackoff,
      backpressureHigh,
      backpressureLow,
      sequentialProcessing,
      skipBytes,
      consumerMetrics,
      errorHandler,
      deadLetterTopic,
      pollTimeout
    );
  }
}
