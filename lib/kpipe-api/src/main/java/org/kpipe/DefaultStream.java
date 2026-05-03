package org.kpipe;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.Operators;
import org.kpipe.sink.CompositeMessageSink;
import org.kpipe.sink.MessageSink;

/// Package-private immutable implementation of [Stream]. Each fluent method returns a NEW
/// `DefaultStream` carrying the updated configuration; the original instance is never mutated.
/// This makes branching safe (`s.pipe(a)` and `s.pipe(b)` produce independent streams) and
/// matches the Java Stream API's functional style.
///
/// On [#toCustom] / [#toConsole] / [#toMulti] the configuration is frozen into a [DefaultSink];
/// any further fluent calls on the original stream cannot affect the already-terminated chain.
///
/// @param <T> the deserialized message type
final class DefaultStream<T> implements Stream<T> {

  private final String topic;
  private final Properties kafkaProps;
  private final MessageFormat<T> format;
  private final Function<T, MessageSink<T>> defaultConsoleSinkFactory;
  private final List<UnaryOperator<T>> operators;
  private final int maxRetries;
  private final Duration retryBackoff;
  private final Long backpressureHigh;
  private final Long backpressureLow;
  private final boolean sequentialProcessing;

  /// Public constructor used by [KPipe] factories — starts with an empty pipeline and default
  /// retry / backpressure settings.
  DefaultStream(
    final String topic,
    final Properties kafkaProps,
    final MessageFormat<T> format,
    final Function<T, MessageSink<T>> defaultConsoleSinkFactory
  ) {
    this(
      Objects.requireNonNull(topic, "topic cannot be null"),
      Objects.requireNonNull(kafkaProps, "kafkaProps cannot be null"),
      Objects.requireNonNull(format, "format cannot be null"),
      Objects.requireNonNull(defaultConsoleSinkFactory, "defaultConsoleSinkFactory cannot be null"),
      List.of(),
      0,
      Duration.ofMillis(500),
      null,
      null,
      false
    );
  }

  private DefaultStream(
    final String topic,
    final Properties kafkaProps,
    final MessageFormat<T> format,
    final Function<T, MessageSink<T>> defaultConsoleSinkFactory,
    final List<UnaryOperator<T>> operators,
    final int maxRetries,
    final Duration retryBackoff,
    final Long backpressureHigh,
    final Long backpressureLow,
    final boolean sequentialProcessing
  ) {
    this.topic = topic;
    this.kafkaProps = kafkaProps;
    this.format = format;
    this.defaultConsoleSinkFactory = defaultConsoleSinkFactory;
    this.operators = operators;
    this.maxRetries = maxRetries;
    this.retryBackoff = retryBackoff;
    this.backpressureHigh = backpressureHigh;
    this.backpressureLow = backpressureLow;
    this.sequentialProcessing = sequentialProcessing;
  }

  private DefaultStream<T> withOperator(final UnaryOperator<T> op) {
    final var next = new ArrayList<>(operators);
    next.add(op);
    return new DefaultStream<>(
      topic,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      List.copyOf(next),
      maxRetries,
      retryBackoff,
      backpressureHigh,
      backpressureLow,
      sequentialProcessing
    );
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
    return new DefaultStream<>(
      topic,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      operators,
      maxRetries,
      Objects.requireNonNull(backoff, "backoff cannot be null"),
      backpressureHigh,
      backpressureLow,
      sequentialProcessing
    );
  }

  @Override
  public Stream<T> withBackpressure() {
    return withBackpressure(10_000L, 7_000L);
  }

  @Override
  public Stream<T> withBackpressure(final long high, final long low) {
    if (high <= 0 || low < 0 || low >= high) throw new IllegalArgumentException(
      "Invalid watermarks: high must be positive and > low (got high=%d, low=%d)".formatted(high, low)
    );
    return new DefaultStream<>(
      topic,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      operators,
      maxRetries,
      retryBackoff,
      high,
      low,
      sequentialProcessing
    );
  }

  @Override
  public Stream<T> withSequentialProcessing(final boolean sequential) {
    return new DefaultStream<>(
      topic,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      operators,
      maxRetries,
      retryBackoff,
      backpressureHigh,
      backpressureLow,
      sequential
    );
  }

  @Override
  public Sink<T> toConsole() {
    return toCustom(defaultConsoleSinkFactory.apply(null));
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

  // -- package-private accessors used by DefaultSink --

  String topic() {
    return topic;
  }

  Properties kafkaProps() {
    return kafkaProps;
  }

  MessageFormat<T> format() {
    return format;
  }

  List<UnaryOperator<T>> operators() {
    return operators;
  }

  int maxRetries() {
    return maxRetries;
  }

  Duration retryBackoff() {
    return retryBackoff;
  }

  Long backpressureHigh() {
    return backpressureHigh;
  }

  Long backpressureLow() {
    return backpressureLow;
  }

  boolean sequentialProcessing() {
    return sequentialProcessing;
  }
}
