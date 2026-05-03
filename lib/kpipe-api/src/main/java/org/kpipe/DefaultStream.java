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
import org.kpipe.Sink;
import org.kpipe.Stream;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.Operators;
import org.kpipe.sink.CompositeMessageSink;
import org.kpipe.sink.MessageSink;

/// Package-private implementation of [Stream] that accumulates configuration and, on
/// `start()`, builds the underlying `MessageProcessorRegistry`, `KPipeConsumer`, and
/// `KPipeRunner` chain.
///
/// This class is mutable internally (operators are appended) but always returns `this` to
/// preserve the fluent-builder pattern users expect.
///
/// @param <T> the deserialized message type
final class DefaultStream<T> implements Stream<T> {

  private final String topic;
  private final Properties kafkaProps;
  private final MessageFormat<T> format;
  private final Function<T, MessageSink<T>> defaultConsoleSinkFactory;
  private final List<UnaryOperator<T>> operators = new ArrayList<>();

  private int maxRetries = 0;
  private Duration retryBackoff = Duration.ofMillis(500);
  private Long backpressureHigh;
  private Long backpressureLow;
  private boolean sequentialProcessing = false;

  DefaultStream(
    final String topic,
    final Properties kafkaProps,
    final MessageFormat<T> format,
    final Function<T, MessageSink<T>> defaultConsoleSinkFactory
  ) {
    this.topic = Objects.requireNonNull(topic, "topic cannot be null");
    this.kafkaProps = Objects.requireNonNull(kafkaProps, "kafkaProps cannot be null");
    this.format = Objects.requireNonNull(format, "format cannot be null");
    this.defaultConsoleSinkFactory = Objects.requireNonNull(
      defaultConsoleSinkFactory,
      "defaultConsoleSinkFactory cannot be null"
    );
  }

  @Override
  public Stream<T> pipe(final UnaryOperator<T> op) {
    operators.add(Objects.requireNonNull(op, "operator cannot be null"));
    return this;
  }

  @Override
  public Stream<T> filter(final Predicate<T> keep) {
    Objects.requireNonNull(keep, "predicate cannot be null");
    operators.add(Operators.filter(keep));
    return this;
  }

  @Override
  public Stream<T> peek(final Consumer<T> sideEffect) {
    Objects.requireNonNull(sideEffect, "sideEffect cannot be null");
    operators.add(Operators.peek(sideEffect));
    return this;
  }

  @Override
  public Stream<T> when(final Predicate<T> cond, final UnaryOperator<T> ifTrue, final UnaryOperator<T> ifFalse) {
    Objects.requireNonNull(cond, "condition cannot be null");
    Objects.requireNonNull(ifTrue, "ifTrue cannot be null");
    Objects.requireNonNull(ifFalse, "ifFalse cannot be null");
    operators.add(value -> cond.test(value) ? ifTrue.apply(value) : ifFalse.apply(value));
    return this;
  }

  @Override
  public Stream<T> withRetry(final int maxRetries, final Duration backoff) {
    if (maxRetries < 0) throw new IllegalArgumentException("maxRetries cannot be negative");
    this.maxRetries = maxRetries;
    this.retryBackoff = Objects.requireNonNull(backoff, "backoff cannot be null");
    return this;
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
    this.backpressureHigh = high;
    this.backpressureLow = low;
    return this;
  }

  @Override
  public Stream<T> withSequentialProcessing(final boolean sequential) {
    this.sequentialProcessing = sequential;
    return this;
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
    return List.copyOf(operators);
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
