package org.kpipe;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
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

  private final Set<String> topics;
  private final Properties kafkaProps;
  private final MessageFormat<T> format;
  private final Supplier<MessageSink<T>> defaultConsoleSinkFactory;
  private final List<UnaryOperator<T>> operators;
  private final int maxRetries;
  private final Duration retryBackoff;
  private final Long backpressureHigh;
  private final Long backpressureLow;
  private final boolean sequentialProcessing;
  private final int skipBytes;

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

  /// Public constructor used by [KPipe] factories — multiple topics, single shared pipeline,
  /// empty operator chain, default retry / backpressure settings.
  ///
  /// Defensively copies `kafkaProps` so that subsequent caller mutations do not silently affect
  /// the in-flight stream. Defensively copies `topics` into an immutable insertion-ordered set.
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
      0
    );
  }

  private static Set<String> validateTopics(final Collection<String> topics) {
    Objects.requireNonNull(topics, "topics cannot be null");
    if (topics.isEmpty()) throw new IllegalArgumentException("topics cannot be empty");
    final var copy = new LinkedHashSet<String>(topics.size());
    for (final var t : topics) {
      if (t == null || t.isBlank()) throw new IllegalArgumentException("topic name cannot be null or blank");
      copy.add(t);
    }
    return Set.copyOf(copy);
  }

  private DefaultStream(
    final Set<String> topics,
    final Properties kafkaProps,
    final MessageFormat<T> format,
    final Supplier<MessageSink<T>> defaultConsoleSinkFactory,
    final List<UnaryOperator<T>> operators,
    final int maxRetries,
    final Duration retryBackoff,
    final Long backpressureHigh,
    final Long backpressureLow,
    final boolean sequentialProcessing,
    final int skipBytes
  ) {
    this.topics = topics;
    this.kafkaProps = kafkaProps;
    this.format = format;
    this.defaultConsoleSinkFactory = defaultConsoleSinkFactory;
    this.operators = operators;
    this.maxRetries = maxRetries;
    this.retryBackoff = retryBackoff;
    this.backpressureHigh = backpressureHigh;
    this.backpressureLow = backpressureLow;
    this.sequentialProcessing = sequentialProcessing;
    this.skipBytes = skipBytes;
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
      skipBytes
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
      topics,
      kafkaProps,
      format,
      defaultConsoleSinkFactory,
      operators,
      maxRetries,
      Objects.requireNonNull(backoff, "backoff cannot be null"),
      backpressureHigh,
      backpressureLow,
      sequentialProcessing,
      skipBytes
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
      skipBytes
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
      skipBytes
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
      n
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

  Set<String> topics() {
    return topics;
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

  int skipBytes() {
    return skipBytes;
  }
}
