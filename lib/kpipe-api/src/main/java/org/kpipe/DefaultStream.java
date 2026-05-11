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
import org.kpipe.consumer.CircuitBreakerController;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.metrics.ConsumerMetrics;
import org.kpipe.producer.tracing.Tracer;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.Operators;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.BatchSink;
import org.kpipe.sink.CompositeMessageSink;
import org.kpipe.sink.MessageSink;

/// Package-private immutable record-based [Stream]. Each fluent method returns a NEW
/// `DefaultStream` carrying the updated configuration; the original instance is never mutated.
/// This makes branching safe (`s.pipe(a)` and `s.pipe(b)` produce independent streams) and
/// matches the Java Stream API's functional style.
///
/// Adding a new fluent setter is a one-place change: declare the record component, mirror it on
/// [Mut], and write the `with*` method as `mutate(m -> m.field = newValue)`. There is no
/// constructor copy-paste — [#mutate(Consumer)] funnels every wither through a single instance
/// of [Mut] and rebuilds via [Mut#build].
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
  Duration pollTimeout,
  Tracer tracer,
  CircuitBreakerController circuitBreaker
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
    return mutate(m -> {
      m.maxRetries = maxRetries;
      m.retryBackoff = backoff;
    });
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
    return mutate(m -> {
      m.backpressureHigh = high;
      m.backpressureLow = low;
    });
  }

  @Override
  public Stream<T> withSequentialProcessing(final boolean sequential) {
    return mutate(m -> m.sequentialProcessing = sequential);
  }

  @Override
  public Stream<T> skipBytes(final int n) {
    if (n < 0) throw new IllegalArgumentException("n cannot be negative");
    return mutate(m -> m.skipBytes = n);
  }

  @Override
  public Stream<T> withMetrics(final ConsumerMetrics metrics) {
    Objects.requireNonNull(metrics, "metrics cannot be null");
    return mutate(m -> m.consumerMetrics = metrics);
  }

  @Override
  public Stream<T> withErrorHandler(final Consumer<KPipeConsumer.ProcessingError<byte[]>> handler) {
    Objects.requireNonNull(handler, "handler cannot be null");
    return mutate(m -> m.errorHandler = handler);
  }

  @Override
  public Stream<T> withDeadLetterTopic(final String dlqTopic) {
    if (dlqTopic == null || dlqTopic.isBlank()) throw new IllegalArgumentException("dlqTopic cannot be null or blank");
    return mutate(m -> m.deadLetterTopic = dlqTopic);
  }

  @Override
  public Stream<T> withPollTimeout(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    return mutate(m -> m.pollTimeout = timeout);
  }

  @Override
  public Stream<T> withTracer(final Tracer tracer) {
    Objects.requireNonNull(tracer, "tracer cannot be null");
    return mutate(m -> m.tracer = tracer);
  }

  @Override
  public Stream<T> withCircuitBreaker(
    final double failureThreshold,
    final int windowSize,
    final Duration openDuration
  ) {
    return withCircuitBreaker(new CircuitBreakerController(failureThreshold, windowSize, openDuration));
  }

  @Override
  public Stream<T> withCircuitBreaker(final CircuitBreakerController controller) {
    Objects.requireNonNull(controller, "controller cannot be null");
    return mutate(m -> m.circuitBreaker = controller);
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
  public Sink<T> toBatch(final BatchSink<T> sink, final BatchPolicy policy) {
    Objects.requireNonNull(sink, "sink cannot be null");
    Objects.requireNonNull(policy, "policy cannot be null");
    return new DefaultBatchSink<>(this, sink, policy);
  }

  @Override
  @SafeVarargs
  public final Sink<T> toMulti(final MessageSink<T>... sinks) {
    Objects.requireNonNull(sinks, "sinks cannot be null");
    if (sinks.length == 0) throw new IllegalArgumentException("at least one sink is required");
    return new DefaultSink<>(this, new CompositeMessageSink<>(List.of(sinks)));
  }

  private DefaultStream<T> withOperator(final UnaryOperator<T> op) {
    return mutate(m -> {
      final var next = new ArrayList<>(operators);
      next.add(op);
      m.operators = List.copyOf(next);
    });
  }

  /// Single funnel for every wither: snapshot this record into a [Mut], let the caller change
  /// what they need, rebuild a new record. Replaces 13 hand-rolled 15-arg constructor calls with
  /// one. New fields slot in at one site (the Mut declaration + its `from`/`build`).
  private DefaultStream<T> mutate(final Consumer<Mut<T>> change) {
    final var m = Mut.from(this);
    change.accept(m);
    return m.build();
  }

  /// Mutable mirror of [DefaultStream]'s components used only inside [#mutate]. Each public
  /// wither hands a freshly-allocated `Mut` to a small lambda that updates one or two fields,
  /// then [#build] returns a new immutable record. Never escapes the package.
  private static final class Mut<T> {

    Set<String> topics;
    Properties kafkaProps;
    MessageFormat<T> format;
    Supplier<MessageSink<T>> defaultConsoleSinkFactory;
    List<UnaryOperator<T>> operators;
    int maxRetries;
    Duration retryBackoff;
    Long backpressureHigh;
    Long backpressureLow;
    boolean sequentialProcessing;
    int skipBytes;
    ConsumerMetrics consumerMetrics;
    Consumer<KPipeConsumer.ProcessingError<byte[]>> errorHandler;
    String deadLetterTopic;
    Duration pollTimeout;
    Tracer tracer;
    CircuitBreakerController circuitBreaker;

    static <T> Mut<T> from(final DefaultStream<T> s) {
      final var m = new Mut<T>();
      m.topics = s.topics;
      m.kafkaProps = s.kafkaProps;
      m.format = s.format;
      m.defaultConsoleSinkFactory = s.defaultConsoleSinkFactory;
      m.operators = s.operators;
      m.maxRetries = s.maxRetries;
      m.retryBackoff = s.retryBackoff;
      m.backpressureHigh = s.backpressureHigh;
      m.backpressureLow = s.backpressureLow;
      m.sequentialProcessing = s.sequentialProcessing;
      m.skipBytes = s.skipBytes;
      m.consumerMetrics = s.consumerMetrics;
      m.errorHandler = s.errorHandler;
      m.deadLetterTopic = s.deadLetterTopic;
      m.pollTimeout = s.pollTimeout;
      m.tracer = s.tracer;
      m.circuitBreaker = s.circuitBreaker;
      return m;
    }

    DefaultStream<T> build() {
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
        pollTimeout,
        tracer,
        circuitBreaker
      );
    }
  }
}
