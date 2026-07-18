package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.BackpressureController;
import io.github.eschizoid.kpipe.consumer.CircuitBreakerController;
import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.KPipeConsumerBuilder;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.format.avro.AvroFormat;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.metrics.ConsumerMetrics;
import io.github.eschizoid.kpipe.producer.tracing.Tracer;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.Operators;
import io.github.eschizoid.kpipe.registry.Result;
import io.github.eschizoid.kpipe.registry.SchemaResolver;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import io.github.eschizoid.kpipe.sink.CompositeMessageSink;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
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
  ProcessingMode processingMode,
  int keyOrderedMaxKeys,
  int skipBytes,
  ConsumerMetrics consumerMetrics,
  Consumer<KPipeConsumer.ProcessingError> errorHandler,
  String deadLetterTopic,
  Duration pollTimeout,
  Tracer tracer,
  CircuitBreakerController circuitBreaker,
  Runnable onFilteredObserver,
  Consumer<Throwable> onFailedObserver,
  Consumer<Result<T>> peekResultObserver
) implements Stream<T> {
  private static final Logger LOGGER = System.getLogger(DefaultStream.class.getName());

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
      ProcessingMode.PARALLEL,
      ProcessingMode.DEFAULT_KEY_ORDERED_MAX_KEYS,
      0,
      null,
      null,
      null,
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
    return withBackpressure(
      BackpressureController.DEFAULT_HIGH_WATERMARK,
      BackpressureController.DEFAULT_LOW_WATERMARK
    );
  }

  @Override
  public Stream<T> withBackpressure(final long high, final long low) {
    if (low < 0 || low >= high) throw new IllegalArgumentException(
      "withBackpressure requires high > low > 0 (got high=%d, low=%d)".formatted(high, low)
    );
    return mutate(m -> {
      m.backpressureHigh = high;
      m.backpressureLow = low;
    });
  }

  @Override
  public Stream<T> withProcessingMode(final ProcessingMode mode) {
    Objects.requireNonNull(mode, "mode cannot be null");
    return mutate(m -> m.processingMode = mode);
  }

  @Override
  public Stream<T> withKeyOrderedMaxKeys(final int maxKeys) {
    if (maxKeys <= 0) throw new IllegalArgumentException("maxKeys must be positive, got " + maxKeys);
    return mutate(m -> m.keyOrderedMaxKeys = maxKeys);
  }

  @Override
  public Stream<T> skipBytes(final int n) {
    if (n < 0) throw new IllegalArgumentException("n cannot be negative");
    if (n > 0 && isRegistryBacked(format)) throw new IllegalArgumentException(
      "skipBytes(" + n + ") cannot be combined with a Schema-Registry-backed format: the format " +
        "reads the Confluent wire envelope itself, and stripping bytes first would corrupt every " +
        "record. Drop the skipBytes(...) call."
    );
    return mutate(m -> m.skipBytes = n);
  }

  @Override
  public Stream<T> withSchemaRegistry(final SchemaResolver resolver) {
    Objects.requireNonNull(resolver, "resolver cannot be null");
    if (skipBytes > 0) throw new IllegalArgumentException(
      "withSchemaRegistry(...) cannot be combined with skipBytes(" + skipBytes + "): the " +
        "registry-backed format reads the Confluent wire envelope itself, and stripping bytes " +
        "first would corrupt every record. Drop the skipBytes(...) call."
    );
    @SuppressWarnings("unchecked")
    final var newFormat = (MessageFormat<T>) registryBackedFormat(format, resolver);
    return mutate(m -> m.format = newFormat);
  }

  /// True when `format` is an Avro/Protobuf codec operating in Schema-Registry mode (per-record
  /// envelope read). Detected via the formats' public mode accessors — both return their static
  /// schema/descriptor, which is null exactly in registry mode.
  private static boolean isRegistryBacked(final MessageFormat<?> format) {
    if (format instanceof AvroFormat avro) return avro.schema() == null;
    if (format instanceof ProtobufFormat proto) return proto.descriptor() == null;
    return false;
  }

  /// Returns a registry-backed variant of `format` for any format that supports per-record schema
  /// lookup — Avro (its schema parser is bundled) and Protobuf (its compiler is discovered via
  /// `ServiceLoader` from `kpipe-format-protobuf-confluent`).
  private static MessageFormat<?> registryBackedFormat(final MessageFormat<?> format, final SchemaResolver resolver) {
    if (format instanceof AvroFormat) return AvroFormat.withRegistry(resolver);
    if (format instanceof ProtobufFormat) return ProtobufFormat.withRegistry(resolver);
    throw new UnsupportedOperationException(
      "withSchemaRegistry is not supported for format " +
        format.getClass().getSimpleName() +
        " — only Avro and Protobuf are wired for per-record schema lookup."
    );
  }

  @Override
  public Stream<T> withMetrics(final ConsumerMetrics metrics) {
    Objects.requireNonNull(metrics, "metrics cannot be null");
    return mutate(m -> m.consumerMetrics = metrics);
  }

  @Override
  public Stream<T> withErrorHandler(final Consumer<KPipeConsumer.ProcessingError> handler) {
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
  public Stream<T> onFiltered(final Runnable observer) {
    Objects.requireNonNull(observer, "observer cannot be null");
    return mutate(m -> m.onFilteredObserver = observer);
  }

  @Override
  public Stream<T> onFailed(final Consumer<Throwable> observer) {
    Objects.requireNonNull(observer, "observer cannot be null");
    return mutate(m -> m.onFailedObserver = observer);
  }

  @Override
  public Stream<T> peekResult(final Consumer<Result<T>> observer) {
    Objects.requireNonNull(observer, "observer cannot be null");
    return mutate(m -> m.peekResultObserver = observer);
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

  /// Applies every consumer-level setting that both [DefaultSink] and [DefaultBatchSink] share onto
  /// `builder`: retry, backpressure, metrics, error handler, dead-letter topic, poll timeout,
  /// tracer, and circuit breaker. All are consumer-wide, so wiring them from one place keeps the
  /// two sink types from drifting — the batch path previously skipped tracer and circuit breaker,
  /// silently dropping both when set on a `toBatch(...)` stream.
  void applyCommonConsumerConfig(final KPipeConsumerBuilder builder) {
    if (maxRetries > 0) builder.withRetry(maxRetries, retryBackoff);
    if (backpressureHigh != null) builder.withBackpressure(backpressureHigh, backpressureLow);
    if (consumerMetrics != null) builder.withMetrics(consumerMetrics);
    if (errorHandler != null) builder.withErrorHandler(errorHandler::accept);
    if (deadLetterTopic != null) builder.withDeadLetterTopic(deadLetterTopic);
    if (pollTimeout != null) builder.withPollTimeout(pollTimeout);
    if (tracer != null) builder.withTracer(tracer);
    if (circuitBreaker != null) builder.withCircuitBreaker(circuitBreaker);
  }

  /// Wraps `base` with dispatch to the configured result observers (`onFiltered` / `onFailed` /
  /// `peekResult`), or returns `base` unchanged when none is set. Lives here — next to
  /// [#applyCommonConsumerConfig], and for the same reason — so both [DefaultSink] and
  /// [DefaultBatchSink] share one wiring site: the batch path previously had no observer dispatch
  /// at all, silently dropping observers set on a `toBatch(...)` stream.
  ///
  /// Observers fire on the PIPELINE outcome at `process()` time — for the batch path that is
  /// before the record is buffered; batch-sink failures are a separate concern routed through the
  /// batch DLQ machinery, not `onFailed`.
  MessagePipeline<T> wrapWithObservers(final MessagePipeline<T> base) {
    final var onFiltered = onFilteredObserver;
    final var onFailed = onFailedObserver;
    final var peek = peekResultObserver;
    if (onFiltered == null && onFailed == null && peek == null) return base;
    return new MessagePipeline<>() {
      @Override
      public T deserialize(final byte[] data) {
        return base.deserialize(data);
      }

      @Override
      public byte[] serialize(final T data) {
        return base.serialize(data);
      }

      @Override
      public Result<T> process(final T data) {
        final var result = base.process(data);
        if (peek != null) safeAccept(peek, result, "peekResult");
        switch (result) {
          case Result.Passed<T> _ -> {
          }
          case Result.Filtered<T> _ -> {
            if (onFiltered != null) safeRun(onFiltered);
          }
          case Result.Failed<T> failed -> {
            if (onFailed != null) safeAccept(onFailed, failed.cause(), "onFailed");
          }
        }
        return result;
      }

      @Override
      public MessageSink<T> getSink() {
        return base.getSink();
      }
    };
  }

  private static void safeRun(final Runnable observer) {
    try {
      observer.run();
    } catch (final RuntimeException e) {
      LOGGER.log(Level.WARNING, "onFiltered observer threw; swallowing to keep the pipeline running", e);
    }
  }

  private static <A> void safeAccept(final Consumer<A> observer, final A arg, final String name) {
    try {
      observer.accept(arg);
    } catch (final RuntimeException e) {
      LOGGER.log(Level.WARNING, () -> name + " observer threw; swallowing to keep the pipeline running", e);
    }
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
    ProcessingMode processingMode;
    int keyOrderedMaxKeys;
    int skipBytes;
    ConsumerMetrics consumerMetrics;
    Consumer<KPipeConsumer.ProcessingError> errorHandler;
    String deadLetterTopic;
    Duration pollTimeout;
    Tracer tracer;
    CircuitBreakerController circuitBreaker;
    Runnable onFilteredObserver;
    Consumer<Throwable> onFailedObserver;
    Consumer<Result<T>> peekResultObserver;

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
      m.processingMode = s.processingMode;
      m.keyOrderedMaxKeys = s.keyOrderedMaxKeys;
      m.skipBytes = s.skipBytes;
      m.consumerMetrics = s.consumerMetrics;
      m.errorHandler = s.errorHandler;
      m.deadLetterTopic = s.deadLetterTopic;
      m.pollTimeout = s.pollTimeout;
      m.tracer = s.tracer;
      m.circuitBreaker = s.circuitBreaker;
      m.onFilteredObserver = s.onFilteredObserver;
      m.onFailedObserver = s.onFailedObserver;
      m.peekResultObserver = s.peekResultObserver;
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
        processingMode,
        keyOrderedMaxKeys,
        skipBytes,
        consumerMetrics,
        errorHandler,
        deadLetterTopic,
        pollTimeout,
        tracer,
        circuitBreaker,
        onFilteredObserver,
        onFailedObserver,
        peekResultObserver
      );
    }
  }
}
