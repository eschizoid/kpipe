package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.CircuitBreakerController;
import io.github.eschizoid.kpipe.consumer.ConsumerSettings;
import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.MetricsExporter;
import io.github.eschizoid.kpipe.consumer.OffsetStore;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.format.avro.AvroFormat;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.pipeline.MessageFormat;
import io.github.eschizoid.kpipe.pipeline.Operators;
import io.github.eschizoid.kpipe.pipeline.SchemaResolver;
import io.github.eschizoid.kpipe.record.RecordContext;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import io.github.eschizoid.kpipe.sink.CompositeMessageSink;
import io.github.eschizoid.kpipe.sink.MessageSink;
import io.github.eschizoid.kpipe.tracing.Tracer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/// Package-private immutable record-based [Stream]. Each fluent method returns a NEW
/// `DefaultStream` carrying the updated configuration; the original instance is never mutated.
/// This makes branching safe (`s.pipe(a)` and `s.pipe(b)` produce independent streams) and
/// matches the Java Stream API's functional style.
///
/// Pipeline-level state (topics, format, operators, skipBytes) lives as record components here.
/// Every consumer-wide setting lives in the one [ConsumerSettings] component, and the
/// consumer-wide `with*` methods below delegate to it, so its validation and defaults apply here
/// unchanged.
///
/// @param <T> the deserialized message type
record DefaultStream<T>(
  Set<String> topics,
  Properties kafkaProps,
  MessageFormat<T> format,
  Supplier<MessageSink<T>> defaultConsoleSinkFactory,
  List<UnaryOperator<T>> operators,
  int skipBytes,
  ConsumerSettings settings
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
      ConsumerSettings.defaults()
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
  public Stream<T> rekey(final Function<? super T, byte[]> keyFn) {
    return withOperator(RecordContext.rekey(keyFn));
  }

  @Override
  public Stream<T> rekeyString(final Function<? super T, String> keyFn) {
    return withOperator(RecordContext.rekeyString(keyFn));
  }

  @Override
  public Stream<T> withHeader(final String name, final Function<? super T, byte[]> valueFn) {
    return withOperator(RecordContext.withHeader(name, valueFn));
  }

  @Override
  public Stream<T> withRetry(final int maxRetries, final Duration backoff) {
    return withSettings(settings.withRetry(maxRetries, backoff));
  }

  @Override
  public Stream<T> withBackpressure() {
    return withSettings(settings.withBackpressure());
  }

  @Override
  public Stream<T> withBackpressure(final long high, final long low) {
    return withSettings(settings.withBackpressure(high, low));
  }

  @Override
  public Stream<T> withProcessingMode(final ProcessingMode mode) {
    return withSettings(settings.withProcessingMode(mode));
  }

  @Override
  public Stream<T> withKeyOrderedMaxKeys(final int maxKeys) {
    return withSettings(settings.withKeyOrderedMaxKeys(maxKeys));
  }

  @Override
  public Stream<T> skipBytes(final int n) {
    if (n < 0) throw new IllegalArgumentException("n cannot be negative");
    if (n > 0 && format.isSchemaRegistryBacked()) throw new IllegalArgumentException(
      "skipBytes(" +
        n +
        ") cannot be combined with a Schema-Registry-backed format: the format " +
        "reads the Confluent wire envelope itself, and stripping bytes first would corrupt every " +
        "record. Drop the skipBytes(...) call."
    );
    return mutate(m -> m.skipBytes = n);
  }

  @Override
  public Stream<T> withSchemaRegistry(final SchemaResolver resolver) {
    Objects.requireNonNull(resolver, "resolver cannot be null");
    if (skipBytes > 0) throw new IllegalArgumentException(
      "withSchemaRegistry(...) cannot be combined with skipBytes(" +
        skipBytes +
        "): the " +
        "registry-backed format reads the Confluent wire envelope itself, and stripping bytes " +
        "first would corrupt every record. Drop the skipBytes(...) call."
    );
    @SuppressWarnings("unchecked")
    final var newFormat = (MessageFormat<T>) registryBackedFormat(format, resolver);
    return mutate(m -> m.format = newFormat);
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
  public Stream<T> withMetrics(final MetricsExporter exporter) {
    return withSettings(settings.withMetrics(exporter));
  }

  @Override
  public Stream<T> withErrorHandler(final Consumer<KPipeConsumer.ProcessingError> handler) {
    return withSettings(settings.withErrorHandler(handler));
  }

  @Override
  public Stream<T> onAttemptFailed(final Consumer<KPipeConsumer.ProcessingError> observer) {
    return withSettings(settings.onAttemptFailed(observer));
  }

  @Override
  public Stream<T> withDeadLetterTopic(final String dlqTopic) {
    return withSettings(settings.withDeadLetterTopic(dlqTopic));
  }

  @Override
  public Stream<T> withPollTimeout(final Duration timeout) {
    return withSettings(settings.withPollTimeout(timeout));
  }

  @Override
  public Stream<T> withCommitInterval(final Duration interval) {
    return withSettings(settings.withCommitInterval(interval));
  }

  @Override
  public Stream<T> withOffsetStore(final OffsetStore store) {
    return withSettings(settings.withOffsetStore(store));
  }

  @Override
  public Stream<T> withMetricsLog(final Duration interval) {
    return withSettings(settings.withMetricsLog(interval));
  }

  @Override
  public Stream<T> withShutdownHook(final boolean useShutdownHook) {
    return withSettings(settings.withShutdownHook(useShutdownHook));
  }

  @Override
  public Stream<T> withThreadTerminationTimeout(final Duration timeout) {
    return withSettings(settings.withThreadTerminationTimeout(timeout));
  }

  @Override
  public Stream<T> withWaitForMessagesTimeout(final Duration timeout) {
    return withSettings(settings.withWaitForMessagesTimeout(timeout));
  }

  @Override
  public Stream<T> withTracer(final Tracer tracer) {
    return withSettings(settings.withTracer(tracer));
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
    return withSettings(settings.withCircuitBreaker(controller));
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
  public Sink<T> toKafka(final String topic) {
    return new DefaultSink<>(this, KafkaTopicSink.of(topic, format));
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
  /// what they need, rebuild a new record. New pipeline-level fields slot in at one site (the
  /// Mut declaration + its `from`/`build`); consumer-wide fields go through [#withSettings].
  private DefaultStream<T> mutate(final Consumer<Mut<T>> change) {
    final var m = Mut.from(this);
    change.accept(m);
    return m.build();
  }

  /// Rebuilds this stream around `newSettings`; a new stream per call, as for every wither.
  private DefaultStream<T> withSettings(final ConsumerSettings newSettings) {
    return mutate(m -> m.settings = newSettings);
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
    int skipBytes;
    ConsumerSettings settings;

    static <T> Mut<T> from(final DefaultStream<T> s) {
      final var m = new Mut<T>();
      m.topics = s.topics;
      m.kafkaProps = s.kafkaProps;
      m.format = s.format;
      m.defaultConsoleSinkFactory = s.defaultConsoleSinkFactory;
      m.operators = s.operators;
      m.skipBytes = s.skipBytes;
      m.settings = s.settings;
      return m;
    }

    DefaultStream<T> build() {
      return new DefaultStream<>(topics, kafkaProps, format, defaultConsoleSinkFactory, operators, skipBytes, settings);
    }
  }
}
