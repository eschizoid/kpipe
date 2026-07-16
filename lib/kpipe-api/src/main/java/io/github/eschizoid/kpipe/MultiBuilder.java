package io.github.eschizoid.kpipe;

import com.google.protobuf.Message;
import io.github.eschizoid.kpipe.consumer.BackpressureController;
import io.github.eschizoid.kpipe.consumer.CircuitBreakerController;
import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.KPipeConsumerBuilder;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.format.avro.AvroFormat;
import io.github.eschizoid.kpipe.format.json.JsonFormat;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.metrics.ConsumerMetrics;
import io.github.eschizoid.kpipe.producer.tracing.Tracer;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.SchemaResolver;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.avro.generic.GenericRecord;

/// Heterogeneous multi-topic builder. Each `route` registers a per-topic pipeline so the consumer
/// can dispatch records of different payload shapes through one consumer-group / one offset
/// manager.
///
/// Usage:
///
/// ```java
/// KPipe.multi(props)
///     .json("events-json", s -> s.pipe(addTimestamp).toCustom(jsonSink))
///     .avro("events-avro", avroFormat, s -> s.filter(active).toCustom(avroSink))
///     .protobuf("events-proto", protoFormat, s -> s.toConsole())
///     .start();
/// ```
///
/// Records arriving for unrouted topics are dropped at WARNING and their offsets are still
/// committed (no infinite retry on a config error).
public final class MultiBuilder {

  private static final Logger LOGGER = System.getLogger(MultiBuilder.class.getName());

  private final Properties kafkaProps;
  private final Map<String, Sink<?>> routes = new LinkedHashMap<>();
  private ConsumerMetrics consumerMetrics;
  private Tracer tracer;
  private CircuitBreakerController circuitBreaker;
  private ProcessingMode processingMode = ProcessingMode.PARALLEL;
  private Integer keyOrderedMaxKeys;
  private Integer maxRetries;
  private Duration retryBackoff;
  private Long backpressureHigh;
  private Long backpressureLow;
  private Consumer<KPipeConsumer.ProcessingError> errorHandler;
  private String deadLetterTopic;
  private Duration pollTimeout;

  MultiBuilder(final Properties kafkaProps) {
    this.kafkaProps = (Properties) Objects.requireNonNull(kafkaProps, "kafkaProps cannot be null").clone();
  }

  /// Attaches an OpenTelemetry-backed (or any custom) [ConsumerMetrics] implementation to the
  /// multi-topic consumer. The metrics carry a `topic` attribute per record so dashboards can
  /// break down by topic across heterogeneous routes.
  ///
  /// @param metrics the metrics implementation (typically `new OtelConsumerMetrics(otel, ...)`
  ///     from `kpipe-metrics-otel`)
  /// @return this builder
  public MultiBuilder withMetrics(final ConsumerMetrics metrics) {
    this.consumerMetrics = Objects.requireNonNull(metrics, "metrics cannot be null");
    return this;
  }

  /// Attaches a [Tracer] to the multi-topic consumer. The tracer wraps every record (regardless
  /// of route) with a consumer span and injects the active context into outbound DLQ headers.
  ///
  /// @param tracer the tracer (typically `new OtelTracer(otel, "my-pipeline")` from
  ///     `kpipe-tracing-otel`); pass `Tracer.noop()` to disable explicitly
  /// @return this builder
  public MultiBuilder withTracer(final Tracer tracer) {
    this.tracer = Objects.requireNonNull(tracer, "tracer cannot be null");
    return this;
  }

  /// Attaches a circuit breaker to the multi-topic consumer. The breaker observes record outcomes
  /// across ALL routes — a sustained failure rate on any one route can trip and pause the whole
  /// consumer. If you need per-route breakers, build separate single-topic consumers.
  ///
  /// @param controller the breaker policy (must not be null)
  /// @return this builder
  public MultiBuilder withCircuitBreaker(final CircuitBreakerController controller) {
    this.circuitBreaker = Objects.requireNonNull(controller, "controller cannot be null");
    return this;
  }

  /// Sets the [ProcessingMode] for the underlying consumer. Processing mode is a consumer-wide
  /// setting (one consumer = one mode), so it lives here rather than on per-route streams.
  /// Attempting to set processing mode inside a route configurator (e.g.
  /// `.json(topic, s -> s.withProcessingMode(KEY_ORDERED))`) is rejected at [#start()] to
  /// prevent the silent-ignore footgun.
  ///
  /// @param mode the processing mode (must not be null)
  /// @return this builder
  public MultiBuilder withProcessingMode(final ProcessingMode mode) {
    this.processingMode = Objects.requireNonNull(mode, "mode cannot be null");
    return this;
  }

  /// Sets the LRU cap on distinct keys for `ProcessingMode.KEY_ORDERED`. No-op for other
  /// modes. Default is `ProcessingMode.DEFAULT_KEY_ORDERED_MAX_KEYS` (10,000).
  ///
  /// @param maxKeys positive LRU cap
  /// @return this builder
  public MultiBuilder withKeyOrderedMaxKeys(final int maxKeys) {
    if (maxKeys <= 0) throw new IllegalArgumentException("maxKeys must be positive, got " + maxKeys);
    this.keyOrderedMaxKeys = maxKeys;
    return this;
  }

  /// Retries a failing record up to `maxRetries` times with a fixed `backoff` before routing it to
  /// the DLQ (or logging, if none). Consumer-wide — the same policy applies to every route, mirror
  /// of `Stream.withRetry(int, Duration)`.
  ///
  /// @param maxRetries retry attempts after the first failure (must be non-negative)
  /// @param backoff delay between attempts (must be non-null)
  /// @return this builder
  public MultiBuilder withRetry(final int maxRetries, final Duration backoff) {
    if (maxRetries < 0) throw new IllegalArgumentException("maxRetries cannot be negative");
    this.maxRetries = maxRetries;
    this.retryBackoff = Objects.requireNonNull(backoff, "backoff cannot be null");
    return this;
  }

  /// Enables in-flight backpressure with the default watermarks (pause at
  /// `BackpressureController.DEFAULT_HIGH_WATERMARK`, resume at `DEFAULT_LOW_WATERMARK`). Mirror of
  /// `Stream.withBackpressure()`.
  ///
  /// @return this builder
  public MultiBuilder withBackpressure() {
    return withBackpressure(
      BackpressureController.DEFAULT_HIGH_WATERMARK,
      BackpressureController.DEFAULT_LOW_WATERMARK
    );
  }

  /// Enables backpressure with custom high/low watermarks (hysteresis). The strategy is derived
  /// from the consumer's processing mode. Consumer-wide, mirror of
  /// `Stream.withBackpressure(long, long)`.
  ///
  /// @param high pause watermark (must be > low)
  /// @param low resume watermark (must be >= 0 and < high)
  /// @return this builder
  public MultiBuilder withBackpressure(final long high, final long low) {
    if (low < 0 || low >= high) throw new IllegalArgumentException(
      "withBackpressure requires high > low > 0 (got high=%d, low=%d)".formatted(high, low)
    );
    this.backpressureHigh = high;
    this.backpressureLow = low;
    return this;
  }

  /// Sets a custom error handler invoked (after offset marking) for every record that fails on any
  /// route. Consumer-wide, mirror of `Stream.withErrorHandler(...)`; default logs at WARNING.
  ///
  /// @param handler the error callback (must be non-null)
  /// @return this builder
  public MultiBuilder withErrorHandler(final Consumer<KPipeConsumer.ProcessingError> handler) {
    this.errorHandler = Objects.requireNonNull(handler, "handler cannot be null");
    return this;
  }

  /// Routes records that exhaust their retries to `dlqTopic`. One DLQ for the whole consumer (there
  /// is no per-route DLQ), mirror of `Stream.withDeadLetterTopic(String)`.
  ///
  /// @param dlqTopic the dead-letter topic (must be non-null and non-blank)
  /// @return this builder
  public MultiBuilder withDeadLetterTopic(final String dlqTopic) {
    if (dlqTopic == null || dlqTopic.isBlank()) throw new IllegalArgumentException("dlqTopic cannot be null or blank");
    this.deadLetterTopic = dlqTopic;
    return this;
  }

  /// Sets the Kafka poll timeout for the underlying consumer. Consumer-wide (one poll loop), mirror
  /// of `Stream.withPollTimeout(Duration)`; default 100ms.
  ///
  /// @param timeout the poll timeout (must be non-null)
  /// @return this builder
  public MultiBuilder withPollTimeout(final Duration timeout) {
    this.pollTimeout = Objects.requireNonNull(timeout, "timeout cannot be null");
    return this;
  }

  /// Registers a JSON route for `topic`. The configurator receives a topic-bound JSON [Stream]
  /// and must return a terminal [Sink] (via `.toConsole()` / `.toCustom(...)` / `.toMulti(...)`).
  ///
  /// @param topic the Kafka topic
  /// @param configurator builds the operator chain and chooses a terminal sink
  /// @return this builder
  public MultiBuilder json(
    final String topic,
    final Function<Stream<Map<String, Object>>, Sink<Map<String, Object>>> configurator
  ) {
    return route(topic, JsonFormat.INSTANCE, JsonFormat::consoleSink, configurator);
  }

  /// Registers an Avro route for `topic` using `format` for SerDe. The schema bound to `format`
  /// is used for both deserialization and the default `toConsole()` sink. Construct the format
  /// explicitly: `new AvroFormat(schema)` or `AvroFormat.of(schemaJson)`.
  ///
  /// @param topic the Kafka topic
  /// @param format the Avro codec (must be non-null)
  /// @param configurator builds the operator chain and chooses a terminal sink
  /// @return this builder
  public MultiBuilder avro(
    final String topic,
    final AvroFormat format,
    final Function<Stream<GenericRecord>, Sink<GenericRecord>> configurator
  ) {
    return route(topic, format, format::consoleSink, configurator);
  }

  /// Registers an Avro route for `topic` with per-record Confluent Schema-Registry lookup — the
  /// multi-topic mirror of [KPipe#avro(String, Properties, SchemaResolver)]. Each record's wire
  /// envelope is read and its schema resolved via `resolver` (wrap with `CachedSchemaResolver`).
  /// Registry mode has no fixed schema, so `.toConsole()` on this route is unsupported.
  ///
  /// @param topic the Kafka topic
  /// @param resolver the schema resolver (must be non-null; typically a `CachedSchemaResolver`)
  /// @param configurator builds the operator chain and chooses a terminal sink
  /// @return this builder
  public MultiBuilder avro(
    final String topic,
    final SchemaResolver resolver,
    final Function<Stream<GenericRecord>, Sink<GenericRecord>> configurator
  ) {
    return route(topic, AvroFormat.withRegistry(resolver), KPipe::registryModeConsoleSinkUnsupported, configurator);
  }

  /// Registers a Protobuf route for `topic` using `format` for SerDe. Construct the format
  /// explicitly: `new ProtobufFormat(descriptor)`.
  ///
  /// @param topic the Kafka topic
  /// @param format the Protobuf codec (must be non-null)
  /// @param configurator builds the operator chain and chooses a terminal sink
  /// @return this builder
  public MultiBuilder protobuf(
    final String topic,
    final ProtobufFormat format,
    final Function<Stream<Message>, Sink<Message>> configurator
  ) {
    return route(topic, format, format::consoleSink, configurator);
  }

  /// Registers a Protobuf route for `topic` with per-record Confluent Schema-Registry lookup — the
  /// multi-topic mirror of [KPipe#protobuf(String, Properties, SchemaResolver)]. Requires
  /// `kpipe-format-protobuf-confluent` on the runtime path (the ServiceLoader-discovered
  /// `.proto`-text compiler). Registry mode has no fixed descriptor, so `.toConsole()` is
  /// unsupported.
  ///
  /// @param topic the Kafka topic
  /// @param resolver the schema resolver (must be non-null; typically a `CachedSchemaResolver`)
  /// @param configurator builds the operator chain and chooses a terminal sink
  /// @return this builder
  public MultiBuilder protobuf(
    final String topic,
    final SchemaResolver resolver,
    final Function<Stream<Message>, Sink<Message>> configurator
  ) {
    return route(
      topic,
      ProtobufFormat.withRegistry(resolver),
      KPipe::registryModeProtobufConsoleSinkUnsupported,
      configurator
    );
  }

  /// Registers a raw `byte[]` route for `topic` — identity passthrough, no SerDe.
  ///
  /// @param topic the Kafka topic
  /// @param configurator builds the operator chain and chooses a terminal sink
  /// @return this builder
  public MultiBuilder bytes(final String topic, final Function<Stream<byte[]>, Sink<byte[]>> configurator) {
    return route(topic, MessageFormat.bytes(), KPipe::bytesConsoleSink, configurator);
  }

  /// Registers a custom-format route. The default `toConsole()` for custom routes logs values via
  /// `String.valueOf(value)` — pass `toCustom(...)` for richer formatting.
  ///
  /// @param topic the Kafka topic
  /// @param format the message format
  /// @param configurator builds the operator chain and chooses a terminal sink
  /// @param <T> the deserialized message type
  /// @return this builder
  public <T> MultiBuilder custom(
    final String topic,
    final MessageFormat<T> format,
    final Function<Stream<T>, Sink<T>> configurator
  ) {
    return route(topic, format, () -> value -> LOGGER.log(Level.INFO, "{0}", value), configurator);
  }

  private <T> MultiBuilder route(
    final String topic,
    final MessageFormat<T> format,
    final Supplier<MessageSink<T>> defaultConsoleSinkFactory,
    final Function<Stream<T>, Sink<T>> configurator
  ) {
    Objects.requireNonNull(topic, "topic cannot be null");
    if (topic.isBlank()) throw new IllegalArgumentException("topic cannot be blank");
    Objects.requireNonNull(format, "format cannot be null");
    Objects.requireNonNull(configurator, "configurator cannot be null");
    if (routes.containsKey(topic)) throw new IllegalArgumentException(
      "Duplicate route for topic '%s'".formatted(topic)
    );

    final var stream = new DefaultStream<>(Set.of(topic), kafkaProps, format, defaultConsoleSinkFactory);
    final var sink = configurator.apply(stream);
    if (!(sink instanceof DefaultSink<?>) && !(sink instanceof DefaultBatchSink<?>)) throw new IllegalStateException(
      "Sink returned by configurator for topic '%s' is not a kpipe-built sink — call .toConsole(), .toCustom(...), .toMulti(...), or .toBatch(...) on the supplied stream".formatted(
        topic
      )
    );
    routes.put(topic, sink);
    return this;
  }

  /// Builds and starts the underlying [KPipeConsumer] with the registered routes
  /// and returns a [Handle] for lifecycle management.
  ///
  /// Routes terminating in `.toBatch(...)` are wired through `withBatchPipeline`; the consumer
  /// subscribes to the union of regular and batch topics and dispatches per-record via the
  /// per-topic batch wrapper map.
  ///
  /// @return a [Handle] for lifecycle management
  /// @throws IllegalStateException if no routes have been registered
  public Handle start() {
    if (routes.isEmpty()) throw new IllegalStateException(
      "MultiBuilder.start() requires at least one route — call .json(...) / .avro(...) / .protobuf(...) / .bytes(...) / .custom(...) before start()."
    );

    final var nonBatchPipelines = new LinkedHashMap<String, MessagePipeline<?>>();
    final var consumerBuilder = KPipeConsumer.builder().withProperties(kafkaProps);

    for (final var entry : routes.entrySet()) {
      final var topic = entry.getKey();
      final var sink = entry.getValue();
      rejectPerRouteConsumerWideSettings(topic, sink);
      if (sink instanceof DefaultSink<?> ds) {
        nonBatchPipelines.put(topic, ds.buildPipeline());
      } else if (sink instanceof DefaultBatchSink<?> dbs) {
        addBatchRoute(consumerBuilder, dbs);
      }
    }

    if (!nonBatchPipelines.isEmpty()) consumerBuilder.withPipelines(nonBatchPipelines);
    consumerBuilder.withProcessingMode(processingMode);
    if (keyOrderedMaxKeys != null) consumerBuilder.withKeyOrderedMaxKeys(keyOrderedMaxKeys);
    if (consumerMetrics != null) consumerBuilder.withMetrics(consumerMetrics);
    if (tracer != null) consumerBuilder.withTracer(tracer);
    if (circuitBreaker != null) consumerBuilder.withCircuitBreaker(circuitBreaker);
    if (maxRetries != null && maxRetries > 0) consumerBuilder.withRetry(maxRetries, retryBackoff);
    if (backpressureHigh != null) consumerBuilder.withBackpressure(backpressureHigh, backpressureLow);
    if (errorHandler != null) consumerBuilder.withErrorHandler(errorHandler::accept);
    if (deadLetterTopic != null) consumerBuilder.withDeadLetterTopic(deadLetterTopic);
    if (pollTimeout != null) consumerBuilder.withPollTimeout(pollTimeout);
    return DefaultHandle.startAndWrap(consumerBuilder.build());
  }

  /// Consumer-wide settings live on one [KPipeConsumer] (one consumer-group, one offset
  /// manager, one poll loop), so a per-route configurator can only ever set them for itself —
  /// [#start()] then drops them on the floor when it folds N routes into a single consumer.
  /// Detect every such case here and fail loud so the misconfig surfaces at construction time
  /// instead of as a silent missing-retry / missing-DLQ at runtime.
  ///
  /// Routes are expected to be terminal sinks built via `toCustom(...)` / `toBatch(...)` /
  /// `toConsole()` etc.; unknown sink shapes are passed through (no false-positives).
  private static void rejectPerRouteConsumerWideSettings(final String topic, final Sink<?> sink) {
    final DefaultStream<?> stream;
    if (sink instanceof DefaultSink<?> ds) stream = ds.stream();
    else if (sink instanceof DefaultBatchSink<?> dbs) stream = dbs.stream();
    else return;
    if (stream.processingMode() != ProcessingMode.PARALLEL) throw new IllegalArgumentException(
      "Route '%s' sets withProcessingMode(%s) on its Stream, but processing mode is a consumer-wide setting. ".formatted(
          topic,
          stream.processingMode()
        ) +
        "Move the call to MultiBuilder.withProcessingMode(...) instead."
    );
    if (stream.keyOrderedMaxKeys() != ProcessingMode.DEFAULT_KEY_ORDERED_MAX_KEYS) throw new IllegalArgumentException(
      "Route '%s' sets withKeyOrderedMaxKeys(%d) on its Stream, but the LRU cap is a consumer-wide setting. ".formatted(
          topic,
          stream.keyOrderedMaxKeys()
        ) +
        "Move the call to MultiBuilder.withKeyOrderedMaxKeys(...) instead."
    );
    if (stream.consumerMetrics() != null) throw new IllegalArgumentException(
      perRouteRejection(topic, "withMetrics", "MultiBuilder.withMetrics(...)")
    );
    if (stream.tracer() != null) throw new IllegalArgumentException(
      perRouteRejection(topic, "withTracer", "MultiBuilder.withTracer(...)")
    );
    if (stream.circuitBreaker() != null) throw new IllegalArgumentException(
      perRouteRejection(topic, "withCircuitBreaker", "MultiBuilder.withCircuitBreaker(...)")
    );
    if (stream.maxRetries() > 0) throw new IllegalArgumentException(
      perRouteRejection(topic, "withRetry", "MultiBuilder.withRetry(...)")
    );
    if (stream.backpressureHigh() != null) throw new IllegalArgumentException(
      perRouteRejection(topic, "withBackpressure", "MultiBuilder.withBackpressure(...)")
    );
    if (stream.deadLetterTopic() != null) throw new IllegalArgumentException(
      perRouteRejection(topic, "withDeadLetterTopic", "MultiBuilder.withDeadLetterTopic(...)")
    );
    if (stream.errorHandler() != null) throw new IllegalArgumentException(
      perRouteRejection(topic, "withErrorHandler", "MultiBuilder.withErrorHandler(...)")
    );
    if (stream.pollTimeout() != null) throw new IllegalArgumentException(
      perRouteRejection(topic, "withPollTimeout", "MultiBuilder.withPollTimeout(...)")
    );
  }

  /// Builds the rejection message for a per-route setting that already has a symmetric
  /// `MultiBuilder.with*` setter — point the user at it.
  private static String perRouteRejection(final String topic, final String setting, final String mirror) {
    return (
      "Route '%s' sets %s on its Stream, but %s is a consumer-wide setting; ".formatted(topic, setting, setting) +
      "set it on %s instead.".formatted(mirror)
    );
  }

  /// Type witness: pulls the typed pipeline + sink off the route, then calls the typed builder
  /// method. Without this helper the casts would litter `start()`.
  private static <T> void addBatchRoute(final KPipeConsumerBuilder consumerBuilder, final DefaultBatchSink<T> route) {
    consumerBuilder.withBatchPipeline(route.topic(), route.buildPipeline(), route.batchSink(), route.batchPolicy());
  }
}
