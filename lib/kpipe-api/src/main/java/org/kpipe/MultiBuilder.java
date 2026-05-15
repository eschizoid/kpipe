package org.kpipe;

import com.google.protobuf.Message;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.avro.generic.GenericRecord;
import org.kpipe.consumer.CircuitBreakerController;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.format.avro.AvroFormat;
import org.kpipe.format.json.JsonFormat;
import org.kpipe.format.protobuf.ProtobufFormat;
import org.kpipe.metrics.ConsumerMetrics;
import org.kpipe.producer.tracing.Tracer;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.MessagePipeline;
import org.kpipe.sink.MessageSink;

/// Heterogeneous multi-topic builder. Each `route` registers a per-topic pipeline so the consumer
/// can dispatch records of different payload shapes through one consumer-group / one offset
/// manager.
///
/// Usage:
///
/// ```java
/// KPipe.multi(props)
///     .json("events-json", s -> s.pipe(addTimestamp).toCustom(jsonSink))
///     .avro("events-avro", s -> s.filter(active).toCustom(avroSink))
///     .protobuf("events-proto", s -> s.toConsole())
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

  /// Registers an Avro route for `topic`. `toConsole()` requires a default schema registered
  /// under key `"1"` on [AvroFormat#INSTANCE]; otherwise call
  /// `.toCustom(AvroFormat.consoleSink(schema))`.
  ///
  /// @param topic the Kafka topic
  /// @param configurator builds the operator chain and chooses a terminal sink
  /// @return this builder
  public MultiBuilder avro(
    final String topic,
    final Function<Stream<GenericRecord>, Sink<GenericRecord>> configurator
  ) {
    return route(topic, AvroFormat.INSTANCE, AvroFormat::defaultConsoleSink, configurator);
  }

  /// Registers a Protobuf route for `topic`.
  ///
  /// @param topic the Kafka topic
  /// @param configurator builds the operator chain and chooses a terminal sink
  /// @return this builder
  public MultiBuilder protobuf(final String topic, final Function<Stream<Message>, Sink<Message>> configurator) {
    return route(topic, ProtobufFormat.INSTANCE, ProtobufFormat::consoleSink, configurator);
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
    if (routes.isEmpty()) throw new IllegalStateException("at least one route is required");

    final var nonBatchPipelines = new LinkedHashMap<String, MessagePipeline<?>>();
    final var consumerBuilder = KPipeConsumer.<byte[]>builder().withProperties(kafkaProps);

    for (final var entry : routes.entrySet()) {
      final var topic = entry.getKey();
      final var sink = entry.getValue();
      if (sink instanceof DefaultSink<?> ds) {
        nonBatchPipelines.put(topic, ds.buildPipeline());
      } else if (sink instanceof DefaultBatchSink<?> dbs) {
        addBatchRoute(consumerBuilder, dbs);
      }
    }

    if (!nonBatchPipelines.isEmpty()) consumerBuilder.withPipelines(nonBatchPipelines);
    if (consumerMetrics != null) consumerBuilder.withMetrics(consumerMetrics);
    if (tracer != null) consumerBuilder.withTracer(tracer);
    if (circuitBreaker != null) consumerBuilder.withCircuitBreaker(circuitBreaker);
    final var consumer = consumerBuilder.build();
    try {
      consumer.start();
    } catch (final RuntimeException e) {
      try {
        consumer.close();
      } catch (final Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
    return new DefaultHandle(consumer);
  }

  /// Type witness: pulls the typed pipeline + sink off the route, then calls the typed builder
  /// method. Without this helper the casts would litter `start()`.
  private static <T> void addBatchRoute(
    final KPipeConsumer.Builder<byte[]> consumerBuilder,
    final DefaultBatchSink<T> route
  ) {
    consumerBuilder.withBatchPipeline(route.topic(), route.buildPipeline(), route.batchSink(), route.batchPolicy());
  }
}
