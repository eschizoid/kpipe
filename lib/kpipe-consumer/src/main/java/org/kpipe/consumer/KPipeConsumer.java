package org.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.channels.ClosedByInterruptException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.kpipe.consumer.config.AppConfig;
import org.kpipe.metrics.ConsumerMetrics;
import org.kpipe.producer.KPipeProducer;
import org.kpipe.producer.tracing.Tracer;
import org.kpipe.registry.MessagePipeline;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.BatchSink;

/// A functional-style Kafka consumer that processes records using a provided function.
///
/// This consumer provides:
///
/// * A simple functional interface for message processing
/// * Built-in retry logic with configurable backoff
/// * Support for sequential or parallel message processing
/// * Thread-safe offset management for concurrent processing scenarios
/// * Customizable error handling
/// * Message sink support for processed records
/// * Built-in metrics tracking
/// * Graceful shutdown handling
///
/// The offset management features:
///
/// * Thread-safe tracking of offsets for concurrent processing
/// * Ensures only contiguous completed offsets are committed
/// * Commits offsets periodically at a configurable interval
/// * Properly handles consumer rebalancing events
/// * Prevents data loss during parallel processing
/// * Handles non-sequential offset completion safely
///
/// When using an KafkaOffsetManager, auto-commit is automatically disabled since offset commits are
/// managed explicitly.
///
/// Example usage:
///
/// ```java
/// final var pipeline = registry.pipeline(JsonFormat.INSTANCE)
///     .add(sanitizeKey)
///     .toSink(MessageSinkRegistry.JSON_LOGGING)
///     .build();
///
/// final var consumer = KPipeConsumer.<String>builder()
///     .withProperties(kafkaProps)
///     .withTopic("example-topic")
///     .withPipeline(pipeline)
///     .withRetry(3, Duration.ofSeconds(1))
///     .build();
///
/// final var runner = KPipeRunner.builder(consumer).build();
/// runner.start();
/// ```
///
/// @param <K> the type of keys in the consumed records
public class KPipeConsumer<K> implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(KPipeConsumer.class.getName());

  private static final String METRIC_MESSAGES_RECEIVED = "messagesReceived";
  private static final String METRIC_MESSAGES_PROCESSED = "messagesProcessed";
  private static final String METRIC_PROCESSING_ERRORS = "processingErrors";
  private static final String METRIC_PROCESSING_DURATION_TOTAL_MS = "processingDurationTotalMs";
  private static final String METRIC_RETRIES = "retries";
  static final String METRIC_BACKPRESSURE_PAUSE_COUNT = "backpressurePauseCount";
  static final String METRIC_BACKPRESSURE_TIME_MS = "backpressureTimeMs";
  static final String METRIC_CIRCUIT_BREAKER_TRIPS = "circuitBreakerTrips";
  static final String METRIC_CIRCUIT_BREAKER_TIME_OPEN_MS = "circuitBreakerTimeOpenMs";
  private static final String METRIC_DLQ_SENT = "dlqSent";

  private final Queue<ConsumerCommand> commandQueue;
  private final Consumer<K, byte[]> kafkaConsumer;
  private final Set<String> topics;
  private final Map<String, MessagePipeline<?>> pipelines;
  private final ExecutorService virtualThreadExecutor;
  private final Duration pollTimeout;
  private final AtomicReference<Thread> consumerThread = new AtomicReference<>();
  private final Duration waitForMessagesTimeout;
  private final Duration threadTerminationTimeout;
  private final Duration executorTerminationTimeout;
  private final OffsetManager<K> offsetManager;
  private final ConsumerRebalanceListener rebalanceListener;
  private final ErrorHandler<K> errorHandler;
  private final int maxRetries;
  private final Duration retryBackoff;
  private final AtomicReference<ConsumerState> state = new AtomicReference<>(ConsumerState.CREATED);
  private final boolean enableMetrics;
  private final boolean sequentialProcessing;
  private final Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();
  private final ConsumerMetrics otelMetrics;
  private final BackpressureController backpressureController;
  private final Tracer tracer;
  private final AtomicLong inFlightCount = new AtomicLong(0);
  private final AtomicBoolean manualPause = new AtomicBoolean(false);
  private final AtomicBoolean backpressurePaused = new AtomicBoolean(false);
  private volatile long backpressurePauseStartNanos;

  private final String deadLetterTopic;
  private final KPipeProducer<K, byte[]> kpipeProducer;

  private final Map<String, BatchPipelineWrapper<K, ?>> batchWrappers;
  /// Shared virtual-thread scheduler. Owns batch-pipeline age ticks AND the circuit-breaker
  /// probe timer. Lazily created when either feature is configured; otherwise stays null.
  private final ScheduledExecutorService scheduler;

  /// Circuit-breaker state. All five fields are inert when `circuitBreakerController` is null —
  /// per-record `recordCircuitBreakerOutcome(...)` short-circuits on the null check, so callers
  /// who don't configure a breaker pay only a single field read per record.
  private final CircuitBreakerController circuitBreakerController;
  private final CircuitBreakerStats circuitBreakerStats;
  private final AtomicReference<CircuitBreakerState> circuitBreakerState = new AtomicReference<>(
    CircuitBreakerState.CLOSED
  );
  private final AtomicLong circuitBreakerOpenedAtNanos = new AtomicLong(0L);
  private volatile ScheduledFuture<?> circuitBreakerProbeFuture;

  /// Represents an error that occurred during record processing. Contains the original record,
  /// the exception that was thrown, and the number of retry attempts made.
  ///
  /// @param <K> the type of the record key
  /// @param record the Kafka record that failed processing
  /// @param exception the exception that occurred during processing
  /// @param retryCount the number of retry attempts made
  public record ProcessingError<K>(ConsumerRecord<K, byte[]> record, Exception exception, int retryCount) {}

  /// A functional interface for handling processing errors.
  ///
  /// @param <K> the type of the record key
  @FunctionalInterface
  public interface ErrorHandler<K> extends java.util.function.Consumer<ProcessingError<K>> {}

  /// Creates a new builder for constructing {@link KPipeConsumer} instances.
  ///
  /// Note: `<K>` is the Kafka record KEY type. Per the byte-boundary architecture, values are
  /// always `byte[]` — the pipeline handles deserialization.
  ///
  /// @param <K> the type of keys in the consumed records
  /// @return a new builder instance
  public static <K> Builder<K> builder() {
    return new Builder<>();
  }

  /// Builder for creating and configuring {@link KPipeConsumer} instances.
  ///
  /// @param <K> the type of keys in the consumed records
  public static class Builder<K> {

    private Builder() {}

    /// Internal record bundling the per-topic batch configuration handed in via
    /// [#withBatchPipeline]. The [BatchSink] returns a [org.kpipe.sink.BatchResult] naming
    /// per-record outcomes; void-style consumers can use [BatchSink#ofVoid] to opt into
    /// whole-batch success/failure semantics.
    record BatchSpec<T>(String topic, MessagePipeline<T> pipeline, BatchSink<T> sink, BatchPolicy policy) {}

    private Properties kafkaProps;
    private Set<String> topics;
    private MessagePipeline<?> pipeline;
    private Map<String, MessagePipeline<?>> pipelinesPerTopic;
    private final Map<String, BatchSpec<?>> batchSpecs = new LinkedHashMap<>();
    private Duration pollTimeout = Duration.ofMillis(100);
    private ErrorHandler<K> errorHandler = e ->
      LOGGER.log(
        Level.WARNING,
        "Failed at offset {0} after {1} retries: {2}",
        e.record().offset(),
        e.retryCount(),
        e.exception().getMessage()
      );
    private int maxRetries = 0;
    private Duration retryBackoff = Duration.ofMillis(500);
    private boolean enableMetrics = true;
    private boolean sequentialProcessing = false;
    private Duration waitForMessagesTimeout = AppConfig.DEFAULT_WAIT_FOR_MESSAGES;
    private Duration threadTerminationTimeout = AppConfig.DEFAULT_THREAD_TERMINATION;
    private Duration executorTerminationTimeout = AppConfig.DEFAULT_EXECUTOR_TERMINATION;
    private OffsetManager<K> offsetManager;
    private Function<Consumer<K, byte[]>, OffsetManager<K>> offsetManagerProvider;
    private Supplier<Consumer<K, byte[]>> consumerProvider;
    private Queue<ConsumerCommand> commandQueue = new ConcurrentLinkedQueue<>();
    private ConsumerRebalanceListener rebalanceListener;
    private BackpressureController backpressureController;
    private String deadLetterTopic;
    private KPipeProducer<K, byte[]> kpipeProducer;
    private ConsumerMetrics consumerMetrics;
    private Tracer tracer;
    private CircuitBreakerController circuitBreakerController;

    /// Sets the properties for the Kafka consumer.
    ///
    /// @param props The Kafka consumer properties
    /// @return This builder instance for method chaining
    public Builder<K> withProperties(final Properties props) {
      this.kafkaProps = props;
      return this;
    }

    /// Sets the Kafka topic to consume from.
    ///
    /// @param topic The topic name
    /// @return This builder instance for method chaining
    public Builder<K> withTopic(final String topic) {
      return withTopics(Set.of(Objects.requireNonNull(topic, "Topic cannot be null")));
    }

    /// Sets multiple Kafka topics to consume from with a single shared pipeline.
    ///
    /// All topics must produce the same payload type because they share one [MessagePipeline].
    /// For per-topic typing, run separate consumers.
    ///
    /// @param topics The topic names (must be non-empty)
    /// @return This builder instance for method chaining
    public Builder<K> withTopics(final Collection<String> topics) {
      Objects.requireNonNull(topics, "Topics cannot be null");
      if (topics.isEmpty()) throw new IllegalArgumentException("Topics cannot be empty");
      this.topics = new LinkedHashSet<>(topics);
      return this;
    }

    /// Sets multiple Kafka topics to consume from with a single shared pipeline (varargs form).
    ///
    /// @param topics The topic names (must be non-empty)
    /// @return This builder instance for method chaining
    public Builder<K> withTopics(final String... topics) {
      return withTopics(java.util.Arrays.asList(Objects.requireNonNull(topics, "Topics cannot be null")));
    }

    /// Sets the pipeline to process each consumed message.
    ///
    /// The consumer always operates on `byte[]` at the boundary; format SerDe lives inside the
    /// pipeline. The pipeline must be a [MessagePipeline] — typically produced by
    /// `MessageProcessorRegistry.pipeline(format).build()`.
    ///
    /// All subscribed topics share this pipeline (homogeneous case). For heterogeneous
    /// per-topic pipelines, use [#withPipelines(Map)].
    ///
    /// @param pipeline The pipeline to apply to message values
    /// @return This builder instance for method chaining
    public Builder<K> withPipeline(final MessagePipeline<?> pipeline) {
      this.pipeline = pipeline;
      return this;
    }

    /// Sets a per-topic pipeline map, enabling heterogeneous payload types across the subscribed
    /// topics. When this is set, the consumer subscribes to the map's keys and dispatches each
    /// record to its topic's pipeline. Records arriving for topics not present in the map are
    /// dropped with a WARNING log; the offset is still marked processed.
    ///
    /// `withTopic` / `withTopics` is unnecessary when `withPipelines` is used — the topic set is
    /// derived from the map keys. Mixing `withPipeline` and `withPipelines` is an error.
    ///
    /// @param pipelinesPerTopic the topic → pipeline map (must be non-empty)
    /// @return This builder instance for method chaining
    public Builder<K> withPipelines(final Map<String, MessagePipeline<?>> pipelinesPerTopic) {
      Objects.requireNonNull(pipelinesPerTopic, "pipelinesPerTopic cannot be null");
      if (pipelinesPerTopic.isEmpty()) throw new IllegalArgumentException("pipelinesPerTopic cannot be empty");
      this.pipelinesPerTopic = Map.copyOf(pipelinesPerTopic);
      return this;
    }

    /// Configures a batch-mode pipeline for `topic`. Records are deserialized + processed via
    /// `pipeline`, buffered until `policy.maxSize()` records have accumulated or
    /// `policy.maxAge()` has elapsed since the oldest buffered record, then flushed in one call
    /// to `sink`. Offsets are marked processed only after the sink returns successfully — a
    /// throwing sink sends every record in the batch to the configured DLQ (if any) and still
    /// commits offsets so the consumer does not loop on a poison batch.
    ///
    /// May be called multiple times to register batch routes for different topics on the same
    /// consumer (heterogeneous multi-topic batch consumption). Cannot be combined with
    /// `withPipeline` / `withPipelines` for the SAME topic — the disjoint-topics check fires in
    /// [#build]. Both sequential and parallel processing are supported. Under parallel mode,
    /// buffered records participate in the in-flight backpressure metric so a slow sink cannot
    /// let the buffer grow unbounded.
    ///
    /// @param topic the Kafka topic
    /// @param pipeline the typed pipeline to deserialize+process raw bytes into `T`
    /// @param sink the batch sink invoked on each flush
    /// @param policy the size/age flush thresholds
    /// @param <T> the deserialized value type
    /// @return this builder instance for method chaining
    public <T> Builder<K> withBatchPipeline(
      final String topic,
      final MessagePipeline<T> pipeline,
      final BatchSink<T> sink,
      final BatchPolicy policy
    ) {
      Objects.requireNonNull(topic, "topic cannot be null");
      if (topic.isBlank()) throw new IllegalArgumentException("topic cannot be blank");
      Objects.requireNonNull(pipeline, "pipeline cannot be null");
      Objects.requireNonNull(sink, "sink cannot be null");
      Objects.requireNonNull(policy, "policy cannot be null");
      if (batchSpecs.containsKey(topic)) throw new IllegalArgumentException(
        "Duplicate batch route for topic '%s'".formatted(topic)
      );
      this.batchSpecs.put(topic, new BatchSpec<>(topic, pipeline, sink, policy));
      return this;
    }

    /// Sets the timeout duration for the consumer's poll operation.
    ///
    /// @param timeout The maximum time to wait for messages in each poll
    /// @return This builder instance for method chaining
    public Builder<K> withPollTimeout(final Duration timeout) {
      this.pollTimeout = timeout;
      return this;
    }

    /// Sets the handler for processing errors.
    ///
    /// @param handler The consumer function that handles processing errors
    /// @return This builder instance for method chaining
    public Builder<K> withErrorHandler(final ErrorHandler<K> handler) {
      this.errorHandler = handler;
      return this;
    }

    /// Configures retry behavior for failed message processing.
    ///
    /// @param maxRetries Maximum number of retry attempts
    /// @param backoff Duration to wait between retry attempts
    /// @return This builder instance for method chaining
    public Builder<K> withRetry(final int maxRetries, final Duration backoff) {
      this.maxRetries = maxRetries;
      this.retryBackoff = backoff;
      return this;
    }

    /// Disables metrics collection. Metrics are enabled by default.
    ///
    /// @return This builder instance for method chaining
    public Builder<K> disableMetrics() {
      this.enableMetrics = false;
      return this;
    }

    /// Configures whether messages should be processed sequentially.
    ///
    /// @param sequential If true, messages will be processed in order; if false, parallel
    ///     processing is used
    /// @return This builder instance for method chaining
    public Builder<K> withSequentialProcessing(final boolean sequential) {
      this.sequentialProcessing = sequential;
      return this;
    }

    /// Sets the timeout for waiting for in-flight messages during shutdown.
    ///
    /// @param timeout Maximum time to wait for in-flight messages to complete
    /// @return This builder instance for method chaining
    public Builder<K> withWaitForMessagesTimeout(final Duration timeout) {
      this.waitForMessagesTimeout = Objects.requireNonNull(timeout);
      return this;
    }

    /// Sets the timeout for waiting for the consumer thread to terminate during shutdown.
    ///
    /// @param timeout Maximum time to wait for the consumer thread to finish
    /// @return This builder instance for method chaining
    public Builder<K> withThreadTerminationTimeout(final Duration timeout) {
      this.threadTerminationTimeout = Objects.requireNonNull(timeout);
      return this;
    }

    /// Sets the timeout for the virtual-thread executor to drain in-flight processing tasks during
    /// shutdown. After this timeout, `executor.shutdownNow()` is invoked to cancel remaining tasks.
    ///
    /// @param timeout Maximum time to wait for the executor to drain
    /// @return This builder instance for method chaining
    public Builder<K> withExecutorTerminationTimeout(final Duration timeout) {
      this.executorTerminationTimeout = Objects.requireNonNull(timeout);
      return this;
    }

    /// Sets a function to create a custom OffsetManager once the consumer is available. This
    /// automatically disables auto-commit.
    ///
    /// @param provider A function that creates an OffsetManager given the consumer instance
    /// @return This builder instance for method chaining
    public Builder<K> withOffsetManagerProvider(final Function<Consumer<K, byte[]>, OffsetManager<K>> provider) {
      this.offsetManagerProvider = consumer -> {
        final var offsetManager = Objects.requireNonNull(provider.apply(consumer), "OffsetManager cannot be null");
        this.rebalanceListener = offsetManager.createRebalanceListener();
        return offsetManager;
      };
      return this;
    }

    /// Enables offset management with a custom offset manager implementation. This
    /// automatically disables auto-commit.
    ///
    /// @param manager The custom offset manager to use
    /// @return This builder instance for method chaining
    public Builder<K> withOffsetManager(final OffsetManager<K> manager) {
      this.offsetManager = Objects.requireNonNull(manager, "OffsetManager cannot be null");
      this.rebalanceListener = manager.createRebalanceListener();
      return this;
    }

    /// Sets a custom command queue for the consumer.
    ///
    /// A command queue is auto-created in the builder, so most users do not need to call this
    /// method. Use it only when you need to share an externally owned queue (for example, when
    /// constructing an {@link OffsetManager} outside of [#withOffsetManagerProvider] and wiring
    /// it to the same queue the consumer will read from). To grab the auto-created queue from
    /// inside an [#withOffsetManagerProvider] lambda, call [#getCommandQueue()] on the builder.
    ///
    /// @param commandQueue the queue to use for consumer commands
    /// @return this Builder instance for method chaining
    public Builder<K> withCommandQueue(final Queue<ConsumerCommand> commandQueue) {
      this.commandQueue = Objects.requireNonNull(commandQueue, "Command queue cannot be null");
      return this;
    }

    /// Returns the {@link ConsumerCommand} queue this builder will hand to the consumer. By
    /// default the builder auto-creates a {@link ConcurrentLinkedQueue}; callers that supplied
    /// their own queue via [#withCommandQueue] will see that instance instead.
    ///
    /// This getter is intended for use inside an [#withOffsetManagerProvider] lambda, so the
    /// offset manager and the consumer share the same queue without forcing the caller to
    /// construct one upfront.
    ///
    /// @return the command queue this builder will pass to the consumer
    public Queue<ConsumerCommand> getCommandQueue() {
      return commandQueue;
    }

    /// Sets the supplier for providing a consumer instance.
    ///
    /// @param provider A supplier that returns a Consumer instance configured for processing
    ///     messages
    /// @return This builder instance for method chaining
    public Builder<K> withConsumer(final Supplier<Consumer<K, byte[]>> provider) {
      this.consumerProvider = provider;
      return this;
    }

    /// Enables backpressure control using the default watermarks: high = 10,000 (pause) and
    /// low = 7,000 (resume). Use {@link #withBackpressure(long, long)} to configure custom values.
    ///
    /// Backpressure is enabled by default.
    ///
    /// @return This builder instance for method chaining
    public Builder<K> withBackpressure() {
      return withBackpressure(10_000, 7_000);
    }

    /// Enables backpressure control using the given high and low watermarks. When the number of
    /// in-flight messages reaches the high watermark, the consumer pauses Kafka polling. It
    /// resumes when the count drops to or below the low watermark (hysteresis).
    ///
    /// Backpressure is enabled by default. Calling this method configures custom thresholds.
    ///
    /// @param highWatermark pause consumption when in-flight count reaches this value (must be
    ///     positive)
    /// @param lowWatermark  resume consumption when in-flight count drops to this value (must be
    ///     less than highWatermark)
    /// @return This builder instance for method chaining
    public Builder<K> withBackpressure(final long highWatermark, final long lowWatermark) {
      this.backpressureController = new BackpressureController(highWatermark, lowWatermark, null);
      return this;
    }

    /// Sets the Dead Letter Queue (DLQ) topic to send failed records after all retries.
    ///
    /// Prefer [#withDeadLetterQueue(String, KPipeProducer)] which sets the topic and producer
    /// atomically. When only the topic is set (no producer), the consumer auto-builds a producer
    /// from the configured Kafka properties at construction time — fine for simple cases, but
    /// `withDeadLetterQueue(...)` is preferred when you have an existing producer to share.
    ///
    /// @param topic The name of the DLQ topic
    /// @return This builder instance for method chaining
    public Builder<K> withDeadLetterTopic(final String topic) {
      this.deadLetterTopic = topic;
      return this;
    }

    /// Sets the Kafka producer to use for DLQ and Kafka sinks. If not provided but a DLQ topic is
    /// set, a new producer will be created using the consumer's configuration.
    ///
    /// Prefer [#withDeadLetterQueue(String, KPipeProducer)] when configuring a DLQ — it sets the
    /// topic and producer atomically.
    ///
    /// @param producer The Kafka producer to use
    /// @return This builder instance for method chaining
    public Builder<K> withKafkaProducer(final Producer<K, byte[]> producer) {
      this.kpipeProducer = KPipeProducer.<K, byte[]>builder().withProducer(producer).build();
      return this;
    }

    /// Sets the KPipe producer wrapper to use for DLQ and Kafka sinks.
    ///
    /// Prefer [#withDeadLetterQueue(String, KPipeProducer)] when configuring a DLQ — it sets the
    /// topic and producer atomically.
    ///
    /// @param producer The KPipe producer wrapper to use
    /// @return This builder instance for method chaining
    public Builder<K> withKafkaProducer(final KPipeProducer<K, byte[]> producer) {
      this.kpipeProducer = Objects.requireNonNull(producer, "Producer cannot be null");
      return this;
    }

    /// Configures the Dead Letter Queue (DLQ) by setting both the topic and the producer
    /// atomically. Failed records will be sent to `topic` via `producer` after all retries are
    /// exhausted.
    ///
    /// Prefer this method over calling [#withDeadLetterTopic] and [#withKafkaProducer]
    /// separately — it ensures the two settings cannot drift out of sync.
    ///
    /// @param topic The name of the DLQ topic (non-null)
    /// @param producer The KPipe producer wrapper to use for DLQ sends (non-null)
    /// @return This builder instance for method chaining
    public Builder<K> withDeadLetterQueue(final String topic, final KPipeProducer<K, byte[]> producer) {
      this.deadLetterTopic = Objects.requireNonNull(topic, "DLQ topic cannot be null");
      this.kpipeProducer = Objects.requireNonNull(producer, "DLQ producer cannot be null");
      return this;
    }

    /// Sets the OpenTelemetry metrics instruments for this consumer.
    ///
    /// Use [org.kpipe.metrics.ConsumerMetrics] to create an instrumented instance, or
    /// [ConsumerMetrics#noop()] for a no-op default.
    ///
    /// @param metrics the consumer metrics instruments
    /// @return This builder instance for method chaining
    public Builder<K> withMetrics(final ConsumerMetrics metrics) {
      this.consumerMetrics = metrics;
      return this;
    }

    /// Sets the tracer used for span propagation across the Kafka boundary.
    ///
    /// On record receive, the tracer extracts any upstream context (e.g. W3C `traceparent`)
    /// from the record's headers and starts a span scoped to the record's processing. On DLQ
    /// send, the tracer injects the active context into outbound headers so downstream consumers
    /// stay on the same trace.
    ///
    /// Add the `kpipe-tracing-otel` module and pass
    /// `new OtelTracer(openTelemetry, "my-pipeline")` to wire OpenTelemetry-backed propagation.
    /// When not set, the default is [Tracer#noop()] (zero overhead).
    ///
    /// @param tracer the tracer; pass `Tracer.noop()` to disable explicitly
    /// @return This builder instance for method chaining
    public Builder<K> withTracer(final Tracer tracer) {
      this.tracer = tracer;
      return this;
    }

    /// Attaches a circuit breaker that watches per-record success / failure outcomes and pauses
    /// Kafka polling once the failure rate crosses the configured threshold. After
    /// `openDuration` elapses the consumer enters HALF_OPEN and the next record acts as the probe
    /// — success returns to CLOSED, failure flips back to OPEN and restarts the timer.
    ///
    /// **Why CB and not just retries.** Retries handle transient failures per record. A circuit
    /// breaker handles *sustained* failures (DB down, downstream 503-storm) by stopping work
    /// entirely until the downstream recovers — preventing DLQ floods and consumer-side resource
    /// burn while the dependency is unhealthy.
    ///
    /// @param controller the failure-rate / window / open-duration policy (must not be null)
    /// @return this builder instance for method chaining
    public Builder<K> withCircuitBreaker(final CircuitBreakerController controller) {
      this.circuitBreakerController = Objects.requireNonNull(controller, "controller cannot be null");
      return this;
    }

    /// Builds a new KPipeConsumer with the configured settings.
    ///
    /// @return a new KPipeConsumer instance
    public KPipeConsumer<K> build() {
      Objects.requireNonNull(kafkaProps, "Kafka properties must be provided");
      if (pipeline != null && pipelinesPerTopic != null) throw new IllegalArgumentException(
        "Use either withPipeline (homogeneous) or withPipelines (heterogeneous), not both"
      );

      // Compute the regular-pipeline topic set. When no batch routes are configured the existing
      // semantics apply (topic + pipeline are both required, missing fields throw NPE). When
      // batch routes ARE configured the regular pipeline is optional — a batch-only consumer is
      // legitimate, and the regular path validates only when the user partly set it up.
      final Set<String> regularTopics;
      if (batchSpecs.isEmpty()) {
        if (pipelinesPerTopic != null) {
          if (topics != null) throw new IllegalArgumentException(
            "withTopic/withTopics is unnecessary with withPipelines — topics are derived from the map keys; remove the explicit topic call"
          );
          regularTopics = pipelinesPerTopic.keySet();
        } else {
          Objects.requireNonNull(topics, "Topic must be provided via withTopic or withTopics");
          if (topics.isEmpty()) throw new IllegalArgumentException("At least one topic must be provided");
          Objects.requireNonNull(pipeline, "Pipeline function must be provided");
          regularTopics = topics;
        }
      } else if (pipelinesPerTopic != null) {
        if (topics != null) throw new IllegalArgumentException(
          "withTopic/withTopics is unnecessary with withPipelines — topics are derived from the map keys; remove the explicit topic call"
        );
        regularTopics = pipelinesPerTopic.keySet();
      } else if (pipeline != null) {
        Objects.requireNonNull(topics, "Topic must be provided via withTopic or withTopics");
        regularTopics = topics;
      } else {
        regularTopics = Set.of();
      }

      // A topic must live in EITHER pipelines/pipelinesPerTopic OR batchSpecs, never both.
      for (final var batchTopic : batchSpecs.keySet()) {
        if (regularTopics.contains(batchTopic)) throw new IllegalArgumentException(
          "Topic '%s' is configured as both a regular pipeline and a batch route — pick one".formatted(batchTopic)
        );
      }

      // The consumer subscribes to the union of regular and batch topics. Always overwrite
      // `this.topics` here — pre-existing code did the same when pipelinesPerTopic was set.
      final var union = new LinkedHashSet<String>(regularTopics.size() + batchSpecs.size());
      union.addAll(regularTopics);
      union.addAll(batchSpecs.keySet());
      this.topics = union;
      if (maxRetries < 0) throw new IllegalArgumentException("Max retries cannot be negative");
      if (pollTimeout.isNegative() || pollTimeout.isZero()) throw new IllegalArgumentException(
        "Poll timeout must be positive"
      );
      if (offsetManager != null || offsetManagerProvider != null) kafkaProps.setProperty("enable.auto.commit", "false");
      if (backpressureController != null && sequentialProcessing) {
        LOGGER.log(
          System.Logger.Level.INFO,
          "Sequential processing enabled with backpressure: switching to lag-based monitoring."
        );
      }
      return new KPipeConsumer<>(this);
    }
  }

  /// Creates a new KPipeConsumer using the provided builder.
  ///
  /// @param builder the builder containing the consumer configuration
  public KPipeConsumer(final Builder<K> builder) {
    this.kafkaConsumer =
      builder.consumerProvider != null
        ? builder.consumerProvider.get()
        : new KafkaConsumer<>(Objects.requireNonNull(builder.kafkaProps));
    this.topics = Set.copyOf(Objects.requireNonNull(builder.topics, "Topics must be set"));
    if (builder.pipelinesPerTopic != null) {
      this.pipelines = builder.pipelinesPerTopic;
    } else if (builder.pipeline != null) {
      // Homogeneous: only the pipeline-bound topics get this pipeline; batch topics live in
      // batchWrappers and are handled separately by tryProcessRecord.
      final var pipelineTopics = builder.topics
        .stream()
        .filter(t -> !builder.batchSpecs.containsKey(t))
        .toList();
      final var map = new LinkedHashMap<String, MessagePipeline<?>>(pipelineTopics.size());
      for (final var t : pipelineTopics) map.put(t, builder.pipeline);
      this.pipelines = Map.copyOf(map);
    } else {
      this.pipelines = Map.of();
    }
    this.pollTimeout = Objects.requireNonNull(builder.pollTimeout);
    this.errorHandler = builder.errorHandler;
    this.maxRetries = builder.maxRetries;
    this.retryBackoff = builder.retryBackoff;
    this.enableMetrics = builder.enableMetrics;
    this.sequentialProcessing = builder.sequentialProcessing;
    this.waitForMessagesTimeout = builder.waitForMessagesTimeout;
    this.threadTerminationTimeout = builder.threadTerminationTimeout;
    this.executorTerminationTimeout = builder.executorTerminationTimeout;
    this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    this.offsetManager =
      builder.offsetManager != null
        ? builder.offsetManager
        : builder.offsetManagerProvider != null
          ? builder.offsetManagerProvider.apply(this.kafkaConsumer)
          : null;
    this.commandQueue = builder.commandQueue != null ? builder.commandQueue : new ConcurrentLinkedQueue<>();
    this.rebalanceListener =
      builder.rebalanceListener != null
        ? builder.rebalanceListener
        : (this.offsetManager != null ? this.offsetManager.createRebalanceListener() : null);

    this.backpressureController = (
      builder.backpressureController != null
        ? builder.backpressureController
        : new BackpressureController(10_000, 7_000, null)
    ).withStrategy(
      this.sequentialProcessing
        ? BackpressureController.lagStrategy()
        : BackpressureController.inFlightStrategy(this::totalInFlight)
    );

    this.tracer = builder.tracer != null ? builder.tracer : Tracer.noop();

    this.deadLetterTopic = builder.deadLetterTopic;
    this.kpipeProducer =
      builder.kpipeProducer != null
        ? builder.kpipeProducer
        : this.deadLetterTopic != null
          ? KPipeProducer.<K, byte[]>builder().withProperties(builder.kafkaProps).withTracer(this.tracer).build()
          : null;

    final var needScheduler = !builder.batchSpecs.isEmpty() || builder.circuitBreakerController != null;
    if (needScheduler) {
      this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        final var t = Thread.ofVirtual().unstarted(r);
        t.setName("kpipe-scheduler");
        return t;
      });
    } else {
      this.scheduler = null;
    }

    if (!builder.batchSpecs.isEmpty()) {
      final var wrappers = new LinkedHashMap<String, BatchPipelineWrapper<K, ?>>(builder.batchSpecs.size());
      for (final var entry : builder.batchSpecs.entrySet()) {
        wrappers.put(entry.getKey(), createBatchWrapper(entry.getValue()));
      }
      this.batchWrappers = Map.copyOf(wrappers);
    } else {
      this.batchWrappers = Map.of();
    }

    this.circuitBreakerController = builder.circuitBreakerController;
    this.circuitBreakerStats =
      this.circuitBreakerController != null
        ? new CircuitBreakerStats(this.circuitBreakerController.windowSize())
        : null;

    if (builder.backpressureController == null) {
      LOGGER.log(
        Level.INFO,
        "No backpressure configured, using default {0} strategy (high={1}, low={2})",
        this.backpressureController.getMetricName(),
        this.backpressureController.highWatermark(),
        this.backpressureController.lowWatermark()
      );
    }

    if (enableMetrics) {
      metrics.putAll(
        Map.of(
          METRIC_MESSAGES_RECEIVED,
          new AtomicLong(0),
          METRIC_MESSAGES_PROCESSED,
          new AtomicLong(0),
          METRIC_PROCESSING_ERRORS,
          new AtomicLong(0),
          METRIC_PROCESSING_DURATION_TOTAL_MS,
          new AtomicLong(0),
          METRIC_RETRIES,
          new AtomicLong(0),
          METRIC_BACKPRESSURE_PAUSE_COUNT,
          new AtomicLong(0),
          METRIC_BACKPRESSURE_TIME_MS,
          new AtomicLong(0),
          METRIC_CIRCUIT_BREAKER_TRIPS,
          new AtomicLong(0),
          METRIC_CIRCUIT_BREAKER_TIME_OPEN_MS,
          new AtomicLong(0),
          METRIC_DLQ_SENT,
          new AtomicLong(0)
        )
      );
    }

    this.otelMetrics = builder.consumerMetrics != null ? builder.consumerMetrics : ConsumerMetrics.noop();
  }

  private <T> BatchPipelineWrapper<K, T> createBatchWrapper(final Builder.BatchSpec<T> spec) {
    // Batch flushes call the OffsetManager directly rather than going through commandQueue. The
    // queue exists to serialize Kafka-consumer calls (pause/resume/commitSync) on the consumer
    // thread; OffsetManager.markOffsetProcessed is already thread-safe and avoiding the queue
    // means the shutdown drain works even after the consumer thread has exited.
    final var callbacks = new BatchPipelineWrapper.BatchCallbacks<K>() {
      @Override
      public void markProcessed(final ConsumerRecord<K, byte[]> record) {
        if (enableMetrics) metrics.get(METRIC_MESSAGES_PROCESSED).incrementAndGet();
        otelMetrics.recordMessageProcessed(record.topic());
        if (offsetManager != null) offsetManager.markOffsetProcessed(record);
      }

      @Override
      public void onBatchFailure(final ConsumerRecord<K, byte[]> record, final Exception cause) {
        if (enableMetrics) metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
        otelMetrics.recordProcessingError(record.topic());
        LOGGER.log(Level.WARNING, "Batch failure for record at offset {0}: {1}", record.offset(), cause.getMessage());
        if (deadLetterTopic != null && kpipeProducer != null) {
          final var sent = kpipeProducer.sendToDlq(deadLetterTopic, record, record.topic(), cause);
          if (sent && enableMetrics) metrics.get(METRIC_DLQ_SENT).incrementAndGet();
        }
        if (offsetManager != null) offsetManager.markOffsetProcessed(record);
        try {
          errorHandler.accept(new ProcessingError<>(record, cause, 0));
        } catch (final Exception ex) {
          LOGGER.log(
            Level.ERROR,
            "Error handler threw on batch failure at offset {0}: {1}",
            record.offset(),
            ex.getMessage(),
            ex
          );
        }
      }
    };
    return new BatchPipelineWrapper<>(spec.topic(), spec.pipeline(), spec.sink(), spec.policy(), scheduler, callbacks);
  }

  /// Creates a message tracker that can monitor the state of in-flight messages. The tracker
  /// uses the consumer's metrics to determine how many messages have been received versus
  /// processed.
  ///
  /// @return a new [MessageTracker] instance
  /// @throws IllegalStateException if metrics are disabled (the tracker has no counters to read)
  public MessageTracker createMessageTracker() {
    if (!enableMetrics) throw new IllegalStateException(
      "Cannot create MessageTracker: metrics are disabled. Remove the disableMetrics() call from " +
        "the builder, or do not call createMessageTracker()."
    );
    return MessageTracker.builder()
      .withMetrics(this::getMetrics)
      .withReceivedMetricKey(METRIC_MESSAGES_RECEIVED)
      .withProcessedMetricKey(METRIC_MESSAGES_PROCESSED)
      .withErrorsMetricKey(METRIC_PROCESSING_ERRORS)
      .build();
  }

  /// Starts the consumer thread and begins consuming messages from the configured topic. The
  /// consumer will poll for records and process them asynchronously in virtual threads.
  ///
  /// @throws IllegalStateException if the consumer has already been started or was previously
  ///     closed
  public void start() {
    if (state.get() == ConsumerState.CLOSED) throw new IllegalStateException("Cannot restart a closed consumer");
    if (!state.compareAndSet(ConsumerState.CREATED, ConsumerState.RUNNING)) return;

    if (offsetManager != null) offsetManager.start();
    batchWrappers.values().forEach(BatchPipelineWrapper::start);
    if (rebalanceListener != null) kafkaConsumer.subscribe(topics, rebalanceListener);
    else kafkaConsumer.subscribe(topics);

    final var threadLabel = topics.size() == 1 ? topics.iterator().next() : "topics-%d".formatted(topics.size());
    final var thread = Thread.ofVirtual()
      .name("kafka-consumer-%s-%s".formatted(threadLabel, UUID.randomUUID().toString().substring(0, 8)))
      .uncaughtExceptionHandler((t, e) -> {
        LOGGER.log(Level.ERROR, "Uncaught exception in thread {0}", t.getName(), e);
        transitionToClosing();
      })
      .start(() -> {
        try {
          while (isRunning()) {
            processCommands();
            checkBackpressure();
            if (!isRunning()) break;
            if (isPaused()) {
              processCommands();
              LockSupport.park();
              if (Thread.interrupted()) break;
              continue;
            }

            final var records = pollRecords();
            if (records != null && !records.isEmpty()) processRecords(records);
          }
        } catch (final Exception e) {
          if (isRunning()) LOGGER.log(Level.WARNING, "Error in consumer thread", e);
        } finally {
          try {
            kafkaConsumer.close();
            LOGGER.log(Level.INFO, "Consumer closed for topics {0}", topics);
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error closing Kafka consumer", e);
          } finally {
            state.set(ConsumerState.CLOSED);
          }
        }
      });

    consumerThread.set(thread);
    LOGGER.log(Level.INFO, "Consumer started for topics {0}", topics);
  }

  /// Pauses consumption from the topic. Any in-flight messages will continue processing, but no new
  /// messages will be consumed until {@link #resume()} is called.
  ///
  /// This method is idempotent - calling it multiple times has no additional effect.
  public void pause() {
    manualPause.set(true);
    internalPause();
  }

  private void internalPause() {
    final var current = state.get();
    if (current != ConsumerState.RUNNING && current != ConsumerState.CREATED) return;
    if (state.compareAndSet(current, ConsumerState.PAUSED)) {
      commandQueue.offer(new ConsumerCommand.Pause());
    }
  }

  /// Processes pending commands from the command queue.
  ///
  /// This method polls commands from the internal command queue and executes the corresponding
  /// actions:
  ///
  /// * `PAUSE` - Pauses consumption by calling `consumer.pause()`
  /// * `RESUME` - Resumes consumption by calling `consumer.resume()`
  /// * `CLOSE` - Initiates shutdown by setting the running flag to false
  ///
  /// Commands are processed in the order they were submitted to the queue. If an exception occurs
  /// while processing a command, it will be caught and logged, allowing subsequent commands to be
  /// processed.
  void processCommands() {
    ConsumerCommand command;

    while ((command = commandQueue.poll()) != null) {
      try {
        switch (command) {
          case ConsumerCommand.Pause _ -> {
            kafkaConsumer.pause(kafkaConsumer.assignment());
            LOGGER.log(Level.INFO, "Consumer paused for topics {0}", topics);
          }
          case ConsumerCommand.Resume _ -> {
            kafkaConsumer.resume(kafkaConsumer.assignment());
            LOGGER.log(Level.INFO, "Consumer resumed for topics {0}", topics);
          }
          case ConsumerCommand.Close _ -> {
            state.set(ConsumerState.CLOSING);
            LOGGER.log(Level.INFO, "Consumer shutdown initiated for topics {0}", topics);
          }
          case ConsumerCommand.TrackOffset cmd -> {
            if (offsetManager != null) {
              @SuppressWarnings("unchecked")
              final var record = (ConsumerRecord<K, byte[]>) cmd.record();
              offsetManager.trackOffset(record);
            }
          }
          case ConsumerCommand.MarkOffsetProcessed cmd -> {
            if (offsetManager != null) {
              @SuppressWarnings("unchecked")
              final var record = (ConsumerRecord<K, byte[]>) cmd.record();
              offsetManager.markOffsetProcessed(record);
            }
          }
          case ConsumerCommand.CommitOffsets cmd -> {
            try {
              kafkaConsumer.commitSync(cmd.offsets());
              if (offsetManager != null) offsetManager.notifyCommitComplete(cmd.commitId(), true);
            } catch (Exception e) {
              LOGGER.log(Level.WARNING, "Failed to commit offsets", e);
              if (offsetManager != null) offsetManager.notifyCommitComplete(cmd.commitId(), false);
            }
          }
        }
      } catch (final Exception e) {
        LOGGER.log(Level.ERROR, "Error processing consumer command: {0}", command, e);
      }
    }
  }

  /// Resumes consumption from the topic after being paused.
  ///
  /// This method is idempotent - calling it multiple times has no additional effect.
  ///
  /// @throws IllegalStateException if the consumer has been closed
  public void resume() {
    if (state.get() == ConsumerState.CLOSED) throw new IllegalStateException("Cannot resume a closed consumer");
    manualPause.set(false);
    if (!backpressurePaused.get()) internalResume();
  }

  private void internalResume() {
    if (!state.compareAndSet(ConsumerState.PAUSED, ConsumerState.RUNNING)) return;
    commandQueue.offer(new ConsumerCommand.Resume());
    final var thread = consumerThread.get();
    if (thread != null) LockSupport.unpark(thread);
  }

  /// Returns whether the consumer is currently paused.
  ///
  /// @return `true` if the consumer is paused, `false` otherwise
  public boolean isPaused() {
    return state.get() == ConsumerState.PAUSED;
  }

  /// Returns a snapshot of the current metrics collected by this consumer.
  ///
  /// Available metrics include:
  ///
  /// * `messagesReceived` - count of records received from Kafka
  /// * `messagesProcessed` - count of records successfully processed
  /// * `processingErrors` - count of records that failed processing after all retries
  /// * `retries` - count of retry attempts made for failed records
  /// * `inFlight` - current number of messages being processed
  ///
  /// @return an unmodifiable map of metric names to their current values, or an empty map if
  // metrics are disabled
  public Map<String, Long> getMetrics() {
    if (!enableMetrics) return Map.of();
    final var snapshot = metrics
      .entrySet()
      .stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(), (a, _) -> a, HashMap::new));
    snapshot.put("inFlight", inFlightCount.get());
    return Collections.unmodifiableMap(snapshot);
  }

  /// Returns the rebalance listener for this consumer.
  ///
  /// @return the rebalance listener for this consumer
  public ConsumerRebalanceListener getRebalanceListener() {
    return rebalanceListener;
  }

  /// Returns whether the consumer is running.
  ///
  /// @return `true` if the consumer is running, `false` if it has been closed or not
  ///     started
  public boolean isRunning() {
    final var s = state.get();
    return s == ConsumerState.RUNNING || s == ConsumerState.PAUSED;
  }

  /// Atomically transitions from any active state (RUNNING or PAUSED) to CLOSING.
  /// Uses a single-read CAS to avoid the double-CAS window.
  ///
  /// @return true if the transition succeeded, false if the state was not active
  private boolean transitionToClosing() {
    final var current = state.get();
    if (current != ConsumerState.RUNNING && current != ConsumerState.PAUSED) return false;
    return state.compareAndSet(current, ConsumerState.CLOSING);
  }

  /// Closes this consumer, stopping message consumption and processing.
  ///
  /// This method performs a graceful shutdown by:
  ///
  /// <ol>
  ///   <li>Setting the state to CLOSING to prevent new operations
  ///   <li>Creating a message tracker to monitor in-flight messages
  ///   <li>Signaling shutdown to stop accepting new messages
  ///   <li>Waiting for all in-flight messages to complete processing
  ///   <li>Waking up the consumer thread and waiting for its termination
  ///   <li>Shutting down the virtual thread executor
  ///   <li>Closing the offset manager to ensure final offsets are committed
  ///   <li>Setting the state to CLOSED
  /// </ol>
  ///
  /// This method is idempotent - calling it multiple times has no additional effect.
  @Override
  public void close() {
    if (!transitionToClosing()) return;

    final var tracker = (waitForMessagesTimeout.toMillis() > 0 && enableMetrics) ? createMessageTracker() : null;
    pause();
    commandQueue.offer(new ConsumerCommand.Close());

    if (tracker != null && tracker.getInFlightMessageCount() > 0) {
      LOGGER.log(Level.INFO, "Waiting for {0} in-flight messages to complete", tracker.getInFlightMessageCount());
      tracker.waitForCompletion(waitForMessagesTimeout.toMillis());
    }

    if (kafkaConsumer != null) {
      try {
        kafkaConsumer.wakeup();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error during wakeup", e);
      }
    }

    final var thread = consumerThread.get();
    if (thread != null) LockSupport.unpark(thread);

    if (thread != null && thread.isAlive()) {
      try {
        thread.join(threadTerminationTimeout.toMillis());
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    try {
      virtualThreadExecutor.shutdown();
      if (
        !virtualThreadExecutor.awaitTermination(executorTerminationTimeout.toMillis(), TimeUnit.MILLISECONDS)
      ) LOGGER.log(Level.WARNING, "{0} tasks not processed", virtualThreadExecutor.shutdownNow().size());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      virtualThreadExecutor.shutdownNow();
    }

    for (final var wrapper : batchWrappers.values()) {
      try {
        wrapper.close();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error draining batch buffer during shutdown", e);
      }
    }
    final var probe = circuitBreakerProbeFuture;
    if (probe != null) probe.cancel(false);
    if (scheduler != null) scheduler.shutdownNow();
    if (offsetManager != null) {
      try {
        offsetManager.close();
      } catch (Exception e) {
        LOGGER.log(Level.WARNING, "Error closing offset manager", e);
      }
    }
    if (kpipeProducer != null) kpipeProducer.close();
    state.set(ConsumerState.CLOSED);
  }

  /// Processes multiple Kafka records by submitting each one to the virtual thread executor.
  ///
  /// @param records the batch of records to process
  protected void processRecords(final ConsumerRecords<K, byte[]> records) {
    for (final var record : records) {
      if (offsetManager != null) commandQueue.offer(new ConsumerCommand.TrackOffset(record));
      inFlightCount.incrementAndGet();

      if (sequentialProcessing) {
        processRecord(record);
      } else {
        try {
          virtualThreadExecutor.submit(() -> processRecord(record));
        } catch (final RejectedExecutionException e) {
          inFlightCount.decrementAndGet();
          if (isRunning()) {
            LOGGER.log(Level.WARNING, "Task submission rejected during shutdown", e);
            if (enableMetrics) metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
            otelMetrics.recordProcessingError(record.topic());
            try {
              errorHandler.accept(new ProcessingError<>(record, e, 0));
            } catch (final Exception ex) {
              LOGGER.log(
                Level.ERROR,
                "Error handler threw while handling rejected task at offset {0}: {1}",
                record.offset(),
                ex.getMessage(),
                ex
              );
            }
          }
        }
      }
    }
  }

  /// Processes a single Kafka consumer record using the configured processor function.
  ///
  /// This method applies the processor function to transform the record value while handling
  /// exceptions with configurable retry logic. Processing occurs in the current virtual thread
  /// without blocking operations that would impact carrier thread performance.
  ///
  /// Metrics tracked during processing:
  ///
  /// <ul>
  ///   <li>messagesReceived - Incremented when a record is received
  ///   <li>messagesProcessed - Incremented for successful processing
  ///   <li>retries - Incremented for each retry attempt (not counting an initial attempt)
  ///   <li>processingErrors - Incremented when processing fails after all retries
  /// </ul>
  ///
  /// @param record The Kafka consumer record to process
  protected void processRecord(final ConsumerRecord<K, byte[]> record) {
    if (enableMetrics) metrics.get(METRIC_MESSAGES_RECEIVED).incrementAndGet();
    otelMetrics.recordMessageReceived(record.topic());

    // Open the span scope BEFORE tryProcessRecord so the entire processing path runs under the
    // span, including retries and DLQ sends. A misbehaving tracer must not crash the consumer —
    // fall back to a noop scope.
    Tracer.SpanScope span;
    try {
      span = tracer.startConsumerSpan(record);
    } catch (final Exception traceEx) {
      LOGGER.log(Level.WARNING, "Tracer.startConsumerSpan threw: {0}", traceEx.getMessage());
      span = Tracer.SpanScope.noop();
    }

    final long startTime = System.currentTimeMillis();
    try {
      final var result = tryProcessRecord(record, span);
      if (result) {
        final var durationMs = System.currentTimeMillis() - startTime;
        if (enableMetrics) {
          metrics.get(METRIC_MESSAGES_PROCESSED).incrementAndGet();
          metrics.get(METRIC_PROCESSING_DURATION_TOTAL_MS).addAndGet(durationMs);
        }
        otelMetrics.recordMessageProcessed(record.topic());
        otelMetrics.recordProcessingDuration(record.topic(), durationMs);
      }
    } finally {
      try {
        span.close();
      } catch (final Exception traceEx) {
        LOGGER.log(Level.WARNING, "Tracer.SpanScope.close threw: {0}", traceEx.getMessage());
      }
      inFlightCount.decrementAndGet();
      if (backpressurePaused.get()) {
        final var thread = consumerThread.get();
        if (thread != null) LockSupport.unpark(thread);
      }
    }
  }

  private boolean tryProcessRecord(final ConsumerRecord<K, byte[]> record, final Tracer.SpanScope span) {
    final var batchWrapper = batchWrappers.get(record.topic());
    if (batchWrapper != null) return tryEnqueueBatchRecord(record, batchWrapper, span);
    final var pipeline = pipelines.get(record.topic());
    if (pipeline == null) {
      // Partial-revocation race: subscribed topic was reassigned mid-poll. Mark processed so we
      // don't loop forever on a config error.
      LOGGER.log(
        Level.WARNING,
        "No pipeline registered for topic {0}; dropping record at offset {1}",
        record.topic(),
        record.offset()
      );
      markOffsetProcessed(record);
      return false;
    }
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      if (attempt > 0) if (!handleRetry(record, attempt)) return false;

      try {
        pipeline.processToSink(record.value());
        markOffsetProcessed(record);
        recordCircuitBreakerOutcome(true);
        return true;
      } catch (final Exception e) {
        if (isInterruptionRelated(e)) {
          Thread.currentThread().interrupt();
          return false;
        }
        if (attempt == maxRetries) {
          handleProcessingError(record, e, attempt, span);
          return false;
        }
      }
    }
    return false;
  }

  /// Returns the total number of records currently being tracked for backpressure: the dispatched
  /// in-flight count plus the sum of records buffered across all configured batch wrappers.
  /// Without the buffered piece, a slow batch sink under parallel mode would let the buffer grow
  /// unbounded — the consumer thread decrements `inFlightCount` the moment a record finishes
  /// `processRecord` (which for batch is "the record was buffered"), so the buffer would be
  /// invisible to the watermark check.
  private long totalInFlight() {
    var total = inFlightCount.get();
    for (final var wrapper : batchWrappers.values()) total += wrapper.bufferedCount();
    return total;
  }

  private <T> boolean tryEnqueueBatchRecord(
    final ConsumerRecord<K, byte[]> record,
    final BatchPipelineWrapper<K, T> wrapper,
    final Tracer.SpanScope span
  ) {
    try {
      final var value = wrapper.pipeline().processToValue(record.value());
      if (value == null) {
        // Intentional filter — mark processed immediately; nothing to buffer.
        markOffsetProcessed(record);
        return true;
      }
      wrapper.enqueue(record, value);
      // Buffered: messagesProcessed will be incremented in the flush callback when the batch is
      // committed to the user sink.
      return false;
    } catch (final Exception e) {
      if (isInterruptionRelated(e)) {
        Thread.currentThread().interrupt();
        return false;
      }
      handleProcessingError(record, e, 0, span);
      return false;
    }
  }

  private boolean handleRetry(final ConsumerRecord<K, byte[]> record, final int attempt) {
    if (enableMetrics) metrics.get(METRIC_RETRIES).incrementAndGet();
    LOGGER.log(Level.INFO, "Retrying message at offset {0} (attempt {1} of {2})", record.offset(), attempt, maxRetries);

    try {
      Thread.sleep(retryBackoff.toMillis());
      return true;
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private void markOffsetProcessed(final ConsumerRecord<K, byte[]> record) {
    if (offsetManager != null) commandQueue.offer(new ConsumerCommand.MarkOffsetProcessed(record));
  }

  private void handleProcessingError(
    final ConsumerRecord<K, byte[]> record,
    final Exception e,
    final int retryCount,
    final Tracer.SpanScope span
  ) {
    if (enableMetrics) metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
    otelMetrics.recordProcessingError(record.topic());
    // Mark the span as errored. Guarded — a misbehaving tracer must never crash the consumer
    // thread, leak in-flight counts, or skip offset marking.
    try {
      span.recordException(e);
    } catch (final Exception traceEx) {
      LOGGER.log(Level.WARNING, "Tracer.SpanScope.recordException threw: {0}", traceEx.getMessage());
    }
    LOGGER.log(
      Level.WARNING,
      "Failed to process message at offset {0} after {1} attempt(s): {2}",
      record.offset(),
      retryCount + 1,
      e.getMessage()
    );
    if (deadLetterTopic != null && kpipeProducer != null) {
      final var sent = kpipeProducer.sendToDlq(deadLetterTopic, record, record.topic(), e);
      if (sent && enableMetrics) metrics.get(METRIC_DLQ_SENT).incrementAndGet();
    }
    markOffsetProcessed(record);
    recordCircuitBreakerOutcome(false);
    try {
      errorHandler.accept(new ProcessingError<>(record, e, retryCount));
    } catch (final Exception ex) {
      LOGGER.log(
        Level.ERROR,
        "Error handler threw while handling failure at offset {0}: {1}",
        record.offset(),
        ex.getMessage(),
        ex
      );
    }
  }

  /// Feeds a per-record outcome into the circuit breaker state machine. No-op if no breaker is
  /// configured. Drives all three state transitions:
  ///
  ///   * `CLOSED + failure crossing threshold` → `OPEN` (CAS, pause, schedule probe)
  ///   * `HALF_OPEN + success` → `CLOSED` (CAS, reset stats — fresh window for the next cycle)
  ///   * `HALF_OPEN + failure` → `OPEN` (CAS, pause, restart probe timer)
  ///
  /// Outcomes that arrive while `OPEN` (in-flight records from before pause) are recorded into
  /// stats but cannot drive transitions — the timer is the only path out of `OPEN`.
  ///
  /// All state transitions follow §11's single-read CAS pattern: read state once, decide,
  /// `compareAndSet`. Lost races are no-ops — another thread already drove the transition.
  private void recordCircuitBreakerOutcome(final boolean success) {
    if (circuitBreakerController == null) return;
    if (success) circuitBreakerStats.recordSuccess();
    else circuitBreakerStats.recordFailure();

    final var current = circuitBreakerState.get();
    switch (current) {
      case CLOSED -> {
        if (!success && circuitBreakerController.shouldTrip(circuitBreakerStats)) {
          if (circuitBreakerState.compareAndSet(CircuitBreakerState.CLOSED, CircuitBreakerState.OPEN)) {
            tripBreaker();
          }
        }
      }
      case HALF_OPEN -> {
        if (success) {
          if (circuitBreakerState.compareAndSet(CircuitBreakerState.HALF_OPEN, CircuitBreakerState.CLOSED)) {
            closeBreaker();
          }
        } else {
          if (circuitBreakerState.compareAndSet(CircuitBreakerState.HALF_OPEN, CircuitBreakerState.OPEN)) {
            tripBreaker();
          }
        }
      }
      case OPEN -> {
        // Outcome from an in-flight record that finished after the breaker tripped. Stats already
        // updated above; only the timer can leave OPEN, so nothing else to do here.
      }
    }
  }

  /// Transitions the breaker into OPEN: pause the consumer, schedule a one-shot probe timer, log
  /// at WARNING. Called on CLOSED→OPEN and HALF_OPEN→OPEN transitions.
  private void tripBreaker() {
    circuitBreakerOpenedAtNanos.set(System.nanoTime());
    LOGGER.log(
      Level.WARNING,
      "Circuit breaker tripped: pausing consumer (failureRate={0}, openDuration={1})",
      circuitBreakerStats.failureRate(),
      circuitBreakerController.openDuration()
    );
    if (enableMetrics) metrics.computeIfAbsent(METRIC_CIRCUIT_BREAKER_TRIPS, _ -> new AtomicLong()).incrementAndGet();
    otelMetrics.recordCircuitBreakerTrip();
    otelMetrics.recordCircuitBreakerStateChange(CircuitBreakerState.OPEN.name());
    internalPause();
    final var existing = circuitBreakerProbeFuture;
    if (existing != null) existing.cancel(false);
    circuitBreakerProbeFuture = scheduler.schedule(
      this::tryHalfOpen,
      circuitBreakerController.openDuration().toMillis(),
      TimeUnit.MILLISECONDS
    );
  }

  /// One-shot timer callback. CAS the state OPEN → HALF_OPEN and resume the consumer; the next
  /// record's outcome will drive the final transition. Lost CAS = already moved on (e.g. a manual
  /// resume / shutdown), no-op.
  private void tryHalfOpen() {
    if (!circuitBreakerState.compareAndSet(CircuitBreakerState.OPEN, CircuitBreakerState.HALF_OPEN)) return;
    final var openedAt = circuitBreakerOpenedAtNanos.get();
    final var openMs = openedAt == 0L ? 0L : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - openedAt);
    LOGGER.log(Level.INFO, "Circuit breaker probing: open for {0} ms, resuming consumer", openMs);
    if (enableMetrics) metrics
      .computeIfAbsent(METRIC_CIRCUIT_BREAKER_TIME_OPEN_MS, _ -> new AtomicLong())
      .addAndGet(openMs);
    otelMetrics.recordCircuitBreakerTimeOpen(openMs);
    otelMetrics.recordCircuitBreakerStateChange(CircuitBreakerState.HALF_OPEN.name());
    internalResume();
  }

  /// Transitions the breaker back to CLOSED after a successful probe. Resets stats so the next
  /// trip cycle starts with a fresh window.
  private void closeBreaker() {
    LOGGER.log(Level.INFO, "Circuit breaker closed: probe succeeded, resuming normal operation");
    circuitBreakerStats.reset();
    circuitBreakerOpenedAtNanos.set(0L);
    otelMetrics.recordCircuitBreakerStateChange(CircuitBreakerState.CLOSED.name());
  }

  private void checkBackpressure() {
    switch (backpressureController.check(kafkaConsumer, isPaused())) {
      case PAUSE -> {
        final long value = backpressureController.getMetric(kafkaConsumer);
        LOGGER.log(
          Level.WARNING,
          "Backpressure triggered: pausing consumer ({0}={1})",
          backpressureController.getMetricName(),
          value
        );
        if (enableMetrics) metrics.get(METRIC_BACKPRESSURE_PAUSE_COUNT).incrementAndGet();
        otelMetrics.recordBackpressurePause();
        backpressurePauseStartNanos = System.nanoTime();
        backpressurePaused.set(true);
        internalPause();
      }
      case RESUME -> {
        backpressurePaused.set(false);
        final long duration = Math.max(
          1,
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - backpressurePauseStartNanos)
        );
        if (enableMetrics) metrics.get(METRIC_BACKPRESSURE_TIME_MS).addAndGet(duration);
        otelMetrics.recordBackpressureTime(duration);
        if (manualPause.get()) {
          LOGGER.log(
            Level.INFO,
            "Backpressure resolved (paused for {0} ms), but consumer remains manually paused",
            duration
          );
          break;
        }
        LOGGER.log(Level.INFO, "Backpressure resolved: resuming consumer (paused for {0} ms)", duration);
        internalResume();
      }
      case NONE -> {
      }
    }
  }

  private static boolean isInterruptionRelated(final Throwable error) {
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof InterruptedException || current instanceof ClosedByInterruptException) return true;
    }
    return false;
  }

  private ConsumerRecords<K, byte[]> pollRecords() {
    try {
      return kafkaConsumer.poll(pollTimeout);
    } catch (final WakeupException e) {
      // Expected during shutdown, no need to log
      return null;
    } catch (final InterruptException e) {
      // Propagate interruption
      Thread.currentThread().interrupt();
      return null;
    } catch (final Exception e) {
      // Only log if we're not shutting down
      if (isRunning()) LOGGER.log(Level.WARNING, "Error during Kafka poll operation", e);
      return null;
    }
  }
}
