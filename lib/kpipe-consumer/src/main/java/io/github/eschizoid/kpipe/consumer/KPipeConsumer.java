package io.github.eschizoid.kpipe.consumer;

import io.github.eschizoid.kpipe.consumer.config.AppConfig;
import io.github.eschizoid.kpipe.metrics.ConsumerMetrics;
import io.github.eschizoid.kpipe.metrics.KPipeMetricsReporter;
import io.github.eschizoid.kpipe.producer.KPipeProducer;
import io.github.eschizoid.kpipe.producer.tracing.Tracer;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.Result;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
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
/// final var registry = new MessageProcessorRegistry();
/// final var consoleSinkKey = RegistryKey.json("jsonConsole");
/// registry.registerSink(consoleSinkKey, new JsonConsoleSink<>());
///
/// final var pipeline = registry.pipeline(JsonFormat.INSTANCE)
///     .toSink(consoleSinkKey)
///     .build();
///
/// final var consumer = KPipeConsumer.<String>builder()
///     .withProperties(kafkaProps)
///     .withTopic("example-topic")
///     .withPipeline(pipeline)
///     .withRetry(3, Duration.ofSeconds(1))
///     .build();
///
/// consumer.start();
/// consumer.awaitShutdown();   // blocks until close() completes
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
  /// Guards [#releaseConstructedResources] so it runs exactly once no matter which path ends
  /// the consumer: external `close()`, the never-started fast path, or the consumer thread
  /// terminating on its own (uncaught exception / interruption). Without this, a self-
  /// terminating consumer thread would set state=CLOSED and a later `close()` would short-
  /// circuit, leaking the dispatcher / scheduler / offset manager / DLQ producer / batch
  /// wrappers for the JVM's lifetime.
  private final AtomicBoolean resourcesReleased = new AtomicBoolean(false);
  private final ProcessingMode processingMode;
  private final Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();
  private final ConsumerMetrics otelMetrics;
  private final Tracer tracer;
  /// Owns the per-mode dispatch strategy and the in-flight counter for non-sequential modes.
  /// Constructed once in the consumer constructor from [#processingMode]; closed in `close()`
  /// before `offsetManager.close()` per the §18 drain order.
  private final Dispatcher<K> dispatcher;
  /// Composes pause arbitration + backpressure decision + circuit-breaker state machine. The
  /// underlying decision modules ([BackpressureController], [CircuitBreakerController]) remain
  /// public + testable on their own; this controller owns the side-effect choreography and
  /// dispatches transitions through a Hook that points back at `internalPause` /
  /// `internalResume` and the metric counters.
  private final ConsumerHealthController health;

  private final String deadLetterTopic;
  private final KPipeProducer<K, byte[]> kpipeProducer;

  private final Map<String, BatchPipelineWrapper<K, ?>> batchWrappers;

  // ── Folded-in lifecycle host concerns (former KPipeRunner) ──
  /// Released by `close()` so callers blocked in [#awaitShutdown(Duration)] unblock when the
  /// consumer reaches CLOSED. Initialised once so the latch is stable across reconfiguration.
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  /// Periodic metrics-reporter thread. Started at the end of `start()` if any reporters are
  /// configured; interrupted at the start of `close()`.
  private volatile Thread metricsReporterThread;
  private final List<KPipeMetricsReporter> metricsReporters;
  private final Duration metricsReporterInterval;
  private final boolean useShutdownHook;
  /// JVM shutdown hook (when `useShutdownHook=true`); kept so `close()` can remove it explicitly.
  /// Without this, every constructed consumer accumulates a hook in the JVM's shutdown-hook set
  /// for the lifetime of the process — a real leak in hosts that build/close many consumers.
  private volatile Thread shutdownHookThread;
  /// Shared virtual-thread scheduler. Owns batch-pipeline age ticks AND the circuit-breaker
  /// probe timer. Lazily created when either feature is configured; otherwise stays null.
  private final ScheduledExecutorService scheduler;

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
    /// [#withBatchPipeline]. The [BatchSink] returns a [io.github.eschizoid.kpipe.sink.BatchResult]
    // naming
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
    private ProcessingMode processingMode = ProcessingMode.PARALLEL;
    private int keyOrderedMaxKeys = KeyOrderedDispatcher.DEFAULT_MAX_KEYS;
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
    private final List<KPipeMetricsReporter> metricsReporters = new ArrayList<>();
    private Duration metricsReporterInterval = Duration.ofMinutes(1);
    private boolean useShutdownHook = false;

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

    /// Selects the processing mode. See [ProcessingMode] for semantics.
    ///
    /// - `SEQUENTIAL`: one record at a time per partition, in offset order. Lag-based
    ///   backpressure.
    /// - `PARALLEL` (default): virtual thread per record, no ordering. In-flight-based
    ///   backpressure.
    /// - `KEY_ORDERED`: per-key serial processing with LRU cap (default 10,000 keys, see
    ///   [#withKeyOrderedMaxKeys]). In-flight-based backpressure.
    ///
    /// @param mode The processing mode (must be non-null)
    /// @return This builder instance for method chaining
    public Builder<K> withProcessingMode(final ProcessingMode mode) {
      this.processingMode = Objects.requireNonNull(mode, "ProcessingMode must not be null");
      return this;
    }

    /// LRU cap on distinct keys held in-memory simultaneously when using
    /// [ProcessingMode#KEY_ORDERED]. Below this many distinct in-flight keys, each key gets
    /// its own serial queue + virtual thread. Above it, least-recently-used keys with empty
    /// queues are evicted to make room.
    ///
    /// Default 10,000. Has no effect for `SEQUENTIAL` or `PARALLEL` modes.
    ///
    /// @param maxKeys LRU cap (must be positive)
    /// @return This builder instance for method chaining
    /// @throws IllegalArgumentException if `maxKeys` is non-positive
    public Builder<K> withKeyOrderedMaxKeys(final int maxKeys) {
      if (maxKeys <= 0) throw new IllegalArgumentException("maxKeys must be positive, got " + maxKeys);
      this.keyOrderedMaxKeys = maxKeys;
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
      return withBackpressure(
        BackpressureController.DEFAULT_HIGH_WATERMARK,
        BackpressureController.DEFAULT_LOW_WATERMARK
      );
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
    /// When only the topic is set, the consumer auto-builds a producer from the configured Kafka
    /// properties at construction time. To share an existing producer instead, prefer
    /// [#withDeadLetterQueue(String, KPipeProducer)] — it sets both atomically so the topic and
    /// producer cannot drift out of sync.
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
    /// Prefer [#withDeadLetterQueue(String, KPipeProducer)] when configuring a DLQ — it pairs the
    /// topic and the producer in a single call.
    ///
    /// @param producer The Kafka producer to use
    /// @return This builder instance for method chaining
    public Builder<K> withKafkaProducer(final Producer<K, byte[]> producer) {
      this.kpipeProducer = KPipeProducer.<K, byte[]>builder().withProducer(producer).build();
      return this;
    }

    /// Sets the KPipe producer wrapper to use for DLQ and Kafka sinks.
    ///
    /// Prefer [#withDeadLetterQueue(String, KPipeProducer)] when configuring a DLQ — it pairs the
    /// topic and the producer in a single call.
    ///
    /// @param producer The KPipe producer wrapper to use
    /// @return This builder instance for method chaining
    public Builder<K> withKafkaProducer(final KPipeProducer<K, byte[]> producer) {
      this.kpipeProducer = Objects.requireNonNull(producer, "Producer cannot be null");
      return this;
    }

    /// Configures the Dead Letter Queue by setting the DLQ topic and the producer that delivers
    /// to it atomically. Failed records are forwarded to `topic` via `producer` after retries are
    /// exhausted.
    ///
    /// Prefer this over calling [#withDeadLetterTopic] and [#withKafkaProducer] separately — the
    /// atomic form makes the intent explicit (the producer is for the DLQ, not a generic
    /// Kafka sink) and prevents the two settings from drifting out of sync.
    ///
    /// @param topic The name of the DLQ topic (non-null)
    /// @param producer The KPipe producer wrapper used for DLQ sends (non-null)
    /// @return This builder instance for method chaining
    public Builder<K> withDeadLetterQueue(final String topic, final KPipeProducer<K, byte[]> producer) {
      this.deadLetterTopic = Objects.requireNonNull(topic, "DLQ topic cannot be null");
      this.kpipeProducer = Objects.requireNonNull(producer, "DLQ producer cannot be null");
      return this;
    }

    /// Sets the OpenTelemetry metrics instruments for this consumer.
    ///
    /// Use [io.github.eschizoid.kpipe.metrics.ConsumerMetrics] to create an instrumented instance,
    // or
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

    /// Adds metrics reporters that run periodically while the consumer is alive. Each reporter is
    /// invoked every `metricsReporterInterval` (defaults to 60s) on a dedicated platform daemon
    /// thread. Reporter exceptions are logged at WARNING but do not crash the thread.
    ///
    /// @param reporters the collection of reporters to run
    /// @return this builder instance for method chaining
    public Builder<K> withMetricsReporters(final Collection<KPipeMetricsReporter> reporters) {
      this.metricsReporters.addAll(Objects.requireNonNull(reporters, "reporters cannot be null"));
      return this;
    }

    /// Sets the interval between metrics-reporter invocations.
    ///
    /// @param interval the reporting interval (must be positive)
    /// @return this builder instance for method chaining
    public Builder<K> withMetricsInterval(final Duration interval) {
      Objects.requireNonNull(interval, "interval cannot be null");
      if (interval.isNegative() || interval.isZero()) {
        throw new IllegalArgumentException("interval must be positive, got " + interval);
      }
      this.metricsReporterInterval = interval;
      return this;
    }

    /// Configures whether to register a JVM shutdown hook that calls `close()` on this consumer.
    /// Use when the host process has no other shutdown coordination.
    ///
    /// @param useShutdownHook true to register a JVM shutdown hook
    /// @return this builder instance for method chaining
    public Builder<K> withShutdownHook(final boolean useShutdownHook) {
      this.useShutdownHook = useShutdownHook;
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
      if (backpressureController != null && processingMode == ProcessingMode.SEQUENTIAL) {
        LOGGER.log(
          System.Logger.Level.INFO,
          "Sequential processing enabled with backpressure: switching to lag-based monitoring."
        );
      }
      if (keyOrderedMaxKeys != KeyOrderedDispatcher.DEFAULT_MAX_KEYS && processingMode != ProcessingMode.KEY_ORDERED) {
        LOGGER.log(
          System.Logger.Level.WARNING,
          "withKeyOrderedMaxKeys({0}) was set but processing mode is {1} — the setting will be ignored. " +
            "Call withProcessingMode(ProcessingMode.KEY_ORDERED) to use it.",
          keyOrderedMaxKeys,
          processingMode
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
    // `build()` sets `topics` to the UNION of regular + batch routes for Kafka subscription, so
    // the homogeneous branch must filter out batch-route topics to avoid replicating the regular
    // pipeline onto a topic that's owned by a batch wrapper.
    if (builder.pipelinesPerTopic != null) {
      this.pipelines = builder.pipelinesPerTopic;
    } else if (builder.pipeline != null) {
      final var map = new LinkedHashMap<String, MessagePipeline<?>>(builder.topics.size());
      for (final var t : builder.topics) if (!builder.batchSpecs.containsKey(t)) map.put(t, builder.pipeline);
      this.pipelines = Map.copyOf(map);
    } else {
      this.pipelines = Map.of();
    }
    this.pollTimeout = Objects.requireNonNull(builder.pollTimeout);
    this.errorHandler = builder.errorHandler;
    this.maxRetries = builder.maxRetries;
    this.retryBackoff = builder.retryBackoff;
    this.processingMode = builder.processingMode;
    this.waitForMessagesTimeout = builder.waitForMessagesTimeout;
    this.threadTerminationTimeout = builder.threadTerminationTimeout;
    this.executorTerminationTimeout = builder.executorTerminationTimeout;
    this.dispatcher = switch (this.processingMode) {
      case SEQUENTIAL -> new SequentialDispatcher<>();
      case PARALLEL -> new ParallelDispatcher<>(this::handleParallelRejection, this.executorTerminationTimeout);
      case KEY_ORDERED -> new KeyOrderedDispatcher<>(builder.keyOrderedMaxKeys);
    };
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

    final var bp = (
      builder.backpressureController != null
        ? builder.backpressureController
        : new BackpressureController(
            BackpressureController.DEFAULT_HIGH_WATERMARK,
            BackpressureController.DEFAULT_LOW_WATERMARK,
            null
          )
    ).withStrategy(
      this.processingMode == ProcessingMode.SEQUENTIAL
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

    this.metricsReporters = List.copyOf(builder.metricsReporters);
    this.metricsReporterInterval = builder.metricsReporterInterval;
    this.useShutdownHook = builder.useShutdownHook;
    if (this.useShutdownHook) {
      this.shutdownHookThread = new Thread(this::close, "kpipe-shutdown-hook");
      Runtime.getRuntime().addShutdownHook(this.shutdownHookThread);
    }

    if (builder.backpressureController == null) {
      LOGGER.log(
        Level.INFO,
        "No backpressure configured, using default {0} strategy (high={1}, low={2})",
        bp.getMetricName(),
        bp.highWatermark(),
        bp.lowWatermark()
      );
    }

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

    this.otelMetrics = builder.consumerMetrics != null ? builder.consumerMetrics : ConsumerMetrics.noop();

    this.health = new ConsumerHealthController(
      bp,
      builder.circuitBreakerController,
      this.scheduler,
      new ConsumerHealthController.Hook() {
        @Override
        public void onPause() {
          internalPause();
        }

        @Override
        public void onResume() {
          internalResume();
        }

        @Override
        public void onBackpressurePause() {
          metrics.get(METRIC_BACKPRESSURE_PAUSE_COUNT).incrementAndGet();
          otelMetrics.recordBackpressurePause();
        }

        @Override
        public void onBackpressureTimeMs(final long ms) {
          metrics.get(METRIC_BACKPRESSURE_TIME_MS).addAndGet(ms);
          otelMetrics.recordBackpressureTime(ms);
        }

        @Override
        public void onCircuitBreakerTrip() {
          metrics.get(METRIC_CIRCUIT_BREAKER_TRIPS).incrementAndGet();
          otelMetrics.recordCircuitBreakerTrip();
        }

        @Override
        public void onCircuitBreakerStateChange(final CircuitBreakerState state) {
          otelMetrics.recordCircuitBreakerStateChange(state);
        }

        @Override
        public void onCircuitBreakerTimeOpenMs(final long ms) {
          metrics.get(METRIC_CIRCUIT_BREAKER_TIME_OPEN_MS).addAndGet(ms);
          otelMetrics.recordCircuitBreakerTimeOpen(ms);
        }
      }
    );
  }

  private <T> BatchPipelineWrapper<K, T> createBatchWrapper(final Builder.BatchSpec<T> spec) {
    // Batch flushes call the OffsetManager directly rather than going through commandQueue. The
    // queue exists to serialize Kafka-consumer calls (pause/resume/commitSync) on the consumer
    // thread; OffsetManager.markOffsetProcessed is already thread-safe and avoiding the queue
    // means the shutdown drain works even after the consumer thread has exited.
    final var callbacks = new BatchPipelineWrapper.BatchCallbacks<K>() {
      @Override
      public void markProcessed(final ConsumerRecord<K, byte[]> record) {
        metrics.get(METRIC_MESSAGES_PROCESSED).incrementAndGet();
        otelMetrics.recordMessageProcessed(record.topic());
        if (offsetManager != null) offsetManager.markOffsetProcessed(record);
      }

      @Override
      public void onBatchFailure(final ConsumerRecord<K, byte[]> record, final Exception cause) {
        metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
        otelMetrics.recordProcessingError(record.topic());
        LOGGER.log(Level.WARNING, "Batch failure for record at offset {0}: {1}", record.offset(), cause.getMessage());
        if (deadLetterTopic != null && kpipeProducer != null) {
          final var sent = kpipeProducer.sendToDlq(deadLetterTopic, record, record.topic(), cause);
          if (sent) metrics.get(METRIC_DLQ_SENT).incrementAndGet();
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

  /// Blocks until the dispatcher's in-flight records finish processing, or `timeout` elapses, or
  /// the calling thread is interrupted. Replaces the former standalone
  /// `MessageTracker.waitForCompletion(...)`.
  ///
  /// Waits on `dispatcher.pendingCount()` — records a worker is actively processing — NOT
  /// `totalInFlight()`. The latter also counts buffered batch records, which never flush during a
  /// drain (only on a size/age trigger or `BatchPipelineWrapper.close()` at teardown), so waiting
  /// on them would always burn the full `timeout`. Buffered batches are flushed + committed by the
  /// teardown that follows, so a `true` return here means "active processing is done, safe to tear
  /// down."
  ///
  /// @param timeout maximum time to wait
  /// @return `true` if active processing drained in time, else `false`
  public boolean waitForInFlightDrain(final Duration timeout) {
    if (dispatcher.pendingCount() == 0) return true;
    final var deadline = System.nanoTime() + timeout.toNanos();
    final var pollNanos = Math.min(timeout.toNanos() / 10, Duration.ofMillis(500).toNanos());
    final var pollMs = Math.max(1, pollNanos / 1_000_000);
    try {
      while (System.nanoTime() < deadline) {
        if (dispatcher.pendingCount() == 0) return true;
        Thread.sleep(pollMs);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
    return dispatcher.pendingCount() == 0;
  }

  /// Runs on the consumer thread, at the top of its `finally`, before any teardown. The poll
  /// loop exits the moment `state` becomes CLOSING, so without this the finally would tear the
  /// dispatcher + offset manager down while in-flight workers are still finishing. We keep
  /// pumping the command queue (so any pending `CommitOffsets` triggered by the scheduler land on
  /// the still-open Kafka consumer) and wait for the dispatcher's pending work to settle, bounded
  /// by `waitForMessagesTimeout`. Finishing workers mark their offsets directly on the
  /// thread-safe `OffsetManager`, so those don't need the command queue. A final
  /// `processCommands()` flushes anything the last workers enqueued. Only the consumer thread may
  /// touch the Kafka consumer, so this must run here, not on the close() thread.
  ///
  /// We wait on `dispatcher.pendingCount()`, NOT `totalInFlight()`: the latter also counts
  /// buffered batch records, which don't enqueue commands and won't flush on their own (size-only
  /// / long-maxAge policies). They're flushed by each `BatchPipelineWrapper.close()` in
  /// `releaseConstructedResources()` right after this returns — so waiting on them here would just
  /// burn the whole timeout while the dispatcher is already idle.
  private void drainInFlightBeforeTeardown() {
    if (waitForMessagesTimeout.toMillis() > 0) {
      final var deadline = System.nanoTime() + waitForMessagesTimeout.toNanos();
      while (dispatcher.pendingCount() > 0 && System.nanoTime() < deadline) {
        processCommands();
        LockSupport.parkNanos(Duration.ofMillis(5).toNanos());
      }
    }
    processCommands();
  }

  /// Blocks indefinitely until [#close()] returns. Use [#awaitShutdown(Duration)] for a bounded
  /// wait that returns a `boolean` instead of throwing.
  ///
  /// @throws InterruptedException if the calling thread is interrupted while waiting; the
  ///     interrupt flag is restored before throwing
  public void awaitShutdown() throws InterruptedException {
    try {
      shutdownLatch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    }
  }

  /// Blocks up to `timeout` for [#close()] to finish.
  ///
  /// @param timeout maximum time to wait
  /// @return `true` if shutdown completed within `timeout`, `false` on timeout or interruption
  public boolean awaitShutdown(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    try {
      return shutdownLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /// Initiates a graceful shutdown overriding the builder's `waitForMessagesTimeout`. Use when the
  /// host wants to choose the drain budget at shutdown time (e.g. a signal handler that knows how
  /// much time it has left). Returns whether the in-flight drain completed within `timeout`.
  ///
  /// @param timeout maximum time to wait for in-flight records to drain
  /// @return `true` if the drain finished cleanly, `false` if records remained when shutdown
  ///     forced through
  public boolean shutdownGracefully(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    // Snapshot state once. CLOSED → nothing to drain, already done.
    // CLOSING → another caller is mid-shutdown; await their completion and report the drain
    // they observed rather than calling pause()/close() on an already-shutting consumer.
    final var snapshot = state.get();
    if (snapshot == ConsumerState.CLOSED) return true;
    if (snapshot == ConsumerState.CLOSING) return awaitShutdown(timeout) && totalInFlight() == 0;
    pause();
    final var drained = waitForInFlightDrain(timeout);
    close();
    return drained;
  }

  /// Starts the metrics-reporter thread if any reporters were configured on the builder. Called
  /// from `start()`; no-op when the list is empty. The thread is a platform daemon so it cannot
  /// keep the JVM alive past the consumer.
  private void startMetricsReporterThread() {
    if (metricsReporters.isEmpty() || metricsReporterInterval.toMillis() <= 0) return;
    metricsReporterThread = Thread.ofPlatform()
      .name("kpipe-metrics-reporter")
      .daemon(true)
      .start(() -> {
        final var current = Thread.currentThread();
        while (state.get() != ConsumerState.CLOSED && !current.isInterrupted()) {
          for (final var reporter : metricsReporters) {
            try {
              reporter.reportMetrics();
            } catch (final Exception e) {
              LOGGER.log(Level.WARNING, "Metrics reporter threw", e);
            }
          }
          try {
            Thread.sleep(metricsReporterInterval.toMillis());
          } catch (final InterruptedException e) {
            current.interrupt();
            break;
          }
        }
      });
  }

  /// Interrupts and joins the metrics-reporter thread if it's running. Called from `close()`.
  private void stopMetricsReporterThread() {
    final var t = metricsReporterThread;
    if (t == null) return;
    t.interrupt();
    try {
      t.join(1_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      metricsReporterThread = null;
    }
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
            health.tickBackpressure(kafkaConsumer);
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
          // Drain in-flight work + commands before teardown (see method docs).
          drainInFlightBeforeTeardown();
          try {
            // Release ctor resources before closing the consumer (see method docs).
            releaseConstructedResources();
          } finally {
            try {
              kafkaConsumer.close();
              LOGGER.log(Level.INFO, "Consumer closed for topics {0}", topics);
            } catch (final Exception e) {
              LOGGER.log(Level.WARNING, "Error closing Kafka consumer", e);
            } finally {
              state.set(ConsumerState.CLOSED);
              shutdownLatch.countDown();
            }
          }
        }
      });

    consumerThread.set(thread);
    startMetricsReporterThread();
    LOGGER.log(Level.INFO, "Consumer started for topics {0}", topics);
  }

  /// Pauses consumption from the topic. Any in-flight messages will continue processing, but no new
  /// messages will be consumed until {@link #resume()} is called.
  ///
  /// This method is idempotent - calling it multiple times has no additional effect.
  public void pause() {
    if (health.requestPause(ConsumerHealthController.Source.MANUAL)) internalPause();
  }

  private void internalPause() {
    final var current = state.get();
    if (current != ConsumerState.RUNNING && current != ConsumerState.CREATED) return;
    if (state.compareAndSet(current, ConsumerState.PAUSED)) {
      commandQueue.offer(new ConsumerCommand.Pause());
    }
  }

  /// Drains the command queue on the consumer thread. Commands are the only mechanism by which
  /// off-thread callers can ask Kafka APIs (`pause` / `resume` / `commitSync`) or the
  /// [OffsetManager] to mutate state — serializing them here keeps those calls single-threaded
  /// without locks.
  ///
  /// Recognized commands:
  ///
  /// * `Pause` — `kafkaConsumer.pause(assignment)`
  /// * `Resume` — `kafkaConsumer.resume(assignment)`
  /// * `Close` — flips the consumer to `CLOSING`
  /// * `CommitOffsets` — `kafkaConsumer.commitSync(offsets)` plus offset-manager notification
  ///
  /// Offset tracking + per-record marking do NOT go through this queue — they call `OffsetManager`
  /// directly from whichever thread finishes processing (the manager is already thread-safe).
  ///
  /// Commands are processed in submission order. Per-command exceptions are logged at ERROR and
  /// swallowed so a malformed command cannot halt subsequent ones.
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
    if (health.releasePause(ConsumerHealthController.Source.MANUAL)) internalResume();
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
  /// Available metrics:
  ///
  /// * `messagesReceived` — records received from Kafka
  /// * `messagesProcessed` — records successfully processed
  /// * `processingErrors` — records that failed processing after all retries
  /// * `processingDurationTotalMs` — total wall-clock time spent inside `processToSink`
  /// * `retries` — retry attempts made for failed records
  /// * `backpressurePauseCount` / `backpressureTimeMs` — backpressure pause count and total ms held
  /// * `circuitBreakerTrips` / `circuitBreakerTimeOpenMs` — CB trip count and total ms in OPEN
  /// * `dlqSent` — records sent to the configured DLQ topic
  /// * `inFlight` — current number of messages held by the consumer (live counter, not a counter
  /// delta).
  ///   Sums the dispatcher's pending count (records currently being processed or queued per-key)
  ///   AND any records buffered inside batch-sink wrappers awaiting size/age flush.
  ///
  /// @return an unmodifiable map of metric names to their current values
  public Map<String, Long> getMetrics() {
    final var snapshot = metrics
      .entrySet()
      .stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(), (a, _) -> a, HashMap::new));
    snapshot.put("inFlight", totalInFlight());
    return Collections.unmodifiableMap(snapshot);
  }

  /// Snapshot of the top `n` keys by current queue depth in the underlying dispatcher,
  /// ordered deepest-first. Returns an empty list for SEQUENTIAL and PARALLEL modes (no
  /// per-key structure); only [ProcessingMode#KEY_ORDERED] populates this. Intended for
  /// ad-hoc diagnostics — heap-dump replacement, REPL inspection, JMX panels — not for
  /// continuous metric emission, since per-key cardinality is unbounded.
  ///
  /// @param n maximum number of entries to return (must be positive)
  /// @return ordered list of `(key, queueDepth)` entries; never null, may be empty
  public List<Map.Entry<Object, Integer>> topKeyQueueDepths(final int n) {
    return dispatcher.topKeyQueueDepths(n);
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
  /// Three terminal paths converge on the same final state (`CLOSED`, latch released, all
  /// resources freed):
  ///
  /// 1. **Never-started fast path.** CREATED → CLOSED via [#closeNeverStarted]: release every
  ///    constructor-created resource, close the Kafka consumer directly, count down the latch.
  /// 2. **Normal shutdown.** Transition to CLOSING, then [#initiateShutdown] (stop reporter,
  ///    queue Close, signal dispatcher, drain in-flight), then [#waitForConsumerThreadToJoin]
  ///    (wakeup + unpark + join). If the thread joined cleanly, [#finalizeAfterThreadJoined]
  ///    runs the (idempotent) release + state=CLOSED + countDown as a safety net — the consumer
  ///    thread's own finally has already done the work in the common case.
  /// 3. **Join-timeout escape.** The consumer thread is still alive after
  ///    `threadTerminationTimeout` (e.g. SEQUENTIAL mode stuck in a hung pipeline). Interrupt
  ///    and return — tearing down resources here would race a live thread that still holds the
  ///    offset manager / producer. The thread's own finally handles the final transition when
  ///    it eventually exits.
  ///
  /// The consumer thread's own finally (in [#start]) mirrors path 2's teardown when it
  /// self-terminates (uncaught throw, internal error). All terminal paths CAS-guard
  /// [#releaseConstructedResources] so resources are freed exactly once regardless of which
  /// path runs first.
  ///
  /// Idempotent — calling it multiple times has no additional effect.
  @Override
  public void close() {
    if (state.compareAndSet(ConsumerState.CREATED, ConsumerState.CLOSED)) {
      closeNeverStarted();
      return;
    }
    if (!transitionToClosing()) return;

    initiateShutdown();
    if (!waitForConsumerThreadToJoin()) return;
    finalizeAfterThreadJoined();
  }

  /// Path 1 of [#close]: the consumer was built but never started, so the consumer thread that
  /// normally closes `kafkaConsumer` in its finally never ran. Release every constructor-created
  /// resource, close the Kafka consumer directly, and unblock [#awaitShutdown] callers.
  private void closeNeverStarted() {
    releaseConstructedResources();
    closeKafkaConsumerQuietly();
    shutdownLatch.countDown();
  }

  /// Stop the metrics reporter, ask the consumer loop to halt, and let any in-flight records
  /// drain before we start waking and joining the consumer thread.
  ///
  /// No pause command needed: CLOSING already halts the poll loop ([#isRunning] is false), and
  /// the subsequent `wakeup`/`unpark` in [#waitForConsumerThreadToJoin] breaks any in-progress
  /// poll. The Close command exists for logging + symmetry; the actual halt signal is the state
  /// transition done by [#transitionToClosing]. The dispatcher signal is a no-op for
  /// SEQUENTIAL/PARALLEL; KEY_ORDERED uses it to break out of its saturation yield-loop so the
  /// consumer thread can drop its pending count and let `close()` proceed promptly instead of
  /// spinning past `threadTerminationTimeout`.
  private void initiateShutdown() {
    stopMetricsReporterThread();
    commandQueue.offer(new ConsumerCommand.Close());
    dispatcher.signalShutdown();
    if (waitForMessagesTimeout.toMillis() > 0 && dispatcher.pendingCount() > 0) {
      LOGGER.log(Level.INFO, "Waiting for {0} in-flight messages to complete", dispatcher.pendingCount());
      waitForInFlightDrain(waitForMessagesTimeout);
    }
  }

  /// Wakes the consumer thread and joins it within `threadTerminationTimeout`.
  ///
  /// @return `true` if the thread terminated (or was never registered, which can happen if
  ///     `close()` races with the tail of `start()` before `consumerThread.set(...)`); `false`
  ///     if the join timed out and the thread is still alive — caller must NOT tear down
  ///     resources in that case (use-after-close hazard).
  private boolean waitForConsumerThreadToJoin() {
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

    if (thread != null && thread.isAlive()) {
      LOGGER.log(
        Level.WARNING,
        "Consumer thread did not terminate within {0}ms; interrupting and leaving teardown to its finally block",
        threadTerminationTimeout.toMillis()
      );
      thread.interrupt();
      return false;
    }
    return true;
  }

  /// Idempotent finalizer for path 2 of [#close]. On the normal path the consumer thread's own
  /// finally has already run [#releaseConstructedResources] (which closes `kafkaConsumer` and
  /// removes the shutdown hook) + state=CLOSED + countDown; this is the safety net for the
  /// `consumerThread.get() == null` race (close() called between the VT spawn and the
  /// `consumerThread.set(...)` line in `start()`), where the VT may exit before either side has
  /// finalized. CAS guards inside `releaseConstructedResources` and on the latch keep the
  /// double-invoke safe.
  private void finalizeAfterThreadJoined() {
    releaseConstructedResources();
    state.set(ConsumerState.CLOSED);
    shutdownLatch.countDown();
  }

  /// Closes every resource the constructor created EXCEPT `kafkaConsumer`. Invoked from three
  /// paths, CAS-guarded so it runs exactly once: (1) the normal `close()` path after the
  /// consumer thread joins; (2) the never-started fast path; (3) the consumer thread's own
  /// finally when it self-terminates (uncaught throwable / interruption) without an external
  /// `close()` — in that case it sets state=CLOSED, which would make a later `close()` a no-op,
  /// so this is the only chance to release these resources.
  ///
  /// `kafkaConsumer` is excluded: on paths (1) and (3) the consumer thread's finally closes it,
  /// and on path (2) the fast path closes it via [#closeKafkaConsumerQuietly]. Closing a
  /// KafkaConsumer from two threads would throw ConcurrentModificationException. Order matters —
  /// dispatcher → batch wrappers → offset manager → producer — so batch buffers flush (marking
  /// their offsets) while the offset manager is still alive. Critically, all three callers run
  /// this BEFORE closing `kafkaConsumer`: `offsetManager.close()` does a final commitSync on the
  /// consumer, which would throw (and silently drop the last offsets) on an already-closed one.
  private void releaseConstructedResources() {
    if (!resourcesReleased.compareAndSet(false, true)) return; // already released by another path
    // Every step is best-effort: the CAS above already fired, so a throw here would leak the
    // remaining resources (no path retries). Catch + log each so teardown always runs to the end.
    try {
      dispatcher.close();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error closing dispatcher during shutdown", e);
    }
    for (final var wrapper : batchWrappers.values()) {
      try {
        wrapper.close();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error draining batch buffer during shutdown", e);
      }
    }
    try {
      health.shutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error shutting down health server during shutdown", e);
    }
    if (scheduler != null) scheduler.shutdownNow();
    if (offsetManager != null) {
      try {
        offsetManager.close();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error closing offset manager", e);
      }
    }
    if (kpipeProducer != null) {
      try {
        kpipeProducer.close();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error closing producer during shutdown", e);
      }
    }
    // Deregister the JVM shutdown hook last. It's registered in the ctor (when
    // useShutdownHook=true), so it exists even for a never-started consumer and a
    // self-terminating consumer thread. Doing it here — inside the CAS-guarded release — means
    // every terminal path removes it; otherwise a self-terminated thread (which sets
    // state=CLOSED, making a later close() a no-op at transitionToClosing) would leave the
    // hook registered for the JVM's lifetime, pinning this consumer in memory.
    tryRemoveShutdownHook();
  }

  /// Closes `kafkaConsumer`, swallowing+logging any error. Used only on the never-started fast
  /// path, where the consumer thread (the normal owner of this close) never ran.
  private void closeKafkaConsumerQuietly() {
    if (kafkaConsumer == null) return;
    try {
      kafkaConsumer.close();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error closing Kafka consumer", e);
    }
  }

  /// Removes the JVM shutdown hook installed in the constructor when `useShutdownHook=true`.
  /// No-op when no hook was installed or when the JVM is already shutting down (in which case
  /// `removeShutdownHook` would throw `IllegalStateException` — caught and ignored, the hook is
  /// running anyway).
  private void tryRemoveShutdownHook() {
    final var hook = shutdownHookThread;
    if (hook == null) return;
    try {
      Runtime.getRuntime().removeShutdownHook(hook);
    } catch (final IllegalStateException e) {
      // JVM already shutting down — the hook is currently executing, which is exactly the path
      // that called us. Leave it alone.
    } finally {
      shutdownHookThread = null;
    }
  }

  /// Processes a batch of Kafka records by dispatching each to the configured [Dispatcher].
  /// The dispatcher chooses the per-record execution thread according to its [ProcessingMode]:
  ///
  /// - `SequentialDispatcher` runs each record inline on the consumer thread.
  /// - `ParallelDispatcher` submits each record to its virtual-thread executor.
  /// - `KeyOrderedDispatcher` enqueues records onto a per-key serial queue + virtual thread.
  ///
  /// The dispatcher also owns the in-flight counter (for non-sequential modes) and feeds
  /// [#totalInFlight] via `dispatcher.pendingCount()`.
  ///
  /// @param records the batch of records to process
  protected void processRecords(final ConsumerRecords<K, byte[]> records) {
    for (final var record : records) {
      if (offsetManager != null) offsetManager.trackOffset(record);
      dispatcher.dispatch(record, () -> processRecord(record), this::afterRecordComplete);
    }
  }

  /// Surfaces a rejection from `ParallelDispatcher`'s executor (typically during shutdown)
  /// back to the consumer's error path. Mirrors the prior inline behavior in `processRecords`.
  private void handleParallelRejection(final ConsumerRecord<K, byte[]> record, final RejectedExecutionException e) {
    if (!isRunning()) return;
    LOGGER.log(Level.WARNING, "Task submission rejected during shutdown", e);
    metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
    otelMetrics.recordProcessingError(record.topic());
    try {
      errorHandler.accept(new ProcessingError<>(record, e, 0));
    } catch (final Exception ex) {
      LOGGER.log(
        Level.ERROR,
        "Error handler threw while handling rejected task at offset %d: %s".formatted(record.offset(), ex.getMessage()),
        ex
      );
    }
  }

  /// Runs after every record finishes (on whichever thread executed it). Unparks the consumer
  /// thread when backpressure is currently holding it — a record completion may have dropped
  /// the in-flight count below the low watermark, so the consumer thread needs to re-evaluate
  /// on its next iteration. Previously inlined into `processRecord`'s finally block; moved
  /// here so the dispatcher owns the post-record callback uniformly across modes.
  private void afterRecordComplete() {
    if (health.isHeldBy(ConsumerHealthController.Source.BACKPRESSURE)) {
      final var thread = consumerThread.get();
      if (thread != null) LockSupport.unpark(thread);
    }
  }

  /// Processes a single Kafka consumer record using the topic's configured pipeline. Runs in the
  /// current virtual thread; retries on exception according to `maxRetries` + `retryBackoff`. On
  /// success the per-record outcome feeds the circuit-breaker window; on retry-exhausted failure
  /// the record is routed to the DLQ (when configured) and the error handler is invoked.
  ///
  /// Metrics tracked during processing:
  ///
  /// * `messagesReceived` — incremented on entry
  /// * `messagesProcessed` — incremented on success
  /// * `processingDurationTotalMs` — incremented on success by the wall-clock duration
  /// * `retries` — incremented per retry attempt (not the initial attempt)
  /// * `processingErrors` — incremented when processing fails after all retries
  ///
  /// @param record the Kafka consumer record to process
  protected void processRecord(final ConsumerRecord<K, byte[]> record) {
    metrics.get(METRIC_MESSAGES_RECEIVED).incrementAndGet();
    otelMetrics.recordMessageReceived(record.topic());

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
        metrics.get(METRIC_MESSAGES_PROCESSED).incrementAndGet();
        metrics.get(METRIC_PROCESSING_DURATION_TOTAL_MS).addAndGet(durationMs);
        otelMetrics.recordMessageProcessed(record.topic());
        otelMetrics.recordProcessingDuration(record.topic(), durationMs);
      }
    } finally {
      try {
        span.close();
      } catch (final Exception traceEx) {
        LOGGER.log(Level.WARNING, "Tracer.SpanScope.close threw: {0}", traceEx.getMessage());
      }
      // In-flight count + backpressure-unpark handled by the dispatcher's `onComplete`
      // callback (`afterRecordComplete`). See `processRecords`.
    }
  }

  private boolean tryProcessRecord(final ConsumerRecord<K, byte[]> record, final Tracer.SpanScope span) {
    final var batchWrapper = batchWrappers.get(record.topic());
    if (batchWrapper != null) return tryEnqueueBatchRecord(record, batchWrapper, span);
    final var pipeline = pipelines.get(record.topic());
    if (pipeline == null) {
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
      if (attempt > 0) {
        metrics.get(METRIC_RETRIES).incrementAndGet();
        LOGGER.log(
          Level.INFO,
          "Retrying message at offset {0} (attempt {1} of {2})",
          record.offset(),
          attempt,
          maxRetries
        );
        try {
          Thread.sleep(retryBackoff.toMillis());
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
      }

      try {
        driveSinkedPipeline(pipeline, record.value());
        markOffsetProcessed(record);
        health.recordOutcome(true);
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

  /// Returns the total number of records currently being tracked for backpressure: whatever
  /// the per-mode dispatcher reports as pending plus the sum of records buffered across all
  /// configured batch wrappers. Without the buffered piece, a slow batch sink would let the
  /// buffer grow unbounded — the dispatcher decrements its counter the moment a record
  /// finishes `processRecord` (which for batch is "the record was buffered"), so the buffer
  /// would otherwise be invisible to the watermark check.
  private long totalInFlight() {
    var total = dispatcher.pendingCount();
    for (final var wrapper : batchWrappers.values()) total += wrapper.bufferedCount();
    return total;
  }

  private <T> boolean tryEnqueueBatchRecord(
    final ConsumerRecord<K, byte[]> record,
    final BatchPipelineWrapper<K, T> wrapper,
    final Tracer.SpanScope span
  ) {
    try {
      final var pipeline = wrapper.pipeline();
      final var deserialized = pipeline.deserializeOrFail(record.value());
      switch (pipeline.process(deserialized)) {
        case Result.Passed<T> p -> {
          wrapper.enqueue(record, p.value());
          // Buffered: messagesProcessed will be incremented in the flush callback when the batch
          // is committed to the user sink.
          return false;
        }
        case Result.Filtered<T> __ -> {
          // Intentional filter — mark processed immediately; nothing to buffer.
          markOffsetProcessed(record);
          return true;
        }
        case Result.Failed<T> f -> throw rethrowResultCause(f.cause());
      }
    } catch (final Exception e) {
      if (isInterruptionRelated(e)) {
        Thread.currentThread().interrupt();
        return false;
      }
      handleProcessingError(record, e, 0, span);
      return false;
    }
  }

  /// Drive a pipeline (with erased element type) from raw bytes to its terminal sink. Throws if
  /// the pipeline reports `Failed` so the calling retry/error path handles it the same way it
  /// always did. Returns normally on `Passed` (after the sink runs) and on `Filtered`.
  private static <T> void driveSinkedPipeline(final MessagePipeline<T> pipeline, final byte[] data) {
    final var deserialized = pipeline.deserializeOrFail(data);
    switch (pipeline.process(deserialized)) {
      case Result.Passed<T> p -> {
        final var sink = pipeline.getSink();
        if (sink != null) sink.accept(p.value());
      }
      case Result.Filtered<T> __ -> {
        /* intentional filter — no sink invocation */
      }
      case Result.Failed<T> f -> throw rethrowResultCause(f.cause());
    }
  }

  /// Re-throw a captured `Result.Failed` cause as an unchecked exception so the retry/error path
  /// can catch it. Mirrors the legacy MessagePipeline byte-level entry point behavior — it just
  /// lives here now, where the catching happens, rather than buried in three duplicated unwrap
  /// blocks inside MessagePipeline.
  private static RuntimeException rethrowResultCause(final Throwable cause) {
    if (cause instanceof RuntimeException re) return re;
    if (cause instanceof Error err) throw err;
    return new RuntimeException(cause);
  }

  private void markOffsetProcessed(final ConsumerRecord<K, byte[]> record) {
    if (offsetManager != null) offsetManager.markOffsetProcessed(record);
  }

  private void handleProcessingError(
    final ConsumerRecord<K, byte[]> record,
    final Exception e,
    final int retryCount,
    final Tracer.SpanScope span
  ) {
    metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
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
      if (sent) metrics.get(METRIC_DLQ_SENT).incrementAndGet();
    }
    markOffsetProcessed(record);
    health.recordOutcome(false);
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
