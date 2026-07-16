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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/// KPipeConsumerBuilder for creating and configuring [KPipeConsumer] instances. Extracted from the former
/// nested `KPipeConsumer.KPipeConsumerBuilder` (1.19.0) so `KPipeConsumer` stays navigable; the consumer's
/// constructor reads this builder's package-private fields to assemble the running consumer.
public final class KPipeConsumerBuilder {

  private static final Logger LOGGER = System.getLogger(KPipeConsumerBuilder.class.getName());

  KPipeConsumerBuilder() {}

  /// Internal record bundling the per-topic batch configuration handed in via
  /// [#withBatchPipeline]. The [BatchSink] returns a [io.github.eschizoid.kpipe.sink.BatchResult]
  /// naming
  /// per-record outcomes; void-style consumers can use [BatchSink#ofVoid] to opt into
  /// whole-batch success/failure semantics.
  record BatchSpec<T>(String topic, MessagePipeline<T> pipeline, BatchSink<T> sink, BatchPolicy policy) {}

  Properties kafkaProps;
  Set<String> topics;
  MessagePipeline<?> pipeline;
  Map<String, MessagePipeline<?>> pipelinesPerTopic;
  final Map<String, BatchSpec<?>> batchSpecs = new LinkedHashMap<>();
  Duration pollTimeout = Duration.ofMillis(100);
  KPipeConsumer.ErrorHandler errorHandler = e ->
    LOGGER.log(
      Level.WARNING,
      "Failed at offset {0} after {1} retries: {2}",
      e.record().offset(),
      e.retryCount(),
      e.exception().getMessage()
    );
  int maxRetries = 0;
  Duration retryBackoff = Duration.ofMillis(500);
  ProcessingMode processingMode = ProcessingMode.PARALLEL;
  int keyOrderedMaxKeys = KeyOrderedDispatcher.DEFAULT_MAX_KEYS;
  Duration waitForMessagesTimeout = AppConfig.DEFAULT_WAIT_FOR_MESSAGES;
  Duration threadTerminationTimeout = AppConfig.DEFAULT_THREAD_TERMINATION;
  final Duration executorTerminationTimeout = AppConfig.DEFAULT_EXECUTOR_TERMINATION;
  OffsetManager offsetManager;
  Function<Consumer<byte[], byte[]>, OffsetManager> offsetManagerProvider;
  Supplier<Consumer<byte[], byte[]>> consumerProvider;
  Queue<ConsumerCommand> commandQueue = new ConcurrentLinkedQueue<>();
  ConsumerRebalanceListener rebalanceListener;
  BackpressureController backpressureController;
  String deadLetterTopic;
  KPipeProducer<byte[], byte[]> kpipeProducer;
  ConsumerMetrics consumerMetrics;
  Tracer tracer;
  CircuitBreakerController circuitBreakerController;
  final List<KPipeMetricsReporter> metricsReporters = new ArrayList<>();
  Duration metricsReporterInterval = Duration.ofMinutes(1);
  boolean useShutdownHook = false;

  /// Sets the properties for the Kafka consumer.
  ///
  /// @param props The Kafka consumer properties
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withProperties(final Properties props) {
    this.kafkaProps = Objects.requireNonNull(props, "props cannot be null");
    return this;
  }

  /// Sets the Kafka topic to consume from.
  ///
  /// @param topic The topic name
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withTopic(final String topic) {
    return withTopics(Set.of(Objects.requireNonNull(topic, "Topic cannot be null")));
  }

  /// Sets multiple Kafka topics to consume from with a single shared pipeline.
  ///
  /// All topics must produce the same payload type because they share one [MessagePipeline].
  /// For per-topic typing, run separate consumers.
  ///
  /// @param topics The topic names (must be non-empty)
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withTopics(final Collection<String> topics) {
    Objects.requireNonNull(topics, "Topics cannot be null");
    if (topics.isEmpty()) throw new IllegalArgumentException("Topics cannot be empty");
    this.topics = new LinkedHashSet<>(topics);
    return this;
  }

  /// Sets multiple Kafka topics to consume from with a single shared pipeline (varargs form).
  ///
  /// @param topics The topic names (must be non-empty)
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withTopics(final String... topics) {
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
  public KPipeConsumerBuilder withPipeline(final MessagePipeline<?> pipeline) {
    this.pipeline = Objects.requireNonNull(pipeline, "pipeline cannot be null");
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
  public KPipeConsumerBuilder withPipelines(final Map<String, MessagePipeline<?>> pipelinesPerTopic) {
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
  public <T> KPipeConsumerBuilder withBatchPipeline(
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
  public KPipeConsumerBuilder withPollTimeout(final Duration timeout) {
    this.pollTimeout = Objects.requireNonNull(timeout, "timeout cannot be null");
    return this;
  }

  /// Sets the handler for processing errors.
  ///
  /// @param handler The consumer function that handles processing errors
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withErrorHandler(final KPipeConsumer.ErrorHandler handler) {
    this.errorHandler = Objects.requireNonNull(handler, "handler cannot be null");
    return this;
  }

  /// Configures retry behavior for failed message processing.
  ///
  /// @param maxRetries Maximum number of retry attempts
  /// @param backoff Duration to wait between retry attempts
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withRetry(final int maxRetries, final Duration backoff) {
    this.maxRetries = maxRetries;
    this.retryBackoff = Objects.requireNonNull(backoff, "backoff cannot be null");
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
  public KPipeConsumerBuilder withProcessingMode(final ProcessingMode mode) {
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
  public KPipeConsumerBuilder withKeyOrderedMaxKeys(final int maxKeys) {
    if (maxKeys <= 0) throw new IllegalArgumentException("maxKeys must be positive, got " + maxKeys);
    this.keyOrderedMaxKeys = maxKeys;
    return this;
  }

  /// Sets the timeout for waiting for in-flight messages during shutdown.
  ///
  /// @param timeout Maximum time to wait for in-flight messages to complete
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withWaitForMessagesTimeout(final Duration timeout) {
    this.waitForMessagesTimeout = Objects.requireNonNull(timeout);
    return this;
  }

  /// Sets the timeout for waiting for the consumer thread to terminate during shutdown.
  ///
  /// @param timeout Maximum time to wait for the consumer thread to finish
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withThreadTerminationTimeout(final Duration timeout) {
    this.threadTerminationTimeout = Objects.requireNonNull(timeout);
    return this;
  }

  /// Sets a function to create a custom OffsetManager once the consumer is available. This
  /// automatically disables auto-commit.
  ///
  /// @param provider A function that creates an OffsetManager given the consumer instance
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withOffsetManagerProvider(
    final Function<Consumer<byte[], byte[]>, OffsetManager> provider
  ) {
    Objects.requireNonNull(provider, "provider cannot be null");
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
  public KPipeConsumerBuilder withOffsetManager(final OffsetManager manager) {
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
  /// @return this KPipeConsumerBuilder instance for method chaining
  public KPipeConsumerBuilder withCommandQueue(final Queue<ConsumerCommand> commandQueue) {
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
  public KPipeConsumerBuilder withConsumer(final Supplier<Consumer<byte[], byte[]>> provider) {
    this.consumerProvider = Objects.requireNonNull(provider, "provider cannot be null");
    return this;
  }

  /// Enables backpressure control using the default watermarks: high = 10,000 (pause) and
  /// low = 7,000 (resume). Use {@link #withBackpressure(long, long)} to configure custom values.
  ///
  /// Backpressure is enabled by default.
  ///
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withBackpressure() {
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
  public KPipeConsumerBuilder withBackpressure(final long highWatermark, final long lowWatermark) {
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
  public KPipeConsumerBuilder withDeadLetterTopic(final String topic) {
    Objects.requireNonNull(topic, "topic cannot be null");
    if (topic.isBlank()) throw new IllegalArgumentException("topic cannot be blank");
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
  public KPipeConsumerBuilder withKafkaProducer(final Producer<byte[], byte[]> producer) {
    Objects.requireNonNull(producer, "producer cannot be null");
    this.kpipeProducer = KPipeProducer.<byte[], byte[]>builder().withProducer(producer).build();
    return this;
  }

  /// Sets the KPipe producer wrapper to use for DLQ and Kafka sinks.
  ///
  /// Prefer [#withDeadLetterQueue(String, KPipeProducer)] when configuring a DLQ — it pairs the
  /// topic and the producer in a single call.
  ///
  /// @param producer The KPipe producer wrapper to use
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withKafkaProducer(final KPipeProducer<byte[], byte[]> producer) {
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
  public KPipeConsumerBuilder withDeadLetterQueue(final String topic, final KPipeProducer<byte[], byte[]> producer) {
    this.deadLetterTopic = Objects.requireNonNull(topic, "DLQ topic cannot be null");
    this.kpipeProducer = Objects.requireNonNull(producer, "DLQ producer cannot be null");
    return this;
  }

  /// Sets the OpenTelemetry metrics instruments for this consumer.
  ///
  /// Use [io.github.eschizoid.kpipe.metrics.ConsumerMetrics] to create an instrumented instance,
  /// or
  /// [ConsumerMetrics#noop()] for a no-op default.
  ///
  /// @param metrics the consumer metrics instruments
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withMetrics(final ConsumerMetrics metrics) {
    this.consumerMetrics = Objects.requireNonNull(metrics, "metrics cannot be null");
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
  public KPipeConsumerBuilder withTracer(final Tracer tracer) {
    this.tracer = Objects.requireNonNull(tracer, "tracer cannot be null");
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
  public KPipeConsumerBuilder withCircuitBreaker(final CircuitBreakerController controller) {
    this.circuitBreakerController = Objects.requireNonNull(controller, "controller cannot be null");
    return this;
  }

  /// Adds metrics reporters that run periodically while the consumer is alive. Each reporter is
  /// invoked every `metricsReporterInterval` (defaults to 60s) on a dedicated platform daemon
  /// thread. Reporter exceptions are logged at WARNING but do not crash the thread.
  ///
  /// @param reporters the collection of reporters to run
  /// @return this builder instance for method chaining
  public KPipeConsumerBuilder withMetricsReporters(final Collection<KPipeMetricsReporter> reporters) {
    this.metricsReporters.addAll(Objects.requireNonNull(reporters, "reporters cannot be null"));
    return this;
  }

  /// Sets the interval between metrics-reporter invocations.
  ///
  /// @param interval the reporting interval (must be positive)
  /// @return this builder instance for method chaining
  public KPipeConsumerBuilder withMetricsInterval(final Duration interval) {
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
  public KPipeConsumerBuilder withShutdownHook(final boolean useShutdownHook) {
    this.useShutdownHook = useShutdownHook;
    return this;
  }

  /// Builds a new KPipeConsumer with the configured settings.
  ///
  /// @return a new KPipeConsumer instance
  public KPipeConsumer build() {
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
    if (maxRetries < 0) throw new IllegalArgumentException("Max retries cannot be negative, got " + maxRetries);
    if (pollTimeout.isNegative() || pollTimeout.isZero()) throw new IllegalArgumentException(
      "Poll timeout must be positive, got " + pollTimeout
    );
    if (offsetManager != null || offsetManagerProvider != null) kafkaProps.setProperty("enable.auto.commit", "false");
    forceByteArrayKeyDeserializer(kafkaProps);
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
    return new KPipeConsumer(this);
  }

  /// Keys are always `byte[]`: pin `key.deserializer` to [ByteArrayDeserializer] regardless of
  /// what the supplied properties say. Historically the key type parameter and the
  /// user-supplied deserializer could disagree, producing a `ClassCastException` deep inside
  /// record processing; pinning the deserializer here removes that failure mode entirely. A
  /// conflicting user-supplied value is logged at INFO before being overridden.
  private static void forceByteArrayKeyDeserializer(final Properties props) {
    final var pinned = ByteArrayDeserializer.class.getName();
    final var existing = props.get("key.deserializer");
    final var existingName = switch (existing) {
      case null -> null;
      case Class<?> c -> c.getName();
      default -> existing.toString();
    };
    if (existingName != null && !pinned.equals(existingName)) {
      LOGGER.log(
        Level.INFO,
        "Ignoring key.deserializer={0} from the supplied properties — KPipe always consumes keys as byte[] and pins {1}",
        existingName,
        pinned
      );
    }
    props.put("key.deserializer", pinned);
  }
}
