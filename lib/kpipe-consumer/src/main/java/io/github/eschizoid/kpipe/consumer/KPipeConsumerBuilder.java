package io.github.eschizoid.kpipe.consumer;

import io.github.eschizoid.kpipe.pipeline.Pipeline;
import io.github.eschizoid.kpipe.producer.KPipeProducer;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import io.github.eschizoid.kpipe.tracing.Tracer;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/// Builder for creating and configuring [KPipeConsumer] instances. Extracted from the former nested
/// `KPipeConsumer.Builder` so `KPipeConsumer` stays navigable; the consumer's constructor reads
/// this builder's package-private fields to assemble the running consumer.
public final class KPipeConsumerBuilder {

  private static final Logger LOGGER = System.getLogger(KPipeConsumerBuilder.class.getName());

  KPipeConsumerBuilder() {}

  /// Internal record bundling the per-topic batch configuration handed in via
  /// [#withBatchPipeline]. The [BatchSink] returns a [io.github.eschizoid.kpipe.sink.BatchResult]
  /// naming
  /// per-record outcomes; void-style consumers can use [BatchSink#ofVoid] to opt into
  /// whole-batch success/failure semantics.
  record BatchSpec<T>(String topic, Pipeline<T> pipeline, BatchSink<T> sink, BatchPolicy policy) {}

  Properties kafkaProps;
  Set<String> topics;
  Pipeline<?> pipeline;
  Map<String, Pipeline<?>> pipelinesPerTopic;
  final Map<String, BatchSpec<?>> batchSpecs = new LinkedHashMap<>();
  ConsumerSettings settings = ConsumerSettings.defaults();
  final Duration executorTerminationTimeout = ConsumerDefaults.EXECUTOR_TERMINATION;
  OffsetManager offsetManager;
  Function<Consumer<byte[], byte[]>, OffsetManager> offsetManagerProvider;
  /// The provider `build()` installed for the default offset manager, so a second `build()` on
  /// the same builder does not mistake it for one the caller supplied.
  private Function<Consumer<byte[], byte[]>, OffsetManager> defaultOffsetManagerProvider;
  /// The offset manager instance the last `build()` used, so a second `build()` cannot hand one
  /// manager to two consumers.
  private OffsetManager builtWithOffsetManager;
  Supplier<Consumer<byte[], byte[]>> consumerProvider;
  Queue<ConsumerCommand> commandQueue = new ConcurrentLinkedQueue<>();
  ConsumerRebalanceListener rebalanceListener;
  KPipeProducer<byte[], byte[]> kpipeProducer;

  /// Sets the properties for the Kafka consumer. `build()` always sets `enable.auto.commit` to
  /// `false`, because offsets are committed by the consumer's [OffsetManager], and always pins the
  /// key deserializer to `ByteArrayDeserializer`.
  ///
  /// @param props The Kafka consumer properties
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withProperties(final Properties props) {
    Objects.requireNonNull(props, "props cannot be null");
    // Defensive clone: build() pins key.deserializer into these properties
    // (forceByteArrayKeyDeserializer), and without a copy that mutation would leak into the
    // caller's own Properties object — or, on the fluent path, into sibling streams sharing one
    // internal copy. `(Properties) clone()`, never `new Properties(parent)` (that makes the
    // parent a getProperty fallback, not a copy).
    this.kafkaProps = (Properties) props.clone();
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
  /// All topics must produce the same payload type because they share one [Pipeline].
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
    return withTopics(Arrays.asList(Objects.requireNonNull(topics, "Topics cannot be null")));
  }

  /// Sets the pipeline to process each consumed message.
  ///
  /// The consumer always operates on `byte[]` at the boundary; format SerDe lives inside the
  /// pipeline. The pipeline must be a [Pipeline] — typically produced by
  /// `Pipeline.from(format).build()`.
  ///
  /// All subscribed topics share this pipeline (homogeneous case). For heterogeneous
  /// per-topic pipelines, use [#withPipelines(Map)].
  ///
  /// @param pipeline The pipeline to apply to message values
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withPipeline(final Pipeline<?> pipeline) {
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
  public KPipeConsumerBuilder withPipelines(final Map<String, Pipeline<?>> pipelinesPerTopic) {
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
  /// Retry interplay: [#withRetry] covers each record's deserialize + operator step before it
  /// is buffered — a transiently failing operator gets the configured attempts, and only
  /// retry-exhausted records go to the DLQ / error handler. The batch sink flush itself is
  /// never retried; a flush failure is handled through the per-record
  /// [io.github.eschizoid.kpipe.sink.BatchResult] outcomes and the DLQ path described above.
  ///
  /// @param topic the Kafka topic
  /// @param pipeline the typed pipeline to deserialize+process raw bytes into `T`
  /// @param sink the batch sink invoked on each flush
  /// @param policy the size/age flush thresholds
  /// @param <T> the deserialized value type
  /// @return this builder instance for method chaining
  public <T> KPipeConsumerBuilder withBatchPipeline(
    final String topic,
    final Pipeline<T> pipeline,
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
  /// @throws NullPointerException if `timeout` is null
  /// @throws IllegalArgumentException if `timeout` is not positive
  public KPipeConsumerBuilder withPollTimeout(final Duration timeout) {
    settings = settings.withPollTimeout(timeout);
    return this;
  }

  /// Sets the handler for processing errors.
  ///
  /// @param handler The consumer function that handles processing errors
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withErrorHandler(final KPipeConsumer.ErrorHandler handler) {
    settings = settings.withErrorHandler(handler);
    return this;
  }

  /// Sets an observer called for every failed processing attempt, at any stage: deserializing,
  /// an operator, the sink, or a batch flush. It is called once per attempt, so a record retried
  /// twice before it fails for good is reported three times, the last before the record is
  /// counted as failed, sent to the dead-letter topic or passed to the error handler.
  /// [KPipeConsumer.ProcessingError#retryCount] is the number of retries before the attempt; a
  /// batch flush failure reports the retries its record took before it was buffered. An
  /// interrupted attempt is not reported, and neither is a record the parallel dispatcher rejects
  /// before it runs, which reaches the error handler without an attempt.
  ///
  /// The observer only watches: it cannot stop a retry or the error handling that follows. It has
  /// no current record (`RecordContext.current()` throws there); it gets the record in its
  /// `ProcessingError`. An observer that throws, an `AssertionError` included, is logged and does
  /// not affect the record.
  ///
  /// @param observer called with each failed attempt
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder onAttemptFailed(final KPipeConsumer.ErrorHandler observer) {
    settings = settings.onAttemptFailed(observer);
    return this;
  }

  /// Configures retry behavior for failed message processing. The backoff is fixed (the same
  /// delay before every retry); an interruption during processing or backoff aborts without
  /// further attempts. A `NonRetryableException`, or a failure caused by one, is not retried and
  /// goes straight to the error handler / DLQ.
  ///
  /// On regular routes the retried unit is the record's full deserialize → operators → sink
  /// pass. On batch routes ([#withBatchPipeline]) the retried unit is the record's
  /// deserialize + operator step before buffering; the batch sink flush is not retried — a
  /// flush failure is handled through the `BatchResult` / DLQ path, not this policy.
  ///
  /// @param maxRetries Maximum number of retry attempts
  /// @param backoff Duration to wait between retry attempts
  /// @return This builder instance for method chaining
  /// @throws IllegalArgumentException if `maxRetries` or `backoff` is negative
  /// @throws NullPointerException if `backoff` is null
  public KPipeConsumerBuilder withRetry(final int maxRetries, final Duration backoff) {
    settings = settings.withRetry(maxRetries, backoff);
    return this;
  }

  /// Selects the processing mode. See [ProcessingMode] for semantics.
  ///
  /// - `SEQUENTIAL`: one record at a time per partition, in offset order. Lag-based
  ///   backpressure.
  /// - `PARALLEL` (default): virtual thread per record, no ordering. In-flight-based
  ///   backpressure.
  /// - `KEY_ORDERED`: per-key serial processing with a distinct-key cap (default 10,000, see
  ///   [#withKeyOrderedMaxKeys]). In-flight-based backpressure.
  ///
  /// @param mode The processing mode (must be non-null)
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withProcessingMode(final ProcessingMode mode) {
    settings = settings.withProcessingMode(mode);
    return this;
  }

  /// Cap on distinct keys held in-memory simultaneously when using
  /// [ProcessingMode#KEY_ORDERED]. Below this many distinct in-flight keys, each key gets
  /// its own serial queue + virtual thread. Above it, keys with empty, idle queues are
  /// evicted to make room.
  ///
  /// Default 10,000. Has no effect for `SEQUENTIAL` or `PARALLEL` modes.
  ///
  /// @param maxKeys distinct-key cap (must be positive)
  /// @return This builder instance for method chaining
  /// @throws IllegalArgumentException if `maxKeys` is non-positive
  public KPipeConsumerBuilder withKeyOrderedMaxKeys(final int maxKeys) {
    settings = settings.withKeyOrderedMaxKeys(maxKeys);
    return this;
  }

  /// Sets how often the default [KafkaOffsetManager] commits the offset frontier. It also commits
  /// on rebalance and at shutdown. The default is 30 seconds. A shorter interval narrows how many
  /// records are reprocessed after a crash, at the cost of more commit requests.
  ///
  /// Only the default offset manager uses this; `build()` rejects it alongside a custom
  /// [OffsetManager], which sets its own commit cadence.
  ///
  /// @param interval the time between periodic commits (at least 1 millisecond)
  /// @return This builder instance for method chaining
  /// @throws NullPointerException if `interval` is null
  /// @throws IllegalArgumentException if `interval` is under 1 millisecond
  public KPipeConsumerBuilder withCommitInterval(final Duration interval) {
    settings = settings.withCommitInterval(interval);
    return this;
  }

  /// Sets the timeout for waiting for in-flight messages during shutdown.
  ///
  /// @param timeout Maximum time to wait for in-flight messages to complete
  /// @return This builder instance for method chaining
  /// @throws NullPointerException if `timeout` is null
  /// @throws IllegalArgumentException if `timeout` is negative
  public KPipeConsumerBuilder withWaitForMessagesTimeout(final Duration timeout) {
    settings = settings.withWaitForMessagesTimeout(timeout);
    return this;
  }

  /// Sets the timeout for waiting for the consumer thread to terminate during shutdown. Unset, it
  /// defaults per processing mode to cover that mode's teardown; see
  /// [ConsumerDefaults#threadTermination].
  ///
  /// @param timeout Maximum time to wait for the consumer thread to finish
  /// @return This builder instance for method chaining
  /// @throws NullPointerException if `timeout` is null
  /// @throws IllegalArgumentException if `timeout` is negative
  public KPipeConsumerBuilder withThreadTerminationTimeout(final Duration timeout) {
    settings = settings.withThreadTerminationTimeout(timeout);
    return this;
  }

  /// Sets a function to create a custom OffsetManager once the consumer is available, in place of
  /// the default [KafkaOffsetManager].
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

  /// Keeps committed offsets in an external store, such as a database table, instead of Kafka.
  /// The default [KafkaOffsetManager] still computes the commit frontier on the same schedule, but
  /// periodic commits run on its commit thread instead of the consumer thread, and each newly
  /// assigned partition the store has an offset for is sought to it before it is polled. See
  /// [OffsetStore].
  ///
  /// @param offsetStore the store that holds this consumer group's offsets
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withOffsetStore(final OffsetStore offsetStore) {
    settings = settings.withOffsetStore(offsetStore);
    return this;
  }

  /// Replaces the default [KafkaOffsetManager] with a custom offset manager implementation.
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
  /// method — it is a test seam for observing or injecting [ConsumerCommand]s.
  ///
  /// @param commandQueue the queue to use for consumer commands
  /// @return this KPipeConsumerBuilder instance for method chaining
  public KPipeConsumerBuilder withCommandQueue(final Queue<ConsumerCommand> commandQueue) {
    this.commandQueue = Objects.requireNonNull(commandQueue, "Command queue cannot be null");
    return this;
  }

  /// Returns a [CommitExecutor] that runs commits on the consumer thread of the consumer this
  /// builder will build. Requests enqueue onto the builder's command queue; the consumer drains
  /// them in `processCommands()` and completes each request's future with the `commitSync`
  /// outcome. Requests submitted before the consumer starts simply wait in the queue.
  ///
  /// Intended for use inside an [#withOffsetManagerProvider] lambda, so an externally
  /// constructed [KafkaOffsetManager] commits through the same consumer thread:
  ///
  /// ```java
  /// builder.withOffsetManagerProvider(consumer ->
  ///     KafkaOffsetManager.builder(consumer)
  ///         .withCommitExecutor(builder.getCommitExecutor())
  ///         .build());
  /// ```
  ///
  /// @return a commit executor bound to this builder's command queue
  public CommitExecutor getCommitExecutor() {
    // Read the queue field lazily (per commit) so a later withCommandQueue(...) still wins.
    return offsets -> {
      final var outcome = new CompletableFuture<Boolean>();
      commandQueue.offer(new ConsumerCommand.CommitOffsets(offsets, outcome));
      return outcome;
    };
  }

  /// Sets the supplier for providing a consumer instance.
  ///
  /// The KPipe consumer owns partition pause state: on every pass it pauses the assignment while
  /// a pause source holds a pause and resumes any paused partition otherwise, so a partition
  /// paused directly through this consumer does not stay paused. Use [KPipeConsumer#pause()].
  ///
  /// @param provider A supplier that returns a Consumer instance configured for processing
  ///     messages
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withConsumer(final Supplier<Consumer<byte[], byte[]>> provider) {
    this.consumerProvider = Objects.requireNonNull(provider, "provider cannot be null");
    return this;
  }

  /// Enables backpressure control using the default watermarks: high = 10,000 (pause) and
  /// low = 7,000 (resume). Use [#withBackpressure(long, long)] to configure custom values.
  ///
  /// Backpressure is enabled by default.
  ///
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withBackpressure() {
    settings = settings.withBackpressure();
    return this;
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
    settings = settings.withBackpressure(highWatermark, lowWatermark);
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
  /// @throws NullPointerException if the topic is null
  /// @throws IllegalArgumentException if the topic is blank
  public KPipeConsumerBuilder withDeadLetterTopic(final String topic) {
    settings = settings.withDeadLetterTopic(topic);
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
    Objects.requireNonNull(producer, "DLQ producer cannot be null");
    settings = settings.withDeadLetterTopic(topic);
    this.kpipeProducer = producer;
    return this;
  }

  /// Exports this consumer's metrics to a monitoring backend, such as OpenTelemetry through
  /// `OtelMetrics.of(openTelemetry)` from `kpipe-metrics-otel`. Without an exporter the metrics are
  /// still counted and readable through [KPipeConsumer#metrics].
  ///
  /// @param exporter the exporter
  /// @return this builder
  public KPipeConsumerBuilder withMetrics(final MetricsExporter exporter) {
    settings = settings.withMetrics(exporter);
    return this;
  }

  /// Traces the consumer's records: the tracer starts one span per record, continuing the trace its
  /// headers carry, and every record written on that record's behalf, to the dead-letter topic or
  /// through `toKafka` and `KafkaMessageSink`, carries that span's context, even when a batch flush
  /// writes it from another thread. The DLQ record carries it whichever producer sends it.
  ///
  /// @param tracer the tracer, such as `new OtelTracer(openTelemetry)` from `kpipe-tracing-otel`;
  ///     [Tracer#noop()], the default, traces nothing
  /// @return This builder instance for method chaining
  public KPipeConsumerBuilder withTracer(final Tracer tracer) {
    settings = settings.withTracer(tracer);
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
    settings = settings.withCircuitBreaker(controller);
    return this;
  }

  /// Logs one line of the consumer's metrics every `interval`, plus a final line when it closes,
  /// after its last records are counted. The line goes to the
  /// `io.github.eschizoid.kpipe.metrics.log` logger at INFO.
  ///
  /// @param interval how often to log (must be positive)
  /// @return this builder instance for method chaining
  public KPipeConsumerBuilder withMetricsLog(final Duration interval) {
    settings = settings.withMetricsLog(interval);
    return this;
  }

  /// Configures whether to register a JVM shutdown hook that calls `close()` on this consumer.
  /// Use when the host process has no other shutdown coordination.
  ///
  /// @param useShutdownHook true to register a JVM shutdown hook
  /// @return this builder instance for method chaining
  public KPipeConsumerBuilder withShutdownHook(final boolean useShutdownHook) {
    settings = settings.withShutdownHook(useShutdownHook);
    return this;
  }

  /// Starts from `settings` for every consumer-wide setting, as the `KPipe` facade does when it
  /// builds a consumer. Call it before any consumer-wide builder method; those called afterwards
  /// change the settings it passed in.
  ///
  /// @param settings the consumer-wide settings
  /// @return This builder instance for method chaining
  /// @throws IllegalStateException if a consumer-wide setting was already set on this builder,
  ///     which the replacement would silently discard
  public KPipeConsumerBuilder withSettings(final ConsumerSettings settings) {
    Objects.requireNonNull(settings, "settings cannot be null");
    if (!this.settings.explicitlySet().isEmpty()) throw new IllegalStateException(
      "withSettings would discard %s already set on this builder; call withSettings first".formatted(
        this.settings.explicitlySet()
      )
    );
    this.settings = settings;
    return this;
  }

  /// @return the consumer-wide settings this builder builds with
  public ConsumerSettings settings() {
    return settings;
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
    // Without an offset manager nothing commits the frontier: with enable.auto.commit=false no
    // offset is ever committed, and Kafka's auto-commit commits polled offsets before their records
    // finish. Every consumer gets a KafkaOffsetManager unless the caller supplies its own.
    final var customProvider = offsetManagerProvider != null && offsetManagerProvider != defaultOffsetManagerProvider;
    if (offsetManager != null && customProvider) throw new IllegalArgumentException(
      "withOffsetManager and withOffsetManagerProvider are alternatives; set one"
    );
    if (offsetManager != null && offsetManager == builtWithOffsetManager) throw new IllegalStateException(
      "This builder's OffsetManager already belongs to a consumer it built; use withOffsetManagerProvider to build several"
    );
    final var customOffsetManager = offsetManager != null || customProvider;
    final var commitInterval = settings.commitInterval();
    final var offsetStore = settings.offsetStore();
    if (customOffsetManager && commitInterval != null) throw new IllegalArgumentException(
      "withCommitInterval configures the default KafkaOffsetManager; a custom OffsetManager sets its own commit interval"
    );
    if (customOffsetManager && offsetStore != null) throw new IllegalArgumentException(
      "withOffsetStore configures the default KafkaOffsetManager; a custom OffsetManager stores its own offsets"
    );
    if (!customOffsetManager) {
      final var interval = commitInterval;
      final var store = offsetStore;
      offsetManagerProvider = consumer -> {
        final var manager = KafkaOffsetManager.builder(consumer).withCommitExecutor(getCommitExecutor());
        if (interval != null) manager.withCommitInterval(interval);
        if (store != null) manager.withOffsetStore(store);
        return manager.build();
      };
      defaultOffsetManagerProvider = offsetManagerProvider;
    }
    disableAutoCommit(kafkaProps);
    forceByteArrayKeyDeserializer(kafkaProps);
    final var processingMode = settings.processingMode();
    final var explicitlySet = settings.explicitlySet();
    if (explicitlySet.contains(ConsumerSettings.Setting.BACKPRESSURE) && processingMode == ProcessingMode.SEQUENTIAL) {
      LOGGER.log(
        System.Logger.Level.INFO,
        "Sequential processing enabled with backpressure: switching to lag-based monitoring."
      );
    }
    if (
      explicitlySet.contains(ConsumerSettings.Setting.KEY_ORDERED_MAX_KEYS) &&
      processingMode != ProcessingMode.KEY_ORDERED
    ) {
      LOGGER.log(
        System.Logger.Level.WARNING,
        "withKeyOrderedMaxKeys({0}) was set but processing mode is {1} — the setting will be ignored. " +
          "Call withProcessingMode(ProcessingMode.KEY_ORDERED) to use it.",
        settings.keyOrderedMaxKeys(),
        processingMode
      );
    }
    final var consumer = new KPipeConsumer(this);
    builtWithOffsetManager = offsetManager;
    return consumer;
  }

  /// Keys are always `byte[]`: pin `key.deserializer` to [ByteArrayDeserializer] regardless of
  /// what the supplied properties say. Historically the key type parameter and the
  /// user-supplied deserializer could disagree, producing a `ClassCastException` deep inside
  /// record processing; pinning the deserializer here removes that failure mode entirely. A
  /// conflicting user-supplied value is logged at INFO before being overridden.
  private static void disableAutoCommit(final Properties props) {
    final var existing = props.get("enable.auto.commit");
    if (existing != null && !"false".equalsIgnoreCase(existing.toString().trim())) {
      LOGGER.log(
        Level.INFO,
        "Ignoring enable.auto.commit={0} from the supplied properties — KPipe commits offsets through its OffsetManager",
        existing
      );
    }
    props.put("enable.auto.commit", "false");
  }

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
