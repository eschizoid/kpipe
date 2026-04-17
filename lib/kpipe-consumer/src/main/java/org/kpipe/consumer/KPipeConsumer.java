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
import org.kpipe.consumer.enums.ConsumerState;
import org.kpipe.consumer.sink.JsonConsoleSink;
import org.kpipe.metrics.ConsumerMetrics;
import org.kpipe.producer.KPipeProducer;
import org.kpipe.registry.MessagePipeline;
import org.kpipe.sink.MessageSink;

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
/// final var pipeline = registry.pipeline(MessageFormat.JSON)
///     .add(sanitizeKey)
///     .toSink(MessageSinkRegistry.JSON_LOGGING)
///     .build();
///
/// final var consumer = KPipeConsumer.<byte[], byte[]>builder()
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
/// @param <V> the type of values in the consumed records
public class KPipeConsumer<K, V> implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(KPipeConsumer.class.getName());

  private static final String METRIC_MESSAGES_RECEIVED = "messagesReceived";
  private static final String METRIC_MESSAGES_PROCESSED = "messagesProcessed";
  private static final String METRIC_PROCESSING_ERRORS = "processingErrors";
  private static final String METRIC_RETRIES = "retries";
  static final String METRIC_BACKPRESSURE_PAUSE_COUNT = "backpressurePauseCount";
  static final String METRIC_BACKPRESSURE_TIME_MS = "backpressureTimeMs";
  private static final String METRIC_DLQ_SENT = "dlqSent";

  private final Queue<ConsumerCommand> commandQueue;
  private final Consumer<K, V> kafkaConsumer;
  private final String topic;
  private final Function<V, V> processor;
  private final ExecutorService virtualThreadExecutor;
  private final Duration pollTimeout;
  private final MessageSink<V> messageSink;
  private final AtomicReference<Thread> consumerThread = new AtomicReference<>();
  private final Duration waitForMessagesTimeout;
  private final Duration threadTerminationTimeout;
  private final Duration executorTerminationTimeout;
  private final OffsetManager<K, V> offsetManager;
  private final ConsumerRebalanceListener rebalanceListener;
  private final ErrorHandler<K, V> errorHandler;
  private final int maxRetries;
  private final Duration retryBackoff;
  private final AtomicReference<ConsumerState> state = new AtomicReference<>(ConsumerState.CREATED);
  private final boolean enableMetrics;
  private final boolean sequentialProcessing;
  private final Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();
  private final ConsumerMetrics otelMetrics;
  private final BackpressureController backpressureController;
  private final AtomicLong inFlightCount = new AtomicLong(0);
  private final AtomicBoolean manualPause = new AtomicBoolean(false);
  private final AtomicBoolean backpressurePaused = new AtomicBoolean(false);
  private volatile long backpressurePauseStartNanos;

  private final String deadLetterTopic;
  private final KPipeProducer<K, V> kpipeProducer;

  /// Represents an error that occurred during record processing. Contains the original record,
  /// the exception that was thrown, and the number of retry attempts made.
  ///
  /// @param <K> the type of the record key
  /// @param <V> the type of the record value
  /// @param record the Kafka record that failed processing
  /// @param exception the exception that occurred during processing
  /// @param retryCount the number of retry attempts made
  public record ProcessingError<K, V>(ConsumerRecord<K, V> record, Exception exception, int retryCount) {}

  /// A functional interface for handling processing errors.
  ///
  /// @param <K> the type of the record key
  /// @param <V> the type of the record value
  @FunctionalInterface
  public interface ErrorHandler<K, V> extends java.util.function.Consumer<ProcessingError<K, V>> {}

  /// Creates a new builder for constructing {@link KPipeConsumer} instances.
  ///
  /// @param <K> the type of keys in the consumed records
  /// @param <V> the type of values in the consumed records
  /// @return a new builder instance
  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  /// Builder for creating and configuring {@link KPipeConsumer} instances.
  ///
  /// @param <K> the type of keys in the consumed records
  /// @param <V> the type of values in the consumed records
  public static class Builder<K, V> {

    private Builder() {}

    private Properties kafkaProps;
    private String topic;
    private Function<V, V> pipeline;
    private Duration pollTimeout = Duration.ofMillis(100);
    private ErrorHandler<K, V> errorHandler = e ->
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
    private MessageSink<V> messageSink;
    private Duration waitForMessagesTimeout = AppConfig.DEFAULT_WAIT_FOR_MESSAGES;
    private Duration threadTerminationTimeout = AppConfig.DEFAULT_THREAD_TERMINATION;
    private Duration executorTerminationTimeout = AppConfig.DEFAULT_EXECUTOR_TERMINATION;
    private OffsetManager<K, V> offsetManager;
    private Function<Consumer<K, V>, OffsetManager<K, V>> offsetManagerProvider;
    private Supplier<Consumer<K, V>> consumerProvider;
    private Queue<ConsumerCommand> commandQueue;
    private ConsumerRebalanceListener rebalanceListener;
    private BackpressureController backpressureController;
    private String deadLetterTopic;
    private KPipeProducer<K, V> kpipeProducer;
    private ConsumerMetrics consumerMetrics;

    /// Sets the properties for the Kafka consumer.
    ///
    /// @param props The Kafka consumer properties
    /// @return This builder instance for method chaining
    public Builder<K, V> withProperties(final Properties props) {
      this.kafkaProps = props;
      return this;
    }

    /// Sets the Kafka topic to consume from.
    ///
    /// @param topic The topic name
    /// @return This builder instance for method chaining
    public Builder<K, V> withTopic(final String topic) {
      this.topic = topic;
      return this;
    }

    /// Sets the pipeline to process each consumed message.
    ///
    /// @param pipeline The pipeline to apply to message values
    /// @return This builder instance for method chaining
    public Builder<K, V> withPipeline(final Function<V, V> pipeline) {
      this.pipeline = pipeline;
      return this;
    }

    /// Sets the timeout duration for the consumer's poll operation.
    ///
    /// @param timeout The maximum time to wait for messages in each poll
    /// @return This builder instance for method chaining
    public Builder<K, V> withPollTimeout(final Duration timeout) {
      this.pollTimeout = timeout;
      return this;
    }

    /// Sets the handler for processing errors.
    ///
    /// @param handler The consumer function that handles processing errors
    /// @return This builder instance for method chaining
    public Builder<K, V> withErrorHandler(final ErrorHandler<K, V> handler) {
      this.errorHandler = handler;
      return this;
    }

    /// Configures retry behavior for failed message processing.
    ///
    /// @param maxRetries Maximum number of retry attempts
    /// @param backoff Duration to wait between retry attempts
    /// @return This builder instance for method chaining
    public Builder<K, V> withRetry(final int maxRetries, final Duration backoff) {
      this.maxRetries = maxRetries;
      this.retryBackoff = backoff;
      return this;
    }

    /// Enables or disables metrics collection.
    ///
    /// @param enable Whether to enable a metrics collection
    /// @return This builder instance for method chaining
    public Builder<K, V> enableMetrics(final boolean enable) {
      this.enableMetrics = enable;
      return this;
    }

    /// Configures whether messages should be processed sequentially.
    ///
    /// @param sequential If true, messages will be processed in order; if false, parallel
    ///     processing is used
    /// @return This builder instance for method chaining
    public Builder<K, V> withSequentialProcessing(final boolean sequential) {
      this.sequentialProcessing = sequential;
      return this;
    }

    /// Sets the message sink that receives processed messages.
    ///
    /// @param messageSink The sink that handles successfully processed messages
    /// @return This builder instance for method chaining
    public Builder<K, V> withMessageSink(final MessageSink<V> messageSink) {
      this.messageSink = messageSink;
      return this;
    }

    /// Sets the timeout for waiting for in-flight messages during shutdown.
    ///
    /// @param timeout Maximum time to wait for in-flight messages to complete
    /// @return This builder instance for method chaining
    public Builder<K, V> withWaitForMessagesTimeout(final Duration timeout) {
      this.waitForMessagesTimeout = Objects.requireNonNull(timeout);
      return this;
    }

    /// Sets the timeout for waiting for the consumer thread to terminate during shutdown.
    ///
    /// @param timeout Maximum time to wait for the consumer thread to finish
    /// @return This builder instance for method chaining
    public Builder<K, V> withThreadTerminationTimeout(final Duration timeout) {
      this.threadTerminationTimeout = Objects.requireNonNull(timeout);
      return this;
    }

    /// Sets a function to create a custom OffsetManager once the consumer is available. This
    /// automatically disables auto-commit.
    ///
    /// @param provider A function that creates an OffsetManager given the consumer instance
    /// @return This builder instance for method chaining
    public Builder<K, V> withOffsetManagerProvider(final Function<Consumer<K, V>, OffsetManager<K, V>> provider) {
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
    public Builder<K, V> withOffsetManager(final OffsetManager<K, V> manager) {
      this.offsetManager = Objects.requireNonNull(manager, "OffsetManager cannot be null");
      this.rebalanceListener = manager.createRebalanceListener();
      return this;
    }

    /// Sets a custom command queue for the consumer.
    ///
    /// @param commandQueue the queue to use for consumer commands
    /// @return this Builder instance for method chaining
    public Builder<K, V> withCommandQueue(final Queue<ConsumerCommand> commandQueue) {
      this.commandQueue = Objects.requireNonNull(commandQueue, "Command queue cannot be null");
      return this;
    }

    /// Sets the supplier for providing a consumer instance.
    ///
    /// @param provider A supplier that returns a Consumer instance configured for processing
    ///     messages
    /// @return This builder instance for method chaining
    public Builder<K, V> withConsumer(final Supplier<Consumer<K, V>> provider) {
      this.consumerProvider = provider;
      return this;
    }

    /// Enables backpressure control using the default watermarks: high = 10,000 (pause) and
    /// low = 7,000 (resume). Use {@link #withBackpressure(long, long)} to configure custom values.
    ///
    /// <p>Backpressure is enabled by default.
    ///
    /// @return This builder instance for method chaining
    public Builder<K, V> withBackpressure() {
      return withBackpressure(10_000, 7_000);
    }

    /// Enables backpressure control using the given high and low watermarks. When the number of
    /// in-flight messages reaches the high watermark, the consumer pauses Kafka polling. It
    /// resumes when the count drops to or below the low watermark (hysteresis).
    ///
    /// <p>Backpressure is enabled by default. Calling this method configures custom thresholds.
    ///
    /// @param highWatermark pause consumption when in-flight count reaches this value (must be
    ///     positive)
    /// @param lowWatermark  resume consumption when in-flight count drops to this value (must be
    ///     less than highWatermark)
    /// @return This builder instance for method chaining
    public Builder<K, V> withBackpressure(final long highWatermark, final long lowWatermark) {
      this.backpressureController = new BackpressureController(highWatermark, lowWatermark, null);
      return this;
    }

    /// Sets the Dead Letter Queue (DLQ) topic to send failed records after all retries.
    ///
    /// @param topic The name of the DLQ topic
    /// @return This builder instance for method chaining
    public Builder<K, V> withDeadLetterTopic(final String topic) {
      this.deadLetterTopic = topic;
      return this;
    }

    /// Sets the Kafka producer to use for DLQ and Kafka sinks. If not provided but a DLQ topic is
    /// set, a new producer will be created using the consumer's configuration.
    ///
    /// @param producer The Kafka producer to use
    /// @return This builder instance for method chaining
    public Builder<K, V> withKafkaProducer(final Producer<K, V> producer) {
      this.kpipeProducer = KPipeProducer.<K, V>builder().withProducer(producer).build();
      return this;
    }

    /// Sets the KPipe producer wrapper to use for DLQ and Kafka sinks.
    ///
    /// @param producer The KPipe producer wrapper to use
    /// @return This builder instance for method chaining
    public Builder<K, V> withKafkaProducer(final KPipeProducer<K, V> producer) {
      this.kpipeProducer = Objects.requireNonNull(producer, "Producer cannot be null");
      return this;
    }

    /// Sets the OpenTelemetry metrics instruments for this consumer.
    ///
    /// Use {@link ConsumerMetrics#ConsumerMetrics(io.opentelemetry.api.OpenTelemetry)} to create an
    /// instrumented instance, or {@link ConsumerMetrics#noop()} for a no-op default.
    ///
    /// @param metrics the consumer metrics instruments
    /// @return This builder instance for method chaining
    public Builder<K, V> withMetrics(final ConsumerMetrics metrics) {
      this.consumerMetrics = metrics;
      return this;
    }

    /// Builds a new KPipeConsumer with the configured settings.
    ///
    /// @return a new KPipeConsumer instance
    public KPipeConsumer<K, V> build() {
      Objects.requireNonNull(kafkaProps, "Kafka properties must be provided");
      Objects.requireNonNull(topic, "Topic must be provided");
      Objects.requireNonNull(pipeline, "Pipeline function must be provided");
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
  public KPipeConsumer(final Builder<K, V> builder) {
    this.kafkaConsumer =
      builder.consumerProvider != null
        ? builder.consumerProvider.get()
        : new KafkaConsumer<>(Objects.requireNonNull(builder.kafkaProps));
    this.topic = Objects.requireNonNull(builder.topic);
    this.processor = Objects.requireNonNull(builder.pipeline);
    this.pollTimeout = Objects.requireNonNull(builder.pollTimeout);
    this.errorHandler = builder.errorHandler;
    this.maxRetries = builder.maxRetries;
    this.retryBackoff = builder.retryBackoff;
    this.enableMetrics = builder.enableMetrics;
    this.sequentialProcessing = builder.sequentialProcessing;
    this.messageSink = builder.messageSink != null ? builder.messageSink : new JsonConsoleSink<>();
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
        : BackpressureController.inFlightStrategy(this.inFlightCount::get)
    );

    this.deadLetterTopic = builder.deadLetterTopic;
    this.kpipeProducer =
      builder.kpipeProducer != null
        ? builder.kpipeProducer
        : this.deadLetterTopic != null
          ? KPipeProducer.<K, V>builder().withProperties(builder.kafkaProps).build()
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
          METRIC_RETRIES,
          new AtomicLong(0),
          METRIC_BACKPRESSURE_PAUSE_COUNT,
          new AtomicLong(0),
          METRIC_BACKPRESSURE_TIME_MS,
          new AtomicLong(0),
          METRIC_DLQ_SENT,
          new AtomicLong(0)
        )
      );
    }

    this.otelMetrics =
      builder.consumerMetrics != null ? builder.consumerMetrics : ConsumerMetrics.noop(this.inFlightCount::get);
  }

  /// Creates a message tracker that can monitor the state of in-flight messages. The tracker
  /// uses the consumer's metrics to determine how many messages have been received versus
  /// processed.
  ///
  /// @return a new {@link MessageTracker} instance, or null if metrics are disabled
  public MessageTracker createMessageTracker() {
    if (!enableMetrics) {
      LOGGER.log(Level.INFO, "Cannot create MessageTracker: metrics are disabled");
      return null;
    }

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
    if (rebalanceListener != null) kafkaConsumer.subscribe(List.of(topic), rebalanceListener);
    else kafkaConsumer.subscribe(List.of(topic));

    final var thread = Thread.ofVirtual()
      .name("kafka-consumer-%s-%s".formatted(topic, UUID.randomUUID().toString().substring(0, 8)))
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
            LOGGER.log(Level.INFO, "Consumer closed for topic {0}", topic);
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error closing Kafka consumer", e);
          } finally {
            state.set(ConsumerState.CLOSED);
          }
        }
      });

    consumerThread.set(thread);
    LOGGER.log(Level.INFO, "Consumer started for topic {0}", topic);
  }

  /// Pauses consumption from the topic. Any in-flight messages will continue processing, but no new
  /// messages will be consumed until {@link #resume()} is called.
  ///
  /// <p>This method is idempotent - calling it multiple times has no additional effect.
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
            LOGGER.log(Level.INFO, "Consumer paused for topic {0}", topic);
          }
          case ConsumerCommand.Resume _ -> {
            kafkaConsumer.resume(kafkaConsumer.assignment());
            LOGGER.log(Level.INFO, "Consumer resumed for topic {0}", topic);
          }
          case ConsumerCommand.Close _ -> {
            state.set(ConsumerState.CLOSING);
            LOGGER.log(Level.INFO, "Consumer shutdown initiated for topic {0}", topic);
          }
          case ConsumerCommand.TrackOffset cmd -> {
            if (offsetManager != null) {
              @SuppressWarnings("unchecked")
              final var record = (ConsumerRecord<K, V>) cmd.record();
              offsetManager.trackOffset(record);
            }
          }
          case ConsumerCommand.MarkOffsetProcessed cmd -> {
            if (offsetManager != null) {
              @SuppressWarnings("unchecked")
              final var record = (ConsumerRecord<K, V>) cmd.record();
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
  /// <p>This method is idempotent - calling it multiple times has no additional effect.
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
  /// <p>This method performs a graceful shutdown by:
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
  /// <p>This method is idempotent - calling it multiple times has no additional effect.
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

    if (offsetManager != null) {
      try {
        offsetManager.close();
      } catch (Exception e) {
        LOGGER.log(Level.WARNING, "Error closing offset manager", e);
      }
    }

    if (kpipeProducer != null) {
      kpipeProducer.close();
    }
    state.set(ConsumerState.CLOSED);
  }

  /// Processes multiple Kafka records by submitting each one to the virtual thread executor.
  ///
  /// @param records the batch of records to process
  protected void processRecords(final ConsumerRecords<K, V> records) {
    for (final var record : records.records(topic)) {
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
            otelMetrics.recordProcessingError();
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
  /// <p>This method applies the processor function to transform the record value while handling
  /// exceptions with configurable retry logic. Processing occurs in the current virtual thread
  /// without blocking operations that would impact carrier thread performance.
  ///
  /// <p>Metrics tracked during processing:
  ///
  /// <ul>
  ///   <li>messagesReceived - Incremented when a record is received
  ///   <li>messagesProcessed - Incremented for successful processing
  ///   <li>retries - Incremented for each retry attempt (not counting an initial attempt)
  ///   <li>processingErrors - Incremented when processing fails after all retries
  /// </ul>
  ///
  /// @param record The Kafka consumer record to process
  protected void processRecord(final ConsumerRecord<K, V> record) {
    if (enableMetrics) metrics.get(METRIC_MESSAGES_RECEIVED).incrementAndGet();
    otelMetrics.recordMessageReceived();

    final long startTime = System.currentTimeMillis();
    try {
      final var result = tryProcessRecord(record);
      if (result) {
        if (enableMetrics) metrics.get(METRIC_MESSAGES_PROCESSED).incrementAndGet();
        otelMetrics.recordMessageProcessed();
        otelMetrics.recordProcessingDuration(System.currentTimeMillis() - startTime);
      }
    } finally {
      inFlightCount.decrementAndGet();
      if (backpressurePaused.get()) {
        final var thread = consumerThread.get();
        if (thread != null) LockSupport.unpark(thread);
      }
    }
  }

  private boolean tryProcessRecord(final ConsumerRecord<K, V> record) {
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      if (attempt > 0) if (!handleRetry(record, attempt)) return false;

      try {
        if (processor instanceof MessagePipeline typedPipeline) {
          processTypedRecord(record, typedPipeline);
          return true;
        }

        final var processedValue = processor.apply(record.value());
        messageSink.accept(processedValue);
        markOffsetProcessed(record);
        return true;
      } catch (final Exception e) {
        if (isInterruptionRelated(e)) {
          Thread.currentThread().interrupt();
          return false;
        }

        if (attempt == maxRetries) {
          handleProcessingError(record, e, attempt);
          return false;
        }
      }
    }
    return false;
  }

  private boolean handleRetry(final ConsumerRecord<K, V> record, final int attempt) {
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

  @SuppressWarnings("unchecked")
  private void processTypedRecord(final ConsumerRecord<K, V> record, final MessagePipeline typedPipeline) {
    if (record.value() == null) throw new IllegalStateException("Record value is null at offset " + record.offset());

    final var recordValue = (byte[]) record.value();
    final var deserialized = typedPipeline.deserialize(recordValue);
    if (deserialized == null) throw new IllegalStateException(
      "Pipeline deserialization returned null at offset " + record.offset()
    );

    final var processed = typedPipeline.process(deserialized);
    if (processed == null) {
      // Null process result is treated as intentional filtering
      markOffsetProcessed(record);
      return;
    }

    final var sink = typedPipeline.getSink();
    if (sink != null) {
      sink.accept(processed);
    } else {
      final var serialized = typedPipeline.serialize(processed);
      messageSink.accept((V) serialized);
    }

    markOffsetProcessed(record);
  }

  private void markOffsetProcessed(final ConsumerRecord<K, V> record) {
    if (offsetManager != null) commandQueue.offer(new ConsumerCommand.MarkOffsetProcessed(record));
  }

  private void handleProcessingError(final ConsumerRecord<K, V> record, final Exception e, final int retryCount) {
    if (enableMetrics) metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
    otelMetrics.recordProcessingError();
    LOGGER.log(
      Level.WARNING,
      "Failed to process message at offset {0} after {1} attempt(s): {2}",
      record.offset(),
      retryCount + 1,
      e.getMessage()
    );
    if (deadLetterTopic != null && kpipeProducer != null) kpipeProducer.sendToDlq(
      deadLetterTopic,
      record,
      topic,
      e,
      enableMetrics ? metrics.get(METRIC_DLQ_SENT) : null
    );
    markOffsetProcessed(record);
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

  private ConsumerRecords<K, V> pollRecords() {
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
