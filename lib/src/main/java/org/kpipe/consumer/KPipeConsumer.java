package org.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.channels.ClosedByInterruptException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.kpipe.config.AppConfig;
import org.kpipe.consumer.enums.ConsumerCommand;
import org.kpipe.consumer.enums.ConsumerState;
import org.kpipe.sink.JsonConsoleSink;
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
/// When using an OffsetManager, auto-commit is automatically disabled since offset commits are
/// managed explicitly.
///
/// Example usage:
///
/// ```java
/// final var consumer = KPipeConsumer.<String, String>builder()
///     .withProperties(kafkaProps)
///     .withTopic("example-topic")
///     .withProcessor(value -> processValue(value))
///     .withRetry(3, Duration.ofSeconds(1))
///     .withSequentialProcessing(false) // Set to true for ordered processing
///     .withOffsetManagerProvider(consumer -> OffsetManager.builder(consumer)
///         .withCommitInterval(Duration.ofSeconds(30))
///         .withCommandQueue(commandQueue)
///         .build())
///     .build();
///
/// consumer.start();
/// // Later when finished
/// consumer.close();
/// ```
///
/// @param <K> the type of keys in the consumed records
/// @param <V> the type of values in the consumed records
public class KPipeConsumer<K, V> implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(KPipeConsumer.class.getName());

  // Metric key constants
  private static final String METRIC_MESSAGES_RECEIVED = "messagesReceived";
  private static final String METRIC_MESSAGES_PROCESSED = "messagesProcessed";
  private static final String METRIC_PROCESSING_ERRORS = "processingErrors";
  private static final String METRIC_RETRIES = "retries";
  static final String METRIC_BACKPRESSURE_PAUSE_COUNT = "backpressurePauseCount";
  static final String METRIC_BACKPRESSURE_TIME_MS = "backpressureTimeMs";

  private final Queue<ConsumerCommand> commandQueue;
  private final Consumer<K, V> kafkaConsumer;
  private final String topic;
  private final Function<V, V> processor;
  private final ExecutorService virtualThreadExecutor;
  private final Duration pollTimeout;
  private final MessageSink<K, V> messageSink;
  private final AtomicReference<Thread> consumerThread = new AtomicReference<>();
  private final Duration waitForMessagesTimeout;
  private final Duration threadTerminationTimeout;
  private final Duration executorTerminationTimeout;
  private final OffsetManager<K, V> offsetManager;
  private final ConsumerRebalanceListener rebalanceListener;
  private final java.util.function.Consumer<ProcessingError<K, V>> errorHandler;
  private final int maxRetries;
  private final Duration retryBackoff;
  private final AtomicReference<ConsumerState> state = new AtomicReference<>(ConsumerState.CREATED);
  private final boolean enableMetrics;
  private final boolean sequentialProcessing;
  private final Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();
  private final BackpressureController backpressureController;
  private final AtomicLong inFlightCount = new AtomicLong(0);
  private long backpressurePauseStartTime;

  /// Represents an error that occurred during record processing. Contains the original record,
  /// the exception that was thrown, and the number of retry attempts made.
  ///
  /// @param <K> the type of the record key
  /// @param <V> the type of the record value
  /// @param record the Kafka record that failed processing
  /// @param exception the exception that occurred during processing
  /// @param retryCount the number of retry attempts made
  public record ProcessingError<K, V>(ConsumerRecord<K, V> record, Exception exception, int retryCount) {}

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
    private Function<V, V> processor;
    private Duration pollTimeout = Duration.ofMillis(100);
    private java.util.function.Consumer<ProcessingError<K, V>> errorHandler = error -> {
      LOGGER.log(
        Level.WARNING,
        "Processing failed for record (topic=%s, partition=%d, offset=%d) after %d retries: %s".formatted(
            error.record().topic(),
            error.record().partition(),
            error.record().offset(),
            error.retryCount(),
            error.exception().getMessage()
          ),
        error.exception()
      );
    };
    private int maxRetries = 0;
    private Duration retryBackoff = Duration.ofMillis(500);
    private boolean enableMetrics = true;
    private boolean sequentialProcessing = false;
    private MessageSink<K, V> messageSink;
    private Duration waitForMessagesTimeout = AppConfig.DEFAULT_WAIT_FOR_MESSAGES;
    private Duration threadTerminationTimeout = AppConfig.DEFAULT_THREAD_TERMINATION;
    private Duration executorTerminationTimeout = AppConfig.DEFAULT_EXECUTOR_TERMINATION;
    private OffsetManager<K, V> offsetManager;
    private Function<Consumer<K, V>, OffsetManager<K, V>> offsetManagerProvider;
    private Supplier<Consumer<K, V>> consumerProvider;
    private Queue<ConsumerCommand> commandQueue;
    private ConsumerRebalanceListener rebalanceListener;
    private BackpressureController backpressureController;

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

    /// Sets the function to process each consumed message value.
    ///
    /// @param processor The function that transforms message values
    /// @return This builder instance for method chaining
    public Builder<K, V> withProcessor(final Function<V, V> processor) {
      this.processor = processor;
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
    public Builder<K, V> withErrorHandler(final java.util.function.Consumer<ProcessingError<K, V>> handler) {
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
    public Builder<K, V> withMetrics(final boolean enable) {
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
    public Builder<K, V> withMessageSink(final MessageSink<K, V> messageSink) {
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
      this.offsetManagerProvider =
        consumer -> {
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
    /// <p>Backpressure is disabled by default. Calling this method enables it.
    ///
    /// @return This builder instance for method chaining
    public Builder<K, V> withBackpressure() {
      return withBackpressure(10_000, 7_000);
    }

    /// Enables backpressure control using the given high and low watermarks. When the number of
    /// in-flight messages reaches the high watermark, the consumer pauses Kafka polling. It
    /// resumes when the count drops to or below the low watermark (hysteresis).
    ///
    /// <p>Backpressure is disabled by default. Calling this method enables it.
    ///
    /// @param highWatermark pause consumption when in-flight count reaches this value (must be
    ///     positive)
    /// @param lowWatermark  resume consumption when in-flight count drops to this value (must be
    ///     less than highWatermark)
    /// @return This builder instance for method chaining
    public Builder<K, V> withBackpressure(final long highWatermark, final long lowWatermark) {
      this.backpressureController = new BackpressureController(highWatermark, lowWatermark);
      return this;
    }

    /// Builds a new KPipeConsumer with the configured settings.
    ///
    /// @return a new KPipeConsumer instance
    public KPipeConsumer<K, V> build() {
      Objects.requireNonNull(kafkaProps, "Kafka properties must be provided");
      Objects.requireNonNull(topic, "Topic must be provided");
      Objects.requireNonNull(processor, "Processor function must be provided");
      if (maxRetries < 0) throw new IllegalArgumentException("Max retries cannot be negative");
      if (pollTimeout.isNegative() || pollTimeout.isZero()) throw new IllegalArgumentException(
        "Poll timeout must be positive"
      );
      if (offsetManager != null || offsetManagerProvider != null) kafkaProps.setProperty("enable.auto.commit", "false");
      if (backpressureController != null && sequentialProcessing) throw new IllegalStateException(
        "Backpressure is not compatible with sequential processing: in sequential mode in-flight count is always ≤ 1"
      );
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
    this.processor = Objects.requireNonNull(builder.processor);
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
        : builder.offsetManagerProvider != null ? builder.offsetManagerProvider.apply(this.kafkaConsumer) : null;
    this.commandQueue = builder.commandQueue != null ? builder.commandQueue : new ConcurrentLinkedQueue<>();
    this.rebalanceListener = builder.rebalanceListener != null ? builder.rebalanceListener : null;
    this.backpressureController = builder.backpressureController;

    initializeMetrics();
  }

  private void initializeMetrics() {
    if (enableMetrics) {
      metrics.put(METRIC_MESSAGES_RECEIVED, new AtomicLong(0));
      metrics.put(METRIC_MESSAGES_PROCESSED, new AtomicLong(0));
      metrics.put(METRIC_PROCESSING_ERRORS, new AtomicLong(0));
      metrics.put(METRIC_RETRIES, new AtomicLong(0));
      if (backpressureController != null) {
        metrics.put(METRIC_BACKPRESSURE_PAUSE_COUNT, new AtomicLong(0));
        metrics.put(METRIC_BACKPRESSURE_TIME_MS, new AtomicLong(0));
      }
    }
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

    return MessageTracker
      .builder()
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

    if (!state.compareAndSet(ConsumerState.CREATED, ConsumerState.RUNNING)) {
      LOGGER.log(Level.WARNING, "Consumer already running for topic {0}", topic);
      return;
    }

    if (offsetManager != null) offsetManager.start();

    if (rebalanceListener != null) kafkaConsumer.subscribe(
      List.of(topic),
      rebalanceListener
    ); else kafkaConsumer.subscribe(List.of(topic));

    Thread.UncaughtExceptionHandler exceptionHandler = (thread, throwable) -> {
      LOGGER.log(Level.ERROR, "Uncaught exception in consumer thread: " + thread.getName(), throwable);
      state.set(ConsumerState.CLOSING);
    };

    final var thread = Thread
      .ofVirtual()
      .name("kafka-consumer-%s-%s".formatted(topic, UUID.randomUUID().toString().substring(0, 8)))
      .uncaughtExceptionHandler(exceptionHandler)
      .start(() -> {
        try {
          while (isRunning()) {
            processCommands();
            checkBackpressure();

            if (!isRunning()) break;

            if (isPaused()) {
              Thread.sleep(100);
              continue;
            }

            final var records = pollRecords();
            if (records != null && !records.isEmpty()) processRecords(records);
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.log(Level.INFO, "Consumer thread interrupted for topic {0}", topic);
        } catch (final Exception e) {
          LOGGER.log(Level.WARNING, "Error in consumer thread", e);
          throw e;
        } finally {
          try {
            kafkaConsumer.close();
            state.set(ConsumerState.CLOSED);
            LOGGER.log(Level.INFO, "Consumer closed for topic {0}", topic);
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error closing Kafka consumer", e);
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
    final var currentState = state.get();

    // Don't send a pause command if already paused or closed
    if (currentState == ConsumerState.PAUSED || currentState == ConsumerState.CLOSED) {
      LOGGER.log(Level.INFO, "Consumer already paused or closed for topic {0}", topic);
      return;
    }

    // Always add command to the queue for proper test verification
    commandQueue.offer(ConsumerCommand.PAUSE);
    LOGGER.log(Level.INFO, "Consumer pause requested for topic {0}", topic);

    // Update state if not closed or closing
    if (currentState != ConsumerState.CLOSING) {
      state.set(ConsumerState.PAUSED);
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
  public void processCommands() {
    ConsumerCommand command;
    while ((command = commandQueue.poll()) != null) {
      try {
        switch (command) {
          case PAUSE -> {
            kafkaConsumer.pause(kafkaConsumer.assignment());
            LOGGER.log(Level.INFO, "Consumer paused for topic {0}", topic);
          }
          case RESUME -> {
            kafkaConsumer.resume(kafkaConsumer.assignment());
            LOGGER.log(Level.INFO, "Consumer resumed for topic {0}", topic);
          }
          case CLOSE -> {
            state.set(ConsumerState.CLOSING);
            LOGGER.log(Level.INFO, "Consumer shutdown initiated for topic {0}", topic);
          }
          case TRACK_OFFSET -> {
            if (offsetManager != null && command.getRecord() != null) {
              @SuppressWarnings("unchecked")
              final var record = (ConsumerRecord<K, V>) command.getRecord();
              offsetManager.trackOffset(record);
            }
          }
          case MARK_OFFSET_PROCESSED -> {
            if (offsetManager != null && command.getRecord() != null) {
              @SuppressWarnings("unchecked")
              final var record = (ConsumerRecord<K, V>) command.getRecord();
              offsetManager.markOffsetProcessed(record);
            }
          }
          case COMMIT_OFFSETS -> {
            if (command.getOffsets() != null) {
              try {
                kafkaConsumer.commitSync(command.getOffsets());
                if (offsetManager != null && command.getCommitId() != null) offsetManager.notifyCommitComplete(
                  command.getCommitId(),
                  true
                );
              } catch (final Exception e) {
                LOGGER.log(Level.WARNING, "Failed to commit offsets", e);
                if (offsetManager != null && command.getCommitId() != null) {
                  offsetManager.notifyCommitComplete(command.getCommitId(), false);
                }
              }
            }
          }
        }
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error processing consumer command: {0}", command);
      }
    }
  }

  /// Resumes consumption from the topic after being paused.
  ///
  /// <p>This method is idempotent - calling it multiple times has no additional effect.
  ///
  /// @throws IllegalStateException if the consumer has been closed
  public void resume() {
    final var currentState = state.get();

    if (currentState == ConsumerState.CLOSED) throw new IllegalStateException("Cannot resume a closed consumer");

    // Don't send a resume command if already running
    if (currentState == ConsumerState.RUNNING) {
      LOGGER.log(Level.INFO, "Consumer already running for topic {0}", topic);
      return;
    }

    // Always add command to the queue for proper test verification
    commandQueue.offer(ConsumerCommand.RESUME);
    LOGGER.log(Level.INFO, "Consumer resume requested for topic {0}", topic);

    // Update state if not closing or closed
    if (currentState != ConsumerState.CLOSING) state.set(ConsumerState.RUNNING);
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
  /// @return a map of metric names to their current values, or an empty map if metrics are disabled
  public Map<String, Long> getMetrics() {
    if (!enableMetrics) return Map.of();
    final var snapshot = metrics.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
    snapshot.put("inFlight", inFlightCount.get());
    return snapshot;
  }

  /// Returns whether the consumer is running.
  ///
  /// @return `true` if the consumer is running, `false` if it has been closed or not
  ///     started
  public boolean isRunning() {
    return (state.get() == ConsumerState.RUNNING || state.get() == ConsumerState.PAUSED);
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
    // Only proceed if not already closed or closing
    if (
      !state.compareAndSet(ConsumerState.RUNNING, ConsumerState.CLOSING) &&
      !state.compareAndSet(ConsumerState.PAUSED, ConsumerState.CLOSING)
    ) {
      return; // Already closed or closing
    }

    // Create a tracker first to avoid missing in-flight messages
    final var tracker = createTrackerIfEnabled(waitForMessagesTimeout.toMillis());

    // Signal shutdown
    signalShutdown();

    // Wait for in-flight messages
    waitForInFlightMessages(tracker, waitForMessagesTimeout.toMillis());

    // Wake up consumer and wait for thread termination
    wakeupAndWaitForConsumerThread(threadTerminationTimeout.toMillis());

    // Shutdown executor
    shutdownExecutor(executorTerminationTimeout.toMillis());

    // Shutdown offset manager if enabled
    closeOffsetManager();

    // Ensure the state is set to CLOSED
    state.set(ConsumerState.CLOSED);
  }

  /// Processes multiple Kafka records by submitting each one to the virtual thread executor.
  ///
  /// @param records the batch of records to process
  protected void processRecords(final ConsumerRecords<K, V> records) {
    if (sequentialProcessing) {
      // Process sequentially for cases where order matters
      for (final var record : records.records(topic)) {
        // Track offset before processing
        if (offsetManager != null) commandQueue.offer(ConsumerCommand.TRACK_OFFSET.withRecord(record));
        inFlightCount.incrementAndGet();
        processRecord(record);
      }
    } else {
      // Process in parallel using virtual threads
      final var topicRecords = records.records(topic);
      for (final var record : topicRecords) {
        // Track offset before submitting to virtual thread
        if (offsetManager != null) commandQueue.offer(ConsumerCommand.TRACK_OFFSET.withRecord(record));
        inFlightCount.incrementAndGet();
        try {
          virtualThreadExecutor.submit(() -> processRecord(record));
        } catch (final RejectedExecutionException e) {
          // Handle task rejection (typically during shutdown)
          inFlightCount.decrementAndGet();
          if (isRunning()) {
            LOGGER.log(Level.WARNING, "Task submission rejected, likely during shutdown", e);
            if (enableMetrics) metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
            errorHandler.accept(new ProcessingError<>(record, e, 0));
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

    try {
      for (int attempt = 0; attempt <= maxRetries; attempt++) {
        if (attempt > 0) {
          if (enableMetrics) metrics.get(METRIC_RETRIES).incrementAndGet();
          LOGGER.log(
            Level.INFO,
            "Retrying message at offset %d (attempt %d of %d)".formatted(record.offset(), attempt, maxRetries)
          );

          try {
            Thread.sleep(retryBackoff.toMillis());
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            return; // Interrupts must not mark the record as processed.
          }
        }

        try {
          final var processedValue = processor.apply(record.value());
          messageSink.send(record, processedValue);
          if (enableMetrics) metrics.get(METRIC_MESSAGES_PROCESSED).incrementAndGet();
          if (offsetManager != null) commandQueue.offer(ConsumerCommand.MARK_OFFSET_PROCESSED.withRecord(record));
          return;
        } catch (final Exception e) {
          if (isInterruptionRelated(e)) {
            Thread.currentThread().interrupt();
            return; // Interrupt-like failures should be retried after restart/rebalance.
          }

          if (attempt == maxRetries) {
            if (enableMetrics) metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
            LOGGER.log(
              Level.WARNING,
              "Failed to process message at offset %d after %d attempts: %s".formatted(
                  record.offset(),
                  maxRetries + 1,
                  e.getMessage()
                ),
              e
            );
            errorHandler.accept(new ProcessingError<>(record, e, maxRetries));
            if (offsetManager != null) commandQueue.offer(ConsumerCommand.MARK_OFFSET_PROCESSED.withRecord(record));
            return;
          }
        }
      }
    } finally {
      inFlightCount.decrementAndGet();
    }
  }

  private void checkBackpressure() {
    if (backpressureController == null) return;
    final long inFlight = inFlightCount.get();

    switch (backpressureController.check(inFlight, isPaused())) {
      case PAUSE -> {
        LOGGER.log(
          Level.WARNING,
          "Backpressure triggered: pausing consumer (in-flight=%d, highWatermark=%d) for topic %s".formatted(
              inFlight,
              backpressureController.highWatermark(),
              topic
            )
        );
        if (enableMetrics) metrics.get(METRIC_BACKPRESSURE_PAUSE_COUNT).incrementAndGet();
        backpressurePauseStartTime = System.currentTimeMillis();
        pause();
      }
      case RESUME -> {
        final long pauseDurationMs = System.currentTimeMillis() - backpressurePauseStartTime;
        if (enableMetrics) metrics.get(METRIC_BACKPRESSURE_TIME_MS).addAndGet(pauseDurationMs);
        LOGGER.log(
          Level.INFO,
          "Backpressure resolved: resuming consumer (paused for %d ms, in-flight=%d) for topic %s".formatted(
              pauseDurationMs,
              inFlight,
              topic
            )
        );
        resume();
      }
      case NONE -> {}
    }
  }

  private static boolean isInterruptionRelated(final Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof InterruptedException || current instanceof ClosedByInterruptException) return true;
      current = current.getCause();
    }
    return false;
  }

  private ConsumerRecords<K, V> pollRecords() {
    return Optional
      .ofNullable(kafkaConsumer)
      .map(consumer -> {
        try {
          return consumer.poll(pollTimeout);
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
      })
      .orElse(null);
  }

  private MessageTracker createTrackerIfEnabled(final long waitForMessagesMs) {
    return (waitForMessagesMs > 0 && enableMetrics) ? createMessageTracker() : null;
  }

  private void signalShutdown() {
    pause();
    commandQueue.offer(ConsumerCommand.CLOSE);
  }

  private void waitForInFlightMessages(final MessageTracker tracker, final long waitForMessagesMs) {
    Optional
      .ofNullable(tracker)
      .ifPresent(t -> {
        try {
          long inFlightCount = t.getInFlightMessageCount();
          if (inFlightCount > 0) {
            LOGGER.log(Level.INFO, "Waiting for %d in-flight messages to complete".formatted(inFlightCount));
            t.waitForCompletion(waitForMessagesMs);
          }
        } catch (Exception e) {
          LOGGER.log(Level.WARNING, "Error waiting for in-flight messages", e);
        }
      });
  }

  private void wakeupAndWaitForConsumerThread(final long threadTerminationMs) {
    // Safe wakeup of the consumer
    Optional
      .ofNullable(kafkaConsumer)
      .ifPresent(consumer -> {
        try {
          consumer.wakeup();
        } catch (Exception e) {
          LOGGER.log(Level.WARNING, "Error during consumer wakeup", e);
        }
      });

    // Wait for thread termination
    Optional
      .ofNullable(consumerThread.get())
      .filter(Thread::isAlive)
      .ifPresent(thread -> {
        try {
          thread.join(threadTerminationMs);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.log(Level.WARNING, "Interrupted while waiting for consumer thread");
        }
      });
  }

  private void shutdownExecutor(final long executorTerminationMs) {
    try {
      virtualThreadExecutor.shutdown();
      if (!virtualThreadExecutor.awaitTermination(executorTerminationMs, TimeUnit.MILLISECONDS)) {
        LOGGER.log(Level.WARNING, "Not all processing tasks completed during shutdown");
        final var pending = virtualThreadExecutor.shutdownNow();
        LOGGER.log(Level.WARNING, "%d tasks were not processed".formatted(pending.size()));
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      virtualThreadExecutor.shutdownNow();
      LOGGER.log(Level.WARNING, "Interrupted while waiting for executor termination");
    }
  }

  private void closeOffsetManager() {
    if (offsetManager != null) {
      try {
        offsetManager.close();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error closing offset manager", e);
      }
    }
  }
}
