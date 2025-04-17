package org.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.kpipe.config.AppConfig;
import org.kpipe.sink.LoggingSink;
import org.kpipe.sink.MessageSink;

/**
 * A high-performance Kafka consumer that processes messages through virtual threads.
 *
 * <p>This class provides a simplified interface for consuming Kafka messages and processing them
 * using a functional approach. Each record is processed asynchronously in a dedicated virtual
 * thread, allowing for high throughput and efficient resource utilization.
 *
 * <p>Features include:
 *
 * <ul>
 *   <li>Functional processing with a simple {@code Function<V, V>} interface
 *   <li>Asynchronous processing using virtual threads
 *   <li>Configurable error handling and retry mechanisms
 *   <li>Pause/resume capabilities for flow control
 *   <li>Built-in metrics collection
 *   <li>Graceful shutdown with in-flight message tracking
 *   <li>Optional sequential processing mode for order-sensitive workloads
 *   <li>Configurable message sink for processed record handling
 *   <li>Flexible timeout configurations for graceful shutdown stages
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * var props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("group.id", "my-group");
 * props.put("key.deserializer", StringDeserializer.class.getName());
 * props.put("value.deserializer", StringDeserializer.class.getName());
 *
 * var consumer = new FunctionalConsumer.Builder<String, String>()
 *     .withProperties(props)
 *     .withTopic("my-topic")
 *     .withProcessor(value -> value.toUpperCase())
 *     .withRetry(3, Duration.ofSeconds(1))
 *     .build();
 *
 * consumer.start();
 * // Later when finished
 * consumer.close();
 * }</pre>
 *
 * @param <K> the type of keys in the consumed records
 * @param <V> the type of values in the consumed records
 */
public class FunctionalConsumer<K, V> implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(FunctionalConsumer.class.getName());

  // Metric key constants
  private static final String METRIC_MESSAGES_RECEIVED = "messagesReceived";
  private static final String METRIC_MESSAGES_PROCESSED = "messagesProcessed";
  private static final String METRIC_PROCESSING_ERRORS = "processingErrors";
  private static final String METRIC_RETRIES = "retries";

  private final Queue<ConsumerCommand> commandQueue = new ConcurrentLinkedQueue<>();

  private final KafkaConsumer<K, V> consumer;
  private final String topic;
  private final Function<V, V> processor;
  private final ExecutorService virtualThreadExecutor;
  private final Duration pollTimeout;
  private final MessageSink<K, V> messageSink;
  private final AtomicReference<Thread> consumerThread = new AtomicReference<>();
  private final Duration waitForMessagesTimeout;
  private final Duration threadTerminationTimeout;
  private final Duration executorTerminationTimeout;

  private Consumer<ProcessingError<K, V>> errorHandler = error -> {
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
  private final int maxRetries;
  private final Duration retryBackoff;

  private final AtomicReference<ConsumerState> state = new AtomicReference<>(ConsumerState.CREATED);

  private final boolean enableMetrics;
  private final boolean sequentialProcessing;
  private final Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();

  /**
   * Represents an error that occurred during record processing. Contains the original record, the
   * exception that was thrown, and the number of retry attempts made.
   *
   * @param <K> the type of the record key
   * @param <V> the type of the record value
   * @param record the Kafka record that failed processing
   * @param exception the exception that occurred during processing
   * @param retryCount the number of retry attempts made
   */
  public record ProcessingError<K, V>(ConsumerRecord<K, V> record, Exception exception, int retryCount) {}

  /**
   * Builder for creating and configuring {@link FunctionalConsumer} instances.
   *
   * @param <K> the type of keys in the consumed records
   * @param <V> the type of values in the consumed records
   */
  public static class Builder<K, V> {

    private Properties kafkaProps;
    private String topic;
    private Function<V, V> processor;
    private Duration pollTimeout = Duration.ofMillis(100);
    private Consumer<ProcessingError<K, V>> errorHandler;
    private int maxRetries = 0;
    private Duration retryBackoff = Duration.ofMillis(500);
    private boolean enableMetrics = true;
    private boolean sequentialProcessing = false;
    private MessageSink<K, V> messageSink;
    private Duration waitForMessagesTimeout = AppConfig.DEFAULT_WAIT_FOR_MESSAGES;
    private Duration threadTerminationTimeout = AppConfig.DEFAULT_THREAD_TERMINATION;
    private Duration executorTerminationTimeout = AppConfig.DEFAULT_EXECUTOR_TERMINATION;

    public Builder<K, V> withProperties(final Properties props) {
      this.kafkaProps = props;
      return this;
    }

    public Builder<K, V> withTopic(final String topic) {
      this.topic = topic;
      return this;
    }

    public Builder<K, V> withProcessor(final Function<V, V> processor) {
      this.processor = processor;
      return this;
    }

    public Builder<K, V> withPollTimeout(final Duration timeout) {
      this.pollTimeout = timeout;
      return this;
    }

    public Builder<K, V> withErrorHandler(final Consumer<ProcessingError<K, V>> handler) {
      this.errorHandler = handler;
      return this;
    }

    public Builder<K, V> withRetry(final int maxRetries, final Duration backoff) {
      this.maxRetries = maxRetries;
      this.retryBackoff = backoff;
      return this;
    }

    public Builder<K, V> withMetrics(final boolean enable) {
      this.enableMetrics = enable;
      return this;
    }

    public Builder<K, V> withSequentialProcessing(final boolean sequential) {
      this.sequentialProcessing = sequential;
      return this;
    }

    public Builder<K, V> withMessageSink(final MessageSink<K, V> messageSink) {
      this.messageSink = messageSink;
      return this;
    }

    public Builder<K, V> withWaitForMessagesTimeout(final Duration timeout) {
      this.waitForMessagesTimeout = Objects.requireNonNull(timeout);
      return this;
    }

    public Builder<K, V> withThreadTerminationTimeout(final Duration timeout) {
      this.threadTerminationTimeout = Objects.requireNonNull(timeout);
      return this;
    }

    public Builder<K, V> withExecutorTerminationTimeout(final Duration timeout) {
      this.executorTerminationTimeout = Objects.requireNonNull(timeout);
      return this;
    }

    /**
     * Builds a new FunctionalConsumer with the configured settings.
     *
     * @return a new FunctionalConsumer instance
     * @throws IllegalArgumentException if any required parameters are invalid
     * @throws NullPointerException if any required parameters are null
     */
    public FunctionalConsumer<K, V> build() {
      Objects.requireNonNull(kafkaProps, "Kafka properties must be provided");
      Objects.requireNonNull(topic, "Topic must be provided");
      Objects.requireNonNull(processor, "Processor function must be provided");

      if (maxRetries < 0) {
        throw new IllegalArgumentException("Max retries cannot be negative");
      }

      if (pollTimeout.isNegative() || pollTimeout.isZero()) {
        throw new IllegalArgumentException("Poll timeout must be positive");
      }

      return new FunctionalConsumer<>(this);
    }
  }

  /**
   * Creates a new FunctionalConsumer using the provided builder.
   *
   * @param builder the builder containing the consumer configuration
   */
  public FunctionalConsumer(final Builder<K, V> builder) {
    this.consumer = createConsumer(Objects.requireNonNull(builder.kafkaProps));
    this.topic = Objects.requireNonNull(builder.topic);
    this.processor = Objects.requireNonNull(builder.processor);
    this.pollTimeout = Objects.requireNonNull(builder.pollTimeout);
    this.errorHandler = builder.errorHandler;
    this.maxRetries = builder.maxRetries;
    this.retryBackoff = builder.retryBackoff;
    this.enableMetrics = builder.enableMetrics;
    this.sequentialProcessing = builder.sequentialProcessing;
    this.messageSink = builder.messageSink != null ? builder.messageSink : new LoggingSink<>();
    this.waitForMessagesTimeout = builder.waitForMessagesTimeout;
    this.threadTerminationTimeout = builder.threadTerminationTimeout;
    this.executorTerminationTimeout = builder.executorTerminationTimeout;
    this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    initializeMetrics();
  }

  private void initializeMetrics() {
    if (enableMetrics) {
      metrics.put(METRIC_MESSAGES_RECEIVED, new AtomicLong(0));
      metrics.put(METRIC_MESSAGES_PROCESSED, new AtomicLong(0));
      metrics.put(METRIC_PROCESSING_ERRORS, new AtomicLong(0));
      metrics.put(METRIC_RETRIES, new AtomicLong(0));
    }
  }

  /**
   * Creates a message tracker that can monitor the state of in-flight messages. The tracker uses
   * the consumer's metrics to determine how many messages have been received versus processed.
   *
   * @return a new {@link MessageTracker} instance, or null if metrics are disabled
   */
  public MessageTracker createMessageTracker() {
    if (!enableMetrics) {
      LOGGER.log(Level.INFO, "Cannot create MessageTracker: metrics are disabled");
      return null;
    }

    return MessageTracker
      .builder()
      .withMetricsSupplier(this::getMetrics)
      .withReceivedMetricKey(METRIC_MESSAGES_RECEIVED)
      .withProcessedMetricKey(METRIC_MESSAGES_PROCESSED)
      .withErrorsMetricKey(METRIC_PROCESSING_ERRORS)
      .build();
  }

  /**
   * Starts the consumer thread and begins consuming messages from the configured topic. The
   * consumer will poll for records and process them asynchronously in virtual threads.
   *
   * @throws IllegalStateException if the consumer has already been started or was previously closed
   */
  public void start() {
    if (state.get() == ConsumerState.CLOSED) {
      throw new IllegalStateException("Cannot restart a closed consumer");
    }

    if (!state.compareAndSet(ConsumerState.CREATED, ConsumerState.RUNNING)) {
      LOGGER.log(Level.WARNING, "Consumer already running for topic {0}", topic);
      return;
    }

    consumer.subscribe(List.of(topic));

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

            if (!isRunning()) {
              break;
            }

            if (isPaused()) {
              Thread.sleep(100);
              continue;
            }

            final var records = pollRecords();
            if (records != null && !records.isEmpty()) {
              processRecords(records);
            }
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.log(Level.INFO, "Consumer thread interrupted for topic {0}", topic);
        } catch (final Exception e) {
          LOGGER.log(Level.WARNING, "Error in consumer thread", e);
          throw e;
        } finally {
          try {
            consumer.close();
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

  /**
   * Pauses consumption from the topic. Any in-flight messages will continue processing, but no new
   * messages will be consumed until {@link #resume()} is called.
   *
   * <p>This method is idempotent - calling it multiple times has no additional effect.
   */
  public void pause() {
    final var currentState = state.get();

    // Don't send pause command if already paused or closed
    if (currentState == ConsumerState.PAUSED || currentState == ConsumerState.CLOSED) {
      LOGGER.log(Level.INFO, "Consumer already paused or closed for topic {0}", topic);
      return;
    }

    // Always add command to queue for proper test verification
    commandQueue.offer(ConsumerCommand.PAUSE);
    LOGGER.log(Level.INFO, "Consumer pause requested for topic {0}", topic);

    // Update state if not closed or closing
    if (currentState != ConsumerState.CLOSING) {
      state.set(ConsumerState.PAUSED);
    }
  }

  /**
   * Processes pending commands from the command queue.
   *
   * <p>This method polls commands from the internal command queue and executes the corresponding
   * actions:
   *
   * <ul>
   *   <li>{@code PAUSE} - Pauses consumption by calling {@code consumer.pause()}
   *   <li>{@code RESUME} - Resumes consumption by calling {@code consumer.resume()}
   *   <li>{@code CLOSE} - Initiates shutdown by setting the running flag to false
   * </ul>
   *
   * <p>Commands are processed in the order they were submitted to the queue. If an exception occurs
   * while processing a command, it will be caught and logged, allowing subsequent commands to be
   * processed.
   */
  public void processCommands() {
    ConsumerCommand command;
    while ((command = commandQueue.poll()) != null) {
      try {
        switch (command) {
          case PAUSE -> {
            consumer.pause(consumer.assignment());
            LOGGER.log(Level.INFO, "Consumer paused for topic {0}", topic);
          }
          case RESUME -> {
            consumer.resume(consumer.assignment());
            LOGGER.log(Level.INFO, "Consumer resumed for topic {0}", topic);
          }
          case CLOSE -> {
            state.set(ConsumerState.CLOSING);
            LOGGER.log(Level.INFO, "Consumer shutdown initiated for topic {0}", topic);
          }
        }
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error processing consumer command: {0}", command);
      }
    }
  }

  /**
   * Resumes consumption from the topic after being paused.
   *
   * <p>This method is idempotent - calling it multiple times has no additional effect.
   *
   * @throws IllegalStateException if the consumer has been closed
   */
  public void resume() {
    final var currentState = state.get();

    if (currentState == ConsumerState.CLOSED) {
      throw new IllegalStateException("Cannot resume a closed consumer");
    }

    // Don't send resume command if already running
    if (currentState == ConsumerState.RUNNING) {
      LOGGER.log(Level.INFO, "Consumer already running for topic {0}", topic);
      return;
    }

    // Always add command to queue for proper test verification
    commandQueue.offer(ConsumerCommand.RESUME);
    LOGGER.log(Level.INFO, "Consumer resume requested for topic {0}", topic);

    // Update state if not closing or closed
    if (currentState != ConsumerState.CLOSING) {
      state.set(ConsumerState.RUNNING);
    }
  }

  /**
   * Returns whether the consumer is currently paused.
   *
   * @return {@code true} if the consumer is paused, {@code false} otherwise
   */
  public boolean isPaused() {
    return state.get() == ConsumerState.PAUSED;
  }

  /**
   * Returns a snapshot of the current metrics collected by this consumer.
   *
   * <p>Available metrics include:
   *
   * <ul>
   *   <li>{@code messagesReceived} - count of records received from Kafka
   *   <li>{@code messagesProcessed} - count of records successfully processed
   *   <li>{@code processingErrors} - count of records that failed processing after all retries
   *   <li>{@code retries} - count of retry attempts made for failed records
   * </ul>
   *
   * @return a map of metric names to their current values, or an empty map if metrics are disabled
   */
  public Map<String, Long> getMetrics() {
    if (!enableMetrics) {
      return Map.of();
    }

    return metrics.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
  }

  /**
   * Returns whether the consumer is running.
   *
   * @return {@code true} if the consumer is running, {@code false} if it has been closed or not
   *     started
   */
  public boolean isRunning() {
    return state.get() == ConsumerState.RUNNING || state.get() == ConsumerState.PAUSED;
  }

  /**
   * Closes this consumer, stopping message consumption and processing.
   *
   * <p>This method performs a graceful shutdown by:
   *
   * <ol>
   *   <li>Setting the running flag to false to stop the main loop
   *   <li>Pausing consumption to prevent new records from being processed
   *   <li>Using a MessageTracker to wait for in-flight messages to complete
   *   <li>Waking up the consumer to unblock any polling operation
   *   <li>Waiting for the consumer thread to finish
   *   <li>Shutting down the executor and waiting for processing tasks to complete
   * </ol>
   *
   * <p>This method is idempotent - calling it multiple times has no additional effect.
   */
  @Override
  public void close() {
    // Only proceed if not already closed or closing
    if (
      !state.compareAndSet(ConsumerState.RUNNING, ConsumerState.CLOSING) &&
      !state.compareAndSet(ConsumerState.PAUSED, ConsumerState.CLOSING)
    ) {
      return; // Already closed or closing
    }

    // Create tracker first to avoid missing in-flight messages
    final var tracker = createTrackerIfEnabled(waitForMessagesTimeout.toMillis());

    // Signal shutdown
    signalShutdown();

    // Wait for in-flight messages
    waitForInFlightMessages(tracker, waitForMessagesTimeout.toMillis());

    // Wake up consumer and wait for thread termination
    wakeupAndWaitForConsumerThread(threadTerminationTimeout.toMillis());

    // Shutdown executor
    shutdownExecutor(executorTerminationTimeout.toMillis());

    // Ensure state is set to CLOSED
    state.set(ConsumerState.CLOSED);
  }

  /**
   * Creates a Kafka consumer with the provided properties. Protected for testing purposes to allow
   * mock consumers.
   *
   * @param kafkaProps the Kafka client properties
   * @return a new KafkaConsumer instance
   */
  protected KafkaConsumer<K, V> createConsumer(final Properties kafkaProps) {
    return new KafkaConsumer<>(kafkaProps);
  }

  /**
   * Processes multiple Kafka records by submitting each one to the virtual thread executor.
   *
   * @param records the batch of records to process
   */
  protected void processRecords(final ConsumerRecords<K, V> records) {
    if (sequentialProcessing) {
      // Process sequentially for cases where order matters
      for (final var record : records.records(topic)) {
        processRecord(record);
      }
    } else {
      // Process in parallel using virtual threads
      StreamSupport
        .stream(records.records(topic).spliterator(), false)
        .forEach(record -> {
          try {
            virtualThreadExecutor.submit(() -> processRecord(record));
          } catch (final RejectedExecutionException e) {
            // Handle task rejection (typically during shutdown)
            if (isRunning()) {
              LOGGER.log(Level.WARNING, "Task submission rejected, likely during shutdown", e);
              if (enableMetrics) {
                metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
              }
              errorHandler.accept(new ProcessingError<>(record, e, 0));
            }
          }
        });
    }
  }

  /**
   * Processes a single Kafka consumer record using the configured processor function.
   *
   * <p>This method applies the processor function to transform the record value while handling
   * exceptions with configurable retry logic. Processing occurs in the current virtual thread
   * without blocking operations that would impact carrier thread performance.
   *
   * <p>Metrics tracked during processing:
   *
   * <ul>
   *   <li>messagesReceived - Incremented when a record is received
   *   <li>messagesProcessed - Incremented for successful processing
   *   <li>retries - Incremented for each retry attempt (not counting initial attempt)
   *   <li>processingErrors - Incremented when processing fails after all retries
   * </ul>
   *
   * @param record The Kafka consumer record to process
   */
  protected void processRecord(final ConsumerRecord<K, V> record) {
    if (enableMetrics) {
      metrics.get(METRIC_MESSAGES_RECEIVED).incrementAndGet();
    }

    var attempts = 0;

    while (attempts <= maxRetries) {
      if (enableMetrics && attempts > 0) {
        metrics.get(METRIC_RETRIES).incrementAndGet();
      }

      try {
        V processed = processor.apply(record.value());
        messageSink.send(record, processed);

        if (enableMetrics) {
          metrics.get(METRIC_MESSAGES_PROCESSED).incrementAndGet();
        }
        return; // Success
      } catch (final Exception e) {
        attempts++;

        if (attempts > maxRetries) {
          if (enableMetrics) {
            metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
          }

          LOGGER.log(
            Level.WARNING,
            "Failed to process message at offset %d after %d attempts: %s".formatted(
                record.offset(),
                attempts,
                e.getMessage()
              ),
            e
          );
          errorHandler.accept(new ProcessingError<>(record, e, maxRetries));
          break;
        }

        try {
          Thread.sleep(retryBackoff.toMillis());
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }

        LOGGER.log(
          Level.INFO,
          "Retrying message at offset %d (attempt %d of %d)".formatted(record.offset(), attempts, maxRetries)
        );
      }
    }
  }

  private ConsumerRecords<K, V> pollRecords() {
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
      if (isRunning()) {
        LOGGER.log(Level.WARNING, "Error during Kafka poll operation", e);
      }
      return null;
    }
  }

  private MessageTracker createTrackerIfEnabled(final long waitForMessagesMs) {
    if (waitForMessagesMs > 0 && enableMetrics) {
      return createMessageTracker();
    }
    return null;
  }

  private void signalShutdown() {
    pause();
    commandQueue.offer(ConsumerCommand.CLOSE);
  }

  private void waitForInFlightMessages(final MessageTracker tracker, final long waitForMessagesMs) {
    if (tracker != null) {
      try {
        long inFlightCount = tracker.getInFlightMessageCount();
        if (inFlightCount > 0) {
          LOGGER.log(Level.INFO, "Waiting for %d in-flight messages to complete".formatted(inFlightCount));
          tracker.waitForCompletion(waitForMessagesMs);
        }
      } catch (Exception e) {
        LOGGER.log(Level.WARNING, "Error waiting for in-flight messages", e);
      }
    }
  }

  private void wakeupAndWaitForConsumerThread(final long threadTerminationMs) {
    // Safe wakeup of the consumer
    final var consumerRef = consumer;
    if (consumerRef != null) {
      try {
        consumerRef.wakeup();
      } catch (Exception e) {
        LOGGER.log(Level.WARNING, "Error during consumer wakeup", e);
      }
    }

    // Wait for thread termination
    final var localThread = consumerThread.get();
    if (localThread != null && localThread.isAlive()) {
      try {
        localThread.join(threadTerminationMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.log(Level.WARNING, "Interrupted while waiting for consumer thread");
      }
    }
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
}
