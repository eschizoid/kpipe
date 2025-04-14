package org.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.kpipe.annotations.VisibleForTesting;
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
  public static final String METRIC_MESSAGES_RECEIVED = "messagesReceived";
  public static final String METRIC_MESSAGES_PROCESSED = "messagesProcessed";
  public static final String METRIC_PROCESSING_ERRORS = "processingErrors";
  public static final String METRIC_RETRIES = "retries";

  private enum ConsumerCommand {
    PAUSE,
    RESUME,
    CLOSE,
  }

  private final Queue<ConsumerCommand> commandQueue = new ConcurrentLinkedQueue<>();

  private final KafkaConsumer<K, V> consumer;
  private final String topic;
  private final Function<V, V> processor;
  private final ExecutorService virtualThreadExecutor;
  private final Duration pollTimeout;
  private final MessageSink<K, V> messageSink;
  private final AtomicReference<Thread> consumerThread = new AtomicReference<>();

  private final Consumer<ProcessingError<K, V>> errorHandler;
  private final int maxRetries;
  private final Duration retryBackoff;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean paused = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);

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
    private Consumer<ProcessingError<K, V>> errorHandler = error -> {};
    private int maxRetries = 0;
    private Duration retryBackoff = Duration.ofMillis(500);
    private boolean enableMetrics = true;
    private boolean sequentialProcessing = false;
    private MessageSink<K, V> messageSink;

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

    ExecutorService executor = null;
    try {
      executor = Executors.newVirtualThreadPerTaskExecutor();
      initializeMetrics();
      this.virtualThreadExecutor = executor;
      executor = null;
    } finally {
      if (executor != null) {
        executor.shutdown();
      }
    }
  }

  /**
   * Creates a new FunctionalConsumer with basic configuration.
   *
   * @param kafkaProps Kafka client properties
   * @param topic the topic to consume from
   * @param processor the function to process each record value
   */
  public FunctionalConsumer(final Properties kafkaProps, final String topic, Function<V, V> processor) {
    this(new Builder<K, V>().withProperties(kafkaProps).withTopic(topic).withProcessor(processor));
  }

  /**
   * Creates a new FunctionalConsumer with custom poll timeout.
   *
   * @param kafkaProps Kafka client properties
   * @param topic the topic to consume from
   * @param processor the function to process each record value
   * @param pollTimeout the duration to wait for records when polling
   */
  public FunctionalConsumer(
    final Properties kafkaProps,
    final String topic,
    final Function<V, V> processor,
    final Duration pollTimeout
  ) {
    this(
      new Builder<K, V>()
        .withProperties(kafkaProps)
        .withTopic(topic)
        .withProcessor(processor)
        .withPollTimeout(pollTimeout)
    );
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
   * Processes pending commands immediately for testing purposes. This method allows tests to verify
   * consumer operations without starting the consumer thread.
   */
  @VisibleForTesting
  public void processCommandsForTest() {
    processCommands();
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
    if (closed.get()) {
      throw new IllegalStateException("Cannot restart a closed consumer");
    }

    if (!running.compareAndSet(false, true)) {
      LOGGER.log(Level.WARNING, "Consumer already running for topic %s".formatted(topic));
      return;
    }

    consumer.subscribe(List.of(topic));

    Thread.UncaughtExceptionHandler exceptionHandler = (thread, throwable) -> {
      LOGGER.log(Level.ERROR, "Uncaught exception in consumer thread: " + thread.getName(), throwable);
      running.set(false);
    };

    final var thread = Thread
      .ofVirtual()
      .name("kafka-consumer-%s-%s".formatted(topic, UUID.randomUUID().toString().substring(0, 8)))
      .uncaughtExceptionHandler(exceptionHandler)
      .start(() -> {
        try {
          while (running.get()) {
            processCommands();

            if (!running.get()) {
              break;
            }

            if (paused.get()) {
              Thread.sleep(100);
              continue;
            }

            final ConsumerRecords<K, V> records = pollRecords();
            if (records != null && !records.isEmpty()) {
              processRecords(records);
            }
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.log(Level.INFO, "Consumer thread interrupted for topic %s".formatted(topic));
        } catch (final Exception e) {
          LOGGER.log(Level.WARNING, "Error in consumer thread", e);
          throw e;
        } finally {
          try {
            consumer.close();
            LOGGER.log(Level.INFO, "Consumer closed for topic %s".formatted(topic));
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error closing Kafka consumer", e);
          }
        }
      });

    consumerThread.set(thread);
    LOGGER.log(Level.INFO, "Consumer started for topic %s".formatted(topic));
  }

  private void processCommands() {
    ConsumerCommand command;
    while ((command = commandQueue.poll()) != null) {
      try {
        switch (command) {
          case PAUSE -> {
            consumer.pause(consumer.assignment());
            LOGGER.log(Level.INFO, "Consumer paused for topic %s".formatted(topic));
          }
          case RESUME -> {
            consumer.resume(consumer.assignment());
            LOGGER.log(Level.INFO, "Consumer resumed for topic %s".formatted(topic));
          }
          case CLOSE -> {
            running.set(false);
            LOGGER.log(Level.INFO, "Consumer shutdown initiated for topic %s".formatted(topic));
          }
        }
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error processing consumer command: %s".formatted(command), e);
      }
    }
  }

  /**
   * Pauses consumption from the topic. Any in-flight messages will continue processing, but no new
   * messages will be consumed until {@link #resume()} is called.
   *
   * <p>This method is idempotent - calling it multiple times has no additional effect.
   */
  public void pause() {
    if (paused.compareAndSet(false, true)) {
      commandQueue.offer(ConsumerCommand.PAUSE);
      LOGGER.log(Level.INFO, "Consumer pause requested for topic %s".formatted(topic));
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
    if (closed.get()) {
      throw new IllegalStateException("Cannot resume a closed consumer");
    }

    if (paused.compareAndSet(true, false)) {
      commandQueue.offer(ConsumerCommand.RESUME);
      LOGGER.log(Level.INFO, "Consumer resume requested for topic %s".formatted(topic));
    }
  }

  /**
   * Returns whether the consumer is currently paused.
   *
   * @return {@code true} if the consumer is paused, {@code false} otherwise
   */
  public boolean isPaused() {
    return paused.get();
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
    return running.get();
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
    if (!closed.compareAndSet(false, true)) {
      return; // Already closed
    }

    // Create tracker first to avoid missing in-flight messages
    final var tracker = createTrackerIfEnabled(AppConfig.DEFAULT_WAIT_FOR_MESSAGES.toMillis());

    // Signal shutdown
    signalShutdown();

    // Wait for in-flight messages
    waitForInFlightMessages(tracker, AppConfig.DEFAULT_WAIT_FOR_MESSAGES.toMillis());

    // Wake up consumer and wait for thread termination
    wakeupAndWaitForConsumerThread(AppConfig.DEFAULT_THREAD_TERMINATION.toMillis());

    // Shutdown executor
    shutdownExecutor(AppConfig.DEFAULT_EXECUTOR_TERMINATION.toMillis());
  }

  private MessageTracker createTrackerIfEnabled(final long waitForMessagesMs) {
    if (waitForMessagesMs > 0 && enableMetrics) {
      return createMessageTracker();
    }
    return null;
  }

  private void signalShutdown() {
    running.set(false);
    pause();
    commandQueue.offer(ConsumerCommand.CLOSE);
  }

  private void waitForInFlightMessages(final MessageTracker tracker, long waitForMessagesMs) {
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

  private void wakeupAndWaitForConsumerThread(long threadTerminationMs) {
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

  private void shutdownExecutor(long executorTerminationMs) {
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
      for (ConsumerRecord<K, V> record : records.records(topic)) {
        processRecord(record);
      }
    } else {
      // Process in parallel using virtual threads
      StreamSupport
        .stream(records.records(topic).spliterator(), false)
        .forEach(record -> {
          try {
            virtualThreadExecutor.submit(() -> processRecord(record));
          } catch (RejectedExecutionException e) {
            // Handle task rejection (typically during shutdown)
            if (running.get()) {
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
      if (running.get()) { // Only log if we're not shutting down
        LOGGER.log(Level.WARNING, "Error during Kafka poll operation", e);
      }
      return null;
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
        messageSink.accept(record, processed);

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
        } catch (InterruptedException ie) {
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
}
