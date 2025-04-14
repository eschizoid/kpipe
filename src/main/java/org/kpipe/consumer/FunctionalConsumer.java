package org.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import org.kpipe.annotations.VisibleForTesting;
import org.kpipe.sink.LoggingSink;
import org.kpipe.sink.MessageSink;

/**
 * A high-performance Kafka consumer that processes messages functionally through virtual threads.
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

  // Command pattern for thread-safe operations
  private enum ConsumerCommand {
    PAUSE,
    RESUME,
    CLOSE,
  }

  private final AtomicReference<ConsumerCommand> pendingCommand = new AtomicReference<>(null);

  // Core components
  private final KafkaConsumer<K, V> consumer;
  private final String topic;
  private final Function<V, V> processor;
  private final ExecutorService virtualThreadExecutor;
  private final Duration pollTimeout;
  private final MessageSink<K, V> messageSink;
  private Thread consumerThread;

  // Error handling and retry
  private final Consumer<ProcessingError<K, V>> errorHandler;
  private final int maxRetries;
  private final Duration retryBackoff;

  // State tracking
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final AtomicBoolean paused = new AtomicBoolean(false);

  // Metrics
  private final boolean enableMetrics;
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

    public Builder<K, V> withMessageSink(final MessageSink<K, V> messageSink) {
      this.messageSink = messageSink;
      return this;
    }

    public FunctionalConsumer<K, V> build() {
      return new FunctionalConsumer<>(this);
    }
  }

  public FunctionalConsumer(final Builder<K, V> builder) {
    this.consumer = createConsumer(Objects.requireNonNull(builder.kafkaProps));
    this.topic = Objects.requireNonNull(builder.topic);
    this.processor = Objects.requireNonNull(builder.processor);
    this.pollTimeout = Objects.requireNonNull(builder.pollTimeout);
    this.errorHandler = builder.errorHandler;
    this.maxRetries = builder.maxRetries;
    this.retryBackoff = builder.retryBackoff;
    this.enableMetrics = builder.enableMetrics;
    this.messageSink = builder.messageSink != null ? builder.messageSink : new LoggingSink<>();
    this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    initializeMetrics();
  }

  public FunctionalConsumer(final Properties kafkaProps, final String topic, Function<V, V> processor) {
    this(kafkaProps, topic, processor, Duration.ofMillis(100));
  }

  public FunctionalConsumer(
    final Properties kafkaProps,
    final String topic,
    final Function<V, V> processor,
    final Duration pollTimeout
  ) {
    this.consumer = createConsumer(Objects.requireNonNull(kafkaProps));
    this.topic = Objects.requireNonNull(topic);
    this.processor = Objects.requireNonNull(processor);
    this.pollTimeout = Objects.requireNonNull(pollTimeout);
    this.errorHandler = error -> {};
    this.maxRetries = 0;
    this.retryBackoff = Duration.ofMillis(500);
    this.enableMetrics = true;
    this.messageSink = new LoggingSink<>();
    this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    initializeMetrics();
  }

  private void initializeMetrics() {
    if (enableMetrics) {
      metrics.put("messagesReceived", new AtomicLong(0));
      metrics.put("messagesProcessed", new AtomicLong(0));
      metrics.put("processingErrors", new AtomicLong(0));
      metrics.put("retries", new AtomicLong(0));
    }
  }

  /**
   * Processes pending commands immediately for testing purposes. This method allows tests to verify
   * consumer operations without starting the consumer thread.
   */
  @VisibleForTesting
  public void processCommandsForTest() {
    processCommand();
  }

  /**
   * Creates a message tracker that can monitor the state of in-flight messages. The tracker uses
   * the consumer's metrics to determine how many messages have been received versus processed.
   *
   * @return a new {@link MessageTracker} instance
   */
  public MessageTracker createMessageTracker() {
    return MessageTracker
      .builder()
      .withMetricsSupplier(this::getMetrics)
      .withReceivedMetricKey("messagesReceived")
      .withProcessedMetricKey("messagesProcessed")
      .withErrorsMetricKey("processingErrors")
      .build();
  }

  /**
   * Starts the consumer thread and begins consuming messages from the configured topic. The
   * consumer will poll for records and process them asynchronously in virtual threads.
   */
  public void start() {
    consumer.subscribe(List.of(topic));

    consumerThread =
      Thread.startVirtualThread(() -> {
        try {
          while (running.get()) {
            // Process any pending commands first
            processCommand();

            // Only poll if not paused
            if (!paused.get()) {
              poll().ifPresent(this::processRecords);
            } else {
              // Sleep briefly when paused to avoid tight loop
              try {
                Thread.sleep(100);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
              }
            }
          }
        } catch (Exception e) {
          LOGGER.log(Level.ERROR, "Error in consumer thread", e);
        } finally {
          consumer.close();
        }
      });
  }

  // Process any pending commands in the consumer thread
  private void processCommand() {
    final var command = pendingCommand.getAndSet(null);
    if (command == null) return;

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
          // Additional close handling if needed
          LOGGER.log(Level.INFO, "Close command received for topic %s".formatted(topic));
        }
      }
    } catch (Exception e) {
      LOGGER.log(Level.ERROR, "Error processing consumer command: %s".formatted(command), e);
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
      pendingCommand.set(ConsumerCommand.PAUSE);
      LOGGER.log(Level.INFO, "Consumer pause requested for topic %s".formatted(topic));
    }
  }

  /**
   * Resumes consumption from the topic after being paused.
   *
   * <p>This method is idempotent - calling it multiple times has no additional effect.
   */
  public void resume() {
    if (paused.compareAndSet(true, false)) {
      pendingCommand.set(ConsumerCommand.RESUME);
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
   * @return {@code true} if the consumer is running, {@code false} if it has been closed
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
   */
  @Override
  public void close() {
    if (isRunning()) {
      running.set(false);

      // First request a pause via the command pattern
      pause();
      pendingCommand.set(ConsumerCommand.CLOSE);

      // Use MessageTracker to wait for in-flight messages
      final var tracker = createMessageTracker();
      tracker.waitForCompletion(5000);

      // First wake up the consumer to unblock any poll operation
      if (consumer != null) {
        consumer.wakeup();
      }

      // Wait for consumer thread to finish naturally
      try {
        if (consumerThread != null) {
          consumerThread.join(5000);
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.log(Level.WARNING, "Interrupted while waiting for consumer thread to complete");
      }

      // Shutdown executor and wait for in-flight tasks
      try {
        virtualThreadExecutor.shutdown();
        if (!virtualThreadExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
          LOGGER.log(Level.WARNING, "Not all processing tasks completed during shutdown");
          virtualThreadExecutor.shutdownNow();
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        virtualThreadExecutor.shutdownNow();
      }
      // Now it's safe to close the consumer - consumer thread will handle this in finally block
    }
  }

  protected KafkaConsumer<K, V> createConsumer(final Properties kafkaProps) {
    return new KafkaConsumer<>(kafkaProps);
  }

  protected void processRecords(final ConsumerRecords<K, V> records) {
    StreamSupport
      .stream(records.records(topic).spliterator(), false)
      .forEach(record -> virtualThreadExecutor.submit(() -> processRecord(record)));
  }

  private Optional<ConsumerRecords<K, V>> poll() {
    try {
      final var records = consumer.poll(pollTimeout);
      return records.isEmpty() ? Optional.empty() : Optional.of(records);
    } catch (final Exception e) {
      LOGGER.log(Level.INFO, "Error during poll", e);
      return Optional.empty();
    }
  }

  public void processRecord(final ConsumerRecord<K, V> record) {
    if (enableMetrics) {
      metrics.get("messagesReceived").incrementAndGet();
    }

    var attempts = 0;

    while (attempts <= maxRetries) {
      // Count retry attempts (not the first attempt)
      if (enableMetrics && attempts > 0) {
        metrics.get("retries").incrementAndGet();
      }

      try {
        V processed = processor.apply(record.value());
        messageSink.accept(record, processed);
        if (enableMetrics) {
          metrics.get("messagesProcessed").incrementAndGet();
        }
        return; // Success
      } catch (final Exception e) {
        attempts++;

        // Last attempt failed
        if (attempts > maxRetries) {
          if (enableMetrics) {
            metrics.get("processingErrors").incrementAndGet();
          }

          LOGGER.log(
            Level.WARNING,
            "Failed to process message at offset %d after %d attempts".formatted(record.offset(), attempts),
            e
          );
          errorHandler.accept(new ProcessingError<>(record, e, maxRetries));
          break;
        }

        // Wait before next retry attempt
        try {
          Thread.sleep(retryBackoff.toMillis());
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }
}
