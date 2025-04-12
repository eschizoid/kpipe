package org.kpipe.consumer;

import com.dslplatform.json.DslJson;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.kpipe.sink.LoggingSink;
import org.kpipe.sink.MessageSink;

/**
 * A modern, functional Kafka consumer implementation leveraging Java 23's virtual threads for
 * concurrent message processing.
 *
 * <p>This consumer processes Kafka messages through a functional pipeline defined by the provided
 * processor function. Each message is processed in its own virtual thread, allowing for massive
 * concurrency with minimal overhead.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create consumer with builder pattern
 * var consumer = new FunctionalKafkaConsumer.Builder<byte[], byte[]>()
 *     .withProperties(KafkaConfigFactory.createConsumerConfig("localhost:9092", "group-id"))
 *     .withTopic("json-topic")
 *     .withProcessor(MessageProcessorRegistry.pipeline("parseJson", "addMetadata"))
 *     .withRetry(3, Duration.ofSeconds(1))
 *     .withErrorHandler(error -> logError(error))
 *     .build();
 *
 * // Start consuming messages
 * consumer.start();
 *
 * // Later, gracefully shut down
 * consumer.close();
 * }</pre>
 *
 * @param <K> The type of Kafka record keys
 * @param <V> The type of Kafka record values
 */
public class FunctionalKafkaConsumer<K, V> implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(FunctionalKafkaConsumer.class.getName());
  private static final DslJson<Map<String, Object>> DSL_JSON = new DslJson<>();

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
   * Record for capturing information about processing errors during message consumption.
   *
   * @param record The Kafka record that caused the error
   * @param exception The exception that occurred during processing
   * @param retryCount The number of retry attempts made before giving up
   * @param <K> The type of Kafka record key
   * @param <V> The type of Kafka record value
   */
  public record ProcessingError<K, V>(ConsumerRecord<K, V> record, Exception exception, int retryCount) {}

  /**
   * Builder for creating instances of the {@link FunctionalKafkaConsumer} with a fluent interface.
   *
   * <p>Example:
   *
   * <pre>{@code
   * var consumer = new FunctionalKafkaConsumer.Builder<byte[], byte[]>()
   *     .withProperties(kafkaProps)
   *     .withTopic("json-topic")
   *     .withProcessor(msg -> processJson(msg))
   *     .withErrorHandler(error -> reportError(error))
   *     .withRetry(3, Duration.ofSeconds(1))
   *     .build();
   * }</pre>
   *
   * @param <K> The type of Kafka record keys
   * @param <V> The type of Kafka record values
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

    /**
     * Sets the Kafka properties for the consumer.
     *
     * @param props Kafka consumer configuration properties
     * @return this builder for method chaining
     */
    public Builder<K, V> withProperties(final Properties props) {
      this.kafkaProps = props;
      return this;
    }

    /**
     * Sets the Kafka topic to consume from.
     *
     * @param topic name of the Kafka topic
     * @return this builder for method chaining
     */
    public Builder<K, V> withTopic(final String topic) {
      this.topic = topic;
      return this;
    }

    /**
     * Sets the function to process each message.
     *
     * @param processor function that processes messages of type V
     * @return this builder for method chaining
     */
    public Builder<K, V> withProcessor(final Function<V, V> processor) {
      this.processor = processor;
      return this;
    }

    /**
     * Sets the poll timeout duration for the Kafka consumer.
     *
     * @param timeout duration to wait for poll operation
     * @return this builder for method chaining
     */
    public Builder<K, V> withPollTimeout(final Duration timeout) {
      this.pollTimeout = timeout;
      return this;
    }

    /**
     * Sets a handler for processing errors.
     *
     * @param handler consumer function that handles processing errors
     * @return this builder for method chaining
     */
    public Builder<K, V> withErrorHandler(final Consumer<ProcessingError<K, V>> handler) {
      this.errorHandler = handler;
      return this;
    }

    /**
     * Configures retry behavior for failed message processing.
     *
     * @param maxRetries maximum number of retry attempts
     * @param backoff duration to wait between retry attempts
     * @return this builder for method chaining
     */
    public Builder<K, V> withRetry(final int maxRetries, final Duration backoff) {
      this.maxRetries = maxRetries;
      this.retryBackoff = backoff;
      return this;
    }

    /**
     * Enables or disables metrics collection.
     *
     * @param enable true to enable metrics, false to disable
     * @return this builder for method chaining
     */
    public Builder<K, V> withMetrics(final boolean enable) {
      this.enableMetrics = enable;
      return this;
    }

    /**
     * Sets the message sink for handling processed records.
     *
     * @param messageSink the sink that will handle processed messages
     * @return this builder for method chaining
     */
    public Builder<K, V> withMessageSink(final MessageSink<K, V> messageSink) {
      this.messageSink = messageSink;
      return this;
    }

    /**
     * Creates a new {@link FunctionalKafkaConsumer} with the configured settings.
     *
     * @return a new consumer instance
     * @throws NullPointerException if required properties are null
     */
    public FunctionalKafkaConsumer<K, V> build() {
      return new FunctionalKafkaConsumer<>(this);
    }
  }

  /**
   * Private constructor used by the Builder.
   *
   * @param builder the builder containing configuration
   */
  public FunctionalKafkaConsumer(final Builder<K, V> builder) {
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

  /**
   * Creates a consumer with default settings.
   *
   * @param kafkaProps Kafka consumer properties
   * @param topic the topic to consume from
   * @param processor function to process each message
   */
  public FunctionalKafkaConsumer(final Properties kafkaProps, final String topic, Function<V, V> processor) {
    this(kafkaProps, topic, processor, Duration.ofMillis(100));
  }

  /**
   * Creates a consumer with custom poll timeout.
   *
   * @param kafkaProps Kafka consumer properties
   * @param topic the topic to consume from
   * @param processor function to process each message
   * @param pollTimeout duration to wait when polling for messages
   */
  public FunctionalKafkaConsumer(
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

  /** Initializes metrics counters if metrics are enabled. */
  private void initializeMetrics() {
    if (enableMetrics) {
      metrics.put("messagesReceived", new AtomicLong(0));
      metrics.put("messagesProcessed", new AtomicLong(0));
      metrics.put("processingErrors", new AtomicLong(0));
      metrics.put("retries", new AtomicLong(0));
    }
  }

  /**
   * Starts the consumer and begins processing messages.
   *
   * <p>The consumer runs in a dedicated virtual thread and processes each received message in its
   * own virtual thread.
   *
   * <p>Example:
   *
   * <pre>{@code
   * var consumer = new FunctionalKafkaConsumer.Builder<byte[], byte[]>()
   *     // ... configure consumer
   *     .build();
   *
   * // Start the consumer
   * consumer.start();
   *
   * // Your application continues running...
   * }</pre>
   */
  public void start() {
    consumer.subscribe(List.of(topic));

    consumerThread =
      Thread.startVirtualThread(() -> {
        try {
          while (running.get()) {
            poll().ifPresent(this::processRecords);
          }
        } catch (Exception e) {
          LOGGER.log(Level.ERROR, "Error in consumer thread", e);
        } finally {
          consumer.close();
        }
      });
  }

  /**
   * Pauses message consumption from Kafka.
   *
   * <p>The consumer will continue running but will not fetch more records from Kafka until {@link
   * #resume()} is called.
   */
  public void pause() {
    if (paused.compareAndSet(false, true)) {
      consumer.pause(consumer.assignment());
      LOGGER.log(Level.INFO, "Consumer paused for topic %s".formatted(topic));
    }
  }

  /**
   * Resumes message consumption after a pause.
   *
   * <p>This method takes effect only if the consumer was previously paused via {@link #pause()}.
   */
  public void resume() {
    if (paused.compareAndSet(true, false)) {
      consumer.resume(consumer.assignment());
      LOGGER.log(Level.INFO, "Consumer resumed for topic %s".formatted(topic));
    }
  }

  /**
   * Checks if the consumer is currently paused.
   *
   * @return true if the consumer is paused, false otherwise
   */
  public boolean isPaused() {
    return paused.get();
  }

  /**
   * Returns the current metrics collected by this consumer.
   *
   * <p>Available metrics when enabled:
   *
   * <ul>
   *   <li>messagesReceived: Total number of messages received from Kafka
   *   <li>messagesProcessed: Successfully processed messages
   *   <li>processingErrors: Number of processing errors encountered
   *   <li>retries: Number of retry attempts made
   * </ul>
   *
   * <p>Example:
   *
   * <pre>{@code
   * Map<String, Long> metrics = consumer.getMetrics();
   * System.out.println("Processed: " + metrics.get("messagesProcessed"));
   * System.out.println("Errors: " + metrics.get("processingErrors"));
   * }</pre>
   *
   * @return map of metric names to values, or empty map if metrics are disabled
   */
  public Map<String, Long> getMetrics() {
    if (!enableMetrics) {
      return Map.of();
    }

    return metrics.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
  }

  /**
   * Checks if the consumer is currently running.
   *
   * @return true if the consumer is running, false if it has been closed
   */
  public boolean isRunning() {
    return running.get();
  }

  /**
   * Closes the consumer and releases all resources.
   *
   * <p>This method is idempotent and can be called multiple times safely.
   */
  @Override
  public void close() {
    if (isRunning()) {
      running.set(false);

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

      // Now it's safe to close the consumer
      if (consumer != null) {
        consumer.close(Duration.ofSeconds(5));
      }
    }
  }

  /**
   * Creates the Kafka consumer instance with the provided properties.
   *
   * <p>This method can be overridden by subclasses to customize consumer creation.
   *
   * @param kafkaProps properties for configuring the Kafka consumer
   * @return a new Kafka consumer instance
   */
  protected KafkaConsumer<K, V> createConsumer(final Properties kafkaProps) {
    return new KafkaConsumer<>(kafkaProps);
  }

  /**
   * Processes a batch of records received from Kafka.
   *
   * <p>Each record is submitted to the virtual thread executor for concurrent processing.
   *
   * @param records batch of consumer records to process
   */
  protected void processRecords(final ConsumerRecords<K, V> records) {
    StreamSupport
      .stream(records.records(topic).spliterator(), false)
      .forEach(record -> virtualThreadExecutor.submit(() -> processRecord(record)));
  }

  /**
   * Polls Kafka for new records.
   *
   * @return an Optional containing the records if any were received, or empty if none
   */
  private Optional<ConsumerRecords<K, V>> poll() {
    try {
      final var records = consumer.poll(pollTimeout);
      return records.isEmpty() ? Optional.empty() : Optional.of(records);
    } catch (final Exception e) {
      LOGGER.log(Level.INFO, "Error during poll", e);
      return Optional.empty();
    }
  }

  /**
   * Processes a single record with error handling and retry logic.
   *
   * @param record the Kafka record to process
   */
  private void processRecord(final ConsumerRecord<K, V> record) {
    if (enableMetrics) {
      metrics.get("messagesReceived").incrementAndGet();
    }

    var attempts = 0;

    while (attempts <= maxRetries) {
      try {
        V processed = processor.apply(record.value());
        messageSink.accept(record, processed); // Use MessageSink instead of logging
        if (enableMetrics) {
          metrics.get("messagesProcessed").incrementAndGet();
        }
        return; // Success
      } catch (final Exception e) {
        attempts++;

        // Only count as a retry if this is a retry attempt, not the first attempt
        if (enableMetrics && attempts > 1) {
          metrics.get("retries").incrementAndGet();
        }

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
          errorHandler.accept(new ProcessingError<>(record, e, attempts - 1));
          break;
        }

        // Wait before next retry attempt
        try {
          Thread.sleep(retryBackoff.toMillis());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }
}
