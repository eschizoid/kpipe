package com.example.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * A functional wrapper around Kafka's consumer API that processes messages using virtual threads.
 *
 * <p>This class provides a modern approach to Kafka message consumption by combining:
 *
 * <ul>
 *   <li>Functional programming (message processor as a function)
 *   <li>Virtual threads for scalable concurrent processing
 *   <li>Structured exception handling
 * </ul>
 *
 * <p>Each consumed message is processed in its own virtual thread, allowing for:
 *
 * <ul>
 *   <li>High throughput with minimal resource usage
 *   <li>Simplified concurrency model (no thread pool management)
 *   <li>Isolated processing failures (one message failure doesn't affect others)
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create Kafka configuration
 * Properties kafkaProps = KafkaConfigFactory.createConsumerConfig(
 *     "localhost:9092",
 *     "my-consumer-group"
 * );
 *
 * // Create message processor
 * Function<byte[], byte[]> processor = MessageProcessorRegistry.pipeline(
 *     "parseJson", "addTimestamp", "markProcessed"
 * );
 *
 * // Create and start the consumer
 * try (FunctionalKafkaConsumer<byte[], byte[]> consumer =
 *         new FunctionalKafkaConsumer<>(kafkaProps, "my-topic", processor)) {
 *     consumer.start();
 *
 *     // Keep application running
 *     Thread.sleep(Duration.ofHours(1));
 * }
 * }</pre>
 *
 * @param <K> Type of Kafka record keys
 * @param <V> Type of Kafka record values
 * @see MessageProcessorRegistry
 * @see KafkaConfigFactory
 */
public class FunctionalKafkaConsumer<K, V> implements AutoCloseable {
  private final KafkaConsumer<K, V> consumer;
  private final String topic;
  private final Function<V, V> processor;
  private final ExecutorService virtualThreadExecutor;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private static final Logger LOGGER = Logger.getLogger(FunctionalKafkaConsumer.class.getName());
  private final Duration pollTimeout;

  /**
   * Creates a new functional Kafka consumer with specified properties.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create consumer with explicit poll timeout
   * Properties props = KafkaConfigFactory.createConsumerConfig(
   *     "kafka-broker:9092",
   *     "analytics-group"
   * );
   *
   * Function<byte[], byte[]> processor = MessageProcessorRegistry.get("parseJson");
   *
   * FunctionalKafkaConsumer<byte[], byte[]> consumer =
   *     new FunctionalKafkaConsumer<>(
   *         props,
   *         "events",
   *         processor,
   *         Duration.ofMillis(200)
   *     );
   * }</pre>
   *
   * @param kafkaProps Kafka consumer configuration properties
   * @param topic Topic to subscribe to
   * @param processor Function to process each message
   * @param pollTimeout Duration to wait for records when polling
   * @throws NullPointerException if any parameter is null
   */
  public FunctionalKafkaConsumer(
      Properties kafkaProps, String topic, Function<V, V> processor, Duration pollTimeout) {
    this.consumer = createConsumer(Objects.requireNonNull(kafkaProps));
    this.topic = Objects.requireNonNull(topic);
    this.processor = Objects.requireNonNull(processor);
    this.pollTimeout = Objects.requireNonNull(pollTimeout);
    this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
  }

  /**
   * Creates a new functional Kafka consumer with default poll timeout (100ms).
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Create consumer with default poll timeout
   * Properties props = KafkaConfigFactory.createConsumerConfig(
   *     "localhost:9092",
   *     "orders-group"
   * );
   *
   * Function<byte[], byte[]> processor = bytes -> {
   *     // Custom processing logic
   *     return processedBytes;
   * };
   *
   * FunctionalKafkaConsumer<byte[], byte[]> consumer =
   *     new FunctionalKafkaConsumer<>(props, "orders", processor);
   * }</pre>
   *
   * @param kafkaProps Kafka consumer configuration properties
   * @param topic Topic to subscribe to
   * @param processor Function to process each message
   * @throws NullPointerException if any parameter is null
   */
  public FunctionalKafkaConsumer(
      final Properties kafkaProps, final String topic, Function<V, V> processor) {
    this(kafkaProps, topic, processor, Duration.ofMillis(100));
  }

  /**
   * Starts the consumer in a dedicated virtual thread.
   *
   * <p>Once started, the consumer will continuously poll for new messages and process them
   * concurrently using virtual threads until {@link #close()} is called.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Start consumer and let it run in background
   * consumer.start();
   *
   * // Application can continue with other work
   * performOtherTasks();
   *
   * // Later, shut down consumer when done
   * consumer.close();
   * }</pre>
   */
  public void start() {
    consumer.subscribe(List.of(topic));

    // Use a virtual thread for the consumer polling loop
    Thread.startVirtualThread(
        () -> {
          try {
            while (running.get()) {
              poll().ifPresent(this::processRecords);
            }
          } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error in consumer thread", e);
          } finally {
            consumer.close();
          }
        });
  }

  /**
   * Polls for new records with error handling.
   *
   * @return Optional containing records if available, empty if no records or error occurred
   */
  private Optional<ConsumerRecords<K, V>> poll() {
    try {
      final var records = consumer.poll(pollTimeout);
      return records.isEmpty() ? Optional.empty() : Optional.of(records);
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Error during poll", e);
      return Optional.empty();
    }
  }

  /**
   * Creates a Kafka consumer instance with the provided properties.
   *
   * <p>This method is protected to allow subclasses to override consumer creation, which is
   * particularly useful for testing with mock consumers.
   *
   * <p>Example override in tests:
   *
   * <pre>{@code
   * @Override
   * protected KafkaConsumer<K, V> createConsumer(Properties kafkaProps) {
   *     return mockConsumer; // Return a mock/spy instance for testing
   * }
   * }</pre>
   *
   * @param kafkaProps Kafka consumer configuration properties
   * @return A new KafkaConsumer instance
   */
  protected KafkaConsumer<K, V> createConsumer(final Properties kafkaProps) {
    return new KafkaConsumer<>(kafkaProps);
  }

  /**
   * Processes a batch of records by submitting each record for processing in its own virtual
   * thread.
   *
   * @param records The batch of records to process
   */
  protected void processRecords(final ConsumerRecords<K, V> records) {
    StreamSupport.stream(records.records(topic).spliterator(), false)
        .forEach(record -> virtualThreadExecutor.submit(() -> processRecord(record)));
  }

  /**
   * Processes a single record by applying the processor function.
   *
   * @param record The record to process
   */
  private void processRecord(final ConsumerRecord<K, V> record) {
    try {
      V processed = processor.apply(record.value());
      logProcessedMessage(record, processed);
    } catch (Exception e) {
      LOGGER.log(
          Level.WARNING,
          String.format("Error processing message at offset %d", record.offset()),
          e);
    }
  }

  /**
   * Logs processed message details.
   *
   * @param record The original record
   * @param processed The processed message value
   */
  private void logProcessedMessage(ConsumerRecord<K, V> record, V processed) {
    LOGGER.info(
        String.format(
            """
            {
              "topic": "%s",
              "partition": %d,
              "offset": %d,
              "key": "%s",
              "processedMessage": "%s"
            }
            """,
            topic, record.partition(), record.offset(), record.key(), processed));
  }

  /**
   * Checks if the consumer is running.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Use in health check or monitoring
   * if (consumer.isRunning()) {
   *     System.out.println("Consumer is active and processing messages");
   * } else {
   *     System.out.println("Consumer has been shut down");
   * }
   * }</pre>
   *
   * @return true if the consumer is running, false if it has been closed
   */
  @Override
  public void close() {
    if (running.compareAndSet(true, false)) {
      LOGGER.info("Shutting down consumer for topic " + topic);
      consumer.wakeup();
      virtualThreadExecutor.close();
    }
  }

  /**
   * Checks if the consumer is running.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Use in health check or monitoring
   * if (consumer.isRunning()) {
   *     System.out.println("Consumer is active and processing messages");
   * } else {
   *     System.out.println("Consumer has been shut down");
   * }
   * }</pre>
   *
   * @return true if the consumer is running, false if it has been closed
   */
  public boolean isRunning() {
    return running.get();
  }
}
