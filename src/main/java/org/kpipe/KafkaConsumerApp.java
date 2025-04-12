package org.kpipe;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.kpipe.config.KafkaConfig;
import org.kpipe.consumer.FunctionalKafkaConsumer;
import org.kpipe.processor.MessageProcessorRegistry;
import org.kpipe.sink.LoggingSink;
import org.kpipe.sink.MessageSink;

/**
 * Application that consumes messages from a Kafka topic and processes them using a configurable
 * pipeline of message processors.
 *
 * <p>This application manages the lifecycle of a Kafka consumer, including:
 *
 * <ul>
 *   <li>Starting and stopping the consumer
 *   <li>Graceful shutdown with in-flight message handling
 *   <li>Periodic metrics reporting
 *   <li>Health status monitoring
 * </ul>
 */
public class KafkaConsumerApp implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(KafkaConsumerApp.class.getName());
  private static final String METRIC_MESSAGES_RECEIVED = "messagesReceived";
  private static final String METRIC_MESSAGES_PROCESSED = "messagesProcessed";
  private static final String METRIC_PROCESSING_ERRORS = "processingErrors";

  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private final FunctionalKafkaConsumer<byte[], byte[]> consumer;
  private final MessageProcessorRegistry registry;
  private final MessageSink<byte[], byte[]> messageSink;
  private final Duration metricsInterval;
  private final Duration shutdownTimeout;
  private volatile boolean running = false;
  private Thread metricsThread;

  /**
   * Main entry point for the Kafka consumer application.
   *
   * <p>The application lifecycle is managed through two complementary mechanisms:
   *
   * <ul>
   *   <li>A JVM shutdown hook that initiates graceful shutdown when the application receives
   *       termination signals (CTRL+C, SIGTERM)
   *   <li>The awaitShutdown method that keeps the main thread alive until shutdown is complete
   * </ul>
   *
   * <p>This ensures both proper operation during normal runtime and clean termination when the
   * application is stopped.
   *
   * @param args command line arguments (not used)
   */
  public static void main(String[] args) {
    final var config = KafkaConfig.fromEnv();

    try (final KafkaConsumerApp app = new KafkaConsumerApp(config)) {
      app.start();

      Runtime
        .getRuntime()
        .addShutdownHook(
          new Thread(() -> {
            long timeoutMillis = app.getShutdownTimeout().toMillis();
            boolean shutdownSuccess = app.shutdownGracefully(timeoutMillis);
            if (!shutdownSuccess) {
              LOGGER.log(Level.WARNING, "Graceful shutdown timed out after %s ms".formatted(timeoutMillis));
            }
          })
        );

      final var normalShutdown = app.awaitShutdown();
      if (!normalShutdown) {
        LOGGER.log(Level.WARNING, "Application didn't shut down cleanly");
      }
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in Kafka consumer application", e);
      System.exit(1);
    }
  }

  /**
   * Creates a new KafkaConsumerApp with the specified configuration.
   *
   * @param config the application configuration
   */
  public KafkaConsumerApp(final KafkaConfig config) {
    final MessageProcessorRegistry registry = new MessageProcessorRegistry(config.appName);
    final MessageSink<byte[], byte[]> messageSink = new LoggingSink<>();

    this.registry = registry;
    this.messageSink = messageSink;
    this.consumer = createConsumer(config, registry, messageSink);
    this.metricsInterval = config.metricsInterval;
    this.shutdownTimeout = config.shutdownTimeout;
  }

  /**
   * Returns the shutdown timeout duration for the application.
   *
   * @return the shutdown timeout duration
   */
  public Duration getShutdownTimeout() {
    return shutdownTimeout;
  }

  /**
   * Creates a Kafka consumer with the specified configuration.
   *
   * @param config the application configuration
   * @param registry the message processor registry to use
   * @return a new functional Kafka consumer
   */
  private static FunctionalKafkaConsumer<byte[], byte[]> createConsumer(
    final KafkaConfig config,
    final MessageProcessorRegistry registry,
    final MessageSink<byte[], byte[]> messageSink
  ) {
    final var kafkaProps = KafkaConfigFactory.createConsumerConfig(config.bootstrapServers, config.consumerGroup);

    return new FunctionalKafkaConsumer.Builder<byte[], byte[]>()
      .withProperties(kafkaProps)
      .withTopic(config.topic)
      .withProcessor(createProcessorPipeline(config, registry))
      .withPollTimeout(config.pollTimeout)
      .withMessageSink(messageSink)
      .build();
  }

  /**
   * Creates a message processing pipeline from the specified configuration.
   *
   * @param config the application configuration
   * @param registry the message processor registry to use
   * @return a function that processes messages
   */
  private static Function<byte[], byte[]> createProcessorPipeline(
    final KafkaConfig config,
    final MessageProcessorRegistry registry
  ) {
    Function<byte[], byte[]> pipeline = config.processors.isEmpty()
      ? registry.pipeline("parseJson", "addSource", "markProcessed", "addTimestamp")
      : registry.pipeline(config.processors.toArray(new String[0]));

    return MessageProcessorRegistry.withErrorHandling(pipeline, null);
  }

  /**
   * Starts the Kafka consumer application.
   *
   * <p>This method initiates:
   *
   * <ul>
   *   <li>The background metrics reporting thread
   *   <li>The Kafka consumer to begin processing messages
   * </ul>
   *
   * <p>If the application is already running, this method has no effect.
   */
  public void start() {
    if (running) {
      return;
    }

    running = true;
    startMetricsReporting();
    consumer.start();
    LOGGER.log(Level.INFO, "Kafka consumer application started successfully");
  }

  /**
   * Blocks the current thread until the application shuts down. This method should block
   * indefinitely until an explicit shutdown is triggered.
   *
   * @return true if the application shut down normally, false otherwise
   */
  public boolean awaitShutdown() {
    try {
      shutdownLatch.await();
      return true;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Shutdown wait interrupted");
      return false;
    }
  }

  /**
   * Initiates a graceful shutdown by allowing in-flight messages to complete processing before
   * shutting down.
   *
   * <p>This method actively monitors the status of in-flight messages and attempts to:
   *
   * <ul>
   *   <li>Prevent new messages from being consumed by pausing the consumer
   *   <li>Wait for any in-flight messages to complete processing
   *   <li>Exit early if all messages are already processed
   *   <li>Ensure proper resource cleanup even if the timeout is reached
   * </ul>
   *
   * <p>The method will monitor the difference between received and processed messages to determine
   * when all in-flight processing has completed.
   *
   * @param drainTimeoutMs maximum time in milliseconds to wait for in-flight messages
   * @return true if all in-flight messages completed processing within the timeout, false otherwise
   */
  public boolean shutdownGracefully(long drainTimeoutMs) {
    // Only allow one shutdown to proceed
    if (!shuttingDown.compareAndSet(false, true)) {
      LOGGER.log(Level.INFO, "Shutdown already in progress, ignoring additional request");
      return true;
    }

    LOGGER.log(Level.INFO, "Initiating graceful shutdown with {0}ms drain timeout", drainTimeoutMs);

    // Signal consumer to stop accepting new messages
    try {
      consumer.pause();
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Error pausing consumer during shutdown", e);
      // Continue with shutdown despite the error
    }

    // Check if consumer is already done processing (no in-flight messages)
    var metrics = consumer.getMetrics();
    var received = metrics.getOrDefault(METRIC_MESSAGES_RECEIVED, 0L);
    var processed = metrics.getOrDefault(METRIC_MESSAGES_PROCESSED, 0L);
    var errors = metrics.getOrDefault(METRIC_PROCESSING_ERRORS, 0L);

    // If all messages are accounted for, we can skip waiting
    if (received == processed + errors) {
      LOGGER.log(Level.INFO, "No in-flight messages detected, proceeding with shutdown");
      close();
      return true;
    }

    // Wait for in-flight messages with periodic checks
    final var startTime = System.currentTimeMillis();
    final var deadline = startTime + drainTimeoutMs;

    try {
      while (System.currentTimeMillis() < deadline) {
        Thread.sleep(Math.min(500, drainTimeoutMs / 10)); // Check periodically

        metrics = consumer.getMetrics();
        processed = metrics.getOrDefault(METRIC_MESSAGES_PROCESSED, 0L);
        errors = metrics.getOrDefault(METRIC_PROCESSING_ERRORS, 0L);

        if (received == processed + errors) {
          LOGGER.log(Level.INFO, "All in-flight messages completed, proceeding with shutdown");
          close();
          return true;
        }
      }

      LOGGER.log(
        Level.WARNING,
        "Drain timeout reached with {0} messages still in-flight",
        (received - processed - errors)
      );
      return false;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Graceful shutdown was interrupted");
      return false;
    } finally {
      // Ensure resources are properly closed exactly once
      close();
    }
  }

  /**
   * Returns whether the consumer application is healthy and functioning properly.
   *
   * <p>This method checks that:
   *
   * <ul>
   *   <li>The application is currently in a running state
   *   <li>The underlying Kafka consumer is active and operational
   * </ul>
   *
   * <p>This method is designed to be used by health monitoring systems such as:
   *
   * <ul>
   *   <li>Docker container health checks (via HEALTHCHECK directive)
   *   <li>Kubernetes liveness and readiness probes
   *   <li>Application monitoring tools
   *   <li>Custom health endpoints in REST APIs
   * </ul>
   *
   * <p>Example usage with a Docker HEALTHCHECK:
   *
   * <pre>
   * HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
   *   CMD java -cp myapp.jar com.example.HealthCheck || exit 1
   * </pre>
   *
   * @return true if the application is running and the consumer is processing messages
   */
  public boolean isHealthy() {
    return running && consumer.isRunning();
  }

  /**
   * Returns whether the application is currently in a running state.
   *
   * @return true if the application is running, false otherwise
   */
  public boolean isRunning() {
    return running;
  }

  /**
   * Returns whether the application is currently shutting down.
   *
   * @return true if the application is shutting down, false otherwise
   */
  public boolean isShuttingDown() {
    return shuttingDown.get();
  }

  /**
   * Closes all resources associated with this application. This method is called automatically when
   * using try-with-resources.
   */
  @Override
  public void close() {
    // If we're already not running, don't try to close again
    if (!running) {
      return;
    }

    // Mark as not running to prevent further operations
    running = false;
    LOGGER.log(Level.INFO, "Closing resources...");

    try {
      // Close the consumer first to stop receiving messages
      try {
        if (consumer != null) {
          consumer.close();
        }
      } catch (Exception e) {
        LOGGER.log(Level.WARNING, "Error closing consumer", e);
      }

      // Stop the metrics thread - no need for join with virtual threads
      if (metricsThread != null) {
        metricsThread.interrupt();
      }
    } finally {
      shutdownLatch.countDown();
    }
  }

  /** Starts a background thread that periodically reports metrics. */
  private void startMetricsReporting() {
    metricsThread =
      Thread.startVirtualThread(() -> {
        try {
          while (running && !Thread.currentThread().isInterrupted()) {
            try {
              reportConsumerMetrics();
              reportProcessorMetrics();
            } catch (Exception e) {
              LOGGER.log(Level.WARNING, "Error during metrics reporting", e);
            }

            Thread.sleep(metricsInterval);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.log(Level.INFO, "Metrics reporting thread interrupted");
        }
      });
  }

  /** Reports consumer metrics to the logger. */
  private void reportConsumerMetrics() {
    try {
      final var consumerMetrics = consumer.getMetrics();
      if (consumerMetrics != null && !consumerMetrics.isEmpty()) {
        LOGGER.log(
          Level.INFO,
          "Consumer metrics: messages received: %s, messages processed: %s, errors: %s, uptime: %s ms".formatted(
              consumerMetrics.getOrDefault(METRIC_MESSAGES_RECEIVED, 0L),
              consumerMetrics.getOrDefault(METRIC_MESSAGES_PROCESSED, 0L),
              consumerMetrics.getOrDefault(METRIC_PROCESSING_ERRORS, 0L),
              System.currentTimeMillis() - startTime.get()
            )
        );
      }
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error retrieving consumer metrics", e);
    }
  }

  /** Reports processor metrics to the logger. */
  private void reportProcessorMetrics() {
    try {
      registry
        .getAll()
        .keySet()
        .forEach(processorName -> {
          try {
            final var metrics = registry.getMetrics(processorName);
            if (!metrics.isEmpty()) {
              LOGGER.log(Level.INFO, "Processor '%s' metrics: %s".formatted(processorName, metrics));
            }
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error retrieving metrics for processor: %s".formatted(processorName));
          }
        });
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Error retrieving processor registry", e);
    }
  }
}
