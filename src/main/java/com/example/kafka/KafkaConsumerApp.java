package com.example.kafka;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

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

  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
  private final FunctionalKafkaConsumer<byte[], byte[]> consumer;
  private final MessageProcessorRegistry registry;
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

      // Add a shutdown hook to use graceful shutdown
      Runtime
        .getRuntime()
        .addShutdownHook(
          new Thread(() -> {
            boolean shutdownSuccess = app.shutdownGracefully(5000); // 5 seconds drain timeout
            if (!shutdownSuccess) {
              LOGGER.log(Level.WARNING, "Graceful shutdown timed out after 5000ms");
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
    this(
      new MessageProcessorRegistry(config.appName),
      createConsumer(config),
      config.metricsInterval,
      config.shutdownTimeout
    );
  }

  /**
   * Creates a new KafkaConsumerApp with the specified components. This constructor is primarily
   * intended for testing and dependency injection.
   *
   * @param registry the message processor registry
   * @param consumer the Kafka consumer
   * @param metricsInterval the interval between metrics reporting
   * @param shutdownTimeout the maximum duration to wait for shutdown
   */
  public KafkaConsumerApp(
    final MessageProcessorRegistry registry,
    final FunctionalKafkaConsumer<byte[], byte[]> consumer,
    final Duration metricsInterval,
    final Duration shutdownTimeout
  ) {
    this.registry = Objects.requireNonNull(registry, "Registry cannot be null");
    this.consumer = Objects.requireNonNull(consumer, "Consumer cannot be null");
    this.metricsInterval = metricsInterval != null ? metricsInterval : KafkaConfig.DEFAULT_METRICS_INTERVAL;
    this.shutdownTimeout = shutdownTimeout != null ? shutdownTimeout : KafkaConfig.DEFAULT_SHUTDOWN_TIMEOUT;
  }

  /**
   * Creates a Kafka consumer with the specified configuration.
   *
   * @param config the application configuration
   * @return a new functional Kafka consumer
   */
  private static FunctionalKafkaConsumer<byte[], byte[]> createConsumer(final KafkaConfig config) {
    final var kafkaProps = KafkaConfigFactory.createConsumerConfig(config.bootstrapServers, config.consumerGroup);

    return new FunctionalKafkaConsumer<>(kafkaProps, config.topic, createProcessorPipeline(config), config.pollTimeout);
  }

  /**
   * Creates a message processing pipeline from the specified configuration.
   *
   * @param config the application configuration
   * @return a function that processes messages
   */
  private static Function<byte[], byte[]> createProcessorPipeline(final KafkaConfig config) {
    final var tempRegistry = new MessageProcessorRegistry(config.appName);

    // Create pipeline with error handling
    Function<byte[], byte[]> pipeline = config.processors.isEmpty()
      ? tempRegistry.pipeline("parseJson", "addSource", "markProcessed", "addTimestamp")
      : tempRegistry.pipeline(config.processors.toArray(new String[0]));

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
   * Blocks the current thread until the application shuts down or the configured shutdown timeout
   * is reached.
   *
   * <p>This method is typically called after {@link #start()} to keep the main thread alive while
   * the consumer runs.
   *
   * @return true if the application shut down normally before timeout, false if timeout occurred
   */
  public boolean awaitShutdown() {
    try {
      final var normalShutdown = shutdownLatch.await(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS);
      if (!normalShutdown) {
        LOGGER.log(
          Level.WARNING,
          "Shutdown timeout reached after {0}ms, forcing termination",
          shutdownTimeout.toMillis()
        );
      }
      return normalShutdown;
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
    LOGGER.log(Level.INFO, "Initiating graceful shutdown with {0}ms drain timeout", drainTimeoutMs);

    // Signal consumer to stop accepting new messages
    consumer.pause();

    // Check if consumer is already done processing (no in-flight messages)
    var metrics = consumer.getMetrics();
    var received = metrics.getOrDefault("messagesReceived", 0L);
    var processed = metrics.getOrDefault("messagesProcessed", 0L);
    var errors = metrics.getOrDefault("processingErrors", 0L);

    // If all messages are accounted for, we can skip waiting
    if (received == processed + errors) {
      LOGGER.log(Level.INFO, "No in-flight messages detected, proceeding with shutdown");
      return true; // Don't call close() here, let the finally block handle it
    }

    // Wait for in-flight messages with periodic checks
    final var startTime = System.currentTimeMillis();
    final var deadline = startTime + drainTimeoutMs;

    try {
      while (System.currentTimeMillis() < deadline) {
        Thread.sleep(Math.min(500, drainTimeoutMs / 10)); // Check periodically

        metrics = consumer.getMetrics();
        processed = metrics.getOrDefault("messagesProcessed", 0L);
        errors = metrics.getOrDefault("processingErrors", 0L);

        if (received == processed + errors) {
          LOGGER.log(Level.INFO, "All in-flight messages completed, proceeding with shutdown");
          return true;
        }
      }

      LOGGER.log(
        Level.WARNING,
        "Drain timeout reached with {0} messages still in-flight",
        (received - processed - errors)
      );
      return false;
    } catch (InterruptedException e) {
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
   * Closes all resources associated with this application. This method is called automatically when
   * using try-with-resources.
   */
  @Override
  public void close() {
    if (!running) {
      return;
    }

    running = false;
    LOGGER.log(Level.INFO, "Closing resources...");

    try {
      consumer.close();
      if (metricsThread != null) {
        metricsThread.interrupt();
      }
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Error during shutdown", e);
    } finally {
      shutdownLatch.countDown();
    }
  }

  /** Starts a background thread that periodically reports metrics. */
  private void startMetricsReporting() {
    metricsThread =
      Thread.startVirtualThread(() -> {
        try {
          while (running) {
            reportConsumerMetrics();
            reportProcessorMetrics();
            Thread.sleep(metricsInterval);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
  }

  /** Reports consumer metrics to the logger. */
  private void reportConsumerMetrics() {
    final var consumerMetrics = consumer.getMetrics();
    if (consumerMetrics != null && !consumerMetrics.isEmpty()) {
      LOGGER.log(
        Level.INFO,
        "Consumer metrics: messages received: {0}, messages processed: {1}, errors: {2}, uptime: {3}ms",
        consumerMetrics.getOrDefault("messagesReceived", 0L),
        consumerMetrics.getOrDefault("messagesProcessed", 0L),
        consumerMetrics.getOrDefault("processingErrors", 0L),
        System.currentTimeMillis() - startTime.get()
      );
    }
  }

  /** Reports processor metrics to the logger. */
  private void reportProcessorMetrics() {
    registry
      .getAll()
      .keySet()
      .forEach(processorName -> {
        final var metrics = registry.getMetrics(processorName);
        if (!metrics.isEmpty()) {
          LOGGER.log(Level.INFO, "Processor '{0}' metrics: {1}", processorName, metrics);
        }
      });
  }
}
