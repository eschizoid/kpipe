package org.kpipe;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.kpipe.config.AppConfig;
import org.kpipe.config.KafkaConsumerConfig;
import org.kpipe.consumer.ConsumerRunner;
import org.kpipe.consumer.FunctionalKafkaConsumer;
import org.kpipe.metrics.ConsumerMetricsReporter;
import org.kpipe.metrics.MetricsReporter;
import org.kpipe.metrics.ProcessorMetricsReporter;
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

  private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
  private final FunctionalKafkaConsumer<byte[], byte[]> consumer;
  private final ConsumerRunner<FunctionalKafkaConsumer<byte[], byte[]>> runner;
  private final Duration metricsInterval;
  private final Duration shutdownTimeout;
  private final MetricsReporter consumerMetricsReporter;
  private final MetricsReporter processorMetricsReporter;
  private final AtomicReference<Map<String, Long>> currentMetrics = new AtomicReference<>();

  private Thread metricsThread;

  /**
   * Main entry point for the Kafka consumer application.
   *
   * @param args command line arguments (not used)
   */
  public static void main(final String[] args) {
    final var config = AppConfig.fromEnv();

    try (final KafkaConsumerApp app = new KafkaConsumerApp(config)) {
      app.start();

      Runtime
        .getRuntime()
        .addShutdownHook(
          new Thread(() -> {
            final long timeoutMillis = app.getShutdownTimeout().toMillis();
            final boolean shutdownSuccess = app.shutdownGracefully(timeoutMillis);
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
  public KafkaConsumerApp(final AppConfig config) {
    final MessageProcessorRegistry registry = new MessageProcessorRegistry(config.appName);
    final MessageSink<byte[], byte[]> messageSink = new LoggingSink<>();

    this.consumer = createConsumer(config, registry, messageSink);
    this.metricsInterval = config.metricsInterval;
    this.shutdownTimeout = config.shutdownTimeout;

    this.consumerMetricsReporter =
      new ConsumerMetricsReporter(consumer::getMetrics, () -> System.currentTimeMillis() - startTime.get(), null);

    this.processorMetricsReporter = new ProcessorMetricsReporter(registry, null);

    // Configure and create the ConsumerRunner
    this.runner =
      ConsumerRunner
        .builder(consumer)
        .withStartAction(c -> {
          startMetricsReporting();
          c.start();
          LOGGER.log(Level.INFO, "Kafka consumer application started successfully");
        })
        .withHealthCheck(FunctionalKafkaConsumer::isRunning)
        .withGracefulShutdown(this::performGracefulShutdown)
        .build();
  }

  /**
   * Performs a graceful shutdown of the consumer, waiting for in-flight messages.
   *
   * @param consumer the consumer to shut down
   * @param timeoutMs maximum time in milliseconds to wait for in-flight messages
   * @return true if all in-flight messages completed, false if timeout was reached
   */
  private boolean performGracefulShutdown(FunctionalKafkaConsumer<byte[], byte[]> consumer, long timeoutMs) {
    LOGGER.log(Level.INFO, "Initiating graceful shutdown with {0}ms drain timeout", timeoutMs);

    try {
      consumer.pause();
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Error pausing consumer during shutdown", e);
    }

    // Check if consumer is already done processing (no in-flight messages)
    var metrics = consumer.getMetrics();
    currentMetrics.set(metrics);
    var received = metrics.getOrDefault(METRIC_MESSAGES_RECEIVED, 0L);
    var processed = metrics.getOrDefault(METRIC_MESSAGES_PROCESSED, 0L);
    var errors = metrics.getOrDefault(METRIC_PROCESSING_ERRORS, 0L);

    // If all messages are accounted for, we can skip waiting
    if (received == processed + errors) {
      LOGGER.log(Level.INFO, "No in-flight messages detected, proceeding with shutdown");
      return true;
    }

    // Wait for in-flight messages with periodic checks
    final var startTime = System.currentTimeMillis();
    final var deadline = startTime + timeoutMs;

    try {
      while (System.currentTimeMillis() < deadline) {
        Thread.sleep(Math.min(500, timeoutMs / 10)); // Check periodically

        metrics = consumer.getMetrics();
        currentMetrics.set(metrics);
        processed = metrics.getOrDefault(METRIC_MESSAGES_PROCESSED, 0L);
        errors = metrics.getOrDefault(METRIC_PROCESSING_ERRORS, 0L);

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
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Graceful shutdown was interrupted");
      return false;
    }
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
   * @param messageSink the message sink to use
   * @return a new functional Kafka consumer
   */
  private static FunctionalKafkaConsumer<byte[], byte[]> createConsumer(
    final AppConfig config,
    final MessageProcessorRegistry registry,
    final MessageSink<byte[], byte[]> messageSink
  ) {
    final var kafkaProps = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers, config.consumerGroup);

    return new FunctionalKafkaConsumer.Builder<byte[], byte[]>()
      .withProperties(kafkaProps)
      .withTopic(config.topic)
      .withProcessor(createProcessorPipeline(config, registry))
      .withPollTimeout(config.pollTimeout)
      .withMessageSink(messageSink)
      .withMetrics(true)
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
    final AppConfig config,
    final MessageProcessorRegistry registry
  ) {
    final Function<byte[], byte[]> pipeline = config.processors.isEmpty()
      ? registry.pipeline("parseJson", "addSource", "markProcessed", "addTimestamp")
      : registry.pipeline(config.processors.toArray(new String[0]));

    return MessageProcessorRegistry.withErrorHandling(pipeline, null);
  }

  /** Starts the Kafka consumer application. */
  public void start() {
    runner.start();
  }

  /**
   * Blocks the current thread until the application shuts down.
   *
   * @return true if the application shut down normally, false otherwise
   */
  public boolean awaitShutdown() {
    return runner.awaitShutdown();
  }

  /**
   * Initiates a graceful shutdown allowing in-flight messages to complete.
   *
   * @param timeoutMs maximum time in milliseconds to wait for in-flight messages
   * @return true if all in-flight messages completed within the timeout, false otherwise
   */
  public boolean shutdownGracefully(final long timeoutMs) {
    return runner.shutdownGracefully(timeoutMs);
  }

  /**
   * Returns whether the consumer application is healthy and functioning properly.
   *
   * @return true if the application is running and the consumer is operational
   */
  public boolean isHealthy() {
    return runner.isHealthy();
  }

  /**
   * Returns whether the application is currently in a running state.
   *
   * @return true if the application is running, false otherwise
   */
  public boolean isRunning() {
    return runner.isHealthy();
  }

  /**
   * Returns whether the application is currently shutting down.
   *
   * @return true if the application is shutting down, false otherwise
   */
  public boolean isShuttingDown() {
    return !runner.isHealthy() && !isRunning();
  }

  /**
   * Returns the current metrics from the consumer.
   *
   * @return a map containing the current metrics
   */
  public Map<String, Long> getMetrics() {
    return currentMetrics.get();
  }

  /** Closes all resources associated with this application. */
  @Override
  public void close() {
    runner.close();

    if (metricsThread != null) {
      metricsThread.interrupt();
    }
  }

  /** Starts a background thread that periodically reports metrics. */
  private void startMetricsReporting() {
    metricsThread =
      Thread.startVirtualThread(() -> {
        try {
          while (!Thread.currentThread().isInterrupted()) {
            try {
              consumerMetricsReporter.reportMetrics();
              processorMetricsReporter.reportMetrics();

              // Store current metrics for health checks
              currentMetrics.set(consumer.getMetrics());
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
}
