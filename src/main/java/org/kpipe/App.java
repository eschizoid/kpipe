package org.kpipe;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.kpipe.config.AppConfig;
import org.kpipe.config.KafkaConsumerConfig;
import org.kpipe.consumer.ConsumerRunner;
import org.kpipe.consumer.FunctionalConsumer;
import org.kpipe.metrics.ConsumerMetricsReporter;
import org.kpipe.metrics.MetricsReporter;
import org.kpipe.metrics.ProcessorMetricsReporter;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.MessageSinkRegistry;
import org.kpipe.sink.MessageSink;

/**
 * Application that consumes messages from a Kafka topic and processes them using a configurable
 * pipeline of message processors.
 */
public class App implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
  private final FunctionalConsumer<byte[], byte[]> consumer;
  private final ConsumerRunner<FunctionalConsumer<byte[], byte[]>> runner;
  private final AtomicReference<Map<String, Long>> currentMetrics = new AtomicReference<>();
  private final MessageProcessorRegistry processorRegistry;
  private final MessageSinkRegistry sinkRegistry;

  /** Main entry point for the Kafka consumer application. */
  public static void main(final String[] args) {
    final var config = AppConfig.fromEnv();

    try (final App app = new App(config)) {
      app.start();
      final var normalShutdown = app.awaitShutdown();
      if (!normalShutdown) {
        LOGGER.log(Level.WARNING, "Application didn't shut down cleanly");
      }
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in Kafka consumer application", e);
      System.exit(1);
    }
  }

  /** Creates a new KafkaConsumerApp with the specified configuration. */
  public App(final AppConfig config) {
    this.processorRegistry = new MessageProcessorRegistry(config.appName());
    this.sinkRegistry = new MessageSinkRegistry();

    this.consumer = createConsumer(config, processorRegistry, sinkRegistry);

    final var consumerMetricsReporter = new ConsumerMetricsReporter(
      consumer::getMetrics,
      () -> System.currentTimeMillis() - startTime.get(),
      null
    );

    final var processorMetricsReporter = new ProcessorMetricsReporter(processorRegistry, null);

    this.runner = createConsumerRunner(config, consumerMetricsReporter, processorMetricsReporter);
  }

  /** Creates the consumer runner with appropriate lifecycle hooks. */
  private ConsumerRunner<FunctionalConsumer<byte[], byte[]>> createConsumerRunner(
    final AppConfig config,
    final MetricsReporter consumerMetricsReporter,
    final MetricsReporter processorMetricsReporter
  ) {
    return ConsumerRunner
      .builder(consumer)
      .withStartAction(c -> {
        c.start();
        LOGGER.log(Level.INFO, "Kafka consumer application started successfully");
      })
      .withHealthCheck(FunctionalConsumer::isRunning)
      .withGracefulShutdown(ConsumerRunner::performGracefulConsumerShutdown)
      .withMetricsReporters(List.of(consumerMetricsReporter, processorMetricsReporter))
      .withMetricsInterval(config.metricsInterval().toMillis())
      .withShutdownTimeout(config.shutdownTimeout().toMillis())
      .withShutdownHook(true)
      .build();
  }

  public static FunctionalConsumer<byte[], byte[]> createConsumer(
    final AppConfig config,
    final MessageProcessorRegistry processorRegistry,
    final MessageSinkRegistry sinkRegistry
  ) {
    final var kafkaProps = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    return new FunctionalConsumer.Builder<byte[], byte[]>()
      .withProperties(kafkaProps)
      .withTopic(config.topic())
      .withProcessor(createProcessorPipeline(processorRegistry))
      .withPollTimeout(config.pollTimeout())
      .withMessageSink(createSinksPipeline(sinkRegistry))
      .withMetrics(true)
      .build();
  }

  /**
   * Creates a message sink pipeline using the provided registry.
   *
   * @param registry the message sink registry
   * @return a message sink that processes messages through the pipeline
   */
  private static MessageSink<byte[], byte[]> createSinksPipeline(final MessageSinkRegistry registry) {
    final var pipeline = registry.<byte[], byte[]>pipeline("logging");
    return MessageSinkRegistry.withErrorHandling(pipeline);
  }

  /**
   * Creates a processor pipeline using the provided registry.
   *
   * @param registry the message processor registry
   * @return a function that processes messages through the pipeline
   */
  private static Function<byte[], byte[]> createProcessorPipeline(final MessageProcessorRegistry registry) {
    final var pipeline = registry.pipeline("parseJson", "addSource", "markProcessed", "addTimestamp");
    return MessageProcessorRegistry.withErrorHandling(pipeline, null);
  }

  /**
   * Gets the processor registry used by this application.
   *
   * @return the message processor registry
   */
  public MessageProcessorRegistry getProcessorRegistry() {
    return processorRegistry;
  }

  /**
   * Gets the sink registry used by this application.
   *
   * @return the message sink registry
   */
  public MessageSinkRegistry getSinkRegistry() {
    return sinkRegistry;
  }

  private void start() {
    runner.start();
  }

  private boolean awaitShutdown() {
    return runner.awaitShutdown();
  }

  private Map<String, Long> getMetrics() {
    return currentMetrics.get();
  }

  @Override
  public void close() {
    runner.close();
  }
}
