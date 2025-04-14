package org.kpipe;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
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
import org.kpipe.processor.MessageProcessorRegistry;
import org.kpipe.sink.LoggingSink;
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
    final MessageProcessorRegistry registry = new MessageProcessorRegistry(config.appName());
    final MessageSink<byte[], byte[]> messageSink = new LoggingSink<>();

    this.consumer = createConsumer(config, registry, messageSink);

    final var consumerMetricsReporter = new ConsumerMetricsReporter(
      consumer::getMetrics,
      () -> System.currentTimeMillis() - startTime.get(),
      null
    );

    final var processorMetricsReporter = new ProcessorMetricsReporter(registry, null);

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
      .withMetricsReporter(consumerMetricsReporter)
      .withMetricsReporter(processorMetricsReporter)
      .withMetricsInterval(config.metricsInterval().toMillis())
      .withShutdownTimeout(config.shutdownTimeout().toMillis())
      .withShutdownHook(true)
      .build();
  }

  private static FunctionalConsumer<byte[], byte[]> createConsumer(
    final AppConfig config,
    final MessageProcessorRegistry registry,
    final MessageSink<byte[], byte[]> messageSink
  ) {
    final var kafkaProps = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    return new FunctionalConsumer.Builder<byte[], byte[]>()
      .withProperties(kafkaProps)
      .withTopic(config.topic())
      .withProcessor(createProcessorPipeline(config, registry))
      .withPollTimeout(config.pollTimeout())
      .withMessageSink(messageSink)
      .withMetrics(true)
      .build();
  }

  private static Function<byte[], byte[]> createProcessorPipeline(
    final AppConfig config,
    final MessageProcessorRegistry registry
  ) {
    final var pipeline = config.processors().isEmpty()
      ? registry.pipeline("parseJson", "addSource", "markProcessed", "addTimestamp")
      : registry.pipeline(config.processors().toArray(new String[0]));

    return MessageProcessorRegistry.withErrorHandling(pipeline, null);
  }

  public void start() {
    runner.start();
  }

  public boolean awaitShutdown() {
    return runner.awaitShutdown();
  }

  public Map<String, Long> getMetrics() {
    return currentMetrics.get();
  }

  @Override
  public void close() {
    runner.close();
  }
}
