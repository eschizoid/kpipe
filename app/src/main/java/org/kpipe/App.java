package org.kpipe;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;
import org.kpipe.config.AppConfig;
import org.kpipe.config.KafkaConsumerConfig;
import org.kpipe.consumer.ConsumerRunner;
import org.kpipe.consumer.FunctionalConsumer;
import org.kpipe.consumer.OffsetManager;
import org.kpipe.consumer.enums.ConsumerCommand;
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
  private final FunctionalConsumer<byte[], byte[]> functionalConsumer;
  private final ConsumerRunner<FunctionalConsumer<byte[], byte[]>> runner;
  private final AtomicReference<Map<String, Long>> currentMetrics = new AtomicReference<>();
  private final MessageProcessorRegistry processorRegistry;
  private final MessageSinkRegistry sinkRegistry;

  /**
   * Main entry point for the Kafka consumer application.
   *
   * @param args Command line arguments
   */
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

  /**
   * Creates a new KafkaConsumerApp with the specified configuration.
   *
   * @param config The application configuration
   */
  public App(final AppConfig config) {
    this.processorRegistry = new MessageProcessorRegistry(config.appName());
    this.sinkRegistry = new MessageSinkRegistry();

    this.functionalConsumer = createConsumer(config, processorRegistry, sinkRegistry);

    final var consumerMetricsReporter = new ConsumerMetricsReporter(
      functionalConsumer::getMetrics,
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
      .builder(functionalConsumer)
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

  /**
   * Creates a configured consumer for processing byte array messages.
   *
   * @param config The application configuration
   * @param processorRegistry Map of processor functions
   * @param sinkRegistry Map of sink functions
   * @return A configured functional consumer
   */
  public static FunctionalConsumer<byte[], byte[]> createConsumer(
    final AppConfig config,
    final MessageProcessorRegistry processorRegistry,
    final MessageSinkRegistry sinkRegistry
  ) {
    final var kafkaProps = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    return FunctionalConsumer
      .<byte[], byte[]>builder()
      .withProperties(kafkaProps)
      .withTopic(config.topic())
      .withProcessor(createProcessorPipeline(processorRegistry))
      .withPollTimeout(config.pollTimeout())
      .withMessageSink(createSinksPipeline(sinkRegistry))
      .withCommandQueue(commandQueue)
      .withOffsetManagerProvider(createOffsetManagerProvider(Duration.ofSeconds(30), commandQueue))
      .withMetrics(true)
      .build();
  }

  /**
   * Creates an OffsetManager provider function that can be used with FunctionalConsumer builder
   *
   * @param commitInterval The interval at which to automatically commit offsets
   * @param commandQueue The command queue
   * @return A function that creates an OffsetManager when given a Consumer
   */
  private static Function<Consumer<byte[], byte[]>, OffsetManager<byte[], byte[]>> createOffsetManagerProvider(
    final Duration commitInterval,
    final Queue<ConsumerCommand> commandQueue
  ) {
    return consumer ->
      OffsetManager.builder(consumer).withCommandQueue(commandQueue).withCommitInterval(commitInterval).build();
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
