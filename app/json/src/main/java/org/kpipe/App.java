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
import org.kpipe.consumer.ConsumerCommand;
import org.kpipe.consumer.ConsumerRunner;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.consumer.KafkaOffsetManager;
import org.kpipe.consumer.OffsetManager;
import org.kpipe.health.HttpHealthServer;
import org.kpipe.metrics.ConsumerMetricsReporter;
import org.kpipe.metrics.MetricsReporter;
import org.kpipe.metrics.ProcessorMetricsReporter;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.MessageSinkRegistry;
import org.kpipe.registry.RegistryKey;
import org.kpipe.sink.MessageSink;

/// Application that consumes messages from a Kafka topic and processes them using a configurable
/// pipeline of message processors.
public class App implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
  private final KPipeConsumer<byte[], byte[]> kpipeConsumer;
  private final ConsumerRunner<KPipeConsumer<byte[], byte[]>> runner;
  private final HttpHealthServer healthServer;
  private final AtomicReference<Map<String, Long>> currentMetrics = new AtomicReference<>();
  private final MessageProcessorRegistry processorRegistry;
  private final MessageSinkRegistry sinkRegistry;

  /// Main entry point for the Kafka consumer application.
  static void main() {
    final var config = AppConfig.fromEnv();

    try (final var app = new App(config)) {
      app.start();
      final var normalShutdown = app.awaitShutdown();
      if (!normalShutdown) LOGGER.log(Level.WARNING, "Application didn't shut down cleanly");
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in Kafka consumer application", e);
      System.exit(1);
    }
  }

  /// Creates a new KafkaConsumerApp with the specified configuration.
  ///
  /// @param config The application configuration
  public App(final AppConfig config) {
    this.processorRegistry = new MessageProcessorRegistry(config.appName(), MessageFormat.JSON);
    this.sinkRegistry = processorRegistry.sinkRegistry();
    // Pre-register loggers
    sinkRegistry.register(RegistryKey.of("jsonLogging", Map.class), new org.kpipe.sink.JsonConsoleSink<>());

    this.kpipeConsumer = createConsumer(config, processorRegistry);
    final var consumerMetricsReporter = ConsumerMetricsReporter.forConsumer(kpipeConsumer::getMetrics);

    final var processorMetricsReporter = ProcessorMetricsReporter.forRegistry(processorRegistry);

    runner = createConsumerRunner(config, consumerMetricsReporter, processorMetricsReporter);
    healthServer = HttpHealthServer.fromEnv(
      runner::isHealthy,
      () -> kpipeConsumer.getMetrics().getOrDefault("inFlight", 0L),
      kpipeConsumer::isPaused,
      config.appName()
    ).orElse(null);
  }

  /// Creates the consumer runner with appropriate lifecycle hooks.
  private ConsumerRunner<KPipeConsumer<byte[], byte[]>> createConsumerRunner(
    final AppConfig config,
    final MetricsReporter consumerMetricsReporter,
    final MetricsReporter processorMetricsReporter
  ) {
    return ConsumerRunner.builder(kpipeConsumer)
      .withStartAction(c -> {
        c.start();
        LOGGER.log(Level.INFO, "Kafka consumer application started successfully");
      })
      .withHealthCheck(KPipeConsumer::isRunning)
      .withGracefulShutdown(ConsumerRunner::performGracefulConsumerShutdown)
      .withMetricsReporters(List.of(consumerMetricsReporter, processorMetricsReporter))
      .withMetricsInterval(config.metricsInterval().toMillis())
      .withShutdownTimeout(config.shutdownTimeout().toMillis())
      .withShutdownHook(true)
      .build();
  }

  /// Creates a configured consumer for processing byte array messages.
  ///
  /// @param config The application configuration
  /// @param processorRegistry Map of processor functions
  /// @return A configured functional consumer
  public static KPipeConsumer<byte[], byte[]> createConsumer(
    final AppConfig config,
    final MessageProcessorRegistry processorRegistry
  ) {
    final var kafkaProps = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    return KPipeConsumer.<byte[], byte[]>builder()
      .withProperties(kafkaProps)
      .withTopic(config.topic())
      .withPipeline(createJsonProcessorPipeline(processorRegistry, config))
      .withPollTimeout(config.pollTimeout())
      .withCommandQueue(commandQueue)
      .withOffsetManagerProvider(createOffsetManagerProvider(Duration.ofSeconds(30), commandQueue))
      .withMetrics(true)
      .build();
  }

  /// Creates an OffsetManager provider function that can be used with KPipeConsumer builder
  ///
  /// @param commitInterval The interval at which to automatically commit offsets
  /// @param commandQueue The command queue
  /// @return A function that creates an OffsetManager when given a Consumer
  private static Function<Consumer<byte[], byte[]>, OffsetManager<byte[], byte[]>> createOffsetManagerProvider(
    final Duration commitInterval,
    final Queue<ConsumerCommand> commandQueue
  ) {
    return consumer ->
      KafkaOffsetManager.builder(consumer).withCommandQueue(commandQueue).withCommitInterval(commitInterval).build();
  }

  /// Creates a message sink pipeline using the provided registry.
  ///
  /// @param registry the message sink registry
  /// @return a message sink that processes messages through the pipeline
  private static MessageSink<Map<String, Object>> createSinksPipeline(final MessageSinkRegistry registry) {
    return registry.pipeline(MessageSinkRegistry.JSON_LOGGING);
  }

  /// Creates a processor pipeline using the provided registry.
  ///
  /// @param registry the message processor registry
  /// @param config the application configuration
  /// @return a function that processes messages through the pipeline
  private static java.util.function.UnaryOperator<byte[]> createJsonProcessorPipeline(
    final MessageProcessorRegistry registry,
    final AppConfig config
  ) {
    final var builder = registry.pipeline(MessageFormat.JSON);
    for (final var name : config.processors()) builder.add(RegistryKey.json(name));
    builder.toSink(RegistryKey.of("jsonLogging", Map.class));
    return builder.build();
  }

  /// Gets the processor registry used by this application.
  ///
  /// @return the message processor registry
  public MessageProcessorRegistry getProcessorRegistry() {
    return processorRegistry;
  }

  /// Gets the sink registry used by this application.
  ///
  /// @return the message sink registry
  public MessageSinkRegistry getSinkRegistry() {
    return sinkRegistry;
  }

  void start() {
    if (healthServer != null) healthServer.start();
    runner.start();
  }

  boolean awaitShutdown() {
    return runner.awaitShutdown();
  }

  private Map<String, Long> getMetrics() {
    return currentMetrics.get();
  }

  @Override
  public void close() {
    if (healthServer != null) healthServer.close();
    runner.close();
  }
}
