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
import org.kpipe.metrics.SinkMetricsReporter;
import org.kpipe.processor.AvroMessageProcessor;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.MessageSinkRegistry;
import org.kpipe.sink.AvroConsoleSink;
import org.kpipe.sink.MessageSink;

/// Application that consumes messages from a Kafka topic and processes them using a configurable
/// pipeline of message processors.
public class App implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(App.class.getName());
  private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://schema-registry:8081";

  private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
  private final FunctionalConsumer<byte[], byte[]> functionalConsumer;
  private final ConsumerRunner<FunctionalConsumer<byte[], byte[]>> runner;
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
    this(config, resolveSchemaRegistryUrl());
  }

  /// Creates a new KafkaConsumerApp with an explicit schema registry URL.
  ///
  /// @param config The application configuration
  /// @param schemaRegistryUrl Schema Registry base URL
  public App(final AppConfig config, final String schemaRegistryUrl) {
    processorRegistry = new MessageProcessorRegistry(config.appName(), MessageFormat.AVRO);
    sinkRegistry = new MessageSinkRegistry();
    functionalConsumer = createConsumer(config, processorRegistry, sinkRegistry, schemaRegistryUrl);

    final var consumerMetricsReporter = new ConsumerMetricsReporter(
      functionalConsumer::getMetrics,
      () -> System.currentTimeMillis() - startTime.get(),
      null
    );

    final var processorMetricsReporter = new ProcessorMetricsReporter(processorRegistry, null);
    final var sinkMetricsReporter = new SinkMetricsReporter(sinkRegistry, null);
    runner = createConsumerRunner(config, consumerMetricsReporter, processorMetricsReporter, sinkMetricsReporter);
  }

  private static String resolveSchemaRegistryUrl() {
    return System.getProperty(
      "SCHEMA_REGISTRY_URL",
      System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY_URL)
    );
  }

  /** Creates the consumer runner with appropriate lifecycle hooks. */
  private ConsumerRunner<FunctionalConsumer<byte[], byte[]>> createConsumerRunner(
    final AppConfig config,
    final MetricsReporter consumerMetricsReporter,
    final MetricsReporter processorMetricsReporter,
    final MetricsReporter sinkMetricsReporter
  ) {
    return ConsumerRunner
      .builder(functionalConsumer)
      .withStartAction(c -> {
        c.start();
        LOGGER.log(Level.INFO, "Kafka consumer application started successfully");
      })
      .withHealthCheck(FunctionalConsumer::isRunning)
      .withGracefulShutdown(ConsumerRunner::performGracefulConsumerShutdown)
      .withMetricsReporters(List.of(consumerMetricsReporter, processorMetricsReporter, sinkMetricsReporter))
      .withMetricsInterval(config.metricsInterval().toMillis())
      .withShutdownTimeout(config.shutdownTimeout().toMillis())
      .withShutdownHook(true)
      .build();
  }

  /// Creates a configured consumer for processing byte array messages.
  ///
  /// @param config The application configuration
  /// @param processorRegistry Map of processor functions
  /// @param sinkRegistry Map of sink functions
  /// @return A configured functional consumer
  public static FunctionalConsumer<byte[], byte[]> createConsumer(
    final AppConfig config,
    final MessageProcessorRegistry processorRegistry,
    final MessageSinkRegistry sinkRegistry,
    final String schemaRegistryUrl
  ) {
    final var kafkaProps = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    return FunctionalConsumer
      .<byte[], byte[]>builder()
      .withProperties(kafkaProps)
      .withTopic(config.topic())
      .withProcessor(createAvroProcessorPipeline(processorRegistry, config, sinkRegistry, schemaRegistryUrl))
      .withPollTimeout(config.pollTimeout())
      .withMessageSink(createSinksPipeline(sinkRegistry))
      .withCommandQueue(commandQueue)
      .withOffsetManagerProvider(createOffsetManagerProvider(Duration.ofSeconds(30), commandQueue))
      .withMetrics(true)
      .build();
  }

  /// Creates an OffsetManager provider function that can be used with FunctionalConsumer builder
  ///
  /// @param commitInterval The interval at which to automatically commit offsets
  /// @param commandQueue The command queue
  /// @return A function that creates an OffsetManager when given a Consumer
  private static Function<Consumer<byte[], byte[]>, OffsetManager<byte[], byte[]>> createOffsetManagerProvider(
    final Duration commitInterval,
    final Queue<ConsumerCommand> commandQueue
  ) {
    return consumer ->
      OffsetManager.builder(consumer).withCommandQueue(commandQueue).withCommitInterval(commitInterval).build();
  }

  /// Creates a message sink pipeline using the provided registry.
  ///
  /// @param registry the message sink registry
  /// @return a message sink that processes messages through the pipeline
  private static MessageSink<byte[], byte[]> createSinksPipeline(final MessageSinkRegistry registry) {
    return registry.pipeline("avroLogging");
  }

  /// Creates a processor pipeline using the provided registry.
  ///
  /// @param registry the message processor registry
  /// @param config the application configuration
  /// @param sinkRegistry the message sink registry
  /// @return a function that processes messages through the pipeline
  private static Function<byte[], byte[]> createAvroProcessorPipeline(
    final MessageProcessorRegistry registry,
    final AppConfig config,
    final MessageSinkRegistry sinkRegistry,
    final String schemaRegistryUrl
  ) {
    registry.addSchema("1", "com.kpipe.customer", schemaRegistryUrl + "/subjects/com.kpipe.customer/versions/latest");

    // Register the sink
    final var schema = AvroMessageProcessor.getSchema("1");
    if (schema != null) sinkRegistry.register("avroLogging", new AvroConsoleSink<>(schema));

    return registry.avroPipeline("1", 5, config.processors().toArray(new String[0]));
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
    runner.close();
  }
}
