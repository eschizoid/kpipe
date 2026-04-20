package org.kpipe;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.kafka.clients.consumer.Consumer;
import org.kpipe.consumer.*;
import org.kpipe.consumer.config.AppConfig;
import org.kpipe.consumer.config.KafkaConsumerConfig;
import org.kpipe.consumer.metrics.ProcessorMetricsReporter;
import org.kpipe.consumer.metrics.SinkMetricsReporter;
import org.kpipe.health.HttpHealthServer;
import org.kpipe.metrics.ConsumerMetricsReporter;
import org.kpipe.metrics.KPipeMetricsReporter;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.MessageSinkRegistry;
import org.kpipe.registry.RegistryKey;

/// Application that consumes messages from a Kafka topic and processes them using a configurable
/// pipeline of message processors.
public class App implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private final KPipeConsumer<byte[], byte[]> kpipeConsumer;
  private final KPipeRunner<KPipeConsumer<byte[], byte[]>> runner;
  private final HttpHealthServer healthServer;
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
    processorRegistry = new MessageProcessorRegistry(config.appName(), MessageFormat.PROTOBUF);
    sinkRegistry = processorRegistry.sinkRegistry();

    kpipeConsumer = createConsumer(config, processorRegistry);

    final var consumerMetricsReporter = ConsumerMetricsReporter.forConsumer(kpipeConsumer::getMetrics);

    final var processorMetricsReporter = ProcessorMetricsReporter.forRegistry(processorRegistry);
    final var sinkMetricsReporter = SinkMetricsReporter.forRegistry(sinkRegistry);
    runner = createConsumerRunner(config, consumerMetricsReporter, processorMetricsReporter, sinkMetricsReporter);
    healthServer = HttpHealthServer.fromEnv(
      runner::isHealthy,
      () -> kpipeConsumer.getMetrics().getOrDefault("inFlight", 0L),
      kpipeConsumer::isPaused,
      config.appName()
    ).orElse(null);
  }

  /// Creates the consumer runner with appropriate lifecycle hooks.
  private KPipeRunner<KPipeConsumer<byte[], byte[]>> createConsumerRunner(
    final AppConfig config,
    final KPipeMetricsReporter consumerMetricsReporter,
    final KPipeMetricsReporter processorMetricsReporter,
    final KPipeMetricsReporter sinkMetricsReporter
  ) {
    return KPipeRunner.builder(kpipeConsumer)
      .withStartAction(c -> {
        c.start();
        LOGGER.log(Level.INFO, "Kafka consumer application started successfully");
      })
      .withHealthCheck(KPipeConsumer::isRunning)
      .withGracefulShutdown(KPipeRunner::performGracefulConsumerShutdown)
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
      .withPipeline(createProtobufProcessorPipeline(processorRegistry, config))
      .withPollTimeout(config.pollTimeout())
      .withCommandQueue(commandQueue)
      .withOffsetManagerProvider(createOffsetManagerProvider(Duration.ofSeconds(30), commandQueue))
      .enableMetrics(true)
      .build();
  }

  /// Creates an OffsetManager provider function that can be used with KPipeConsumer builder.
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

  /// Creates a processor pipeline using the provided registry.
  ///
  /// @param registry the message processor registry
  /// @param config the application configuration
  /// @return a function that processes messages through the pipeline
  private static UnaryOperator<byte[]> createProtobufProcessorPipeline(
    final MessageProcessorRegistry registry,
    final AppConfig config
  ) {
    final var protoFormat = MessageFormat.PROTOBUF;
    // Register the Customer descriptor programmatically
    protoFormat.addDescriptor("customer", buildCustomerDescriptor());
    protoFormat.withDefaultDescriptor("customer");

    final var builder = registry.pipeline(protoFormat);
    for (final var name : config.processors()) builder.add(RegistryKey.protobuf(name));
    builder.toSink(RegistryKey.protobuf("protobufLogging"));
    return builder.build();
  }

  /// Builds a Customer descriptor programmatically (no protoc codegen required).
  ///
  /// @return The Customer message descriptor
  static Descriptors.Descriptor buildCustomerDescriptor() {
    try {
      final var customerMsg = DescriptorProtos.DescriptorProto.newBuilder()
        .setName("Customer")
        .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
        .addField(field("name", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
        .addField(field("email", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
        .addField(field("active", 4, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL))
        .addField(field("registration_date", 5, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
        .build();

      final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
        .setName("customer.proto")
        .setPackage("com.kpipe.customer")
        .setSyntax("proto3")
        .addMessageType(customerMsg)
        .build();

      final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
      return fileDescriptor.findMessageTypeByName("Customer");
    } catch (final Descriptors.DescriptorValidationException e) {
      throw new RuntimeException("Failed to build Customer descriptor", e);
    }
  }

  private static DescriptorProtos.FieldDescriptorProto field(
    final String name,
    final int number,
    final DescriptorProtos.FieldDescriptorProto.Type type
  ) {
    return DescriptorProtos.FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type).build();
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

  @Override
  public void close() {
    if (healthServer != null) healthServer.close();
    runner.close();
  }
}
