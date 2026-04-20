package org.kpipe.demo;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import io.opentelemetry.api.GlobalOpenTelemetry;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.kpipe.consumer.*;
import org.kpipe.consumer.config.AppConfig;
import org.kpipe.consumer.config.KafkaConsumerConfig;
import org.kpipe.consumer.metrics.ProcessorMetricsReporter;
import org.kpipe.consumer.metrics.SinkMetricsReporter;
import org.kpipe.consumer.sink.AvroConsoleSink;
import org.kpipe.consumer.sink.JsonConsoleSink;
import org.kpipe.consumer.sink.ProtobufConsoleSink;
import org.kpipe.metrics.ConsumerMetrics;
import org.kpipe.metrics.ConsumerMetricsReporter;
import org.kpipe.processor.JsonMessageProcessor;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.RegistryKey;
import org.apache.kafka.clients.consumer.Consumer;

/// Demo application that runs JSON, Avro, and Protobuf consumer pipelines concurrently.
public class DemoApp implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(DemoApp.class.getName());
  private static final String TEST_AVRO_SCHEMA_PATH = "build/resources/test/avro/customer.avsc";

  private final KPipeRunner<KPipeConsumer<byte[], byte[]>> jsonRunner;
  private final KPipeRunner<KPipeConsumer<byte[], byte[]>> avroRunner;
  private final KPipeRunner<KPipeConsumer<byte[], byte[]>> protoRunner;

  /// Main entry point.
  static void main() {
    final var config = DemoConfig.fromEnv();

    try (final var app = new DemoApp(config)) {
      app.start();
      LOGGER.log(Level.INFO, "Demo application started — JSON, Avro, and Protobuf pipelines running");
      // Block on the JSON runner (all three run concurrently)
      app.jsonRunner.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in demo application", e);
      System.exit(1);
    }
  }

  /// Creates the demo application with all three pipelines.
  public DemoApp(final DemoConfig config) {
    jsonRunner = buildJsonPipeline(config);
    avroRunner = buildAvroPipeline(config);
    protoRunner = buildProtobufPipeline(config);
  }

  void start() {
    jsonRunner.start();
    avroRunner.start();
    protoRunner.start();
  }

  @Override
  public void close() {
    jsonRunner.close();
    avroRunner.close();
    protoRunner.close();
  }

  /// ── JSON pipeline ──────────────────────────────────────────────────────

  private static KPipeRunner<KPipeConsumer<byte[], byte[]>> buildJsonPipeline(final DemoConfig config) {
    final var registry = new MessageProcessorRegistry("demo-json", MessageFormat.JSON);
    final var sinkRegistry = registry.sinkRegistry();
    sinkRegistry.register(RegistryKey.json("jsonLogging"), new JsonConsoleSink<>());

    // Register demo processors
    registry.register(RegistryKey.json("addSource"), JsonMessageProcessor.addFieldOperator("source", "demo-app"));
    registry.register(RegistryKey.json("markProcessed"), JsonMessageProcessor.addFieldOperator("status", "processed"));
    registry.register(RegistryKey.json("addTimestamp"), JsonMessageProcessor.addTimestampOperator("processedAt"));
    registry.register(RegistryKey.json("removeSecrets"), JsonMessageProcessor.removeFieldsOperator("password", "ssn"));

    final var pipeline = registry.pipeline(MessageFormat.JSON);
    pipeline.add(RegistryKey.json("addSource"));
    pipeline.add(RegistryKey.json("markProcessed"));
    pipeline.add(RegistryKey.json("addTimestamp"));
    pipeline.add(RegistryKey.json("removeSecrets"));
    pipeline.toSink(RegistryKey.json("jsonLogging"));

    final var appConfig = toAppConfig(
      config,
      config.jsonTopic(),
      "demo-json",
      List.of("addSource", "markProcessed", "addTimestamp", "removeSecrets")
    );
    final var consumer = buildConsumer(appConfig, pipeline.build());
    return buildRunner(appConfig, consumer, registry);
  }

  /// ── Avro pipeline ─────────────────────────────────────────────────────

  private static KPipeRunner<KPipeConsumer<byte[], byte[]>> buildAvroPipeline(final DemoConfig config) {
    final var registry = new MessageProcessorRegistry("demo-avro", MessageFormat.AVRO);
    final var sinkRegistry = registry.sinkRegistry();
    sinkRegistry.register(RegistryKey.avro("avroLogging"), new AvroConsoleSink<>());

    final var avroFormat = MessageFormat.AVRO;
    final String avroSchemaLocation = isTestMode()
      ? TEST_AVRO_SCHEMA_PATH
      : config.schemaRegistryUrl() + "/subjects/com.kpipe.customer/versions/latest";
    avroFormat.addSchema("1", "com.kpipe.customer", avroSchemaLocation);
    avroFormat.withDefaultSchema("1");

    final var pipeline = registry.pipeline(avroFormat);
    pipeline.skipBytes(5); // Skip Confluent Schema Registry wire-format prefix
    pipeline.toSink(RegistryKey.avro("avroLogging"));

    final var appConfig = toAppConfig(config, config.avroTopic(), "demo-avro", List.of());
    final var consumer = buildConsumer(appConfig, pipeline.build());
    return buildRunner(appConfig, consumer, registry);
  }

  /** Returns true if running in test mode (system property or env var set to "true"). */
  private static boolean isTestMode() {
    return (
      "true".equalsIgnoreCase(System.getProperty("kpipe.test.mode")) ||
      "true".equalsIgnoreCase(System.getenv("KPIPE_TEST_MODE"))
    );
  }

  /// ── Protobuf pipeline ─────────────────────────────────────────────────
  private static KPipeRunner<KPipeConsumer<byte[], byte[]>> buildProtobufPipeline(final DemoConfig config) {
    final var registry = new MessageProcessorRegistry("demo-protobuf", MessageFormat.PROTOBUF);
    final var sinkRegistry = registry.sinkRegistry();
    sinkRegistry.register(RegistryKey.protobuf("protobufLogging"), new ProtobufConsoleSink<>());

    final var protoFormat = MessageFormat.PROTOBUF;
    protoFormat.addDescriptor("customer", buildCustomerDescriptor());
    protoFormat.withDefaultDescriptor("customer");

    final var pipeline = registry.pipeline(protoFormat);
    pipeline.skipBytes(5); // Skip Confluent Schema Registry wire-format prefix
    pipeline.toSink(RegistryKey.protobuf("protobufLogging"));

    final var appConfig = toAppConfig(config, config.protoTopic(), "demo-protobuf", List.of());
    final var consumer = buildConsumer(appConfig, pipeline.build());
    return buildRunner(appConfig, consumer, registry);
  }

  /// ── Shared helpers ────────────────────────────────────────────────────

  private static KPipeConsumer<byte[], byte[]> buildConsumer(
    final AppConfig appConfig,
    final UnaryOperator<byte[]> pipeline
  ) {
    final var kafkaProps = KafkaConsumerConfig.createConsumerConfig(
      appConfig.bootstrapServers(),
      appConfig.consumerGroup()
    );
    final Queue<ConsumerCommand> commandQueue = new ConcurrentLinkedQueue<>();

    return KPipeConsumer.<byte[], byte[]>builder()
      .withProperties(kafkaProps)
      .withTopic(appConfig.topic())
      .withPipeline(pipeline)
      .withPollTimeout(appConfig.pollTimeout())
      .withCommandQueue(commandQueue)
      .withOffsetManagerProvider(createOffsetManagerProvider(Duration.ofSeconds(30), commandQueue))
      .withMetrics(new ConsumerMetrics(GlobalOpenTelemetry.get()))
      .enableMetrics(true)
      .build();
  }

  private static KPipeRunner<KPipeConsumer<byte[], byte[]>> buildRunner(
    final AppConfig appConfig,
    final KPipeConsumer<byte[], byte[]> consumer,
    final MessageProcessorRegistry registry
  ) {
    final var consumerMetrics = ConsumerMetricsReporter.forConsumer(consumer::getMetrics);
    final var processorMetrics = ProcessorMetricsReporter.forRegistry(registry);
    final var sinkMetrics = SinkMetricsReporter.forRegistry(registry.sinkRegistry());

    return KPipeRunner.builder(consumer)
      .withStartAction(c -> {
        c.start();
        LOGGER.log(Level.INFO, "Pipeline started for topic: {0}", appConfig.topic());
      })
      .withHealthCheck(KPipeConsumer::isRunning)
      .withGracefulShutdown(KPipeRunner::performGracefulConsumerShutdown)
      .withMetricsReporters(List.of(consumerMetrics, processorMetrics, sinkMetrics))
      .withMetricsInterval(appConfig.metricsInterval().toMillis())
      .withShutdownTimeout(appConfig.shutdownTimeout().toMillis())
      .withShutdownHook(true)
      .build();
  }

  private static Function<Consumer<byte[], byte[]>,
    OffsetManager<byte[], byte[]>
  > createOffsetManagerProvider(final Duration commitInterval, final Queue<ConsumerCommand> commandQueue) {
    return consumer ->
      KafkaOffsetManager.builder(consumer).withCommandQueue(commandQueue).withCommitInterval(commitInterval).build();
  }

  private static AppConfig toAppConfig(
    final DemoConfig config,
    final String topic,
    final String appName,
    final List<String> processors
  ) {
    return new AppConfig(
      config.bootstrapServers(),
      config.consumerGroup() + "-" + appName,
      topic,
      appName,
      config.pollTimeout(),
      config.shutdownTimeout(),
      config.metricsInterval(),
      processors
    );
  }

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
}
