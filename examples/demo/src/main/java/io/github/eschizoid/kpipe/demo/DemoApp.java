package io.github.eschizoid.kpipe.demo;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import io.github.eschizoid.kpipe.Handle;
import io.github.eschizoid.kpipe.KPipe;
import io.github.eschizoid.kpipe.consumer.config.KafkaConsumerConfig;
import io.github.eschizoid.kpipe.format.avro.AvroFormat;
import io.github.eschizoid.kpipe.format.json.JsonConsoleSink;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.metrics.otel.OtelConsumerMetrics;
import io.github.eschizoid.kpipe.registry.Operators;
import io.github.eschizoid.kpipe.schemaregistry.confluent.ConfluentSchemaResolver;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;

/// Demo application that consumes JSON, Avro, and Protobuf payloads through a single
/// `KPipe.multi(...)` consumer. One Kafka consumer-group, one offset manager, three typed
/// pipelines dispatched by `record.topic()`. OpenTelemetry metrics carry a `topic` attribute so
/// the Grafana dashboards can break down by topic across the heterogeneous routes.
public class DemoApp implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(DemoApp.class.getName());
  private static OpenTelemetry SHARED_OTEL;

  private final Handle handle;

  static void main() {
    final var config = DemoConfig.fromEnv();

    try {
      SHARED_OTEL = initOpenTelemetry();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "OTel SDK init failed; falling back to GlobalOpenTelemetry: {0}", e.getMessage());
      SHARED_OTEL = GlobalOpenTelemetry.get();
    }

    final AvroFormat avroFormat;
    try (final var resolver = new ConfluentSchemaResolver(config.schemaRegistryUrl())) {
      avroFormat = AvroFormat.of(resolver.lookupBySubjectVersion("com.kpipe.customer", "latest"));
    }
    final var protoFormat = new ProtobufFormat(buildCustomerDescriptor());

    try (final var app = new DemoApp(config, avroFormat, protoFormat)) {
      LOGGER.log(Level.INFO, "Demo application started — JSON/Avro/Protobuf routes via KPipe.multi");
      app.handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in demo application", e);
      System.exit(1);
    }
  }

  /// Creates and starts the demo application with all three routes attached to a single consumer.
  /// The Avro [AvroFormat] and Protobuf [ProtobufFormat] are passed in explicitly — the caller is
  /// responsible for resolving the Avro schema (e.g. via Schema Registry) and building the
  /// Protobuf descriptor.
  public DemoApp(final DemoConfig config, final AvroFormat avroFormat, final ProtobufFormat protoFormat) {
    final var kafkaProps = KafkaConsumerConfig.createConsumerConfig(
      config.bootstrapServers(),
      config.consumerGroup() + "-multi"
    );
    final var otel = SHARED_OTEL != null ? SHARED_OTEL : GlobalOpenTelemetry.get();

    handle = KPipe.multi(kafkaProps)
      .withMetrics(new OtelConsumerMetrics(otel, "demo-multi"))
      .json(config.jsonTopic(), s ->
        s
          .pipe(Operators.addField("source", "demo-app"))
          .pipe(Operators.addField("status", "processed"))
          .pipe(Operators.addField("processedAt", System.currentTimeMillis()))
          .pipe(Operators.removeFields("password", "ssn"))
          .toCustom(new JsonConsoleSink<>())
      )
      .avro(avroFormat, config.avroTopic(), s -> s.skipBytes(5).toConsole())
      .protobuf(protoFormat, config.protoTopic(), s -> s.skipBytes(6).toConsole())
      .start();
  }

  @Override
  public void close() {
    handle.close();
  }

  /// Initialises an OTLP/gRPC OpenTelemetry SDK pointed at the demo's collector. Reads
  /// `OTEL_EXPORTER_OTLP_ENDPOINT` (default <http://otel-collector:4317>).
  private static OpenTelemetry initOpenTelemetry() {
    final var endpoint = System.getenv().getOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317");
    final var exporter = OtlpGrpcMetricExporter.builder().setEndpoint(endpoint).build();
    final var reader = PeriodicMetricReader.builder(exporter).setInterval(Duration.ofSeconds(10)).build();
    final var meterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build();
    final var sdk = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).buildAndRegisterGlobal();

    Runtime.getRuntime().addShutdownHook(
      new Thread(() -> {
        try {
          meterProvider.close();
        } catch (final Exception ignored) {}
      })
    );
    return sdk;
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
