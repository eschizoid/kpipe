package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.config.AppConfig;
import io.github.eschizoid.kpipe.consumer.config.KafkaConsumerConfig;
import io.github.eschizoid.kpipe.tracing.otel.OtelTracer;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

/// Reads JSON records from `KAFKA_TOPIC`, tags each one, and writes it to `OUTPUT_TOPIC` (default
/// `<topic>-traced`), tracing every record. Each record gets a `process <topic>` span that
/// continues the trace in its `traceparent` header, and the record written to the output topic,
/// or to the dead-letter topic `DLQ_TOPIC` (default `<topic>-dlq`) when it fails, carries that
/// span's context, so a consumer downstream continues the same trace.
///
/// OpenTelemetry is configured from the standard `OTEL_*` environment variables through the SDK
/// autoconfigure module, which uses W3C trace-context propagation by default; for example
/// `OTEL_TRACES_EXPORTER=otlp` and `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317`.
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private App() {}

  static void main() {
    final var config = AppConfig.fromEnv();
    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());
    final var outputTopic = AppConfig.getEnvOrDefault("OUTPUT_TOPIC", config.topic() + "-traced");
    final var dlqTopic = AppConfig.getEnvOrDefault("DLQ_TOPIC", config.topic() + "-dlq");
    final var openTelemetry = AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();

    try (
      final var handle = KPipe.json(config.topic(), props)
        .withTracer(new OtelTracer(openTelemetry))
        .withDeadLetterTopic(dlqTopic)
        .peek(msg -> LOGGER.log(Level.INFO, "received {0}", msg))
        .pipe(msg -> {
          msg.put("traced", true);
          return msg;
        })
        .toKafka(outputTopic)
        .start()
    ) {
      LOGGER.log(Level.INFO, "Tracing {0} into {1}", config.topic(), outputTopic);
      handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in tracing consumer", e);
      System.exit(1);
    } finally {
      openTelemetry.close();
    }
  }
}
