package org.kpipe;

import io.opentelemetry.api.GlobalOpenTelemetry;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import org.kpipe.consumer.config.AppConfig;
import org.kpipe.consumer.config.KafkaConsumerConfig;
import org.kpipe.tracing.otel.OtelTracer;

/// JSON consumer that demonstrates `.withTracer(...)` for W3C trace context propagation. The
/// consumer extracts the upstream context from each record's `traceparent` / `tracestate` Kafka
/// headers, starts a CONSUMER span around the pipeline, and (when the sink produces back to
/// Kafka via `KafkaMessageSink` or writes to the DLQ) injects the active context into outbound
/// headers so downstream consumers see the same trace.
///
/// The OTel SDK comes from `GlobalOpenTelemetry.get()` — typically set up by the
/// `opentelemetry-sdk-extension-autoconfigure` agent reading `OTEL_*` env vars. With no agent
/// configured, the global is a no-op and tracing is effectively disabled.
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private App() {}

  static void main() {
    final var config = AppConfig.fromEnv();
    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    final var tracer = new OtelTracer(GlobalOpenTelemetry.get(), "tracing-example");

    try (
      final var handle = KPipe
        .json(config.topic(), props)
        .withTracer(tracer)
        .peek(msg -> LOGGER.log(Level.INFO, "received {0}", msg))
        .toConsole()
        .start()
    ) {
      LOGGER.log(Level.INFO, "Tracing consumer started for topic {0}", config.topic());
      handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in tracing consumer", e);
      System.exit(1);
    }
  }
}
