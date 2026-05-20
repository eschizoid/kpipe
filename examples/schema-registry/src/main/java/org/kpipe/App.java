package org.kpipe;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import org.kpipe.consumer.config.AppConfig;
import org.kpipe.consumer.config.KafkaConsumerConfig;
import org.kpipe.schemaregistry.confluent.CachedSchemaResolver;
import org.kpipe.schemaregistry.confluent.ConfluentSchemaResolver;

/// Avro consumer that resolves the writer's schema from Confluent Schema Registry per record.
/// The format reads the 5-byte wire envelope itself (1 magic byte + 4-byte schema id), looks up
/// the schema through the cached resolver, and decodes the remaining bytes against it. This is
/// the schema-evolution-correct path — producer evolution can't silently corrupt the consumer
/// the way the older "fetch once at startup, treat as fixed" pattern could.
///
/// Environment variables (in addition to the standard `AppConfig`):
///
/// - `SCHEMA_REGISTRY_URL` — base URL of the Schema Registry (e.g. `http://schema-registry:8081`).
///
/// Do NOT add `.skipBytes(5)` here — `withSchemaRegistry(...)` consumes the envelope itself.
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private App() {}

  static void main() {
    final var config = AppConfig.fromEnv();
    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    final var srUrl = requireEnv("SCHEMA_REGISTRY_URL");
    LOGGER.log(Level.INFO, "Wiring per-record SR auto-lookup against {0}", srUrl);

    try (
      final var resolver = new CachedSchemaResolver(new ConfluentSchemaResolver(srUrl, Duration.ofSeconds(10)));
      final var handle = KPipe.avro(config.topic(), props, resolver)
        .peek(rec -> LOGGER.log(Level.INFO, "received {0}", rec))
        .toCustom(rec -> LOGGER.log(Level.INFO, "decoded {0}", rec))
        .start()
    ) {
      LOGGER.log(Level.INFO, "Schema Registry Avro consumer started for topic {0}", config.topic());
      handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in Schema Registry consumer", e);
      System.exit(1);
    }
  }

  private static String requireEnv(final String key) {
    final var v = System.getenv(key);
    if (v == null || v.isBlank()) throw new IllegalStateException("Missing required env var: " + key);
    return v;
  }
}
