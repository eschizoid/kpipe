package org.kpipe;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import org.kpipe.consumer.config.AppConfig;
import org.kpipe.consumer.config.KafkaConsumerConfig;
import org.kpipe.format.avro.AvroFormat;
import org.kpipe.schemaregistry.confluent.ConfluentSchemaResolver;

/// Avro consumer that fetches its schema from Confluent Schema Registry at startup, registers it
/// with `AvroFormat.INSTANCE`, and then runs a normal `KPipe.avro(...)` pipeline.
///
/// Environment variables (in addition to the standard `AppConfig`):
///
/// - `SCHEMA_REGISTRY_URL` — base URL of the Schema Registry (e.g. `http://schema-registry:8081`).
/// - `SCHEMA_SUBJECT` — the SR subject name (commonly `<topic>-value`).
/// - `SCHEMA_VERSION` — version identifier (`"latest"` or a numeric version). Defaults to `latest`.
/// - `SCHEMA_KEY` — the registry key the format will register the schema under. Defaults to
///   the subject name.
///
/// Confluent producers wrap each record in a 5-byte wire envelope (1 magic byte + 4-byte schema
/// id). The pipeline uses `.skipBytes(5)` to drop that prefix before deserialization. To wire
/// runtime ID-based lookups in 1.13, replace `skipBytes(5)` + the startup pre-registration with
/// the `SchemaResolver` SPI once `AvroFormat` calls `resolver.lookupById(...)` automatically.
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private App() {}

  static void main() {
    final var config = AppConfig.fromEnv();
    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    final var srUrl = requireEnv("SCHEMA_REGISTRY_URL");
    final var subject = requireEnv("SCHEMA_SUBJECT");
    final var version = System.getenv().getOrDefault("SCHEMA_VERSION", "latest");
    final var schemaKey = System.getenv().getOrDefault("SCHEMA_KEY", subject);

    try (final var resolver = new ConfluentSchemaResolver(srUrl, Duration.ofSeconds(10))) {
      final var schemaJson = resolver.lookupBySubjectVersion(subject, version);
      AvroFormat.INSTANCE.addSchema(schemaKey, schemaJson);
      AvroFormat.INSTANCE.withDefaultSchema(schemaKey);
      LOGGER.log(Level.INFO, "Registered schema {0} (subject={1}, version={2})", schemaKey, subject, version);
    }

    try (
      final var handle = KPipe
        .avro(config.topic(), props)
        .skipBytes(5)
        .peek(rec -> LOGGER.log(Level.INFO, "received {0}", rec))
        .toConsole()
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
