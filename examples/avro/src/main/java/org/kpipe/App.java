package org.kpipe;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import org.kpipe.consumer.config.AppConfig;
import org.kpipe.consumer.config.KafkaConsumerConfig;
import org.kpipe.format.avro.AvroFormat;
import org.kpipe.schemaregistry.confluent.ConfluentSchemaResolver;

/// Minimal Avro consumer demonstrating the KPipe facade against a Confluent Schema Registry.
/// Reads `AppConfig` from environment, fetches the `com.kpipe.customer` schema from the registry
/// URL, and starts a `KPipe.avro(...)` stream that strips the 5-byte Confluent envelope before
/// decoding.
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());
  private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://schema-registry:8081";

  private App() {}

  static void main() {
    final var config = AppConfig.fromEnv();
    final var schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY_URL);

    try (final var resolver = new ConfluentSchemaResolver(schemaRegistryUrl)) {
      AvroFormat.INSTANCE.addSchema(
        "1",
        "com.kpipe.customer",
        resolver.lookupBySubjectVersion("com.kpipe.customer", "latest")
      );
    }
    AvroFormat.INSTANCE.withDefaultSchema("1");

    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    try (final var handle = KPipe.avro(config.topic(), props).skipBytes(5).toConsole().start()) {
      LOGGER.log(Level.INFO, "Avro consumer started for topic {0}", config.topic());
      handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in Avro consumer", e);
      System.exit(1);
    }
  }
}
