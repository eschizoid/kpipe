package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.config.AppConfig;
import io.github.eschizoid.kpipe.consumer.config.KafkaConsumerConfig;
import io.github.eschizoid.kpipe.format.avro.AvroFormat;
import io.github.eschizoid.kpipe.schemaregistry.confluent.ConfluentSchemaResolver;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

/// Minimal Avro consumer demonstrating the KPipe facade against a Confluent Schema Registry.
/// Reads `AppConfig` from environment, fetches the `com.kpipe.customer` schema from the registry
/// URL, and starts a `KPipe.avro(...)` stream that strips the 5-byte Confluent envelope before
/// decoding.
///
/// ## Static-fetch footgun (this example is the deliberately UNSAFE path)
///
/// The `AvroFormat.of(resolver.lookupBySubjectVersion(...))` call below fetches ONE schema at
/// startup and then decodes every record against it, forever — the schema id in each record's
/// 5-byte wire envelope is thrown away by `skipBytes(5)`. This is fine only while the producer
/// never rolls the schema. The moment a producer writes with an evolved v2 (a field removed,
/// promoted, or a union reordered under FORWARD compatibility), those v2 bytes are silently
/// mis-decoded against the frozen v1 reader — extra fields bleed into the next field, or offsets
/// shift — and the corruption is invisible because deserialization still "succeeds".
///
/// For per-record correctness use registry mode instead:
/// `KPipe.avro(topic, props, resolver)`. It reads the schema id from each record's envelope,
/// looks the writer schema up (cached by id, immutable in SR), and projects to the reader schema
/// via Avro's schema-resolution rules — so schema evolution decodes correctly. Registry mode reads
/// the envelope itself, so drop the `skipBytes(5)` when you switch. This example keeps the static
/// path on purpose to demonstrate the fetch-at-startup shape and its hazard.
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());
  private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://schema-registry:8081";

  private App() {}

  static void main() {
    final var config = AppConfig.fromEnv();
    final var schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY_URL);

    final AvroFormat format;
    try (final var resolver = new ConfluentSchemaResolver(schemaRegistryUrl)) {
      format = AvroFormat.of(resolver.lookupBySubjectVersion("com.kpipe.customer", "latest"));
    }

    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    try (final var handle = KPipe.avro(config.topic(), props, format).skipBytes(5).toConsole().start()) {
      LOGGER.log(Level.INFO, "Avro consumer started for topic {0}", config.topic());
      handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in Avro consumer", e);
      System.exit(1);
    }
  }
}
