package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.config.AppConfig;
import io.github.eschizoid.kpipe.consumer.config.KafkaConsumerConfig;
import io.github.eschizoid.kpipe.schemaregistry.confluent.CachedSchemaResolver;
import io.github.eschizoid.kpipe.schemaregistry.confluent.ConfluentSchemaResolver;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;

/// Protobuf consumer that resolves the writer's schema from Confluent Schema Registry per record.
/// The format reads the 6-byte wire envelope itself (1 magic byte + 4-byte schema id + a
/// message-index varint array), looks up the `.proto` schema through the cached resolver, compiles
/// it to a descriptor, and decodes the remaining bytes against it — the mirror of the Avro-SR
/// example. It is proven wire-compatible with Confluent's own `KafkaProtobufSerializer`.
///
/// **Runtime module.** protobuf-java has no `.proto`-text parser, so the compiler ships separately
/// in `kpipe-format-protobuf-confluent` (a shaded module, mirroring the `kpipe-metrics` →
/// `kpipe-metrics-otel` split). It is discovered via `ServiceLoader`, so it only needs to be on the
/// runtime path — this example declares it `runtimeOnly`.
///
/// Environment variables (in addition to the standard `AppConfig`):
///
/// - `SCHEMA_REGISTRY_URL` — base URL of the Schema Registry (e.g.
///   `<http://schema-registry:8081>`).
///
/// Do NOT add `.skipBytes(6)` here — `KPipe.protobuf(topic, props, resolver)` consumes the envelope
/// itself.
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private App() {}

  static void main() {
    final var config = AppConfig.fromEnv();
    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    final var srUrl = requireEnv("SCHEMA_REGISTRY_URL");
    LOGGER.log(Level.INFO, "Wiring per-record Protobuf SR auto-lookup against {0}", srUrl);

    try (
      final var resolver = new CachedSchemaResolver(new ConfluentSchemaResolver(srUrl, Duration.ofSeconds(10)));
      final var handle = KPipe.protobuf(config.topic(), props, resolver)
        .peek(rec -> LOGGER.log(Level.INFO, "received {0}", rec))
        .toCustom(rec -> LOGGER.log(Level.INFO, "decoded {0}", rec))
        .start()
    ) {
      LOGGER.log(Level.INFO, "Schema Registry Protobuf consumer started for topic {0}", config.topic());
      handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in Schema Registry Protobuf consumer", e);
      System.exit(1);
    }
  }

  private static String requireEnv(final String key) {
    final var v = System.getenv(key);
    if (v == null || v.isBlank()) throw new IllegalStateException("Missing required env var: " + key);
    return v;
  }
}
