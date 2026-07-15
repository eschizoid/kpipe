package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/// Pins the `MultiBuilder` Schema-Registry route overloads (`avro(topic, resolver, cfg)` /
/// `protobuf(topic, resolver, cfg)`) — the multi-topic mirror of `KPipe.avro/protobuf(...,
/// resolver)`. Each proves the overload wired a registry-mode format, without needing a broker.
class MultiBuilderRegistryTest {

  private static final SchemaResolver RESOLVER = schemaId -> "x";

  private static Properties props() {
    final var props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "test-group");
    return props;
  }

  @Test
  void avroRegistryRouteWiresTheRegistryModeConsoleUnsupportedFactory() {
    // A registry-mode Avro format has no fixed schema, so the route's default console-sink factory
    // is the "unsupported" one. Calling .toConsole() in the configurator triggers it — reachable
    // only if the resolver overload passed the registry-mode console factory to route().
    final var multi = KPipe.multi(props());
    final var ex = assertThrows(IllegalStateException.class, () ->
      multi.avro("avro-topic", RESOLVER, Stream::toConsole)
    );
    assertTrue(
      ex.getMessage().contains("Schema-Registry mode has none"),
      "expected the registry-mode console-unsupported error, got: " + ex.getMessage()
    );
  }

  @Test
  void protobufRegistryRouteRequiresTheConfluentCompiler() {
    // kpipe-api's test path has no kpipe-format-protobuf-confluent, so ProtobufFormat.withRegistry
    // fails ServiceLoader lookup — reachable only if the resolver overload called withRegistry.
    final var multi = KPipe.multi(props());
    final var ex = assertThrows(IllegalStateException.class, () ->
      multi.protobuf("proto-topic", RESOLVER, s -> s.toCustom(value -> {}))
    );
    assertTrue(
      ex.getMessage().contains("ProtobufDescriptorCompiler"),
      "expected the ServiceLoader compiler-missing error, got: " + ex.getMessage()
    );
  }
}
