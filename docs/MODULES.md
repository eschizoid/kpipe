# Modules, dependencies, and JPMS

How the published artifacts fit together, what to depend on, and how the Java module system view differs from the
Gradle/Maven view.

## The normal path

Most applications need exactly three lines: the BOM, the fluent API, and one format module.

```kotlin
implementation(platform("io.github.eschizoid:kpipe-bom:1.18.0"))
implementation("io.github.eschizoid:kpipe-api")
implementation("io.github.eschizoid:kpipe-format-json")   // or -avro / -protobuf
```

Use `platform(...)`, not `enforcedPlatform(...)`. `platform` contributes the BOM's versions to normal dependency
conflict resolution; `enforcedPlatform` forces them over anything else on your classpath, which can silently downgrade
a dependency you upgraded deliberately. Reach for `enforcedPlatform` only if you have a specific conflict you have
diagnosed and want to override.

Maven equivalent:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.github.eschizoid</groupId>
      <artifactId>kpipe-bom</artifactId>
      <version>1.18.0</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>io.github.eschizoid</groupId>
    <artifactId>kpipe-api</artifactId>
  </dependency>
  <dependency>
    <groupId>io.github.eschizoid</groupId>
    <artifactId>kpipe-format-json</artifactId>
  </dependency>
</dependencies>
```

`kpipe-api` pulls `kpipe-consumer`, `kpipe-producer`, `kpipe-core`, and `kpipe-metrics` transitively, so those never
appear in your build file. Format modules are deliberately **not** transitive — you add only the one(s) you use.

Skip `kpipe-api` only if you want the explicit registry/builder API without the fluent facade; in that case depend on
`kpipe-consumer` directly.

## Module catalog

| Module                            | What it gives you                                                                                                        |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `kpipe-api`                       | Fluent entry point: `KPipe`, `Stream<T>`, `Sink<T>`, `Handle`                                                            |
| `kpipe-bom`                       | Maven BOM — pins all `kpipe-*` artifacts to matching versions                                                            |
| `kpipe-core`                      | Registries, `MessageFormat`, `MessageSink`, operators, `BatchSink`                                                       |
| `kpipe-consumer`                  | `KPipeConsumer`, `KPipeConsumerBuilder`, backpressure, circuit breaker, offset management, health server                 |
| `kpipe-producer`                  | Kafka producer wrapper, `KafkaMessageSink`, DLQ producer, `Tracer` SPI                                                   |
| `kpipe-metrics`                   | Metrics interfaces (`ConsumerMetrics`, `ProducerMetrics`) + log-based reporters                                          |
| `kpipe-metrics-otel`              | OpenTelemetry-backed metrics implementation (opt-in)                                                                     |
| `kpipe-tracing-otel`              | W3C trace-context propagation through Kafka headers (opt-in)                                                             |
| `kpipe-schema-registry-confluent` | Confluent Schema Registry HTTP client + in-process cache (opt-in)                                                        |
| `kpipe-format-json`               | `JsonFormat` (payload type `Map<String, Object>`), `JsonConsoleSink`                                                     |
| `kpipe-format-avro`               | `AvroFormat` (payload type `GenericRecord`), `AvroConsoleSink`                                                           |
| `kpipe-format-protobuf`           | `ProtobufFormat` (payload type `Message`), `ProtobufConsoleSink`, `ProtobufDescriptorCompiler` SPI                       |
| `kpipe-format-protobuf-confluent` | Confluent SR `.proto`-text compiler (shaded; needed only for Protobuf Schema Registry mode)                              |
| `kpipe-test`                      | `TestStream<T>`, `CapturingSink<T>`, `CrashRestartHarness` — broker-free pipeline tests (`testImplementation` scope)     |

Dependency direction (from the `module-info` declarations):

```
kpipe-core, kpipe-metrics          — roots, no kpipe dependencies
kpipe-producer                     → core, metrics
kpipe-consumer                     → core, producer (metrics transitively)
kpipe-format-{json,avro,protobuf}  → core
kpipe-api                          → consumer + producer; format modules are compile-only (requires static)
kpipe-metrics-otel                 → metrics
kpipe-tracing-otel                 → producer
kpipe-schema-registry-confluent    → core
kpipe-format-protobuf-confluent    → format-protobuf
kpipe-test                         → consumer
```

## JPMS (`module-info.java`)

Two different notions of "transitive" are in play:

- **Gradle/Maven transitivity** decides what jars land on your classpath/modulepath. Depending on `kpipe-api` brings
  the consumer, producer, core, and metrics jars along.
- **JPMS readability** (`requires transitive`) decides which module's types your code can *name*. The
  `io.github.eschizoid.kpipe` module (the `kpipe-api` jar) declares `requires transitive` on consumer, producer, and
  core, so requiring it makes those APIs readable too. Format modules are declared `requires static` — compile-only —
  so your application must require the format module it uses explicitly.

A modular application using the fluent API with JSON:

```java
module my.application {
  requires io.github.eschizoid.kpipe;              // the kpipe-api jar; consumer/producer/core readable transitively
  requires io.github.eschizoid.kpipe.format.json;  // formats are opt-in — require the one you use
  requires io.github.eschizoid.kpipe.metrics.otel; // optional: OTel-backed metrics
}
```

If you use the explicit API without the facade, `requires io.github.eschizoid.kpipe.consumer;` instead of the first
line.

One caveat: `kpipe-format-protobuf-confluent` shades its `.proto`-text compiler and ships as an automatic module — an
unavoidable tradeoff of relocating a dependency that split packages. Everything else is a real JPMS module.
