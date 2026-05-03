# <img src="img/kpipe_1.png" width="200" height="200">

# KPipe

**KPipe is a lightweight Kafka processing library for modern Java that lets you build safe, high‑performance message
pipelines using virtual threads and a functional API.**

[![JVM 25+](https://img.shields.io/badge/JVM-25%2B-brightgreen.svg?&logo=openjdk)](https://openjdk.org/projects/jdk/25/)
[![Build](https://github.com/eschizoid/kpipe/actions/workflows/ci.yaml/badge.svg)](https://github.com/eschizoid/kpipe/actions/workflows/ci.yaml)
[![Codecov](https://codecov.io/gh/eschizoid/kpipe/graph/badge.svg?token=X50GBU4X7J)](https://codecov.io/gh/eschizoid/kpipe)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.eschizoid/kpipe-consumer.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.eschizoid/kpipe-consumer)
[![Javadoc](https://javadoc.io/badge2/io.github.eschizoid/kpipe-consumer/javadoc.svg?color=purple)](https://javadoc.io/doc/io.github.eschizoid/kpipe-consumer)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

- **Modern Java concurrency** (virtual threads)
- **Composable functional pipelines**
- **Safe at-least-once processing**
- **Backpressure** to protect downstream systems
- **High throughput with minimal framework overhead**

It is designed for Kafka consumer services performing transformations, enrichment, or routing.

---

## Getting Started

### 1. Add the dependencies

For the 5-line fluent path (recommended), pull `kpipe-api` plus the format module(s) you need:

```kotlin
// Gradle (Kotlin) — JSON via the fluent API
implementation("io.github.eschizoid:kpipe-api:1.11.0")
implementation("io.github.eschizoid:kpipe-format-json:1.11.0")
```

```xml
<!-- Maven — JSON via the fluent API -->
<dependency>
  <groupId>io.github.eschizoid</groupId>
  <artifactId>kpipe-api</artifactId>
  <version>1.11.0</version>
</dependency>
<dependency>
  <groupId>io.github.eschizoid</groupId>
  <artifactId>kpipe-format-json</artifactId>
  <version>1.11.0</version>
</dependency>
```

`kpipe-api` transitively pulls `kpipe-consumer` + `kpipe-producer` + `kpipe-core`. Skip `kpipe-api` only if you want the
explicit registry / builder API (see "Advanced API" further down) — for that case, depend on `kpipe-consumer` directly.

> **Tip:** A `kpipe-bom` is published so you only pin one version across modules. Use it via `dependencyManagement`
> (Maven) or `enforcedPlatform` (Gradle) and drop versions from individual `kpipe-*` dependencies.

<details>
<summary>Module catalog & other build tools</summary>

| Module                  | What it gives you                                                                |
| ----------------------- | -------------------------------------------------------------------------------- |
| `kpipe-api`             | High-level fluent entry point: `KPipe`, `Stream<T>`, `Sink<T>`, `Handle`         |
| `kpipe-bom`             | Maven BOM — pins all `kpipe-*` artifacts to matching versions                    |
| `kpipe-core`            | Low-level building blocks: registries, `MessageFormat`, `MessageSink`, operators |
| `kpipe-metrics`         | Metrics interfaces (`ConsumerMetrics`, `ProducerMetrics`) + log-based reporters  |
| `kpipe-metrics-otel`    | OpenTelemetry-backed implementation (opt-in)                                     |
| `kpipe-producer`        | Kafka producer wrapper, `KafkaMessageSink`                                       |
| `kpipe-consumer`        | `KPipeConsumer`, `KPipeRunner`, `BackpressureController`                         |
| `kpipe-format-json`     | `JsonFormat`, `JsonMessageProcessor`, `JsonConsoleSink`                          |
| `kpipe-format-avro`     | `AvroFormat`, `AvroMessageProcessor`, `AvroConsoleSink`                          |
| `kpipe-format-protobuf` | `ProtobufFormat`, `ProtobufMessageProcessor`, `ProtobufConsoleSink`              |

**Gradle (Kotlin) with BOM**

```kotlin
implementation(platform("io.github.eschizoid:kpipe-bom:1.11.0"))
implementation("io.github.eschizoid:kpipe-api")
implementation("io.github.eschizoid:kpipe-format-json")
// add kpipe-metrics-otel only if you want OpenTelemetry-backed metrics
implementation("io.github.eschizoid:kpipe-metrics-otel")
```

**Gradle (Groovy)**

```groovy
implementation platform('io.github.eschizoid:kpipe-bom:1.11.0')
implementation 'io.github.eschizoid:kpipe-api'
implementation 'io.github.eschizoid:kpipe-format-json'
```

**Maven (with BOM)**

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.github.eschizoid</groupId>
      <artifactId>kpipe-bom</artifactId>
      <version>1.11.0</version>
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

**SBT**

```sbt
libraryDependencies += "io.github.eschizoid" % "kpipe-api" % "1.11.0"
libraryDependencies += "io.github.eschizoid" % "kpipe-format-json" % "1.11.0"
```

</details>

### 2. Hello, KPipe — five lines

```java
import org.kpipe.KPipe;
import static org.kpipe.registry.Operators.removeFields;

KPipe.json("events", kafkaProps)
    .pipe(msg -> { msg.put("ts", System.currentTimeMillis()); return msg; })
    .pipe(removeFields("password"))
    .toConsole()
    .start();   // returns AutoCloseable Handle (call .close() to shut down)
```

That's it — a working JSON Kafka consumer that strips the `password` field, stamps a timestamp, and logs to console. The
chain auto-builds the underlying `MessageProcessorRegistry`, `KPipeConsumer`, and `KPipeRunner` for you.

### 3. The full fluent surface

The `Stream<T>` returned by `KPipe.json/avro/protobuf/bytes/custom(...)` exposes the entire usable API in one place (IDE
auto-complete after `.` shows you everything you need):

| Method                                                 | What it does                                            |
| ------------------------------------------------------ | ------------------------------------------------------- |
| `.pipe(UnaryOperator<T> op)`                           | append an operator to the pipeline                      |
| `.filter(Predicate<T> keep)`                           | drop messages where predicate returns false             |
| `.peek(Consumer<T> sideEffect)`                        | observe without modifying (logging, metrics)            |
| `.when(Predicate, ifTrue, ifFalse)`                    | branch the pipeline conditionally                       |
| `.withRetry(int max, Duration backoff)`                | configure retry behavior                                |
| `.withBackpressure()` / `.withBackpressure(high, low)` | enable backpressure with default or explicit watermarks |
| `.withSequentialProcessing(boolean)`                   | force one-at-a-time per partition                       |
| `.toConsole()`                                         | terminate with the format-appropriate console sink      |
| `.toCustom(MessageSink<T> sink)`                       | terminate with your own sink                            |
| `.toMulti(MessageSink<T>... sinks)`                    | fan-out to multiple sinks                               |

The terminal `Sink<T>.start()` returns a `Handle` exposing `isHealthy()`, `metrics()`, `awaitShutdown(Duration)`,
`shutdownGracefully(Duration)`, and `close()`.

### 4. Common operator patterns

`org.kpipe.registry.Operators` exposes pure-function helpers ready to drop into `.pipe(...)`:

```java
import static org.kpipe.registry.Operators.*;

KPipe.json("events", kafkaProps)
    .filter(msg -> "active".equals(msg.get("status")))            // drop inactive
    .peek(msg -> log.info("processing {}", msg.get("id")))        // log without modifying
    .pipe(rename("user_id", "userId"))                            // rename a field
    .pipe(removeFields("password", "ssn"))                        // strip sensitive fields
    .pipe(safe(msg -> riskyEnrich(msg)))                          // wrap in error-handling
    .toConsole()
    .start();
```

The full operator vocabulary: `filter`, `drop`, `tap`, `peek`, `map`, `compose`, `safe`, `requireField`, `rename`,
`removeFields`. All return `UnaryOperator<T>` (or `UnaryOperator<Map<String, Object>>` for the JSON-specific ones).

---

## When to Use KPipe

KPipe works well for:

- Kafka consumer microservices
- event enrichment pipelines
- lightweight transformation services
- I/O-bound processing (REST calls, database lookups)
- teams adopting **modern Java concurrency**

KPipe is not intended to replace large streaming frameworks. It focuses on **simple, composable Kafka consumer
pipelines.**

---

## Why Not Just Use Kafka Streams?

Kafka Streams is powerful but introduces a full topology framework and state management layer that many services do not
need.

KPipe focuses on **code-first pipelines with minimal infrastructure overhead.**

| Capability                       | Kafka Streams | Reactor Kafka | KPipe |
| -------------------------------- | ------------- | ------------- | ----- |
| Full stream processing framework | Yes           | No            | No    |
| Lightweight consumer pipelines   | Partial       | Yes           | Yes   |
| Virtual-thread friendly          | No            | No            | Yes   |
| Functional pipeline API          | Yes           | Yes           | Yes   |
| Minimal dependencies             | No            | Yes           | Yes   |

KPipe sits between **raw KafkaConsumer code and full streaming frameworks.**

---

## Architecture and Reliability

KPipe is designed to be a lightweight, high-performance alternative to existing Kafka consumer libraries, focusing on
modern Java 25+ features (Virtual Threads, Scoped Values) and predictable behavior.

### 1. Modular Architecture (JPMS)

KPipe ships eight focused JPMS modules with a clean dependency direction (no cycles, no sideways leaks):

```
kpipe-metrics ← kpipe-core ← kpipe-consumer
                          ← kpipe-producer
                          ← kpipe-format-{json, avro, protobuf}
kpipe-metrics-otel ← kpipe-metrics      (opt-in OTel implementation)
kpipe-bom                                (Maven BOM — pins versions)
```

- **kpipe-core**: format-agnostic pipeline machinery — `MessageFormat`, `MessagePipeline`, `MessageProcessorRegistry`,
  `MessageSinkRegistry`, `MessageSink`, `CompositeMessageSink`, `RegistryKey`, `RegistryFunctions`. No Kafka, no
  third-party runtime deps.
- **kpipe-metrics**: pure interfaces (`ProducerMetrics`, `ConsumerMetrics`) plus log-based reporters. No OTel API on the
  classpath.
- **kpipe-metrics-otel**: OpenTelemetry implementation (`OtelConsumerMetrics`, `OtelProducerMetrics`). Add this only if
  you want OTel-backed metrics.
- **kpipe-producer**: Kafka producer wrapper, `KafkaMessageSink` (in `org.kpipe.producer.sink`).
- **kpipe-consumer**: `KPipeConsumer`, `KPipeRunner`, `BackpressureController`, `KafkaOffsetManager`,
  `HttpHealthServer`, `consumer.metrics` reporters.
- **kpipe-format-json / -avro / -protobuf**: per-format `XFormat.INSTANCE`, processors, and console sinks. Pull only the
  format(s) you need.

Use KPipe in a modular project:

```java
module my.application {
  requires org.kpipe.consumer; // includes core + producer + metrics transitively
  requires org.kpipe.format.json; // add only the formats you use
  requires org.kpipe.metrics.otel; // optional — enables OTel-backed metrics
}
```

### 2. The "Single SerDe Cycle" Strategy

Unlike traditional pipelines that often perform `byte[] -> Object -> byte[]` at every transformation step, KPipe
optimizes for throughput:

- **Single Deserialization**: Messages are deserialized **once** into a mutable representation (e.g., `Map` for JSON,
  `GenericRecord` for Avro) via the `MessagePipeline`.
- **In-Place Transformations**: A chain of `UnaryOperator` functions is applied to the same object.
- **Single Serialization**: The final object is serialized back to `byte[]` only once.
- **Integrated Sinks**: Typed sinks can be attached directly to the pipeline, receiving the object before final
  serialization.

This approach significantly reduces CPU overhead and GC pressure.

### 3. Virtual Threads and Scoped Values

KPipe uses **Java Virtual Threads** (Project Loom) for high-concurrency message processing.

- **Efficient Resource Reuse**: Heavy objects like `Schema.Parser`, `ByteArrayOutputStream`, and Avro encoders are
  cached per virtual thread using `ScopedValue`, which is significantly more lightweight than `ThreadLocal`.
  - **Optimization**: `ScopedValue` allows KPipe to share these heavy resources across all transformations in a single
    pipeline without the memory leak risks or scalability bottlenecks of `ThreadLocal` in a virtual-thread-per-record
    model.
- **Thread-Per-Record**: Each message is processed in its own virtual thread, allowing KPipe to scale to millions of
  concurrent operations without the overhead of complex thread pools.

### 4. Reliable "At-Least-Once" Delivery

KPipe implements a **Lowest Pending Offset** strategy to ensure reliability even with parallel processing:

- **Pluggable Offset Management**: Use the `OffsetManager` interface to customize how offsets are stored (Kafka-based or
  external database).
- **In-Flight Tracking**: Every record's offset is tracked in a `ConcurrentSkipListSet` per partition (in
  `KafkaOffsetManager`).
- **No-Gap Commits**: Even if message 102 finishes before 101, offset 102 will **not** be committed until 101 is
  successfully processed.
- **Crash Recovery**: If the consumer crashes, it will resume from the last committed "safe" offset. While this may
  result in some records being re-processed (standard "at-least-once" behavior), it guarantees no message is ever
  skipped.

### 5. Parallel vs. Sequential Processing

KPipe supports two modes of execution depending on your ordering and throughput requirements:

- **Parallel Mode (Default)**: Best for stateless transformations (enrichment, masking). High throughput via virtual
  threads. Offsets are committed based on the **lowest pending offset** to ensure no gaps.
- **Sequential Mode (`.withSequentialProcessing(true)`)**: Best for stateful transformations where order per partition
  is critical (e.g., balance updates, sequence-dependent events). In this mode, only one message per partition is
  processed at a time. Backpressure is supported and operates by monitoring the **consumer lag** (the difference between
  the partition end-offset and the consumer's current position).

### 6. External Offset Management

While Kafka-based offset storage is the default, KPipe supports external storage (e.g., PostgreSQL) for **exactly-once
processing** or specific architectural needs.

1.  **Seek on Assignment**: When partitions are assigned, fetch the last processed offset from your database and call
    `consumer.seek(partition, offset + 1)`.
2.  **Update on Processed**: Implement `markOffsetProcessed` to save the offset to the database.

```java
public class PostgresOffsetManager<K, V> implements OffsetManager<K, V> {

  private final Consumer<K, V> consumer;

  // ... DB connection ...

  @Override
  public void markOffsetProcessed(final ConsumerRecord<K, V> record) {
    // SQL: UPDATE offsets SET offset = ? WHERE partition = ?
  }

  @Override
  public ConsumerRebalanceListener createRebalanceListener() {
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        for (final var tp : partitions) {
          long lastOffset = fetchFromDb(tp);
          consumer.seek(tp, lastOffset + 1);
        }
      }
      // ...
    };
  }
  // ...
}
```

### 7. Error Handling & Retries

KPipe provides a robust, multi-layered error handling mechanism:

- **Built-in Retries**: Configure `.withRetry(maxRetries, backoff)` to automatically retry transient failures.
- **Dead Letter Queue (DLQ)**: Enable high-reliability processing with built-in DLQ support:
  ```java
  final var consumer = KPipeConsumer.<byte[]>builder()
    .withDeadLetterTopic("events-dlq") // Automatically sends to DLQ after retries are exhausted
    .build();
  ```
- **Dead Letter Handling**: Provide a custom `.withErrorHandler()` to redirect messages to an external database.
- **Safe Pipelines**: Use `MessageProcessorRegistry.withErrorHandling()` to wrap individual processors, or
  `MessageSinkRegistry.withErrorHandling()` to wrap sinks.

### 8. Backpressure

When a downstream sink (database, HTTP API, another Kafka topic) is slow, KPipe can automatically pause Kafka polling to
prevent unbounded resource consumption or excessive lag.

Backpressure uses **two configurable watermarks** (hysteresis) to avoid rapid pause/resume oscillation:

- **High watermark** — pause Kafka polling when the monitored metric reaches this value.
- **Low watermark** — resume Kafka polling when the metric drops to or below this value.

#### Backpressure Strategies

KPipe automatically selects the optimal backpressure strategy based on your processing mode:

| Mode                   | Strategy         | Metric Monitored               | Use Case                                                       |
| :--------------------- | :--------------- | :----------------------------- | :------------------------------------------------------------- |
| **Parallel** (Default) | **In-Flight**    | Total active virtual threads   | Prevent memory exhaustion from too many concurrent tasks.      |
| **Sequential**         | **Consumer Lag** | Total unread messages in Kafka | Prevent the consumer from falling too far behind the producer. |

##### 1. In-Flight Strategy (Parallel Mode)

In parallel mode, multiple messages are processed concurrently using Virtual Threads. The backpressure controller
monitors the number of messages currently "in-flight" (started but not yet finished).

- **High Watermark Default**: 10,000
- **Low Watermark Default**: 7,000

##### 2. Consumer Lag Strategy (Sequential Mode)

In sequential mode, messages are processed one by one to maintain strict ordering. Since only one message is ever
in-flight, KPipe instead monitors the **total consumer lag** across all assigned partitions.

The lag is calculated using the formula:

```
lag = Σ (endOffset - position)
```

Where:

- `endOffset`: The highest available offset in a partition.
- `position`: The offset of the next record to be fetched by this consumer.

- **High Watermark Default**: 10,000
- **Low Watermark Default**: 7,000

#### Configuration

```java
final var consumer = KPipeConsumer.<byte[]>builder()
  .withProperties(kafkaProps)
  .withTopic("events")
  .withPipeline(pipeline)
  // Enable backpressure with default watermarks (10k / 7k)
  .withBackpressure()
  // Or configure explicit watermarks:
  // .withBackpressure(5_000, 3_000)
  .build();
```

**Backpressure is enabled by default** with watermarks 10,000 (high) / 7,000 (low). Call `.withBackpressure(high, low)`
to override the watermarks for your workload.

**Observability:** backpressure events are logged (WARNING on pause, INFO on resume) and tracked via two dedicated
metrics: `backpressurePauseCount` and `backpressureTimeMs`.

### 9. Graceful Shutdown & Interrupt Handling

KPipe respects JVM signals and ensures timely shutdown without data loss:

- **Interrupt Awareness**: Interrupts trigger a coordinated shutdown sequence. They do **not** cause records to be
  skipped.
- **Reliable Redelivery**: If a record's processing is interrupted (e.g., during retry backoff or transformation), the
  offset is NOT marked as processed. This ensures it will be safely picked up by the next consumer instance,
  guaranteeing "at-least-once" delivery even during shutdown.

```java
// Initiate graceful shutdown with 5-second timeout
boolean allProcessed = runner.shutdownGracefully(5000);

// Or register as JVM shutdown hook (KPipeRunner handles this with .withShutdownHook(true))
Runtime.getRuntime().addShutdownHook(new Thread(() -> runner.close()));
```

---

## Working with Messages

### JSON Processing

Add `kpipe-format-json`. Operators are `UnaryOperator<Map<String, Object>>`:

```java
import org.kpipe.format.json.JsonFormat;
import org.kpipe.format.json.JsonMessageProcessor;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.RegistryKey;

final var registry = new MessageProcessorRegistry("myApp");

final var stampKey = RegistryKey.json("addTimestamp");
registry.register(stampKey, JsonMessageProcessor.addTimestampOperator("processedAt"));

final var sanitizeKey = RegistryKey.json("sanitize");
registry.register(sanitizeKey, JsonMessageProcessor.removeFieldsOperator("password", "ssn"));

// Metadata merging
final var metadata = Map.of("version", "1.0", "env", "prod");
final var metaKey = RegistryKey.json("addMetadata");
registry.register(metaKey, JsonMessageProcessor.mergeWithOperator(metadata));

// Single deserialization → many transformations → single serialization
final var pipeline = registry.pipeline(JsonFormat.INSTANCE).add(sanitizeKey).add(stampKey).add(metaKey).build();
```

### Avro Processing

Add `kpipe-format-avro`. Operators are `UnaryOperator<GenericRecord>`:

```java
import org.kpipe.format.avro.AvroFormat;
import org.kpipe.format.avro.AvroMessageProcessor;
import org.kpipe.format.avro.AvroRegistryKey;
import org.kpipe.registry.MessageProcessorRegistry;

final var registry = new MessageProcessorRegistry("myApp");

// Register the schema directly on the format
AvroFormat.INSTANCE.addSchema("user", "com.kpipe.User", "schemas/user.avsc");
AvroFormat.INSTANCE.withDefaultSchema("user");
final var schema = AvroMessageProcessor.getSchema("user");

// Register operators with type-safe keys via AvroRegistryKey.of(...)
final var sanitizeKey = AvroRegistryKey.of("sanitize");
registry.register(sanitizeKey, AvroMessageProcessor.removeFieldsOperator(schema, "password", "creditCard"));

final var upperKey = AvroRegistryKey.of("uppercaseName");
registry.register(
  upperKey,
  AvroMessageProcessor.transformFieldOperator(schema, "name", value -> {
    if (value instanceof String text) return text.toUpperCase();
    return value;
  })
);

final var pipeline = registry.pipeline(AvroFormat.INSTANCE).add(sanitizeKey).add(upperKey).build();

// For Confluent Wire Format (1 magic byte + 4-byte schema ID), skip the prefix:
final var confluentPipeline = registry.pipeline(AvroFormat.INSTANCE).skipBytes(5).add(sanitizeKey).build();
```

### POJO Processing

For high-performance processing of Java records or POJOs, use `JsonFormat.pojo(...)` from `kpipe-format-json`. This
leverages DSL-JSON annotation processing for near-native performance.

```java
import org.kpipe.format.json.JsonFormat;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.RegistryKey;

final var registry = new MessageProcessorRegistry("myApp");

final var userKey = RegistryKey.of("userTransform", UserRecord.class);
registry.register(userKey, user -> new UserRecord(user.id(), user.name().toUpperCase(), user.email()));

final var pipeline = registry.pipeline(JsonFormat.pojo(UserRecord.class)).add(userKey).build();
```

### Protobuf Processing

Add `kpipe-format-protobuf`. Operators are `UnaryOperator<Message>`:

```java
import org.kpipe.format.protobuf.ProtobufFormat;
import org.kpipe.format.protobuf.ProtobufMessageProcessor;
import org.kpipe.format.protobuf.ProtobufRegistryKey;
import org.kpipe.registry.MessageProcessorRegistry;

final var registry = new MessageProcessorRegistry("myApp", ProtobufFormat.INSTANCE);

// Register a descriptor from your generated Protobuf class
ProtobufFormat.INSTANCE.addDescriptor("customer", CustomerProto.Customer.getDescriptor());
ProtobufFormat.INSTANCE.withDefaultDescriptor("customer");

final var sanitizeKey = ProtobufRegistryKey.of("sanitize");
registry.register(sanitizeKey, ProtobufMessageProcessor.removeFieldsOperator("email", "address"));

final var upperKey = ProtobufRegistryKey.of("uppercaseName");
registry.register(
  upperKey,
  ProtobufMessageProcessor.transformFieldOperator("name", value -> {
    if (value instanceof String text) return text.toUpperCase();
    return value;
  })
);

// Register the protobuf console sink yourself (defaults are no longer auto-registered)
final var protoLoggingKey = ProtobufRegistryKey.of("protobufLogging");
registry.sinkRegistry().register(protoLoggingKey, new org.kpipe.format.protobuf.ProtobufConsoleSink<>());

final var pipeline = registry
  .pipeline(ProtobufFormat.INSTANCE)
  .add(sanitizeKey)
  .add(upperKey)
  .toSink(protoLoggingKey)
  .build();
```

---

## Message Sinks

Message sinks provide destinations for processed messages. The `MessageSink` interface is a functional interface that
extends `Consumer<T>`:

```java
@FunctionalInterface
public interface MessageSink<T> extends Consumer<T> {}
```

### Built-in Sinks

Console sinks live in their format modules. The registry no longer auto-registers them — register the ones you want:

```java
import org.kpipe.format.json.JsonConsoleSink;
import org.kpipe.format.avro.AvroConsoleSink;
import org.kpipe.format.protobuf.ProtobufConsoleSink;

// Direct instantiation
final var jsonConsoleSink = new JsonConsoleSink<Map<String, Object>>();
final var avroConsoleSink = new AvroConsoleSink<GenericRecord>(schema);
final var protobufConsoleSink = new ProtobufConsoleSink<Message>();

// Register and use via the pipeline builder
registry.sinkRegistry().register(MessageSinkRegistry.JSON_LOGGING, jsonConsoleSink);

final var pipeline = registry
  .pipeline(JsonFormat.INSTANCE)
  .add(RegistryKey.json("sanitize"))
  .toSink(MessageSinkRegistry.JSON_LOGGING)
  .build();
```

### Custom Sinks

```java
final MessageSink<Map<String, Object>> databaseSink = (processedMap) -> {
  databaseService.insert(processedMap);
};
```

### Message Sink Registry

`MessageProcessorRegistry` exposes a `sinkRegistry()` accessor — sink and processor registries are separate components
with parallel APIs (`register` / `unregister` / `getMetrics`):

```java
// Create a registry
final var registry = new MessageProcessorRegistry("my-app");

// Register sinks with explicit types via the sink registry
final var dbKey = RegistryKey.of("database", Map.class);
registry.sinkRegistry().register(dbKey, databaseSink);

// Use the sink by key in the pipeline
final var pipeline = registry.pipeline(JsonFormat.INSTANCE).add(RegistryKey.json("enrich")).toSink(dbKey).build();
```

### Error Handling in Sinks

```java
// Wrap a sink or operator with error handling (suppresses exceptions, logs errors)
final var safeSink = MessageSinkRegistry.withErrorHandling(riskySink);
final var safeOperator = MessageProcessorRegistry.withErrorHandling(riskyOperator);

registry.sinkRegistry().register(RegistryKey.json("safeDatabase"), safeSink);
```

### Kafka Producer Sink

Produce processed messages back to a Kafka topic. `KafkaMessageSink` lives in `org.kpipe.producer.sink`:

```java
import org.kpipe.producer.KPipeProducer;
import org.kpipe.producer.sink.KafkaMessageSink;

final var producer = KPipeProducer.<byte[], byte[]>builder().withProperties(kafkaProps).build();

final var pipeline = registry
  .pipeline(JsonFormat.INSTANCE)
  .add(RegistryKey.json("transform"))
  .toSink(KafkaMessageSink.of(producer.getProducer(), "output-topic", JsonFormat.INSTANCE::serialize))
  .build();
```

### Composite Sink (Broadcasting)

`CompositeMessageSink` (in `org.kpipe.sink`, part of `kpipe-core`) broadcasts to multiple sinks. Failures in one sink do
not prevent other sinks from receiving the data:

```java
import org.kpipe.sink.CompositeMessageSink;

final var compositeSink = new CompositeMessageSink<>(List.of(postgresSink, consoleSink));

final var pipeline = registry.pipeline(JsonFormat.INSTANCE).toSink(compositeSink).build();
```

---

## Built-in Metrics

### Programmatic Access

```java
final var metrics = consumer.getMetrics();
log.log(Level.INFO, "Messages received: " + metrics.get("messagesReceived"));
log.log(Level.INFO, "Successfully processed: " + metrics.get("messagesProcessed"));
log.log(Level.INFO, "Processing errors: " + metrics.get("processingErrors"));
log.log(Level.INFO, "Messages in-flight: " + metrics.get("inFlight"));
// Backpressure metrics (present only when withBackpressure() is configured)
log.log(Level.INFO, "Backpressure pauses: " + metrics.get("backpressurePauseCount"));
log.log(Level.INFO, "Time spent paused (ms): " + metrics.get("backpressureTimeMs"));
```

### OpenTelemetry Integration

OTel support is opt-in via the `kpipe-metrics-otel` module. `kpipe-metrics` itself only ships interfaces — it does not
pull `opentelemetry-api` onto your classpath. Add `kpipe-metrics-otel` (and your OTel SDK at runtime) to enable real
telemetry:

```java
import org.kpipe.metrics.otel.OtelConsumerMetrics;

final var consumer = KPipeConsumer.<byte[]>builder()
  .withProperties(kafkaProps)
  .withTopic("events")
  .withPipeline(pipeline)
  .withMetrics(new OtelConsumerMetrics(openTelemetry, "my-pipeline"))
  .build();
```

When `withMetrics(...)` is omitted, `ConsumerMetrics.noop()` / `ProducerMetrics.noop()` is used automatically — zero
allocation overhead, no OTel API on the classpath.

| Instrument                           | Type      | Description                            |
| ------------------------------------ | --------- | -------------------------------------- |
| `kpipe.consumer.messages.received`   | Counter   | Records polled from Kafka              |
| `kpipe.consumer.messages.processed`  | Counter   | Records successfully processed         |
| `kpipe.consumer.messages.errors`     | Counter   | Records that failed processing         |
| `kpipe.consumer.processing.duration` | Histogram | Per-record processing time (ms)        |
| `kpipe.consumer.messages.inflight`   | Gauge     | Current number of in-flight messages   |
| `kpipe.consumer.backpressure.pauses` | Counter   | Times backpressure paused the consumer |
| `kpipe.consumer.backpressure.time`   | Counter   | Total time paused due to backpressure  |
| `kpipe.producer.messages.sent`       | Counter   | Records successfully produced          |
| `kpipe.producer.messages.failed`     | Counter   | Records that failed to produce         |
| `kpipe.producer.dlq.sent`            | Counter   | Records sent to DLQ                    |

---

## KPipe Runner

The `KPipeRunner` provides lifecycle management, metrics reporting, and graceful shutdown for Kafka consumers:

```java
final var runner = KPipeRunner.builder(consumer)
  .withMetricsReporters(List.of(
    ConsumerMetricsReporter.forConsumer(consumer::getMetrics),
    ProcessorMetricsReporter.forRegistry(processorRegistry)
  ))
  .withMetricsInterval(30_000)
  .withHealthCheck(KPipeConsumer::isRunning)
  .withShutdownTimeout(10_000)
  .withShutdownHook(true)
  .build();

runner.start();
runner.awaitShutdown();
```

The runner implements `AutoCloseable` for use with try-with-resources:

```java
try (final var runner = KPipeRunner.builder(consumer).build()) {
  runner.start();
  runner.awaitShutdown();
}
```

---

## Full Application Example

For complete working applications, see the [`examples/`](examples/) directory.

<details>
<summary>Expand condensed application example</summary>

```java
public class KPipeApp implements AutoCloseable {

  private static final System.Logger LOGGER = System.getLogger(KPipeApp.class.getName());
  private final KPipeRunner<KPipeConsumer<byte[]>> runner;

  static void main() {
    final var config = AppConfig.fromEnv();
    try (final var app = new KPipeApp(config)) {
      app.start();
      app.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in application", e);
      System.exit(1);
    }
  }

  public KPipeApp(final AppConfig config) {
    final var processorRegistry = new MessageProcessorRegistry(config.appName());
    processorRegistry.sinkRegistry().register(MessageSinkRegistry.JSON_LOGGING, new JsonConsoleSink<>());

    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    final var functionalConsumer = KPipeConsumer.<byte[]>builder()
      .withProperties(KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup()))
      .withTopic(config.topic())
      .withDeadLetterTopic(config.topic() + ".dlq")
      .withPipeline(
        processorRegistry
          .pipeline(JsonFormat.INSTANCE)
          .add(RegistryKey.json("addSource"), RegistryKey.json("markProcessed"), RegistryKey.json("addTimestamp"))
          .toSink(MessageSinkRegistry.JSON_LOGGING)
          .build()
      )
      .withCommandQueue(commandQueue)
      .withOffsetManagerProvider((consumer) ->
        KafkaOffsetManager.builder(consumer)
          .withCommandQueue(commandQueue)
          .withCommitInterval(Duration.ofSeconds(30))
          .build()
      )
      .enableMetrics(true)
      .build();

    runner = KPipeRunner.builder(functionalConsumer)
      .withMetricsInterval(config.metricsInterval().toMillis())
      .withShutdownTimeout(config.shutdownTimeout().toMillis())
      .withShutdownHook(true)
      .build();
  }

  public void start() {
    runner.start();
  }

  public boolean awaitShutdown() {
    return runner.awaitShutdown();
  }

  public void close() {
    runner.close();
  }
}
```

**Environment variables:**

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_CONSUMER_GROUP=my-group
export KAFKA_TOPIC=json-events
export PROCESSOR_PIPELINE=addSource,markProcessed,addTimestamp
export METRICS_INTERVAL_SEC=30
export SHUTDOWN_TIMEOUT_SEC=5
```

</details>

---

## Requirements

- **Java 25+**
- Gradle (for building the project)
- [kcat](https://github.com/edenhill/kcat) (for testing)
- Docker (for local Kafka setup)

---

## Testing

KPipe includes a pre-configured `docker-compose.yaml` that starts a full local environment including Kafka, Zookeeper,
and Confluent Schema Registry.

```bash
# Format code and build all modules
./gradlew clean spotlessApply build

# Build the consumer app container and start all services
docker compose build --no-cache --build-arg APP=<json|avro|protobuf|demo>
docker compose down -v
docker compose up -d

# Publish test messages
for i in {1..10}; do echo "{\"id\":$i,\"message\":\"Test message $i\"}" | \
  kcat -P -b kafka:9092 -t json-topic; done
```

<details>
<summary>Working with the Schema Registry and Avro</summary>

```bash
# Register an Avro schema
curl -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": $(cat lib/kpipe-consumer/src/test/resources/avro/customer.avsc | jq tostring)}" \
  http://localhost:8081/subjects/com.kpipe.customer/versions

# Read registered schema
curl -s http://localhost:8081/subjects/com.kpipe.customer/versions/latest | jq -r '.schema' | jq --indent 2 '.'

# Produce an Avro message using kafka-avro-console-producer
cat <<'JSON' | docker run -i --rm --network kpipe_default \
-v "$PWD/lib/kpipe-consumer/src/test/resources/avro/customer.avsc:/tmp/customer.avsc:ro" \
confluentinc/cp-schema-registry:8.2.0 \
sh -ec 'kafka-avro-console-producer \
  --bootstrap-server kafka:9092 \
  --topic avro-topic \
  --property schema.registry.url=http://schema-registry:8081 \
  --property value.schema="$(cat /tmp/customer.avsc)"'
{"id":1,"name":"Mariano Gonzalez","email":{"string":"mariano@example.com"},"active":true,"registrationDate":1635724800000,"address":{"com.kpipe.customer.Address":{"street":"123 Main St","city":"Chicago","zipCode":"00000","country":"USA"}},"tags":["premium","verified"],"preferences":{"notifications":"email"}}
JSON
```

</details>

<details>
<summary>Working with the Schema Registry and Protobuf</summary>

```bash
# Register a Protobuf schema
curl -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schemaType\": \"PROTOBUF\", \"schema\": $(cat lib/kpipe-consumer/src/test/resources/protobuf/customer.proto | jq -Rs .)}" \
  http://localhost:8081/subjects/com.kpipe.customer-protobuf/versions

# Read registered schema
curl -s http://localhost:8081/subjects/com.kpipe.customer-protobuf/versions/latest | jq '.'

# Produce a Protobuf message using kafka-protobuf-console-producer
cat <<'JSON' | docker run -i --rm --network kpipe_default \
confluentinc/cp-schema-registry:8.2.0 \
sh -ec 'kafka-protobuf-console-producer \
  --bootstrap-server kafka:9092 \
  --topic protobuf-topic \
  --property schema.registry.url=http://schema-registry:8081 \
  --property value.schema.id=1'
{"id":"1","name":"Mariano Gonzalez","email":"mariano@example.com","active":true,"registrationDate":"1635724800000","tags":["premium","verified"],"preferences":{"notifications":"email"}}
JSON
```

</details>

---

## Advanced Patterns

### Enum-Based Registry (Static Type Safety)

Define operators as an `Enum` that implements `UnaryOperator<T>` for bulk registration and discoverability:

```java
public enum StandardProcessors implements UnaryOperator<Map<String, Object>> {
  TIMESTAMP(JsonMessageProcessor.addTimestampOperator("ts")),
  SOURCE(JsonMessageProcessor.addFieldOperator("src", "app"));

  private final UnaryOperator<Map<String, Object>> op;

  StandardProcessors(final UnaryOperator<Map<String, Object>> op) {
    this.op = op;
  }

  @Override
  public Map<String, Object> apply(final Map<String, Object> t) {
    return op.apply(t);
  }
}

// Bulk register all enum constants
registry.registerEnum(Map.class, StandardProcessors.class);

// Now they can be used by name in configuration
// PROCESSOR_PIPELINE=TIMESTAMP,SOURCE
```

### Conditional Processing

```java
final var pipeline = registry
  .pipeline(JsonFormat.INSTANCE)
  .when(
    (map) -> "VIP".equals(map.get("level")),
    (map) -> {
      map.put("priority", "high");
      return map;
    },
    (map) -> {
      map.put("priority", "low");
      return map;
    }
  )
  .build();
```

### Filtering Messages

Return `null` from an operator to skip a message. KPipe will stop processing the current record and will not send it to
any downstream operators or sinks:

```java
registry.register(RegistryKey.json("filter"), map -> {
  if ("internal".equals(map.get("type"))) return null; // Skip this message
  return map;
});
```

### Thread-Safety and Resource Management

- Message processors should be stateless and thread-safe.
- KPipe automatically handles resource reuse via `ScopedValue` (optimized for Virtual Threads). Avoid manual
  `ThreadLocal` usage.
- For processors with side effects (like database calls), ensure they are compatible with high-concurrency virtual
  threads.

---

## Performance

KPipe is designed for high-throughput, low-overhead Kafka processing using modern Java features and pipeline
optimizations. Performance depends on workload shape (I/O vs CPU bound), partitioning, and message size.

- **Zero-Copy Magic Byte Handling**: For Avro data (especially from Confluent Schema Registry), KPipe supports an
  `offset` parameter that allows skipping magic bytes and schema IDs without performing expensive `Arrays.copyOfRange`
  operations.
- **DslJson Integration**: Uses a high-performance JSON library to reduce parsing overhead and GC pressure.

Latest parallel benchmark snapshot (see [`benchmarks/README.md`](benchmarks/README.md)) shows a throughput edge for
KPipe over Confluent Parallel Consumer, with a higher allocation footprint. Treat these as scenario-specific results,
not universal guarantees.

---

## Key-Level Ordering for Critical Systems (e.g., Payments)

For systems like **payment processors** where the order of operations (Authorize → Capture) is vital:

- **Consistent Partitioning**: Ensure your producer uses a consistent key (e.g., `transaction_id` or `customer_id`).
  Kafka guarantees all messages with the same key land in the same partition.
- **Safety**: KPipe manages each partition as an independent, ordered sequence of offsets. As long as related events
  share a key, KPipe's commitment strategy ensures they are handled reliably without skipping steps.

---

## Metrics Dashboard

KPipe ships a local observability stack under `infra/observability/` that spins up with Docker Compose:

- **OpenTelemetry Collector** — receives OTLP metrics from KPipe and exports to Prometheus.
- **Prometheus** — scrapes the collector and stores time-series data.
- **Grafana** — pre-provisioned with a **KPipe Overview** dashboard covering all consumer and producer metrics.

The stack is automatically included when you run any example via `docker compose`. To point your own OTel Collector at a
running KPipe app, configure the `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://your-collector:4318
OTEL_METRICS_EXPORTER=otlp
```

Once running, open Grafana at [http://localhost:3000](http://localhost:3000) (admin/admin) to view the KPipe Overview
dashboard.

---

## Running the Demo

The `examples/demo` module runs JSON, Avro, and Protobuf pipelines concurrently in a single application, with full
observability wired in.

```bash
# Build and start everything (Kafka, Schema Registry, OTel, Prometheus, Grafana, demo app, seed data)
./scripts/run-demo.sh

# Or manually:
cd examples/demo
docker compose up --build
```

This will:

1. Start Kafka (KRaft) and Schema Registry
2. Create topics and register Avro + Protobuf schemas
3. Start the observability stack (OTel Collector → Prometheus → Grafana)
4. Build and start the demo application with all three pipelines
5. Seed JSON messages for immediate processing

Open [http://localhost:3000](http://localhost:3000) to view metrics in Grafana, or
[http://localhost:8080/health](http://localhost:8080/health) to check the app health endpoint.

---

## Inspiration

This library is inspired by the best practices from:

- [Project Loom](https://openjdk.org/projects/loom/)
- [High-performance JSON libraries (DslJson)](https://github.com/ngs-doo/dsl-json)

---

## Contributing

If you're using this library, feel free to:

- Register custom processors
- Add metrics/observability hooks
- Share improvements or retry strategies

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
