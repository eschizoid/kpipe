<p align="center">
  <img src="img/kpipe.png" alt="kpipe — a Kafka consumer library for Java 25" width="320" />
</p>

# kpipe

A Kafka consumer library for Java 25. Built around virtual threads, a functional pipeline API, and at-least-once
delivery.

[![JVM 25+](https://img.shields.io/badge/JVM-25%2B-brightgreen.svg?&logo=openjdk)](https://openjdk.org/projects/jdk/25/)
[![Build](https://github.com/eschizoid/kpipe/actions/workflows/ci.yaml/badge.svg)](https://github.com/eschizoid/kpipe/actions/workflows/ci.yaml)
[![Codecov](https://codecov.io/gh/eschizoid/kpipe/graph/badge.svg?token=X50GBU4X7J)](https://codecov.io/gh/eschizoid/kpipe)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.eschizoid/kpipe-consumer.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.eschizoid/kpipe-consumer)
[![Javadoc](https://javadoc.io/badge2/io.github.eschizoid/kpipe-api/javadoc.svg?color=purple)](https://javadoc.io/doc/io.github.eschizoid/kpipe-api)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

What it does:

- Client-side parallelism — virtual thread per record, no thread pool to tune, no scaling by consumer count
- Composable `UnaryOperator<T>` pipelines that deserialize once and serialize once
- At-least-once delivery via lowest-pending-offset commits, even with parallel processing
- In-flight or lag-based backpressure with hysteresis
- Dead-letter routing, retries, and a circuit breaker — one fluent setter each
- Multi-topic dispatch (homogeneous or heterogeneous-format) through a single consumer / consumer-group
- Minimal framework code on the hot path

The target audience is anyone running Kafka consumers that transform, enrich, or route — and would rather not glue their
own consumer loop together every time.

### Two API surfaces

There are two public entry points; pick whichever matches the shape of your problem:

| Surface                                                | What it gives you                                                                                                                                                                   | When to use                                                                    |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| **`KPipe` fluent facade** (`kpipe-api`)                | 5-line `KPipe.json("topic", props).pipe(...).toConsole().start()`. Returns a `Stream<T>` → `Sink<T>` → `Handle` chain. Immutable, IDE-discoverable.                                 | The common path — most users start here.                                       |
| **Registry + Builder explicit API** (`kpipe-consumer`) | `MessageProcessorRegistry` + `KPipeConsumer.Builder`. Multi-step, supports custom registries, shared pipelines, custom offset managers, periodic metrics reporting via the builder. | Custom offset managers, multi-pipeline-per-consumer, advanced lifecycle hooks. |

The facade is a thin layer on top of the explicit API, so dropping down when you outgrow it doesn't cost anything. See
[`docs/escape-hatches.md`](docs/escape-hatches.md) for the full capability map and worked examples of the explicit-only
features (custom `OffsetManager`, rebalance listeners, pre-shared registries, etc.).

---

## Getting started

### 1. Add the dependencies

For the 5-line fluent path (recommended), pull `kpipe-api` plus the format module(s) you need:

```kotlin
// Gradle (Kotlin) — JSON via the fluent API
implementation("io.github.eschizoid:kpipe-api:1.15.0")
implementation("io.github.eschizoid:kpipe-format-json:1.15.0")
```

```xml
<!-- Maven — JSON via the fluent API -->
<dependency>
  <groupId>io.github.eschizoid</groupId>
  <artifactId>kpipe-api</artifactId>
  <version>1.15.0</version>
</dependency>
<dependency>
  <groupId>io.github.eschizoid</groupId>
  <artifactId>kpipe-format-json</artifactId>
  <version>1.15.0</version>
</dependency>
```

`kpipe-api` transitively pulls `kpipe-consumer` + `kpipe-producer` + `kpipe-core`. Skip `kpipe-api` only if you want the
explicit registry / builder API (see "Advanced API" further down) — for that case, depend on `kpipe-consumer` directly.

There's also a `kpipe-bom` so you only pin one version across modules — use it via `dependencyManagement` (Maven) or
`enforcedPlatform` (Gradle) and drop versions from the individual `kpipe-*` dependencies.

<details>
<summary>Module catalog & other build tools</summary>

| Module                            | What it gives you                                                                                                                                           |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `kpipe-api`                       | High-level fluent entry point: `KPipe`, `Stream<T>`, `Sink<T>`, `Handle`                                                                                    |
| `kpipe-bom`                       | Maven BOM — pins all `kpipe-*` artifacts to matching versions                                                                                               |
| `kpipe-core`                      | Low-level building blocks: registries, `MessageFormat`, `MessageSink`, operators, `BatchSink`                                                               |
| `kpipe-metrics`                   | Metrics interfaces (`ConsumerMetrics`, `ProducerMetrics`) + log-based reporters                                                                             |
| `kpipe-metrics-otel`              | OpenTelemetry-backed implementation (opt-in)                                                                                                                |
| `kpipe-tracing-otel`              | W3C trace context propagation through Kafka headers (opt-in)                                                                                                |
| `kpipe-schema-registry-confluent` | Confluent Schema Registry client — `lookupById` + `lookupBySubjectVersion` (opt-in)                                                                         |
| `kpipe-producer`                  | Kafka producer wrapper, `KafkaMessageSink`, `Tracer` SPI                                                                                                    |
| `kpipe-consumer`                  | `KPipeConsumer` (hosts lifecycle, metrics-reporter thread, shutdown hook), `BackpressureController`, `CircuitBreakerController`, `ConsumerHealthController` |
| `kpipe-format-json`               | `JsonFormat`, `JsonConsoleSink`                                                                                                                             |
| `kpipe-format-avro`               | `AvroFormat`, `AvroConsoleSink`                                                                                                                             |
| `kpipe-format-protobuf`           | `ProtobufFormat`, `ProtobufConsoleSink`                                                                                                                     |

**Gradle (Kotlin) with BOM**

```kotlin
implementation(platform("io.github.eschizoid:kpipe-bom:1.15.0"))
implementation("io.github.eschizoid:kpipe-api")
implementation("io.github.eschizoid:kpipe-format-json")
// add kpipe-metrics-otel only if you want OpenTelemetry-backed metrics
implementation("io.github.eschizoid:kpipe-metrics-otel")
```

**Gradle (Groovy)**

```groovy
implementation platform('io.github.eschizoid:kpipe-bom:1.15.0')
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
      <version>1.15.0</version>
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
libraryDependencies += "io.github.eschizoid" % "kpipe-api" % "1.15.0"
libraryDependencies += "io.github.eschizoid" % "kpipe-format-json" % "1.15.0"
```

</details>

### 2. Hello, KPipe — five lines

```java
import io.github.eschizoid.kpipe.KPipe;
import static io.github.eschizoid.kpipe.registry.Operators.removeFields;

KPipe.json("events", kafkaProps)
    .pipe(msg -> { msg.put("ts", System.currentTimeMillis()); return msg; })
    .pipe(removeFields("password"))
    .toConsole()
    .start();   // returns AutoCloseable Handle (call .close() to shut down)
```

A working JSON Kafka consumer that strips the `password` field, stamps a timestamp, and logs to console. Behind the
scenes the chain assembles a `MessageProcessorRegistry` and a `KPipeConsumer` — you don't have to touch either of them.

### 2b. Production wiring — ten lines

The same shape with the operational stack turned on: retry, backpressure with default watermarks, a dead-letter topic,
and a custom sink instead of the console. Everything below is one-liner toggle on top of the §2 chain.

```java
try (var handle = KPipe.json("orders", kafkaProps)
    .pipe(enrich)
    .filter(order -> order.total() > 0)
    .withRetry(3, Duration.ofMillis(100))
    .withBackpressure()                            // pause at 10k in-flight, resume at 7k
    .withDeadLetterTopic("orders.dlq")             // bad records flow here after retries exhaust
    .onFailed(cause -> log.warn("processing failed", cause))   // observer, not a handler — log only
    .toCustom(WarehouseSink.create())
    .start()) {
  handle.awaitShutdown();
}
```

`Handle` is `AutoCloseable` and `close()` calls `shutdownGracefully(Duration.ofSeconds(5))` by default. The DLQ wires a
`KafkaProducer` from the same Kafka properties; to share a pre-built producer instead, drop down to the explicit
`KPipeConsumer.Builder` and call `.withDeadLetterQueue(topic, kpipeProducer)`. The second argument is a
`KPipeProducer<K, byte[]>` (not a raw `KafkaProducer`) — wrap your producer with
`KPipeProducer.<K, byte[]>builder().withProducer(rawProducer).build()` if you only have the raw Kafka client. The atomic
form keeps the topic and producer from drifting out of sync. See [`docs/escape-hatches.md`](docs/escape-hatches.md) for
the full set of explicit-only options (custom `OffsetManager`, multi-topic heterogeneous dispatch).

### 3. The full fluent surface

The `Stream<T>` returned by `KPipe.json/avro/protobuf/bytes/custom(...)` is the API. Type a `.` after it and the IDE
shows you everything that exists:

| Method                                                             | What it does                                                                                 |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------- |
| `.pipe(UnaryOperator<T> op)`                                       | append an operator to the pipeline                                                           |
| `.filter(Predicate<T> keep)`                                       | drop messages where predicate returns false                                                  |
| `.peek(Consumer<T> sideEffect)`                                    | observe without modifying (logging, metrics)                                                 |
| `.when(Predicate, ifTrue, ifFalse)`                                | branch the pipeline conditionally                                                            |
| `.skipBytes(int n)`                                                | drop a leading wire-format prefix (Confluent: 5 for Avro, 6 for Proto)                       |
| `.withRetry(int max, Duration backoff)`                            | configure retry behavior                                                                     |
| `.withBackpressure()` / `.withBackpressure(high, low)`             | enable backpressure with default or explicit watermarks                                      |
| `.withProcessingMode(ProcessingMode)`                              | `SEQUENTIAL` (per-partition serial), `PARALLEL` (default), or `KEY_ORDERED` (per-key serial) |
| `.withKeyOrderedMaxKeys(int)`                                      | LRU cap on distinct keys in `KEY_ORDERED` (default 10,000)                                   |
| `.withCircuitBreaker(double threshold, int window, Duration open)` | open the circuit when sink failure rate exceeds threshold (see below)                        |
| `.withTracer(Tracer t)`                                            | propagate W3C trace context through Kafka headers                                            |
| `.withDeadLetterTopic(String)`                                     | route failed records to a DLQ topic after retries are exhausted                              |
| `.withErrorHandler(Consumer<ProcessingError<byte[]>>)`             | custom failure handler (DB, alerting, anything not a Kafka topic)                            |
| `.withMetrics(ConsumerMetrics m)`                                  | wire OTel or custom metrics (default `ConsumerMetrics.noop()`)                               |
| `.withPollTimeout(Duration d)`                                     | override Kafka poll timeout (default 100ms)                                                  |
| `.toConsole()`                                                     | terminate with the format-appropriate console sink                                           |
| `.toCustom(MessageSink<T> sink)`                                   | terminate with your own sink                                                                 |
| `.toMulti(MessageSink<T>... sinks)`                                | fan-out to multiple sinks                                                                    |
| `.toBatch(BatchSink<T> sink, BatchPolicy policy)`                  | terminate with a batch sink (size / age flush, optional per-record DLQ)                      |

The terminal `Sink<T>.start()` returns a `Handle` exposing `isHealthy()`, `metrics()`, `awaitShutdown(Duration)`,
`shutdownGracefully(Duration)`, `topKeyQueueDepths(int)` (for `KEY_ORDERED` diagnostics — empty list for other modes),
and `close()`.

### 4. Consuming from multiple topics

Two flavors, depending on whether the topics share a payload type.

Homogeneous — many topics, one shared pipeline (the "partitioned-by-region versions of the same event" case):

```java
KPipe.json(Set.of("events-us", "events-eu", "events-ap"), kafkaProps)
    .pipe(addTimestamp)
    .toCustom(captureSink)
    .start();
```

The `Collection<String>` overload exists on every entry point — `KPipe.json/avro/protobuf/bytes/custom`. Use
`KPipe.custom(topic, props, format)` (or its `Collection<String>` overload) when you have a user-supplied
`MessageFormat<T>` and want the same fluent surface as the bundled formats.

Heterogeneous — many topics, each with its own payload type and its own pipeline, all dispatched through one consumer /
one consumer-group / one offset manager:

```java
KPipe.multi(kafkaProps)
    .json("events-json",  s -> s.pipe(addTimestamp).toCustom(jsonSink))
    .avro("events-avro",  s -> s.filter(active).toCustom(avroSink))
    .bytes("events-raw",  s -> s.toCustom(rawSink))
    .start();
```

Each route gets its own typed `Stream<T>`. Records arriving for topics not registered with a route are dropped at
`WARNING` and their offsets are still committed (no infinite retry on a config error).

### 5. Common operator patterns

`io.github.eschizoid.kpipe.registry.Operators` exposes pure-function helpers ready to drop into `.pipe(...)`:

```java
import static io.github.eschizoid.kpipe.registry.Operators.*;

KPipe.json("events", kafkaProps)
    .filter(msg -> "active".equals(msg.get("status")))            // drop inactive
    .peek(msg -> log.info("processing {}", msg.get("id")))        // log without modifying
    .pipe(rename("user_id", "userId"))                            // rename a field
    .pipe(removeFields("password", "ssn"))                        // strip sensitive fields
    .pipe(safe(msg -> riskyEnrich(msg)))                          // wrap in error-handling
    .toConsole()
    .start();
```

The full operator vocabulary: `filter`, `drop`, `peek`, `map`, `compose`, `safe`, `requireField`, `rename`,
`removeFields`, `addField`. All return `UnaryOperator<T>` (or `UnaryOperator<Map<String, Object>>` for the `Map`-typed
ones).

---

## When to use KPipe

KPipe is a good fit if you're building Kafka consumer microservices, event enrichment pipelines, or lightweight
transformation services — especially anything I/O-bound (REST calls, DB lookups) where virtual threads pay off. If
you're already moving toward Java 25 concurrency, KPipe assumes that world by default.

It's not trying to replace Kafka Streams or Flink. The scope ends at "consume, transform, route, produce" — no
windowing, no joins, no state stores.

---

## Why not Kafka Streams or Spring Kafka?

KPipe is for code-first pipelines with minimal infrastructure overhead.

| Capability                       | Kafka Streams | Spring Kafka            | Reactor Kafka | KPipe |
| -------------------------------- | ------------- | ----------------------- | ------------- | ----- |
| Full stream processing framework | Yes           | No                      | No            | No    |
| Lightweight consumer pipelines   | Partial       | Yes                     | Yes           | Yes   |
| Virtual-thread friendly          | No            | Listener-container only | No            | Yes   |
| Functional pipeline API          | Yes           | No (annotations)        | Yes           | Yes   |
| Minimal runtime dependencies     | No            | No (Spring Boot)        | Yes           | Yes   |
| JPMS modules (no Spring context) | No            | No                      | Partial       | Yes   |
| Native multi-topic dispatch      | Yes (DSL)     | Per `@KafkaListener`    | Manual        | Yes   |

KPipe sits between raw `KafkaConsumer` code and full streaming frameworks.

### Coming from Spring Kafka?

Spring Kafka covers the same niche — "I want a Kafka consumer with retries, DLQ, backpressure, and a typed payload" —
but bundles Spring Boot, the application context, and annotation-driven wiring. KPipe targets the same workload with no
Spring runtime, no annotation processor, no `KafkaListenerContainerFactory` to configure, and a fluent API that's
discoverable via IDE autocomplete.

What changes:

- No Spring Boot on the classpath. KPipe runs on plain `java -jar` — eight JPMS modules, no reflection, no AOP, no
  application context to manage.
- Virtual threads by default. Spring Kafka's listener container is still thread-pool-per-partition; KPipe runs
  thread-per-record on virtual threads, which is where I/O-bound enrichment scales without pool tuning.
- No `@KafkaListener` indirection. Pipelines are values you build in `main` and pass around. No classpath scanning, no
  proxied beans, no `@EnableKafka` toggle to remember.
- Per-topic typing without a class per listener. `KPipe.multi(props).json("a", ...).avro("b", ...)` wires heterogeneous
  routes through one consumer group and one offset manager. Spring's equivalent is one `@KafkaListener` method per
  topic, each with its own container.
- Errors throw instead of getting swallowed. `MessagePipeline.apply()` throws on failure; `null` means "intentionally
  filtered" and only that. Spring Kafka's default `ErrorHandlingDeserializer` + `RetryableTopic` chain is more flexible
  but also easier to misconfigure into silent drops.
- Metrics are pluggable. `ProducerMetrics` and `ConsumerMetrics` are interfaces; the `kpipe-metrics-otel` adapter is
  opt-in. No `MeterRegistry` autowiring.

Rough migration sketch — a Spring Kafka listener:

```java
// Spring Kafka
@KafkaListener(topics = "events", groupId = "my-app")
public void onMessage(ConsumerRecord<String, Map<String, Object>> rec) {
  rec.value().put("ts", System.currentTimeMillis());
  sink.accept(rec.value());
}
```

becomes:

```java
// KPipe
KPipe.json("events", kafkaProps)
    .pipe(msg -> { msg.put("ts", System.currentTimeMillis()); return msg; })
    .toCustom(sink::accept)
    .start();
```

Retry, DLQ, backpressure, and graceful shutdown are fluent calls on the same `Stream<T>`. No `RetryTemplate`, no
`DeadLetterPublishingRecoverer`, no `ContainerProperties.AckMode` to pick.

If you depend on `@KafkaListener` lifecycle integration with Spring Boot actuator, transactional Kafka producers wired
into `@Transactional`, or the broader Spring ecosystem (security, config-server, sleuth tracing), stay where you are.
KPipe doesn't have those.

---

## Architecture and reliability

KPipe is a lightweight Kafka consumer library that leans hard on Java 25 features — virtual threads, sealed types,
records, JPMS — to keep the runtime predictable.

### 1. Modular Architecture (JPMS)

KPipe ships eight focused JPMS modules with a clean dependency direction (no cycles, no sideways leaks):

```
kpipe-metrics ← kpipe-core ← kpipe-consumer
                          ← kpipe-producer
                          ← kpipe-format-{json, avro, protobuf}
                          ← kpipe-api  (KPipe fluent facade)
kpipe-metrics-otel              ← kpipe-metrics    (opt-in OTel metrics)
kpipe-tracing-otel              ← kpipe-producer   (opt-in OTel tracing)
kpipe-schema-registry-confluent ← kpipe-core       (opt-in Confluent SR client)
kpipe-bom                                          (Maven BOM — pins versions)
```

- **kpipe-core**: format-agnostic pipeline machinery — `MessageFormat`, `MessagePipeline`, `MessageProcessorRegistry`,
  `MessageSink`, `CompositeMessageSink`, `RegistryKey`, `RegistryFunctions`. No Kafka, no third-party runtime deps.
- **kpipe-metrics**: pure interfaces (`ProducerMetrics`, `ConsumerMetrics`) plus log-based reporters. No OTel API on the
  classpath.
- **kpipe-metrics-otel**: OpenTelemetry implementation (`OtelConsumerMetrics`, `OtelProducerMetrics`). Add this only if
  you want OTel-backed metrics.
- **kpipe-producer**: Kafka producer wrapper, `KafkaMessageSink` (in `io.github.eschizoid.kpipe.producer.sink`),
  `Tracer` SPI.
- **kpipe-consumer**: `KPipeConsumer` (hosts lifecycle: `start` / `close` / `awaitShutdown` / `shutdownGracefully` /
  `waitForInFlightDrain` + optional metrics-reporter thread and JVM shutdown hook), `BackpressureController`,
  `CircuitBreakerController`, `ConsumerHealthController`, `KafkaOffsetManager`, `HttpHealthServer`, `consumer.metrics`
  reporters.
- **kpipe-tracing-otel**: W3C trace context propagation — extract on consume, inject on produce / DLQ. Opt-in module
  (see "Distributed tracing" below).
- **kpipe-schema-registry-confluent**: Confluent Schema Registry client (`ConfluentSchemaResolver`) for schema-by-ID and
  by-subject-version lookup. Opt-in (see "Confluent Schema Registry" below).
- **kpipe-format-json / -avro / -protobuf**: per-format `XFormat.INSTANCE`, processors, and console sinks. Pull only the
  format(s) you need.

Use KPipe in a modular project:

```java
module my.application {
  requires io.github.eschizoid.kpipe.consumer; // includes core + producer + metrics transitively
  requires io.github.eschizoid.kpipe.format.json; // add only the formats you use
  requires io.github.eschizoid.kpipe.metrics.otel; // optional — enables OTel-backed metrics
}
```

### 2. Single SerDe cycle

Most pipelines do `byte[] → Object → byte[]` at every step. KPipe doesn't:

- Deserialize once into a mutable representation (`Map` for JSON, `GenericRecord` for Avro) via `MessagePipeline`.
- Apply a chain of `UnaryOperator` functions to that same object.
- Serialize back to `byte[]` once at the end.
- Typed sinks can attach directly to the pipeline and receive the object before final serialization, skipping it
  entirely.

The win is fewer allocations and less CPU spent on intermediate SerDe steps. The benchmark numbers are in
`benchmarks/README.md`.

### 3. Virtual threads

KPipe runs on Java virtual threads (Project Loom) for concurrency.

- Thread-per-record. Each message gets its own virtual thread, so I/O-bound enrichment scales without explicit pool
  sizing.
- No `ThreadLocal` on the hot path. `ThreadLocal` doesn't compose with thread-per-record — every record would get a
  fresh map, defeating any caching intent. If a future need for thread-local-like state turns up (tenant context, span
  propagation), we'll reach for `ScopedValue`; the codebase currently has neither.

### 4. At-least-once delivery via lowest pending offset

KPipe never commits past an in-flight record. The implementation:

- `OffsetManager` is an interface — Kafka-backed by default, but you can plug in external storage.
- Every in-flight offset is tracked in a `ConcurrentSkipListSet` per partition (see `KafkaOffsetManager`).
- Offset 102 cannot be committed until 101 finishes, even if 102 completes first. No gaps.
- On crash, the consumer resumes from the last committed offset. Some records may be reprocessed — standard
  at-least-once behavior — but nothing is skipped.

### 5. Parallel vs. sequential processing

Three modes via `.withProcessingMode(ProcessingMode)`:

- `PARALLEL` (default). Stateless transformations like enrichment or masking. Virtual thread per record, no ordering.
  Offsets commit by lowest pending offset.
- `SEQUENTIAL` (`.withProcessingMode(ProcessingMode.SEQUENTIAL)`). Stateful transformations where strict per-partition
  order matters. One message per partition at a time. Backpressure switches to monitoring consumer lag — the gap between
  partition end-offset and consumer position — since in-flight count is always ≤ 1.
- `KEY_ORDERED` (`.withProcessingMode(ProcessingMode.KEY_ORDERED)`). Records sharing a key process serially on a per-key
  virtual thread; different keys process in parallel. The production sweet spot for stateful workloads where order
  matters per entity (per user, per order, per session) but entities are independent. LRU cap on active keys defaults to
  10,000; override with `.withKeyOrderedMaxKeys(int)`. Records with `null` keys all serialize through a single sentinel
  queue. When the cap saturates with every queue non-empty, dispatch stalls the consumer thread until a queue drains —
  this acts as implicit backpressure, and a one-shot `WARNING` log fires the first time it happens to hint at raising
  the cap. For diagnostics under high cardinality, `Handle.topKeyQueueDepths(int n)` returns a snapshot of the deepest
  per-key queues.

### 6. External offset management

Kafka-backed offset storage is the default. For exactly-once processing or external coordination needs, plug in your own
`OffsetManager`.

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

### 7. Error handling and retries

Error handling is layered:

- Retries: `.withRetry(maxRetries, backoff)` for transient failures.
- Dead-letter topic:
  ```java
  final var consumer = KPipeConsumer.<byte[]>builder()
    .withDeadLetterTopic("events-dlq") // sent here after retries are exhausted
    .build();
  ```
- Custom error handler: `.withErrorHandler(...)` to route failures somewhere other than a Kafka topic — an external DB,
  alerting system, whatever.
- Per-processor and per-sink wrapping: `MessageProcessorRegistry.withOperatorErrorHandling()` and
  `MessageProcessorRegistry.withSinkErrorHandling()` swallow failures at the boundary so one bad processor doesn't take
  down the pipeline.

### 8. Backpressure

When a downstream sink (database, HTTP API, another Kafka topic) is slow, KPipe pauses Kafka polling instead of letting
in-flight work or lag pile up unbounded.

It uses two watermarks with hysteresis so it doesn't oscillate:

- High watermark — pause polling when the monitored metric crosses this value.
- Low watermark — resume polling when the metric drops back to or below this value.

#### Strategies

KPipe picks the strategy based on processing mode:

| Mode               | Strategy     | Monitored              | Why                                                |
| :----------------- | :----------- | :--------------------- | :------------------------------------------------- |
| Parallel (default) | In-flight    | Active virtual threads | Cap concurrent in-flight work to bound memory      |
| Sequential         | Consumer lag | Total unread messages  | In-flight is always ≤ 1, so lag is the real signal |

##### Parallel mode: in-flight count

Many records run concurrently on virtual threads. The controller watches how many are currently in-flight (started but
not finished).

- High watermark default: 10,000
- Low watermark default: 7,000

##### Sequential mode: consumer lag

Sequential mode processes one record per partition at a time, so in-flight is always ≤ 1. KPipe watches consumer lag
across all assigned partitions instead:

```
lag = Σ (endOffset - position)
```

- `endOffset` — highest offset available in the partition.
- `position` — offset of the next record this consumer will fetch.

- High watermark default: 10,000
- Low watermark default: 7,000

#### Configuration

```java
final var consumer = KPipeConsumer.<byte[]>builder()
  .withProperties(kafkaProps)
  .withTopic("events")
  .withPipeline(pipeline)
  // Default watermarks: 10k high / 7k low
  .withBackpressure()
  // Or explicit:
  // .withBackpressure(5_000, 3_000)
  .build();
```

Backpressure is on by default with 10k/7k watermarks. Override with `.withBackpressure(high, low)` for your workload.

Pauses log at WARNING, resumes at INFO. Two dedicated metrics track them: `backpressurePauseCount` and
`backpressureTimeMs`.

### 9. Circuit breaker

Backpressure caps in-flight work and prevents memory blow-ups, but it doesn't stop the bleeding when a downstream sink
is _failing_ (DB connection refused, HTTP 503). Without a circuit breaker, every record during an outage runs
`maxRetries + 1` attempts and lands in the DLQ. The breaker stops the cascade by pausing polling once the sink failure
rate crosses a threshold, then probes recovery with a single record after a cool-down window.

```java
import java.time.Duration;
import io.github.eschizoid.kpipe.KPipe;

KPipe.json("events", kafkaProps)
    .pipe(enrich)
    .withCircuitBreaker(
        0.5,                          // failure threshold (50% over the window)
        100,                          // window size (last N outcomes)
        Duration.ofSeconds(30))       // open duration before half-open probe
    .toCustom(flakySink)
    .start();
```

The state machine is the standard CLOSED → OPEN → HALF_OPEN → CLOSED cycle:

- **CLOSED**: outcomes feed a sliding window. When the rolling failure rate exceeds the threshold, transition to OPEN.
- **OPEN**: poll is paused. After `openDuration`, transition to HALF_OPEN.
- **HALF_OPEN**: poll resumes; the next record is the probe. Success → CLOSED (and the window resets). Failure → back to
  OPEN for another `openDuration`.

CB and backpressure pause through the same `ConsumerHealthController` (bitmask of MANUAL / BACKPRESSURE /
CIRCUIT_BREAKER sources), so they don't fight each other — releasing the backpressure source while the CB still holds
keeps the consumer paused, and vice-versa.

OTel counters (opt-in via `kpipe-metrics-otel`):

| Instrument                                     | Type    | Description                                  |
| ---------------------------------------------- | ------- | -------------------------------------------- |
| `kpipe.consumer.circuit_breaker.trips`         | Counter | Times the breaker transitioned CLOSED → OPEN |
| `kpipe.consumer.circuit_breaker.state_changes` | Counter | Any state transition, attribute-tagged       |
| `kpipe.consumer.circuit_breaker.time_open`     | Counter | Total ms spent in OPEN                       |

When `.withCircuitBreaker(...)` is omitted, the consumer never trips and the counters stay at zero.

### 10. Distributed tracing (W3C trace context)

KPipe propagates W3C trace context (`traceparent` / `tracestate` Kafka headers) end-to-end: extract on consume, inject
on produce and on DLQ writes. The implementation lives in the opt-in `kpipe-tracing-otel` module — `kpipe-core` and
`kpipe-consumer` are dependency-free, you bring the OTel SDK at runtime.

Add the dependency:

```kotlin
implementation("io.github.eschizoid:kpipe-tracing-otel:1.15.0")
```

Wire it on the stream:

```java
import io.opentelemetry.api.OpenTelemetry;
import io.github.eschizoid.kpipe.KPipe;
import io.github.eschizoid.kpipe.tracing.otel.OtelTracer;

final OpenTelemetry otel = /* GlobalOpenTelemetry.get() or your SDK */;

KPipe.json("events", kafkaProps)
    .withTracer(new OtelTracer(otel, "events-consumer"))
    .pipe(enrich)
    .toCustom(producerSink)
    .start();
```

What it does on the hot path:

1. On poll, extracts the upstream context from `traceparent` / `tracestate` headers using the standard W3C propagator.
2. Starts a CONSUMER span with `messaging.kafka.{topic,partition,offset}` attributes; the upstream span is the parent.
3. Closes the span in a nested `finally` so a throwing user callback can't leak the scope.
4. On produce (`KafkaMessageSink.accept`) and DLQ writes (`KPipeProducer.sendToDlq`), injects the _current_ context into
   outbound headers — downstream consumers see the same trace.

Without `.withTracer(...)`, `Tracer.noop()` is used: zero allocation, no OTel API on the classpath.

### 11. Graceful shutdown and interrupts

KPipe respects JVM signals without losing records:

- Interrupts trigger a coordinated shutdown. They don't cause records to be skipped.
- If a record's processing is interrupted mid-flight (during retry backoff or transformation), its offset is not marked
  as processed. The next consumer instance picks it up — at-least-once still holds during shutdown.

```java
// Initiate graceful shutdown with 5-second timeout
boolean allProcessed = runner.shutdownGracefully(5000);

// Or register a JVM shutdown hook via the builder (default off)
KPipeConsumer.<byte[]>builder()
  .withShutdownHook(true)
  // ...
  .build();
```

---

## Working with messages

Pipelines deserialize once, transform many times, serialize once. Operators are `UnaryOperator<T>` where `T` is the
format's typed payload — `Map<String, Object>` for JSON, `GenericRecord` for Avro, `Message` for Protobuf. The generic
helpers in [`Operators`](lib/kpipe-core/src/main/java/io/github/eschizoid/kpipe/registry/Operators.java) (`filter`,
`drop`, `peek`, `map`, `compose`, `safe`, plus the map-specific `addField`, `removeFields`, `requireField`, `rename`)
cover the common cases. For anything else, inline lambdas using the format's native API.

### JSON

Add `kpipe-format-json`. Operators are `UnaryOperator<Map<String, Object>>`:

```java
import static io.github.eschizoid.kpipe.registry.Operators.*;

import io.github.eschizoid.kpipe.format.json.JsonFormat;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.RegistryKey;

final var registry = new MessageProcessorRegistry();

final var stampKey = RegistryKey.json("addTimestamp");
registry.registerOperator(stampKey, addField("processedAt", System.currentTimeMillis()));

final var sanitizeKey = RegistryKey.json("sanitize");
registry.registerOperator(sanitizeKey, removeFields("password", "ssn"));

final var lowerEmailKey = RegistryKey.json("lowerEmail");
registry.registerOperator(lowerEmailKey, msg -> {
  // Returning null from an operator filters the record: the pipeline yields Result.filtered()
  // and downstream sinks are skipped, but the offset is still marked processed. Same convention
  // as the bundled Operators.* helpers (filter, drop, removeFields, ...).
  if (msg == null) return null;
  final var email = msg.get("email");
  if (email instanceof String s) msg.put("email", s.toLowerCase());
  return msg;
});

// Single deserialization → many transformations → single serialization
final var pipeline = registry.pipeline(JsonFormat.INSTANCE)
    .add(sanitizeKey)
    .add(lowerEmailKey)
    .add(stampKey)
    .build();
```

### Avro

Add `kpipe-format-avro`. Operators are `UnaryOperator<GenericRecord>`:

```java
import io.github.eschizoid.kpipe.format.avro.AvroFormat;
import io.github.eschizoid.kpipe.format.avro.AvroRegistryKey;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;

// Build an AvroFormat bound to a single schema. Use new AvroFormat(schema) when you already have a
// parsed Schema, or AvroFormat.of(schemaJson) for inline JSON. For multiple schemas keyed by name
// use AvroSchemaCatalog and pass catalog.get("user") into AvroFormat.
final var format = AvroFormat.of("""
  {"type":"record","name":"User","namespace":"com.kpipe","fields":[
    {"name":"id","type":"string"},{"name":"name","type":"string"}
  ]}""");

final var registry = new MessageProcessorRegistry();

// Avro records are schema-bound: use inline lambdas with the native Avro API for value transforms.
// `Operators.filter`, `peek`, `compose` etc. work for any payload type, including GenericRecord.
final var lowerNameKey = AvroRegistryKey.of("lowerName");
registry.registerOperator(lowerNameKey, record -> {
  if (record.get("name") != null) record.put("name", record.get("name").toString().toLowerCase());
  return record;
});

final var pipeline = registry.pipeline(format).add(lowerNameKey).build();

// For Confluent Wire Format (1 magic byte + 4-byte schema ID), skip the prefix:
final var confluentPipeline = registry.pipeline(format).skipBytes(5).add(lowerNameKey).build();
```

#### Confluent Schema Registry

For Confluent-style deployments where schemas live in a registry rather than on the classpath, the
`kpipe-schema-registry-confluent` module ships an HTTP client and an in-process cache. Per-record auto-lookup is wired
into `AvroFormat`: the format reads the wire envelope (1-byte magic + 4-byte schema ID) off each record, resolves the
writer's schema, caches it by ID, and decodes the remaining bytes against it. This is the schema-evolution-correct path
— every record decodes against its actual writer schema, so producer evolution doesn't silently corrupt consumer output
the way a static-fetch-at-startup pattern would.

Add the dependency:

```kotlin
implementation("io.github.eschizoid:kpipe-schema-registry-confluent:1.15.0")
```

Wire the fluent facade with the SR-shorthand factory:

```java
import java.time.Duration;
import io.github.eschizoid.kpipe.KPipe;
import io.github.eschizoid.kpipe.schemaregistry.confluent.CachedSchemaResolver;
import io.github.eschizoid.kpipe.schemaregistry.confluent.ConfluentSchemaResolver;

// One resolver for the process; CachedSchemaResolver caches by ID with no TTL (Confluent SR IDs
// are immutable, so cache-by-ID is trivially correct).
try (final var resolver = new CachedSchemaResolver(
        new ConfluentSchemaResolver("http://schema-registry:8081", Duration.ofSeconds(10)));
     final var handle = KPipe.avro("orders", kafkaProps, resolver)   // ← one-line SR consumer
         .pipe(record -> enrich(record))
         .toCustom(WarehouseSink.create())
         .start()) {
  handle.awaitShutdown();
}
```

The factory above is equivalent to `KPipe.avro(AvroFormat.withRegistry(resolver), "orders", props)`. The format reads
the envelope per record. Do **not** combine `withSchemaRegistry(...)` with `skipBytes(5)` — the format already consumes
the envelope. The static-mode path is still supported for shops with strict append-only evolution who fetch the schema
once at startup:

```java
try (final var resolver = new ConfluentSchemaResolver("http://schema-registry:8081")) {
  final var schemaJson = resolver.lookupBySubjectVersion("orders-value", "latest");
  final var format = AvroFormat.of(schemaJson);
  // Pass `format` to KPipe.avro(topic, props, format) and pair with .skipBytes(5).
}
```

The explicit `lookupById(int)` and `lookupBySubjectVersion(subject, version)` calls remain available on
`ConfluentSchemaResolver` for users who want to manage their own caching or fetch a known schema at startup.

**Cache semantics.** `CachedSchemaResolver` uses a `ConcurrentHashMap` with no TTL and no LRU eviction. Confluent SR
schema IDs are immutable — ID 42 today means the same schema tomorrow — so cache-by-ID is trivially correct and cache
cardinality is naturally bounded (typically tens of distinct schemas across the lifetime of a topic, even with active
evolution). The first record carrying a new schema ID costs one HTTP round trip; every subsequent record with that ID is
a hit. Hit / miss / size counters are exposed via the resolver's accessors and can be bound to OTel via
`PipelineMetricsObserver.bindSchemaRegistryCache(...)` (see [Observability](#observability) below).

**Scope.** Avro only for now. Protobuf SR auto-lookup needs runtime `.proto` text compilation and has not shipped — use
`kpipe-format-protobuf` with a compiled descriptor and `skipBytes(6)` for the single-top-level-message case. No schema
publishing (Confluent's own producer client handles that). No compatibility checks at the consumer — those run at
registration time inside SR.

### Protobuf

Add `kpipe-format-protobuf`. Operators are `UnaryOperator<Message>`. Protobuf messages are immutable, so every transform
builds a new message via `toBuilder().setField(...).build()`.

```java
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufRegistryKey;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;

// Build a ProtobufFormat bound to a single descriptor. Use ProtobufDescriptorCatalog for keyed
// lookup of multiple descriptors.
final var format = new ProtobufFormat(CustomerProto.Customer.getDescriptor());
final var registry = new MessageProcessorRegistry();

final var clearEmailKey = ProtobufRegistryKey.of("clearEmail");
registry.registerOperator(clearEmailKey, msg -> {
  final var emailField = msg.getDescriptorForType().findFieldByName("email");
  return msg.toBuilder().clearField(emailField).build();
});

// Register the protobuf console sink yourself (defaults are no longer auto-registered)
final var protoLoggingKey = ProtobufRegistryKey.of("protobufLogging");
registry.registerSink(protoLoggingKey, new io.github.eschizoid.kpipe.format.protobuf.ProtobufConsoleSink<>());

final var pipeline = registry
  .pipeline(format)
  .add(clearEmailKey)
  .toSink(protoLoggingKey)
  .build();
```

---

## Message sinks

Sinks are where processed messages go. `MessageSink` is just a functional `Consumer<T>`:

```java
@FunctionalInterface
public interface MessageSink<T> extends Consumer<T> {}
```

### Built-in sinks

Console sinks live in their format modules. The registry doesn't auto-register them — register the ones you want:

```java
import io.github.eschizoid.kpipe.format.json.JsonConsoleSink;
import io.github.eschizoid.kpipe.format.avro.AvroConsoleSink;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufConsoleSink;

// Direct instantiation
final var jsonConsoleSink = new JsonConsoleSink<Map<String, Object>>();
final var avroConsoleSink = new AvroConsoleSink<GenericRecord>(schema);
final var protobufConsoleSink = new ProtobufConsoleSink<Message>();

// Register and use via the pipeline builder
final var consoleSinkKey = RegistryKey.json("jsonConsole");
registry.registerSink(consoleSinkKey, jsonConsoleSink);

final var pipeline = registry
  .pipeline(JsonFormat.INSTANCE)
  .add(RegistryKey.json("sanitize"))
  .toSink(consoleSinkKey)
  .build();
```

### Custom sinks

```java
final MessageSink<Map<String, Object>> databaseSink = (processedMap) -> {
  databaseService.insert(processedMap);
};
```

### Registering sinks

`MessageProcessorRegistry` holds operators and sinks in two namespaces under the same key shape.
`registerOperator(key, op)` and `registerSink(key, sink)` are the entry points; lookups use `getOperator(key)` and
`getSink(key)`. Per-namespace utilities (`getAllSinks`, `getSinkMetrics`, `unregisterSink`, `clearSinks`,
`compositeSink`) keep the surfaces separate without forcing users through a sub-object.

```java
final var registry = new MessageProcessorRegistry();

// Register a sink under a typed key
final var dbKey = RegistryKey.of("database", Map.class);
registry.registerSink(dbKey, databaseSink);

// Use it in a pipeline
final var pipeline = registry.pipeline(JsonFormat.INSTANCE).add(RegistryKey.json("enrich")).toSink(dbKey).build();
```

### Error handling in sinks

```java
// Wrap a sink or operator with error handling (suppresses exceptions, logs errors)
final var safeSink = MessageProcessorRegistry.withSinkErrorHandling(riskySink);
final var safeOperator = MessageProcessorRegistry.withOperatorErrorHandling(riskyOperator);

registry.registerSink(RegistryKey.json("safeDatabase"), safeSink);
```

### Kafka producer sink

To produce processed messages back to a Kafka topic, use `KafkaMessageSink` (in
`io.github.eschizoid.kpipe.producer.sink`):

```java
import io.github.eschizoid.kpipe.producer.KPipeProducer;
import io.github.eschizoid.kpipe.producer.sink.KafkaMessageSink;

final var producer = KPipeProducer.<byte[], byte[]>builder().withProperties(kafkaProps).build();

final var pipeline = registry
  .pipeline(JsonFormat.INSTANCE)
  .add(RegistryKey.json("transform"))
  .toSink(KafkaMessageSink.of(producer.getProducer(), "output-topic", JsonFormat.INSTANCE::serialize))
  .build();
```

### Composite sink (fanout)

`CompositeMessageSink` (in `io.github.eschizoid.kpipe.sink`, part of `kpipe-core`) broadcasts to multiple sinks. A
failure in one sink doesn't stop the others from receiving the message:

```java
import io.github.eschizoid.kpipe.sink.CompositeMessageSink;

final var compositeSink = new CompositeMessageSink<>(List.of(postgresSink, consoleSink));

final var pipeline = registry.pipeline(JsonFormat.INSTANCE).toSink(compositeSink).build();
```

### Batch sinks

Single-record sinks pay the destination's per-call cost on every message. When that cost is non-trivial — a JDBC commit,
an HTTP POST, an S3 PUT — batching amortizes it. `BatchSink<T>` is a `Function<List<T>, BatchResult>` that flushes at a
configurable size or age:

```java
import java.time.Duration;
import io.github.eschizoid.kpipe.KPipe;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;

KPipe.json("events", kafkaProps)
    .pipe(addTimestamp)
    .toBatch(
        BatchSink.ofVoid(batch -> jdbc.bulkInsert(batch)),     // void-style: success on return, fail on throw
        new BatchPolicy(100, Duration.ofSeconds(5)))           // flush at 100 records OR 5 seconds, whichever first
    .start();
```

`BatchSink.ofVoid(...)` wraps a void-style consumer (throw → whole-batch DLQ). For per-record outcomes — say a bulk HTTP
API that returns which rows failed — return `BatchResult` directly with the succeeded / failed indexes; the wrapper
routes only the failures to the DLQ.

Highlights:

- **Both modes.** Sequential and parallel processing both work; in parallel mode the buffer participates in the
  in-flight backpressure metric so a slow batch sink can't let the buffer grow unbounded.
- **Multi-topic.** Compose with `KPipe.multi(...)` — each route can choose `.toBatch(...)` independently.
- **Coverage contract enforced.** A `BatchResult` that doesn't cover every position `[0, batchSize)` is treated as a
  contract violation and routed to the DLQ rather than silently marked processed.
- **Shutdown drain.** A final flush runs before the offset manager closes, so partially-filled buffers commit cleanly.

Headline number from `BatchSinkLatencyBenchmark` at 1ms-per-call sink latency: **84× throughput at batch=100** versus
the per-record control. Full numbers in [`benchmarks/README.md`](benchmarks/README.md).

---

## Built-in metrics

### Programmatic access

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

### OpenTelemetry

OTel is opt-in via the `kpipe-metrics-otel` module. `kpipe-metrics` ships interfaces only and doesn't pull
`opentelemetry-api` onto your classpath. Add `kpipe-metrics-otel` (plus your OTel SDK at runtime) to wire real
telemetry:

```java
import io.github.eschizoid.kpipe.metrics.otel.OtelConsumerMetrics;

final var consumer = KPipeConsumer.<byte[]>builder()
  .withProperties(kafkaProps)
  .withTopic("events")
  .withPipeline(pipeline)
  .withMetrics(new OtelConsumerMetrics(openTelemetry, "my-pipeline"))
  .build();
```

When `withMetrics(...)` is omitted, `ConsumerMetrics.noop()` / `ProducerMetrics.noop()` is used. Zero allocation, no
OTel API on the classpath.

| Instrument                                     | Type      | Description                                        |
| ---------------------------------------------- | --------- | -------------------------------------------------- |
| `kpipe.consumer.messages.received`             | Counter   | Records polled from Kafka                          |
| `kpipe.consumer.messages.processed`            | Counter   | Records successfully processed                     |
| `kpipe.consumer.messages.errors`               | Counter   | Records that failed processing                     |
| `kpipe.consumer.processing.duration`           | Histogram | Per-record processing time (ms)                    |
| `kpipe.consumer.messages.inflight`             | Gauge     | Current number of in-flight messages               |
| `kpipe.consumer.backpressure.pauses`           | Counter   | Times backpressure paused the consumer             |
| `kpipe.consumer.backpressure.time`             | Counter   | Total time paused due to backpressure              |
| `kpipe.consumer.circuit_breaker.trips`         | Counter   | Times the breaker tripped CLOSED → OPEN            |
| `kpipe.consumer.circuit_breaker.state_changes` | Counter   | Any CB state transition (tagged with target state) |
| `kpipe.consumer.circuit_breaker.time_open`     | Counter   | Total time the breaker spent in OPEN (ms)          |
| `kpipe.producer.messages.sent`                 | Counter   | Records successfully produced                      |
| `kpipe.producer.messages.failed`               | Counter   | Records that failed to produce                     |
| `kpipe.producer.dlq.sent`                      | Counter   | Records sent to DLQ                                |

---

## Consumer lifecycle

Since 1.13.0 the consumer hosts its own lifecycle — start, periodic metrics reporting, JVM shutdown hook, in-flight
drain, graceful shutdown. The standalone `KPipeRunner` was deleted in the runner+tracker fold; everything is on
`KPipeConsumer` directly.

```java
final var consumer = KPipeConsumer.<byte[]>builder()
  .withProperties(kafkaProps)
  .withTopic("events")
  .withPipeline(pipeline)
  .withMetricsReporters(List.of(
    ConsumerMetricsReporter.forConsumer(c -> consumer.getMetrics()),
    EntryMetricsReporter.forProcessors(processorRegistry)
  ))
  .withMetricsInterval(Duration.ofSeconds(30))
  .withShutdownHook(true)   // installs Runtime.getRuntime().addShutdownHook(consumer::close)
  .build();

consumer.start();
consumer.awaitShutdown();             // blocks until close() completes
```

Use try-with-resources for explicit cleanup:

```java
try (final var consumer = KPipeConsumer.<byte[]>builder()
    .withProperties(kafkaProps)
    .withTopic("events")
    .withPipeline(pipeline)
    .build()) {
  consumer.start();
  consumer.awaitShutdown(Duration.ofMinutes(5));   // bounded wait
}
```

Targeted operations: `consumer.shutdownGracefully(Duration)` initiates close with a custom in-flight drain budget and
returns whether the drain finished cleanly; `consumer.waitForInFlightDrain(Duration)` blocks until the in-flight counter
hits zero (useful during reconfiguration without closing).

The facade (`KPipe.json(...).start()`) wraps this in a `Handle` so users of the fluent path never see these methods
directly — the handle's `awaitShutdown` / `shutdownGracefully` / `close` delegate straight through.

---

## Full application example

The [`examples/`](examples/) directory has complete working apps. Below is a condensed sketch.

<details>
<summary>Expand condensed application example</summary>

```java
public class KPipeApp implements AutoCloseable {

  private static final System.Logger LOGGER = System.getLogger(KPipeApp.class.getName());
  private final KPipeConsumer<byte[]> consumer;

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
    final var processorRegistry = new MessageProcessorRegistry();
    final var consoleSinkKey = RegistryKey.json("jsonConsole");
    processorRegistry.registerSink(consoleSinkKey, new JsonConsoleSink<>());

    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    consumer = KPipeConsumer.<byte[]>builder()
      .withProperties(KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup()))
      .withTopic(config.topic())
      .withDeadLetterTopic(config.topic() + ".dlq")
      .withPipeline(
        processorRegistry
          .pipeline(JsonFormat.INSTANCE)
          .add(RegistryKey.json("addSource"), RegistryKey.json("markProcessed"), RegistryKey.json("addTimestamp"))
          .toSink(consoleSinkKey)
          .build()
      )
      .withCommandQueue(commandQueue)
      .withOffsetManagerProvider((c) ->
        KafkaOffsetManager.builder(c).withCommandQueue(commandQueue).withCommitInterval(Duration.ofSeconds(30)).build()
      )
      .withMetricsInterval(config.metricsInterval())
      .withShutdownHook(true)
      .build();
  }

  public void start() {
    consumer.start();
  }

  public boolean awaitShutdown() {
    return consumer.awaitShutdown();
  }

  public void close() {
    consumer.close();
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

- Java 25 or newer
- Gradle (for building from source)
- [kcat](https://github.com/edenhill/kcat) for ad-hoc testing
- Docker for local Kafka via Testcontainers

---

## Testing

There's a `docker-compose.yaml` for spinning up Kafka, Zookeeper, and Confluent Schema Registry locally.

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

## Advanced patterns

### Enum-based registry

For statically typed bulk registration, define operators as an enum implementing `UnaryOperator<T>`:

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

### Conditional processing

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

### Filtering messages

Return `null` from an operator to skip a record. KPipe stops processing it — no downstream operators, no sink. The
offset is still committed (the record is treated as intentionally filtered, not failed).

```java
registry.registerOperator(RegistryKey.json("filter"), map -> {
  if ("internal".equals(map.get("type"))) return null; // Skip this message
  return map;
});
```

### Thread safety and resource management

- Processors should be stateless and thread-safe.
- Don't add `ThreadLocal` of your own — under thread-per-record it gets a new map for every message and doesn't cache
  anything. If you need thread-local-like state in a future feature (tenant context, span propagation), reach for
  `ScopedValue` instead.
- Side-effectful processors (DB calls, HTTP) need to be safe under high concurrency. With virtual threads you can easily
  have thousands in flight; pool sizing on connection pools matters more than thread count.

---

## Performance

A few specifics worth calling out, since "fast" without context is meaningless. Performance always depends on workload
shape (I/O vs CPU bound), partitioning, and message size — these are micro-level optimizations.

- Zero-copy magic-byte handling. For Avro from Confluent Schema Registry, KPipe takes an `offset` parameter to skip
  magic bytes and schema IDs without `Arrays.copyOfRange`.
- fastjson2 for JSON parsing. Faster than Jackson on the hot path with similar GC pressure, and actively maintained.

The latest parallel benchmark in [`benchmarks/README.md`](benchmarks/README.md) shows KPipe ahead of Confluent Parallel
Consumer on throughput, with a higher allocation footprint. Scenario-specific — not a blanket claim.

---

## Key-level ordering (payments, balances, anything sequential)

If your domain requires per-entity ordering — Authorize before Capture, balance updates in sequence — Kafka already
guarantees per-partition ordering. Use that:

- Have your producer key by the entity (`transaction_id`, `customer_id`, etc). Kafka routes all messages with the same
  key to the same partition.
- KPipe commits per-partition lowest-pending-offset, so as long as related events share a key, they land on one
  partition and KPipe processes them in order without skipping.
- For strict per-partition serial processing (one record at a time), set
  `.withProcessingMode(ProcessingMode.SEQUENTIAL)`.
- For per-key serial processing where different keys run in parallel, set
  `.withProcessingMode(ProcessingMode.KEY_ORDERED)`. This is the production sweet spot — entity-level ordering with
  cross-entity parallelism.

---

## Observability

Two OTel pieces ship in `kpipe-metrics-otel`:

- **`OtelConsumerMetrics`** — implements `ConsumerMetrics`. Wire on the consumer builder / `Stream.withMetrics(...)` and
  get the standard `kpipe.consumer.*` counters and histograms (received, processed, errors, processing duration,
  in-flight gauge, backpressure pauses, circuit-breaker state changes).
- **`PipelineMetricsObserver`** — implements `Consumer<Result<?>>`. Hand to `Stream.peekResult(observer)` and every
  `Passed` / `Filtered` / `Failed` outcome increments the matching `kpipe.pipeline.*` counter. This is how you make the
  §12 "processed counter rises but sink stays at 0" condition visible at the metrics layer instead of having to scrape
  logs.

```java
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.github.eschizoid.kpipe.metrics.otel.OtelConsumerMetrics;
import io.github.eschizoid.kpipe.metrics.otel.PipelineMetricsObserver;

final var otel = GlobalOpenTelemetry.get();
final var consumerMetrics = new OtelConsumerMetrics(otel, "orders-consumer");
final var observer = new PipelineMetricsObserver(otel, "orders");

try (var handle = KPipe.json("orders", kafkaProps)
    .pipe(enrich)
    .withMetrics(consumerMetrics)             // standard kpipe.consumer.* metrics
    .peekResult(observer)                     // per-Result-variant counters
    .toCustom(WarehouseSink.create())
    .start()) {
  handle.awaitShutdown();
}
```

If you're using Confluent SR auto-lookup, bind the cache counters too so cache hit rate is visible alongside the
pipeline outcomes:

```java
final var resolver = new CachedSchemaResolver(new ConfluentSchemaResolver("http://schema-registry:8081"));

final var observer = new PipelineMetricsObserver(otel, "orders").bindSchemaRegistryCache(
  resolver::hitCount,
  resolver::missCount,
  () -> (long) resolver.size()
);
```

The observer takes `LongSupplier`s rather than the resolver itself so `kpipe-metrics-otel` doesn't acquire a transitive
dependency on `kpipe-schema-registry-confluent`. Wire whichever cache you actually use; the suppliers don't care.

---

## Metrics dashboard

There's a local observability stack under `infra/observability/` that runs via Docker Compose:

- OpenTelemetry Collector — receives OTLP metrics from KPipe, exports to Prometheus.
- Prometheus — scrapes the collector and stores time-series data.
- Grafana — pre-provisioned with a "KPipe Overview" dashboard covering all consumer and producer metrics.

Any of the example apps brings this stack up via `docker compose`. To point your own collector at a running KPipe app,
set the `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://your-collector:4318
OTEL_METRICS_EXPORTER=otlp
```

Open Grafana at [http://localhost:3000](http://localhost:3000) (admin/admin) for the KPipe Overview dashboard.

---

## Running the demo

`examples/demo` runs JSON, Avro, and Protobuf pipelines side-by-side in one app with observability wired up.

```bash
# Brings up Kafka, Schema Registry, OTel Collector, Prometheus, Grafana, the demo app, and seeds data
./scripts/run-demo.sh

# Or, by hand:
cd examples/demo
docker compose up --build
```

The script starts Kafka (KRaft mode) + Schema Registry, creates topics, registers the Avro and Protobuf schemas, brings
up the observability stack, then builds and starts the demo app with all three pipelines and seeds JSON messages so
there's something to process immediately.

Grafana is at [http://localhost:3000](http://localhost:3000); the app health endpoint is at
[http://localhost:8080/health](http://localhost:8080/health).

---

## Inspiration

KPipe leans on:

- [Project Loom](https://openjdk.org/projects/loom/) for virtual threads.
- [fastjson2](https://github.com/alibaba/fastjson2) for the JSON hot path.

---

## Contributing

Custom processors, metrics hooks, retry strategies — PRs welcome.

---

## License

Apache 2.0. See [LICENSE](LICENSE).
