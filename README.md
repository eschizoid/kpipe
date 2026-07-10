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

## The numbers, up front

Two claims, both backed in this repo.

**1. The fastest at-least-once runtime we measured, and the gap widens where real work happens.** The
[`benchmarks/`](benchmarks/) module runs KPipe head-to-head against Confluent Parallel Consumer, Reactor Kafka, the
KIP-932 share consumer, and a raw `KafkaConsumer` + virtual-threads loop — same broker, same workload, simulated
per-record work. The headline regime is 10–100ms per record, the range where consumers doing real DB or HTTP hops live:

| Runtime (delivery guarantee)                          |   10ms / record | 100ms / record   |
| ----------------------------------------------------- | --------------: | :--------------- |
| **KPipe `PARALLEL`** (at-least-once)                  | 94.8k rec/s ±2% | 46.8k ±1%        |
| **KPipe `KEY_ORDERED`** (at-least-once, per-key FIFO) |       41.0k ±1% | 5.4k             |
| Confluent PC `UNORDERED` (at-least-once)              |        9.6k ±1% | 996 ±0.1%        |
| Confluent PC `KEY` (at-least-once, per-key FIFO)      |            9.7k | 996              |
| Kafka share consumer, KIP-932 (at-least-once)         |            4.4k | 2.8k             |
| Reactor Kafka (at-least-once)                         |             397 | — (not captured) |

That is **~10× Confluent PC at 10ms and ~47× at 100ms**. CPC's numbers sit exactly at its architectural ceiling: 100
workers divided by per-record work-time predicts 10,000 rec/s at 10ms and 1,000 at 100ms; the capture measured 9,639
and 996. Platform pools saturate; virtual-thread-per-record doesn't. That's a threading-model verdict, not a tuning gap.

The sub-millisecond regime tells the same story with smaller margins: at fork=5, KPipe `KEY_ORDERED` does 93.8k rec/s
(±0.8%) against Confluent PC `KEY`'s 65.9k (±1%) at 1ms of per-record work — the same per-key-FIFO guarantee at 1.42×
the throughput, with cleanly separated error bars. Full tables, error bars, and every caveat:
[`benchmarks/results/2026-07-09.md`](benchmarks/results/2026-07-09.md), captured per the
[methodology](benchmarks/METHODOLOGY.md).

The only faster arm in the capture is the hand-rolled `KafkaConsumer` + virtual-threads loop (525k at 10ms) — and it is
unsafe: no honest offset commit, records lost on rebalance. KPipe is what that loop becomes once you make it
at-least-once. One more disclosure that is also part of the pitch: Confluent Parallel Consumer 0.5.3.3 is the last
release before the project was officially retired in 2026-05, and its successor fork has not yet published an artifact.

The honest tradeoff: KPipe allocates the most per record of any arm measured (1,628 B/op vs Confluent PC's 33 B/op).
Roughly 62% of that is the fresh virtual thread per record — the same property that buys the throughput lead under
blocking work. The attribution profile is in
[`benchmarks/results/2026-06-21-allocation-attribution.md`](benchmarks/results/2026-06-21-allocation-attribution.md).
Not free, and not hidden.

Caveats that carry: everything runs on a shared GitHub-hosted CI runner, so read the orderings and the error bars, not
the absolute magnitudes. KPipe `PARALLEL`'s sub-millisecond cells are single-sample point estimates (a harness bug,
documented in the snapshot); its 10–100ms numbers in the table above are fully sampled.

**2. The at-least-once claim is verified, not asserted.** Every CI run gates on 16
[jcstress](https://openjdk.org/projects/code-tools/jcstress/) concurrency-stress tests across 4 modules, plus jqwik
property-based suites over the offset lifecycle and chaos-rebalance + crash-restart Testcontainers E2E tests against a
real broker. Building that suite found and fixed three real data-loss and correctness bugs before any user hit them (an
offset-tracking race, silent loss on DLQ send failure, a circuit breaker blind to the batch path). None of the runtimes
we benchmark against state — or test — their delivery guarantee this way.

## Two API surfaces

There are two public entry points; pick whichever matches the shape of your problem:

| Surface                                                | What it gives you                                                                                                                                                                   | When to use                                                                    |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| **`KPipe` fluent facade** (`kpipe-api`)                | 5-line `KPipe.json("topic", props).pipe(...).toConsole().start()`. Returns a `Stream<T>` → `Sink<T>` → `Handle` chain. Immutable, IDE-discoverable.                                 | The common path — most users start here.                                       |
| **Registry + Builder explicit API** (`kpipe-consumer`) | `MessageProcessorRegistry` + `KPipeConsumer.Builder`. Multi-step, supports custom registries, shared pipelines, custom offset managers, periodic metrics reporting via the builder. | Custom offset managers, multi-pipeline-per-consumer, advanced lifecycle hooks. |

The facade is a thin layer on top of the explicit API, so dropping down when you outgrow it doesn't cost anything. See
[`docs/ESCAPE-HATCHES.md`](docs/ESCAPE-HATCHES.md) for the full capability map and worked examples of the explicit-only
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

`kpipe-api` transitively pulls `kpipe-consumer` + `kpipe-producer` + `kpipe-core`. Skip `kpipe-api` only if you want the
explicit registry / builder API ([Two API surfaces](#two-api-surfaces)) — for that case, depend on `kpipe-consumer`
directly. There's also a `kpipe-bom` so you only pin one version across modules — use it via `dependencyManagement`
(Maven) or `enforcedPlatform` (Gradle) and drop versions from the individual `kpipe-*` dependencies. Maven and BOM
snippets are in the catalog below.

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

Gradle (Groovy) and SBT users: the same coordinates apply — `io.github.eschizoid:kpipe-*:1.15.0`.

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
and a custom sink instead of the console. Everything below is one-liner toggles on top of the "Hello, KPipe" chain
above.

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
form keeps the topic and producer from drifting out of sync. See [`docs/ESCAPE-HATCHES.md`](docs/ESCAPE-HATCHES.md) for
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
| `.withSchemaRegistry(SchemaResolver r)`                            | Confluent SR per-record schema auto-lookup (Avro only; don't pair with `.skipBytes`)         |
| `.withRetry(int max, Duration backoff)`                            | configure retry behavior                                                                     |
| `.withBackpressure()` / `.withBackpressure(high, low)`             | enable backpressure with default or explicit watermarks                                      |
| `.withProcessingMode(ProcessingMode)`                              | `SEQUENTIAL` (per-partition serial), `PARALLEL` (default), or `KEY_ORDERED` (per-key serial) |
| `.withKeyOrderedMaxKeys(int)`                                      | LRU cap on distinct keys in `KEY_ORDERED` (default 10,000)                                   |
| `.withCircuitBreaker(double threshold, int window, Duration open)` | open the circuit when sink failure rate trips (see [Circuit breaker](#9-circuit-breaker))    |
| `.withTracer(Tracer t)`                                            | propagate W3C trace context through Kafka headers                                            |
| `.withDeadLetterTopic(String)`                                     | route failed records to a DLQ topic after retries are exhausted                              |
| `.withErrorHandler(Consumer<ProcessingError<byte[]>>)`             | custom failure handler (DB, alerting, anything not a Kafka topic)                            |
| `.withMetrics(ConsumerMetrics m)`                                  | wire OTel or custom metrics (default `ConsumerMetrics.noop()`)                               |
| `.withPollTimeout(Duration d)`                                     | override Kafka poll timeout (default 100ms)                                                  |
| `.onFiltered(Runnable observer)`                                   | observe intentional filtering (an operator returned null) — visibility only                  |
| `.onFailed(Consumer<Throwable> observer)`                          | observe pipeline failures — doesn't suppress retry / DLQ / error-handler flow                |
| `.peekResult(Consumer<Result<T>> observer)`                        | observe every `Passed` / `Filtered` / `Failed` outcome; feeds `PipelineMetricsObserver`      |
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
windowing, no joins, no state stores. KPipe sits between raw `KafkaConsumer` code and full streaming frameworks:

| Capability                       | Kafka Streams | Spring Kafka            | Reactor Kafka | KPipe |
| -------------------------------- | ------------- | ----------------------- | ------------- | ----- |
| Full stream processing framework | Yes           | No                      | No            | No    |
| Lightweight consumer pipelines   | Partial       | Yes                     | Yes           | Yes   |
| Virtual-thread friendly          | No            | Listener-container only | No            | Yes   |
| Functional pipeline API          | Yes           | No (annotations)        | Yes           | Yes   |
| Minimal runtime dependencies     | No            | No (Spring Boot)        | Yes           | Yes   |
| JPMS modules (no Spring context) | No            | No                      | Partial       | Yes   |
| Native multi-topic dispatch      | Yes (DSL)     | Per `@KafkaListener`    | Manual        | Yes   |

### Coming from Spring Kafka?

Spring Kafka covers the same niche — "a Kafka consumer with retries, DLQ, backpressure, and a typed payload" — but
bundles Spring Boot, the application context, and annotation-driven wiring. KPipe targets the same workload with no
Spring runtime and a fluent API that's discoverable via IDE autocomplete. What changes:

- No Spring Boot on the classpath. KPipe runs on plain `java -jar` — JPMS modules, no reflection, no AOP, no application
  context to manage.
- Virtual threads by default. Spring Kafka's listener container is still thread-pool-per-partition; KPipe runs
  thread-per-record, which is where I/O-bound enrichment scales without pool tuning.
- No `@KafkaListener` indirection. Pipelines are values you build in `main` and pass around — no classpath scanning, no
  proxied beans. `KPipe.multi(props).json("a", ...).avro("b", ...)` replaces one listener class per topic.
- Failures are typed, not swallowed. Pipeline outcomes are a sealed `Result` (`Passed` / `Filtered` / `Failed`), so
  filtering and failure are distinct types — harder to misconfigure into silent drops than an
  `ErrorHandlingDeserializer` + `RetryableTopic` chain.

A `@KafkaListener` method that stamps a timestamp and hands off to a sink becomes:

```java
KPipe.json("events", kafkaProps)
    .pipe(msg -> { msg.put("ts", System.currentTimeMillis()); return msg; })
    .toCustom(sink::accept)
    .start();
```

Retry, DLQ, backpressure, and graceful shutdown are fluent calls on the same `Stream<T>`. No `RetryTemplate`, no
`DeadLetterPublishingRecoverer`, no `ContainerProperties.AckMode` to pick. If you depend on Spring Boot actuator
lifecycle integration, transactional producers wired into `@Transactional`, or the broader Spring ecosystem, stay where
you are — KPipe doesn't have those.

---

## Architecture and reliability

KPipe leans hard on Java 25 features — virtual threads, sealed types, records, JPMS — to keep the runtime predictable.

### 1. Modular architecture (JPMS)

KPipe ships focused JPMS modules with a clean dependency direction (no cycles, no sideways leaks). The per-module
breakdown is in the module catalog under [Getting started](#1-add-the-dependencies).

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

The win is fewer allocations and less CPU spent on intermediate SerDe steps. Two hot-path specifics: magic-byte handling
is zero-copy (`skipBytes` passes an offset instead of `Arrays.copyOfRange`), and JSON parsing uses
[fastjson2](https://github.com/alibaba/fastjson2) — faster than Jackson on the hot path with similar GC pressure.
Benchmark numbers are in [`benchmarks/README.md`](benchmarks/README.md).

### 3. Virtual threads

KPipe runs on Java virtual threads ([Project Loom](https://openjdk.org/projects/loom/)) for concurrency.

- Thread-per-record. Each message gets its own virtual thread, so I/O-bound enrichment scales without explicit pool
  sizing. Side-effectful operators (DB calls, HTTP) can easily have thousands in flight — connection-pool sizing matters
  more than thread count.
- No `ThreadLocal` on the hot path. `ThreadLocal` doesn't compose with thread-per-record — every record would get a
  fresh map, defeating any caching intent. If a future need for thread-local-like state turns up (tenant context, span
  propagation), we'll reach for `ScopedValue`; the codebase currently has neither.

### 4. At-least-once delivery via lowest pending offset

KPipe never commits past an in-flight record. The implementation:

- `OffsetManager` is an interface — Kafka-backed by default, but you can plug in external storage.
- Every in-flight offset is tracked in a sorted primitive-`long` pending set per partition (see `KafkaOffsetManager` /
  `PendingOffsetSet` — allocation-free on the track/mark hot path).
- Offset 102 cannot be committed until 101 finishes, even if 102 completes first. No gaps.
- On crash, the consumer resumes from the last committed offset. Some records may be reprocessed — standard
  at-least-once behavior — but nothing is skipped.

The checkable contract the correctness harness verifies against is written down in
[`docs/OFFSET-INVARIANTS.md`](docs/OFFSET-INVARIANTS.md).

### 5. Processing modes

Three modes via `.withProcessingMode(ProcessingMode)`:

- `PARALLEL` (default). Stateless transformations like enrichment or masking. Virtual thread per record, no ordering.
  Offsets commit by lowest pending offset.
- `SEQUENTIAL`. Stateful transformations where strict per-partition order matters. One message per partition at a time.
  Backpressure switches to monitoring consumer lag — the gap between partition end-offset and consumer position — since
  in-flight count is always ≤ 1.
- `KEY_ORDERED`. Records sharing a key process serially on a per-key virtual thread; different keys process in parallel.
  The production sweet spot for stateful workloads where order matters per entity (per user, per order, per session) but
  entities are independent. LRU cap on active keys defaults to 10,000; override with `.withKeyOrderedMaxKeys(int)`.
  Records with `null` keys all serialize through a single sentinel queue. When the cap saturates with every queue
  non-empty, dispatch stalls the consumer thread until a queue drains — implicit backpressure — and a one-shot `WARNING`
  log fires the first time to hint at raising the cap. For diagnostics under high cardinality,
  `Handle.topKeyQueueDepths(int n)` returns a snapshot of the deepest per-key queues.

For per-entity ordering (Authorize before Capture, balance updates in sequence), have your producer key by the entity
(`transaction_id`, `customer_id`, ...) so Kafka routes related events to one partition, then pick `KEY_ORDERED` for
entity-level ordering with cross-entity parallelism, or `SEQUENTIAL` for strict per-partition serial processing.

### 6. External offset management

Kafka-backed offset storage is the default. For external coordination — offsets in Postgres, Redis, or anywhere else —
implement the `OffsetManager` interface: persist offsets in `markOffsetProcessed`, and seek to the stored offset in the
rebalance listener returned by `createRebalanceListener()`. Wire it with `KPipeConsumer.Builder.withOffsetManager(...)`
(or `withOffsetManagerProvider(...)` when the manager needs the live Kafka consumer). A worked `PostgresOffsetManager`
example lives in [`docs/ESCAPE-HATCHES.md`](docs/ESCAPE-HATCHES.md).

### 7. Error handling and retries

Error handling is layered:

- Retries: `.withRetry(maxRetries, backoff)` for transient failures.
- Dead-letter topic: `.withDeadLetterTopic("events-dlq")` — records land there after retries are exhausted.
- Custom error handler: `.withErrorHandler(...)` to route failures somewhere other than a Kafka topic — an external DB,
  alerting system, whatever.
- Per-processor and per-sink wrapping: `MessageProcessorRegistry.withOperatorErrorHandling()` and
  `MessageProcessorRegistry.withSinkErrorHandling()` swallow failures at the boundary so one bad processor doesn't take
  down the pipeline.

### 8. Backpressure

When a downstream sink (database, HTTP API, another Kafka topic) is slow, KPipe pauses Kafka polling instead of letting
in-flight work or lag pile up unbounded. Two watermarks with hysteresis prevent oscillation: pause when the monitored
metric crosses the high watermark, resume when it drops back to or below the low one. Defaults are 10,000 high / 7,000
low; override with `.withBackpressure(high, low)`.

The monitored metric follows the processing mode:

| Mode                       | Strategy     | Monitored                  | Why                                                |
| :------------------------- | :----------- | :------------------------- | :------------------------------------------------- |
| `PARALLEL` / `KEY_ORDERED` | In-flight    | Active virtual threads     | Cap concurrent in-flight work to bound memory      |
| `SEQUENTIAL`               | Consumer lag | `Σ (endOffset - position)` | In-flight is always ≤ 1, so lag is the real signal |

Pauses log at WARNING, resumes at INFO. Two dedicated metrics track them: `backpressurePauseCount` and
`backpressureTimeMs`.

### 9. Circuit breaker

Backpressure caps in-flight work and prevents memory blow-ups, but it doesn't stop the bleeding when a downstream sink
is _failing_ (DB connection refused, HTTP 503). Without a circuit breaker, every record during an outage runs
`maxRetries + 1` attempts and lands in the DLQ. The breaker stops the cascade by pausing polling once the sink failure
rate crosses a threshold, then probes recovery with a single record after a cool-down window.

```java
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
keeps the consumer paused, and vice-versa. Three OTel counters (trips, state changes, time in OPEN) ship via
`kpipe-metrics-otel` — see the instrument table under [Observability](#observability). When `.withCircuitBreaker(...)`
is omitted, the consumer never trips and the counters stay at zero.

### 10. Distributed tracing (W3C trace context)

KPipe propagates W3C trace context (`traceparent` / `tracestate` Kafka headers) end-to-end: extract on consume, inject
on produce and on DLQ writes. The implementation lives in the opt-in `kpipe-tracing-otel` module — `kpipe-core` and
`kpipe-consumer` are dependency-free, you bring the OTel SDK at runtime.

```kotlin
implementation("io.github.eschizoid:kpipe-tracing-otel:1.15.0")
```

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

On the hot path: the upstream context is extracted from headers on poll, a CONSUMER span with
`messaging.kafka.{topic,partition,offset}` attributes wraps processing (closed in a nested `finally` so a throwing user
callback can't leak the scope), and the current context is injected into outbound headers on produce and DLQ writes.
Without `.withTracer(...)`, `Tracer.noop()` is used: zero allocation, no OTel API on the classpath.

### 11. Graceful shutdown and interrupts

KPipe respects JVM signals without losing records. Interrupts trigger a coordinated shutdown; they don't cause records
to be skipped. If a record's processing is interrupted mid-flight (during retry backoff or transformation), its offset
is not marked as processed — the next consumer instance picks it up, so at-least-once still holds during shutdown. The
drain and shutdown-hook API is covered under [Consumer lifecycle](#consumer-lifecycle).

---

## Working with messages

Pipelines deserialize once, transform many times, serialize once. Operators are `UnaryOperator<T>` where `T` is the
format's typed payload — `Map<String, Object>` for JSON, `GenericRecord` for Avro, `Message` for Protobuf. The
[`Operators`](lib/kpipe-core/src/main/java/io/github/eschizoid/kpipe/registry/Operators.java) helpers listed under
[Common operator patterns](#5-common-operator-patterns) cover the usual cases; for anything else, inline lambdas using
the format's native API. The examples below use the explicit registry API.

### JSON

Add `kpipe-format-json`. Operators are `UnaryOperator<Map<String, Object>>`:

```java
import static io.github.eschizoid.kpipe.registry.Operators.*;

import io.github.eschizoid.kpipe.format.json.JsonFormat;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.RegistryKey;

final var registry = new MessageProcessorRegistry();

final var sanitizeKey = RegistryKey.json("sanitize");
registry.registerOperator(sanitizeKey, removeFields("password", "ssn"));

// Inline lambdas work too. Returning null from an operator filters the record: downstream
// sinks are skipped but the offset is still marked processed (see "Filtering messages" below).
final var lowerEmailKey = RegistryKey.json("lowerEmail");
registry.registerOperator(lowerEmailKey, msg -> {
  if (msg.get("email") instanceof String s) msg.put("email", s.toLowerCase());
  return msg;
});

// Single deserialization → many transformations → single serialization
final var pipeline = registry.pipeline(JsonFormat.INSTANCE)
    .add(sanitizeKey)
    .add(lowerEmailKey)
    .build();
```

### Avro

Add `kpipe-format-avro`. Operators are `UnaryOperator<GenericRecord>`:

```java
import io.github.eschizoid.kpipe.format.avro.AvroFormat;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.RegistryKey;
import org.apache.avro.generic.GenericRecord;

// Build an AvroFormat bound to a single schema. Use new AvroFormat(schema) when you already have a
// parsed Schema, or AvroFormat.of(schemaJson) for inline JSON.
final var format = AvroFormat.of("""
  {"type":"record","name":"User","namespace":"com.kpipe","fields":[
    {"name":"id","type":"string"},{"name":"name","type":"string"}
  ]}""");

final var registry = new MessageProcessorRegistry();

// Avro records are schema-bound: use inline lambdas with the native Avro API for value transforms.
// `Operators.filter`, `peek`, `compose` etc. work for any payload type, including GenericRecord.
final var lowerNameKey = RegistryKey.of("lowerName", GenericRecord.class);
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

```kotlin
implementation("io.github.eschizoid:kpipe-schema-registry-confluent:1.15.0")
```

```java
import java.time.Duration;
import io.github.eschizoid.kpipe.KPipe;
import io.github.eschizoid.kpipe.schemaregistry.confluent.CachedSchemaResolver;
import io.github.eschizoid.kpipe.schemaregistry.confluent.ConfluentSchemaResolver;

// One resolver for the process; CachedSchemaResolver caches by ID with no TTL — Confluent SR IDs
// are immutable, so cache-by-ID is trivially correct and cardinality stays naturally bounded.
try (final var resolver = new CachedSchemaResolver(
        new ConfluentSchemaResolver("http://schema-registry:8081", Duration.ofSeconds(10)));
     final var handle = KPipe.avro("orders", kafkaProps, resolver)   // ← one-line SR consumer
         .pipe(record -> enrich(record))
         .toCustom(WarehouseSink.create())
         .start()) {
  handle.awaitShutdown();
}
```

The factory above is equivalent to `KPipe.avro("orders", props, AvroFormat.withRegistry(resolver))`; the fluent
`.withSchemaRegistry(resolver)` setter does the same on an existing Avro stream. Do **not** combine either with
`.skipBytes(5)` — the format already consumes the envelope. Only the first record carrying a new schema ID costs an HTTP
round trip; cache hit / miss / size counters are exposed on the resolver and can be bound to OTel (see
[Observability](#observability)).

The static-mode path is still supported for shops with strict append-only evolution who fetch the schema once at
startup: `resolver.lookupBySubjectVersion("orders-value", "latest")` → `AvroFormat.of(schemaJson)`, paired with
`.skipBytes(5)`. `lookupById(int)` is also available for managing your own caching.

**Scope.** Avro only for now. Protobuf SR auto-lookup needs runtime `.proto` text compilation and has not shipped — use
`kpipe-format-protobuf` with a compiled descriptor and `skipBytes(6)` for the single-top-level-message case. No schema
publishing (Confluent's own producer client handles that). No compatibility checks at the consumer — those run at
registration time inside SR.

### Protobuf

Add `kpipe-format-protobuf`. Operators are `UnaryOperator<Message>`. Protobuf messages are immutable, so every transform
builds a new message via `toBuilder().setField(...).build()`.

```java
import com.google.protobuf.Message;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.RegistryKey;

// Build a ProtobufFormat bound to a single descriptor.
final var format = new ProtobufFormat(CustomerProto.Customer.getDescriptor());
final var registry = new MessageProcessorRegistry();

final var clearEmailKey = RegistryKey.of("clearEmail", Message.class);
registry.registerOperator(clearEmailKey, msg -> {
  final var emailField = msg.getDescriptorForType().findFieldByName("email");
  return msg.toBuilder().clearField(emailField).build();
});

// Register the protobuf console sink yourself (defaults are no longer auto-registered)
final var protoLoggingKey = RegistryKey.of("protobufLogging", Message.class);
registry.registerSink(protoLoggingKey, new io.github.eschizoid.kpipe.format.protobuf.ProtobufConsoleSink<>());

final var pipeline = registry
  .pipeline(format)
  .add(clearEmailKey)
  .toSink(protoLoggingKey)
  .build();
```

---

## Message sinks

Sinks are where processed messages go. `MessageSink` is just a functional `Consumer<T>` — any lambda works:

```java
final MessageSink<Map<String, Object>> databaseSink = (processedMap) -> {
  databaseService.insert(processedMap);
};
```

### Registering sinks

`MessageProcessorRegistry` holds operators and sinks in two namespaces under the same key shape.
`registerOperator(key, op)` and `registerSink(key, sink)` are the entry points; lookups use `getOperator(key)` and
`getSink(key)`. Per-namespace utilities (`getAllSinks`, `getSinkMetrics`, `unregisterSink`, `clearSinks`,
`compositeSink`) keep the surfaces separate. Console sinks (`JsonConsoleSink`, `AvroConsoleSink`, `ProtobufConsoleSink`)
live in their format modules and are not auto-registered — register the ones you want:

```java
final var registry = new MessageProcessorRegistry();

// Register sinks under typed keys
registry.registerSink(RegistryKey.json("jsonConsole"), new JsonConsoleSink<Map<String, Object>>());
registry.registerSink(RegistryKey.of("database", Map.class), databaseSink);

// Use one in a pipeline
final var pipeline = registry
  .pipeline(JsonFormat.INSTANCE)
  .add(RegistryKey.json("sanitize"))
  .toSink(RegistryKey.of("database", Map.class))
  .build();

// Wrap a sink or operator with error handling (suppresses exceptions, logs errors)
final var safeSink = MessageProcessorRegistry.withSinkErrorHandling(riskySink);
final var safeOperator = MessageProcessorRegistry.withOperatorErrorHandling(riskyOperator);
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

## Observability

### Programmatic access

```java
final var metrics = consumer.getMetrics();   // or handle.metrics() on the fluent path
log.log(Level.INFO, "Successfully processed: " + metrics.get("messagesProcessed"));
log.log(Level.INFO, "Messages in-flight: " + metrics.get("inFlight"));
// Also: messagesReceived, processingErrors, and — when withBackpressure() is configured —
// backpressurePauseCount and backpressureTimeMs.
```

### OpenTelemetry

OTel is opt-in via the `kpipe-metrics-otel` module. `kpipe-metrics` ships interfaces only and doesn't pull
`opentelemetry-api` onto your classpath. Add `kpipe-metrics-otel` (plus your OTel SDK at runtime) and wire
`.withMetrics(new OtelConsumerMetrics(openTelemetry, "my-pipeline"))` on the stream or builder. When `withMetrics(...)`
is omitted, `ConsumerMetrics.noop()` / `ProducerMetrics.noop()` is used: zero allocation, no OTel API on the classpath.

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

### Pipeline outcome counters

`PipelineMetricsObserver` (also in `kpipe-metrics-otel`) implements `Consumer<Result<?>>`. Hand it to
`Stream.peekResult(observer)` and every `Passed` / `Filtered` / `Failed` outcome increments the matching
`kpipe.pipeline.*` counter. This is how you make the "processed counter rises but the sink stays at 0" condition (a
silent flood of `Filtered` or `Failed` records) visible at the metrics layer instead of having to scrape logs.

```java
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.github.eschizoid.kpipe.metrics.otel.OtelConsumerMetrics;
import io.github.eschizoid.kpipe.metrics.otel.PipelineMetricsObserver;

final var otel = GlobalOpenTelemetry.get();

try (var handle = KPipe.json("orders", kafkaProps)
    .pipe(enrich)
    .withMetrics(new OtelConsumerMetrics(otel, "orders-consumer"))   // standard kpipe.consumer.* metrics
    .peekResult(new PipelineMetricsObserver(otel, "orders"))         // per-Result-variant counters
    .toCustom(WarehouseSink.create())
    .start()) {
  handle.awaitShutdown();
}
```

If you're using Confluent SR auto-lookup, bind the cache counters too so cache hit rate is visible alongside the
pipeline outcomes:

```java
final var observer = new PipelineMetricsObserver(otel, "orders").bindSchemaRegistryCache(
  resolver::hitCount,
  resolver::missCount,
  () -> (long) resolver.size()
);
```

The observer takes `LongSupplier`s rather than the resolver itself so `kpipe-metrics-otel` doesn't acquire a transitive
dependency on `kpipe-schema-registry-confluent`. Wire whichever cache you actually use; the suppliers don't care.

### Local dashboard

A local observability stack under `infra/observability/` runs via Docker Compose: OTel Collector → Prometheus → Grafana,
pre-provisioned with a "KPipe Overview" dashboard covering all consumer and producer metrics. Any of the example apps
brings it up; to point your own collector at a running KPipe app set
`OTEL_EXPORTER_OTLP_ENDPOINT=http://your-collector:4318` and `OTEL_METRICS_EXPORTER=otlp`. Grafana is at
[http://localhost:3000](http://localhost:3000) (admin/admin).

---

## Consumer lifecycle

The consumer hosts its own lifecycle — start, periodic metrics reporting, JVM shutdown hook, in-flight drain, graceful
shutdown. There is no separate runner class; everything is on `KPipeConsumer` directly.

```java
try (final var consumer = KPipeConsumer.<byte[]>builder()
    .withProperties(kafkaProps)
    .withTopic("events")
    .withPipeline(pipeline)
    .withMetricsReporters(List.of(
      ConsumerMetricsReporter.forConsumer(c -> consumer.getMetrics()),
      EntryMetricsReporter.forProcessors(processorRegistry)
    ))
    .withMetricsInterval(Duration.ofSeconds(30))
    .withShutdownHook(true)   // installs Runtime.getRuntime().addShutdownHook(consumer::close)
    .build()) {
  consumer.start();
  consumer.awaitShutdown(Duration.ofMinutes(5));   // bounded wait; no-arg blocks until close()
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

  private final Handle handle;

  static void main() {
    final var config = AppConfig.fromEnv();
    try (final var app = new KPipeApp(config)) {
      LOGGER.log(Level.INFO, "JSON consumer started for topic {0}", config.topic());
      app.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in application", e);
      System.exit(1);
    }
  }

  public KPipeApp(final AppConfig config) {
    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    handle = KPipe.json(config.topic(), props)
      .withDeadLetterTopic(config.topic() + ".dlq")
      .pipe(Operators.addField("source", "kpipe-app"))
      .pipe(Operators.addField("status", "processed"))
      .pipe(Operators.addField("processedAt", System.currentTimeMillis()))
      .toCustom(new JsonConsoleSink<>())
      .start();
  }

  public boolean awaitShutdown() {
    return handle.awaitShutdown();
  }

  @Override
  public void close() {
    handle.close();
  }
}
```

For a multi-format consumer (JSON + Avro + Protobuf on one consumer-group, dispatched per topic), swap `KPipe.json(...)`
for `KPipe.multi(props).json(...).avro(...).protobuf(...).start()` — see [`examples/demo`](examples/demo/) for the full
shape. `AppConfig.fromEnv()` reads `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_CONSUMER_GROUP`, `KAFKA_TOPIC`, plus optional vars
(`APP_NAME`, `KAFKA_POLL_TIMEOUT_MS`, `SHUTDOWN_TIMEOUT_SEC`, `METRICS_INTERVAL_SEC`, `PROCESSOR_PIPELINE`) that feed
the explicit Builder and metrics paths.

</details>

---

## Requirements

- Java 25 or newer
- Gradle (for building from source)
- [kcat](https://github.com/edenhill/kcat) for ad-hoc testing
- Docker for local Kafka via Testcontainers

---

## Testing

There's a `docker-compose.yaml` for spinning up Kafka (KRaft mode) and Confluent Schema Registry locally.

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
  TIMESTAMP(msg -> { msg.put("ts", System.currentTimeMillis()); return msg; }),
  SOURCE(Operators.addField("src", "app"));

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
there's something to process immediately. Grafana is at [http://localhost:3000](http://localhost:3000); the app health
endpoint is at [http://localhost:8080/health](http://localhost:8080/health).

---

## Contributing

Custom processors, metrics hooks, retry strategies — PRs welcome.

---

## License

Apache 2.0. See [LICENSE](LICENSE).
