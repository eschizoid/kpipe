<p align="center">
  <img src="img/kpipe.png" alt="kpipe — a Kafka consumer library for Java" width="320" />
</p>

# kpipe

[![JVM 25+](https://img.shields.io/badge/JVM-25%2B-brightgreen.svg?&logo=openjdk)](https://openjdk.org/projects/jdk/25/)
[![Build](https://github.com/eschizoid/kpipe/actions/workflows/ci.yaml/badge.svg)](https://github.com/eschizoid/kpipe/actions/workflows/ci.yaml)
[![Codecov](https://codecov.io/gh/eschizoid/kpipe/graph/badge.svg?token=X50GBU4X7J)](https://codecov.io/gh/eschizoid/kpipe)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.eschizoid/kpipe-api.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.eschizoid/kpipe-api)
[![Javadoc](https://javadoc.io/badge2/io.github.eschizoid/kpipe-api/javadoc.svg?color=purple)](https://javadoc.io/doc/io.github.eschizoid/kpipe-api)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Parallel Kafka consumption at full speed, without giving up at-least-once.**

KPipe is a Kafka consumer runtime for Java 25+. You declare a pipeline (deserialize, transform, sink) and KPipe runs it:
every record on its own virtual thread, and no commit ever passes a record that has not finished.

## The problem KPipe solves

A `Consumer.poll` loop is simple while it handles one record at a time. It is also slow, because each database write or
HTTP call waits for the one before it.

Running records in parallel fixes the speed and breaks the commits. A fast record finishes before a slow one that came
earlier. Commit the fast record's offset, crash, and the slow record is lost. Most services then grow their own offset
tracking, retries, dead-letter handling, backpressure and shutdown logic around the business code, and most of the bugs
live there.

KPipe is that surrounding code, written once and tested hard:

- **Parallel by default.** Each record runs on its own virtual thread, so I/O-bound work overlaps with no worker pool to
  size.
- **At-least-once, always.** An offset commits only after its record has finished: the sink completed, a filter dropped
  it, or it was parked in the dead-letter topic.
- **Ordering when you need it.** One setting switches between fully parallel, ordered per key, and strictly sequential.
- **Operations as configuration.** Retries, a dead-letter topic, backpressure, a circuit breaker, metrics and tracing
  are each one fluent call.
- **Testable without a broker.** Your real pipeline runs in a unit test in milliseconds.

Use KPipe for a consumer that transforms, enriches, validates, routes, or writes to a database, where the hard part is
coordinating parallel I/O with commits. Use Kafka Streams when you need joins, windows, state stores, or exactly-once
processing.

## Thirty-second example

A complete, runnable consumer. It is the same class as
[`ReadmeQuickstart.java`](examples/json/src/main/java/io/github/eschizoid/kpipe/ReadmeQuickstart.java) in the examples,
and a check keeps the two identical:

```java
package io.github.eschizoid.kpipe;

import java.util.Properties;

public final class ReadmeQuickstart {

  static void main() throws InterruptedException {
    final var props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "orders-service");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("enable.auto.commit", "false"); // KPipe manages offset commits itself
    props.put("auto.offset.reset", "earliest");

    try (final var handle = KPipe.json("orders", props)
        .pipe(order -> {
          order.put("processedAt", System.currentTimeMillis());
          return order;
        })
        .filter(order -> order.get("customerId") != null)
        .toConsole()
        .start()) {
      handle.awaitShutdown(); // blocks until close() or a JVM shutdown signal
    }
  }
}
```

`start()` returns at once with a `Handle`, and the consumer runs on its own thread. Closing the handle shuts down
gracefully: it drains in-flight records for up to 5 seconds, flushes batch buffers and commits. `awaitShutdown()` keeps
the process alive until then. KPipe installs no JVM shutdown hook unless you ask for one.

## Installation

```kotlin
implementation(platform("io.github.eschizoid:kpipe-bom:1.25.0"))
implementation("io.github.eschizoid:kpipe-api")
implementation("io.github.eschizoid:kpipe-format-json")   // or -avro / -protobuf; formats are opt-in
```

`kpipe-api` brings the consumer, producer, core and tracing modules with it, and its only external runtime dependency is
`kafka-clients`. Maven snippets, the module catalog and JPMS guidance are in [docs/MODULES.md](docs/MODULES.md). Minor
releases may remove public API through 1.x; [docs/VERSIONING.md](docs/VERSIONING.md) explains what is covered.

## What happens to each record

Every polled record takes the same path: **track offset → deserialize → operators → sink → mark processed**. The whole
path runs on one virtual thread, and the decoded value belongs to that thread for the attempt. Operators work on the
typed payload (`Map`, `GenericRecord`, `Message`) from a single decode, and KPipe never re-encodes between steps
([docs/FORMATS.md](docs/FORMATS.md)).

A record ends in exactly one of three outcomes, a sealed type:

- **Passed.** The sink received it.
- **Filtered.** A `filter` returned false or an operator returned null. This is deliberate: the sink is skipped and the
  offset still commits.
- **Failed.** An operator, the deserializer or the sink threw. After the configured retries the record goes to the error
  handler and, if configured, to the dead-letter topic. A `NonRetryableException` skips the retries, and so do bytes
  that cannot be decoded.

Because "skipped on purpose" and "broken" are different types, they cannot be confused in code or in metrics.

Commits happen every 30 seconds by default, always at the **commit frontier**: the lowest offset still in flight on each
partition. A fast record cannot commit past a slow one, and that is how at-least-once holds under parallel processing.
The full lifecycle, the failure matrix, and rebalance and shutdown behavior are in
[docs/GUARANTEES.md](docs/GUARANTEES.md).

Any operator or sink can read the record's key, headers, topic, offset and timestamp through `RecordContext.current()`.
`rekey` and `withHeader` change the key and headers, and `toKafka(topic)` writes each value with them.

## Choosing an ordering

| Mode                 | Concurrency                          | Ordering                                                        |
| -------------------- | ------------------------------------ | --------------------------------------------------------------- |
| `PARALLEL` (default) | One virtual thread per record        | None; side effects for consecutive offsets may interleave       |
| `KEY_ORDERED`        | Serial per key, parallel across keys | Offset order within each key; `null` keys share one queue       |
| `SEQUENTIAL`         | One record at a time, whole consumer | Poll order (per-partition offset order, partitions interleaved) |

Ordering needs help from your producer: records with the same key are ordered only if they land on the same partition.
For "authorize before capture, per account", key the producer by account and run `KEY_ORDERED`. To keep that order in an
output topic, end the stream with `toKafka`, which keeps each record's key unless a `rekey` changes it. Details are in
[docs/GUARANTEES.md](docs/GUARANTEES.md#ordering).

## Running it in production

The quickstart grows into a production consumer one call at a time:

```java
final var metrics = KPipeOtel.metrics(openTelemetry); // one per OpenTelemetry, shared

KPipe.json("orders", props)
  .withProcessingMode(ProcessingMode.KEY_ORDERED)
  .withRetry(3, Duration.ofMillis(200))
  .withDeadLetterTopic("orders.dlq")
  .withCircuitBreaker(0.5, 100, Duration.ofSeconds(30))
  .withTracer(KPipeOtel.tracer(openTelemetry))
  .withMetrics(metrics)
  .pipe(enrich)
  .toKafka("orders-enriched")
  .start();
```

**When a record fails**

- **Retries.** `withRetry(maxRetries, backoff)` retries with a fixed backoff. `maxRetries` counts the retries after the
  first attempt.
- **Dead-letter topic.** `withDeadLetterTopic("orders.dlq")` sends a record that failed for good there, with its
  original headers plus `x-dlq-*` headers (error, source topic, partition, offset, timestamp), so it can be replayed. If
  the dead-letter send itself fails, the offset stays uncommitted and the record is redelivered on restart. Nothing is
  dropped.
- **Circuit breaker.** `withCircuitBreaker(threshold, window, openDuration)` pauses consumption when too many records
  fail, then probes for recovery after a cool-down.

**When the consumer falls behind or stops**

- **Backpressure.** On by default. Consumption pauses at 10,000 records in flight and resumes at 7,000. A paused
  consumer keeps polling, so it keeps its place in the group.
- **Graceful shutdown.** Closing the handle drains in-flight records, flushes batches and commits. Interrupted records
  are not marked processed, so they are redelivered and not lost.

**Seeing what it does**

- **Metrics.** `withMetrics(...)` exports counters, lag and processing time through OpenTelemetry (opt-in `kpipe-otel`).
  `withMetricsLog(Duration)` writes one log line per interval, and `Handle.metrics()` returns a snapshot in code.
- **Tracing.** `withTracer(...)` gives each record one span that continues the trace in its headers. Every record
  written on its behalf, to Kafka or to the dead-letter topic, carries that trace.
- **Health.** `Handle.health()` reports whether the consumer is running, why it is paused, and the breaker's state.

Exact semantics are in [docs/API.md](docs/API.md) and [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md). Multi-topic
consumers (`KPipe.multi(...)`), batch sinks and fan-out are in [docs/API.md](docs/API.md) and
[docs/SINKS.md](docs/SINKS.md).

## Testing your pipeline

`kpipe-test` runs your pipeline through the real consumer over an in-memory `MockConsumer`. It needs no broker and no
Docker, and a test takes milliseconds. `flush()` returns only after every sent record has settled, so assertions see a
stable state:

```java
final var captured = new CapturingSink<Map<String, Object>>();
try (final var driver = TestStream.<Map<String, Object>>builder(JsonFormat.INSTANCE)
    .pipe(addTimestamp)          // placeholders: any UnaryOperator / Predicate
    .filter(active)
    .toCustom(captured)
    .build()) {
  driver.send(Map.of("id", "a", "active", true));
  driver.flush();
  assertEquals(1, captured.count());
}
```

When keys or headers matter, send a `TestRecord` and read each value back with the record its sink saw, after any
`rekey` or `withHeader`. Inside the same `try` block:

```java
driver.send(TestRecord.of(order).withKey("order-1").withHeader("tenant", "acme"));
driver.flush();
assertEquals("order-1", captured.records().getFirst().keyString());
```

A `CrashRestartHarness` answers the harder question, also without a broker: does the consumer reprocess the right
records after a crash? The kit lives in [`lib/kpipe-test`](lib/kpipe-test):

```kotlin
testImplementation("io.github.eschizoid:kpipe-test")
```

## Performance

The guarantees cost little. KPipe `PARALLEL` matches Atleon, the fastest alternative that keeps at-least-once, within
the error bands at every load, and runs 5 to 8 times faster than Confluent Parallel Consumer. Thousands of records per
second, all runtimes on the same machine:

| Runtime                                 | 0 µs work per record |   100 µs |     1 ms |
| --------------------------------------- | -------------------: | -------: | -------: |
| **KPipe `PARALLEL`**                    |             713 ± 59 | 516 ± 72 | 544 ± 38 |
| Atleon parallel                         |             656 ± 26 | 543 ± 19 | 522 ± 27 |
| **KPipe `KEY_ORDERED`**                 |             658 ± 38 | 503 ± 35 | 316 ± 12 |
| Atleon key-ordered                      |             778 ± 33 | 496 ± 20 | 335 ± 10 |
| Confluent Parallel Consumer             |              117 ± 2 |   96 ± 2 |   70 ± 1 |
| Confluent Parallel Consumer key-ordered |              106 ± 2 |   83 ± 2 |   62 ± 1 |
| Reactor Kafka                           |             863 ± 37 |  348 ± 8 |   85 ± 1 |
| Raw `KafkaConsumer` + virtual threads   |           1,001 ± 85 | 707 ± 55 | 685 ± 47 |

The raw loop and, with no work per record, Reactor Kafka run faster, and neither holds the guarantee. The raw loop has
no backpressure and no offset tracking; KPipe `PARALLEL` runs at 0.71 to 0.79 of it, and that gap is what the guarantees
cost. The Reactor Kafka run committed the latest acknowledged offset, so it did not hold at-least-once when records
finished out of order. With no work per record, KPipe `KEY_ORDERED` runs at 0.85 of Atleon's key-ordered mode.

The at-least-once claim is tested, not asserted. [Fray](https://github.com/cmu-pasta/fray) controlled-concurrency suites
and property tests explore thread interleavings over the offset lifecycle, and breaking an invariant on purpose makes
them fail ([docs/OFFSET-INVARIANTS.md](docs/OFFSET-INVARIANTS.md)). Latency, allocation, methodology and raw data are in
[`benchmarks/`](benchmarks).

## When to use something else

KPipe sits between a hand-rolled `KafkaConsumer` loop and a stream-processing framework. It is a consumer runtime for
transform, enrich and route services, not a stream processor. These projects evolve, so check the versions you run:

| Concern                                      | KPipe                                               | Spring Kafka                                                                                      | Kafka Streams                        | Reactor Kafka                   |
| -------------------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------ | ------------------------------- |
| Primary abstraction                          | Fluent record pipeline                              | Listener containers + templates                                                                   | Stateful stream topology             | Reactive `Flux` over Kafka      |
| Runtime model                                | Plain `java -jar`; no DI container. JPMS modules    | Usable without Spring Boot; designed around the Spring context, programmatic or annotation-driven | Library, no container                | Library, no container           |
| Record concurrency                           | Virtual thread per record; serial and per-key modes | Container concurrency (threads per container/partition); async and VT executors configurable      | Stream threads / tasks per partition | Reactive schedulers             |
| Ordering options                             | None / global serial / per-key                      | Per-partition via container model                                                                 | Per-partition (task model)           | Per-partition (reactive chains) |
| Offset tracking under parallelism            | Lowest-pending-offset frontier, built in            | Ack modes; out-of-order commit management is manual or container-managed                          | Managed by streams runtime           | Manual or auto-ack variants     |
| Retries + DLT                                | Built-in fixed-backoff retry + replayable DLQ       | Extensive (error handlers, `@RetryableTopic`, `DeadLetterPublishingRecoverer`, backoff policies)  | Application-level                    | Application-level               |
| Transactions / EOS                           | **Not supported**                                   | Supported (transactional templates, listeners)                                                    | Supported (EOS v2)                   | Sender transactions             |
| Stateful processing (joins, windows, stores) | **Not supported**                                   | Not built-in                                                                                      | Core feature                         | Not built-in                    |
| Ecosystem & maturity                         | Young, small API surface                            | Large ecosystem, long history                                                                     | Large ecosystem, long history        | Established in reactive stacks  |

Choose Spring Kafka when you live in the Spring ecosystem or need transactions, Kafka Streams when you need stateful
processing, and Reactor Kafka when your service is already reactive. KPipe fits the plain-JVM consumer service that does
I/O-bound work per record and wants parallelism, ordering and delivery guarantees without building them by hand.

KPipe is not a stream processor (no windows, joins or state stores), an exactly-once framework (no Kafka transactions),
a workflow engine, an event store, a schema registry (it is a registry client), or a reactive framework. It also cannot
make your side effects idempotent: at-least-once means your sink may see a record twice after a crash.

## Documentation

| Document                                               | Contents                                                                                                                                                                                    |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [docs/API.md](docs/API.md)                             | The fluent surface, grouped by type; multi-topic routing; built-in operators                                                                                                                |
| [docs/GUARANTEES.md](docs/GUARANTEES.md)               | Delivery guarantee boundary, failure matrix, ordering, backpressure                                                                                                                         |
| [docs/FORMATS.md](docs/FORMATS.md)                     | JSON / Avro / Protobuf, wire envelopes, Confluent Schema Registry modes                                                                                                                     |
| [docs/SINKS.md](docs/SINKS.md)                         | Custom, Kafka-producer, fan-out, and batch sinks                                                                                                                                            |
| [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)         | Metrics tables, the metrics log, tracing, local Grafana stack                                                                                                                               |
| [docs/MODULES.md](docs/MODULES.md)                     | Module catalog, BOM usage, JPMS                                                                                                                                                             |
| [docs/ESCAPE-HATCHES.md](docs/ESCAPE-HATCHES.md)       | The explicit builder API: custom offset managers, seams                                                                                                                                     |
| [docs/OFFSET-INVARIANTS.md](docs/OFFSET-INVARIANTS.md) | The machine-checked offset invariants                                                                                                                                                       |
| [docs/VERSIONING.md](docs/VERSIONING.md)               | What breaks in a minor, what counts as public API, and when the rule changes                                                                                                                |
| [docs/adr/](docs/adr)                                  | Architecture decision records — why the concurrency tooling is what it is                                                                                                                   |
| [benchmarks/](benchmarks)                              | Methodology, raw results, dated capture snapshots                                                                                                                                           |
| [examples/](examples)                                  | Runnable apps per format, a full demo with observability stack (`./scripts/run-demo.sh`), and offset stores in Postgres and DynamoDB, with multi-region failover (`examples/offset-stores`) |

## Requirements

- **Java 25+.** The runtime is built on virtual threads, scoped values, records and sealed types.
- **kafka-clients 4.x**, managed by the BOM.
- **Docker** only for the integration examples and benchmarks. The library and `kpipe-test` need none.

## Contributing

Issues and PRs are welcome. Each module holds its unit tests in `src/test`, and the modules that talk to Kafka also hold
Testcontainers integration tests there. The offset code has Fray suites in `src/frayTest`, and each app in `examples/`
has its own integration tests. Contributions are held to the same bar.

## License

Apache 2.0. See [LICENSE](LICENSE).
