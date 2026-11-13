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

KPipe is a plain-JVM Kafka consumer runtime for Java 25+. It runs every record on its own virtual thread and never
commits past a record that has not finished. In `PARALLEL` mode it is as fast as the fastest alternative that keeps that
guarantee, and 5 to 8 times faster than Confluent Parallel Consumer, behind a typed, fluent API you can test without a
broker. It replaces the hand-rolled `Consumer.poll` loop that most Kafka services grow around their business logic: you
declare a pipeline (deserialize, transform, sink) and KPipe runs it.

## Why KPipe

- **Fast.** KPipe `PARALLEL` runs within the error bands of Atleon, the fastest benchmarked alternative that keeps
  at-least-once, at every load we measured, and 5 to 8 times faster than Confluent Parallel Consumer. The numbers are in
  [Performance](#performance).
- **Correct, and proven.** An offset commits only after its record reaches a terminal state (sink completed, filtered,
  or parked in the dead-letter topic), and commits never pass a record still in flight. Every CI run checks this with
  [Fray](https://github.com/cmu-pasta/fray) controlled-concurrency suites and property tests over the offset lifecycle,
  which fail when an invariant is broken. The exact guarantee boundary and failure matrix:
  [docs/GUARANTEES.md](docs/GUARANTEES.md).
- **A virtual thread per record.** I/O-bound work (DB writes, HTTP calls) overlaps without a worker pool to configure.
  Backpressure still bounds the records in flight by pausing consumption past a watermark, and your downstream limits,
  such as connection pools and rate limits, still apply.
- **The whole record when you need it.** Any operator or sink reads the record's key, headers, topic, offset and
  timestamp through `RecordContext.current()`, and a batch sink through `RecordContext.currentBatch()`. `rekey` and
  `withHeader` change the key and headers, and `toKafka(topic)` produces each value with its key and headers, so in
  `KEY_ORDERED` and `SEQUENTIAL` mode records with the same consumed key reach the output topic in order.
- **One decode per attempt, typed outcomes.** Operators work on the typed payload (`Map`, `GenericRecord`, `Message`)
  from a single decode to the sink, and KPipe never re-encodes between steps ([docs/FORMATS.md](docs/FORMATS.md)). Every
  record ends as `Passed`, `Filtered` or `Failed`, a sealed type, so "intentionally skipped" and "broken" cannot be
  confused in code or in metrics.
- **Testable without a broker.** `kpipe-test` drives the real consumer over an in-memory `MockConsumer`, in milliseconds
  per test ([Testing your pipeline](#testing-your-pipeline)).
- **The operational stack as configuration.** Retries, a replayable dead-letter topic, backpressure, a circuit breaker,
  OpenTelemetry metrics and W3C trace propagation are each one fluent call.

Use it when you are building a transform, enrich, validate, route, or write-to-database consumer and the hard part is
coordinating parallel I/O with commits. Reach for Kafka Streams instead when you need joins, windows, state stores, or
exactly-once processing.

## Thirty-second example

A complete, runnable consumer — this exact class is compiled in CI as
[`examples/json/.../ReadmeQuickstart.java`](examples/json/src/main/java/io/github/eschizoid/kpipe/ReadmeQuickstart.java),
so it cannot drift from the API:

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

`start()` returns immediately with a `Handle`; the consumer runs on its own thread. Keep the handle: try-with-resources
gives you a bounded graceful shutdown (`close()` = drain in-flight work for up to 5 seconds, flush batch buffers, commit
synchronously), and `awaitShutdown()` keeps the process alive until you stop it. No JVM shutdown hook is installed
unless you opt in.

## Installation

```kotlin
implementation(platform("io.github.eschizoid:kpipe-bom:1.24.0"))
implementation("io.github.eschizoid:kpipe-api")
implementation("io.github.eschizoid:kpipe-format-json")   // or -avro / -protobuf — formats are opt-in
```

Minor releases may remove public API through 1.x — see [docs/VERSIONING.md](docs/VERSIONING.md) for what is covered, how
a break reaches you, and when that rule changes.

`kpipe-api` brings the consumer, producer, core and tracing modules transitively; its only external runtime dependency
is `kafka-clients`. Maven snippets, the full module catalog, `platform` vs `enforcedPlatform`, and JPMS
(`module-info.java`) guidance: [docs/MODULES.md](docs/MODULES.md).

## How processing works

Each polled record flows through: **track offset → deserialize → operators → sink → mark processed**. In the standard
sink path the whole unit runs on one virtual thread, and the payload is confined to it for the attempt — operators may
mutate a JSON `Map` or Avro `GenericRecord` in place (several built-in helpers do), because no other thread touches it
mid-attempt and a retry re-deserializes from the raw bytes rather than reusing a possibly-mutated object. (The batch
path hands buffered values to the flusher under a lock.)

The pipeline _definition_ is immutable: every fluent call returns a new `Stream<T>`, so two pipelines can branch from a
shared prefix without affecting each other.

A record's outcome is one of three sealed variants:

- **Passed** — the sink received it; the offset is marked processed.
- **Filtered** — a `filter` predicate returned false or an operator returned null. Deliberate: the sink is skipped and
  the offset still commits.
- **Failed** — an operator, the deserializer, or the sink threw. After configured retries, the record goes to the error
  handler and (if configured) the dead-letter topic. A `NonRetryableException` skips the retries.

Offsets commit on a 30-second cadence (synchronously, on the consumer thread), always at the **commit frontier** — the
lowest offset still in flight per partition. A fast record cannot commit past a slow one, which is how at-least-once
holds under parallel processing. Full lifecycle, failure matrix, and rebalance/shutdown behavior:
[docs/GUARANTEES.md](docs/GUARANTEES.md).

## Processing modes

| Mode                 | Concurrency                          | Ordering                                                        |
| -------------------- | ------------------------------------ | --------------------------------------------------------------- |
| `PARALLEL` (default) | One virtual thread per record        | None — side effects for consecutive offsets may interleave      |
| `SEQUENTIAL`         | One record at a time, whole consumer | Poll order (per-partition offset order, partitions interleaved) |
| `KEY_ORDERED`        | Serial per key, parallel across keys | Offset order within each key; `null` keys share one queue       |

Ordering requires cooperation from your producer: same-key records are only ordered if they land on the same partition,
which is the producer's partitioner's job. Offset tracking alone does not order side effects — for "Authorize before
Capture per account", key the producer by account **and** run `KEY_ORDERED` (or `SEQUENTIAL`). To keep that order in an
output topic, end the stream with `toKafka`, which keeps each record's key unless a `rekey` changes it. Details,
including the `KEY_ORDERED` key cap and eviction behavior: [docs/GUARANTEES.md](docs/GUARANTEES.md#ordering).

## Operational features

Each is one fluent call; exact semantics are in [docs/API.md](docs/API.md).

- **Retries** — `withRetry(maxRetries, backoff)`: fixed backoff, `maxRetries` counts retries after the initial attempt.
  A `NonRetryableException`, or a failure caused by one, is not retried.
- **Dead-letter topic** — `withDeadLetterTopic("orders.dlq")`: terminally-failed records are produced there with their
  original headers plus an `x-dlq-*` envelope (error, source topic/partition/offset, timestamp), making the dead-letter
  topic (DLQ) replayable. A failed DLQ send leaves the offset uncommitted — a down DLQ applies backpressure rather than
  dropping records.
- **Backpressure** — on by default: pauses all assigned partitions at 10,000 in-flight records, resumes at 7,000
  (hysteresis prevents flapping). A paused consumer keeps polling, which keeps its group membership alive (no
  `max.poll.interval.ms` eviction while paused).
- **Circuit breaker** — `withCircuitBreaker(threshold, window, openDuration)`: pauses consumption when the terminal
  failure rate trips, probes recovery after a cool-down. Off unless configured.
- **Metrics** — `withMetrics(...)`: OpenTelemetry-backed counters/histograms via the opt-in `kpipe-metrics-otel`, or
  `withMetricsLog(Duration)` for one log line per interval; `Handle.metrics()` for programmatic snapshots, including
  lag. [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md).
- **Tracing** — `withTracer(...)`: W3C `traceparent` extraction on consume, injection on produce and DLQ writes (opt-in
  `kpipe-tracing-otel`).
- **Graceful shutdown** — bounded in-flight drain, batch-buffer flush, then a final synchronous commit. Interrupted
  records are not marked processed, so they are redelivered rather than lost.

Multi-topic consumers (one consumer group, per-topic typed pipelines via `KPipe.multi(...)`), batch sinks, fan-out, and
producing to another topic: [docs/API.md](docs/API.md) and [docs/SINKS.md](docs/SINKS.md).

## Performance

KPipe `PARALLEL` runs within the error bands of Atleon, the fastest of the benchmarked alternatives that keep
at-least-once, at every delay, and 5 to 8 times faster than Confluent Parallel Consumer. Thousands of records per
second, every arm on one 4-vCPU GitHub runner with five forks
([2026-10-22 capture](benchmarks/results/2026-10-22/2026-10-22-ci-parallel.md)):

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

With no work per record, Reactor Kafka and the raw loop run faster than KPipe `PARALLEL`, and Atleon's key-ordered mode
scores higher within the error bands; KPipe's key-ordered mode runs at 0.85 of Atleon's there. The Reactor arm committed
the latest acknowledged offset (`maxDeferredCommits` 0), so it did not hold at-least-once when records finished out of
order. The raw loop has no backpressure and no offset tracking, and KPipe `PARALLEL` runs at 0.71 to 0.79 of it; that
gap is what those guarantees cost. The Kafka share consumer arm ran at about 5 thousand records per second.

The at-least-once claim is itself tested: every CI run gates on [Fray](https://github.com/cmu-pasta/fray)
controlled-concurrency suites and property tests over the offset lifecycle, and deliberately breaking an invariant makes
them fail ([docs/OFFSET-INVARIANTS.md](docs/OFFSET-INVARIANTS.md)). Latency, allocation, methodology and every capture's
raw data are in [`benchmarks/`](benchmarks).

## Positioning

KPipe sits between a hand-rolled `KafkaConsumer` loop and a stream-processing framework: it is a **consumer runtime**
for transform/enrich/route services, not a stream processor. Comparison across current versions (verify against the
versions you run — these projects evolve):

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

In the 2026-10-22 capture, the benchmark arms ran Atleon 0.47.1, Confluent Parallel Consumer 0.5.3.3 and Reactor Kafka
1.3.25 on kafka-clients 4.3.0; the Spring Kafka and Kafka Streams columns describe documented behavior, not versions we
test.

Choose Spring Kafka when you live in the Spring ecosystem or need transactions; Kafka Streams when you need stateful
processing; Reactor Kafka when your service is already reactive. KPipe's case is the plain-JVM consumer service doing
I/O-bound per-record work that wants parallelism, ordering options, and delivery guarantees without assembling them by
hand.

### Non-goals

KPipe is not: a stream processor (no windowing, joins, or state stores), an exactly-once framework (no Kafka
transactions), a workflow engine, an event store, a schema registry (it is a registry _client_), a reactive framework,
or a substitute for producer-side partitioning. It also cannot make your side effects idempotent — at-least-once means
your sink may see a record twice after a crash.

## Testing your pipeline

`kpipe-test` drives your pipeline through a real `KPipeConsumer` over an in-memory `MockConsumer` — no broker, no
Docker, milliseconds per test. `flush()` returns only after every sent record has settled, so assertions run against a
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

A `CrashRestartHarness` covers the harder question, whether the resume window reprocesses correctly after a crash,
without a broker. Add the kit to your tests; the full kit lives in [`lib/kpipe-test`](lib/kpipe-test):

```kotlin
testImplementation("io.github.eschizoid:kpipe-test")
```

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

- **Java 25+** — the project's compilation target (`--release 25`). The runtime model is built on virtual threads,
  records, and sealed types; Java 25 is the supported baseline, not a hint that a specific 25-only API is required.
- **kafka-clients 4.x** (managed by the BOM).
- **Docker** only for the integration examples and benchmarks — the library and `kpipe-test` need none.

## Contributing

Issues and PRs welcome. Modules hold unit tests in `src/test`, and those that talk to Kafka also hold Testcontainers
integration tests there. The offset modules hold Fray suites in `src/frayTest`, and each app in `examples/` has its own
integration tests. Together they are the bar contributions are held to.

## License

Apache 2.0. See [LICENSE](LICENSE).
