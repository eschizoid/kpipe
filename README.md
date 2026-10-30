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

KPipe is a plain-JVM Kafka consumer runtime for Java 25+: one virtual thread per record, a typed pipeline API, and
at-least-once delivery that still holds while records run in parallel.

KPipe replaces the hand-rolled `Consumer.poll` loop that most Kafka services grow around their business logic. You
declare a pipeline — deserialize, transform, sink — and KPipe runs it with:

- **A virtual thread per record.** I/O-bound work (DB writes, HTTP calls) overlaps without a worker pool to configure.
  You still bound in-flight work — records currently being processed — which KPipe's backpressure does by pausing
  consumption past a watermark; and your downstream resources — connection pools and rate limits — don't disappear
  because threads got cheap.
- **At-least-once delivery, held under parallelism.** An offset is committed only after its record reaches a terminal
  state (sink completed, filtered, or parked in the dead-letter topic, the DLQ), and commits never pass a record that is
  still in flight. The exact guarantee boundary and failure matrix: [docs/GUARANTEES.md](docs/GUARANTEES.md).
- **One decode per processing attempt, no re-encode between steps.** Operators work on the typed payload (`Map`,
  `GenericRecord`, `Message`) from a single decode to the sink; only a sink that writes bytes back to Kafka encodes,
  once. See [docs/FORMATS.md](docs/FORMATS.md) for what each kind of transform costs.
- **Typed outcomes instead of nulls.** Every record ends as `Passed`, `Filtered`, or `Failed` — a sealed type, so
  "intentionally skipped" and "broken" cannot be confused, in code or in metrics.
- **The operational stack as configuration.** Retries, dead-letter routing, backpressure, a circuit breaker, metrics,
  and W3C trace propagation are fluent calls, not code you write.

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
implementation(platform("io.github.eschizoid:kpipe-bom:1.20.0"))
implementation("io.github.eschizoid:kpipe-api")
implementation("io.github.eschizoid:kpipe-format-json")   // or -avro / -protobuf — formats are opt-in
```

Minor releases may remove public API through 1.x — see [docs/VERSIONING.md](docs/VERSIONING.md) for what is covered, how
a break reaches you, and when that rule changes.

`kpipe-api` brings the consumer, producer, core, metrics, and tracing modules transitively; its only external runtime
dependency is `kafka-clients`. Maven snippets, the full module catalog, `platform` vs `enforcedPlatform`, and JPMS
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
  handler and (if configured) the dead-letter topic.

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
Capture per account", key the producer by account **and** run `KEY_ORDERED` (or `SEQUENTIAL`). Details, including the
`KEY_ORDERED` key cap and eviction behavior: [docs/GUARANTEES.md](docs/GUARANTEES.md#ordering).

## Operational features

Each is one fluent call; exact semantics are in [docs/API.md](docs/API.md).

- **Retries** — `withRetry(maxRetries, backoff)`: fixed backoff, `maxRetries` counts retries after the initial attempt.
- **Dead-letter topic** — `withDeadLetterTopic("orders.dlq")`: terminally-failed records are produced there with their
  original headers plus an `x-dlq-*` envelope (error, source topic/partition/offset, timestamp), making the DLQ
  replayable. A failed DLQ send leaves the offset uncommitted — a down DLQ applies backpressure rather than dropping
  records.
- **Backpressure** — on by default: pauses all assigned partitions at 10,000 in-flight records, resumes at 7,000
  (hysteresis prevents flapping). A paused consumer keeps polling, which keeps its group membership alive (no
  `max.poll.interval.ms` eviction while paused).
- **Circuit breaker** — `withCircuitBreaker(threshold, window, openDuration)`: pauses consumption when the terminal
  failure rate trips, probes recovery after a cool-down. Off unless configured.
- **Metrics** — `withMetrics(...)`: OpenTelemetry-backed counters/histograms via the opt-in `kpipe-metrics-otel`, or the
  log-based reporters; `Handle.metrics()` for programmatic snapshots. [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md).
- **Tracing** — `withTracer(...)`: W3C `traceparent` extraction on consume, injection on produce and DLQ writes (opt-in
  `kpipe-tracing-otel`).
- **Graceful shutdown** — bounded in-flight drain, batch-buffer flush, then a final synchronous commit. Interrupted
  records are not marked processed, so they are redelivered rather than lost.

Multi-topic consumers (one consumer group, per-topic typed pipelines via `KPipe.multi(...)`), batch sinks, fan-out, and
producing to another topic: [docs/API.md](docs/API.md) and [docs/SINKS.md](docs/SINKS.md).

## Performance

Benchmarked against Atleon, Confluent Parallel Consumer, Reactor Kafka, the KIP-932 share consumer, and a raw
`KafkaConsumer` + virtual-threads loop — same broker, same workload, JMH with published raw JSON. The reference numbers
come from CI captures on a GitHub runner, described below. The CI captures do not yet compare KPipe with Confluent
Parallel Consumer at 10 to 100ms of work per record, but earlier local captures do. In the 2026-07 capture on one 6-core
machine, with that much work, KPipe `PARALLEL` sustained roughly 6.6× Confluent Parallel Consumer's throughput at 10ms
and 41× at 100ms, tracking the machine's capacity where a fixed worker pool is bounded by `workers ÷ work-time`.

The [2026-10-22 capture](benchmarks/results/2026-10-22/2026-10-22-ci-parallel.md) is the reference. It runs all nine
arms in one job on a 4-vCPU GitHub runner at 0, 100 and 1000µs of work per record, with five forks and JMH 1.37
allocation accounting, and KPipe tracks every record for its offset manager, as Atleon does for its acknowledgements in
`COMPACT_EAGER` mode. KPipe `PARALLEL` and Atleon parallel are within each other's error bands at every delay (1.09×,
0.95× and 1.04×). In key-ordered mode Atleon leads at 0µs, where KPipe runs at 0.85× with the error bands apart, and the
two are within the error bands at 100 and 1000µs. KPipe ran at 5.4× to 7.8× Confluent Parallel Consumer's unordered
throughput and at 5.1× to 6.2× its per-key throughput. Reactor Kafka ran at 1.21× KPipe at 0µs and at 0.67× and 0.16× at
100 and 1000µs, because its 100 rails process at most 100 records at a time. In that capture Reactor committed the
latest acknowledged offset per partition rather than a lowest-pending frontier, which is cheaper and does not hold
at-least-once when records finish out of order; the Reactor arm now defers out-of-order commits behind the same
10,000-record watermark as KPipe, so later captures compare like with like. The
[2026-10-21](benchmarks/results/2026-10-21/2026-10-21-ci-parallel.md),
[2026-10-15](benchmarks/results/2026-10-15/2026-10-15-ci-parallel.md) and
[2026-10-16](benchmarks/results/2026-10-16/2026-10-16-ci-parallel-jmh137.md) captures ran KPipe without offset tracking
and Reactor on a one-thread-per-CPU scheduler, so they flatter KPipe; they stay in the results folder. The
[2026-10-23 latency capture](benchmarks/results/2026-10-23/2026-10-23-ci-latency.md), five forks with offset tracking
on, times how long each arm takes to drain a 25,000-record batch. In unordered mode KPipe and Atleon are within each
other's error bands on average at every delay, and KPipe's p99 is 8 to 18 ms lower. In key-ordered mode Atleon is faster
on average at 0 and 1000µs, while KPipe's p99.9 is lower at every delay. Confluent Parallel Consumer takes 5 to 8 times
as long as KPipe in both modes.

The costs: KPipe `PARALLEL` ran at 0.71× to 0.79× the raw loop. Against the 2026-10-21 capture, its throughput fell 22%
at 0µs while the raw loop and Atleon fell 4% to 6%, so tracking each record costs KPipe `PARALLEL` roughly 15% to 20% at
0µs; at 100 and 1000µs the change between the two runs is within their error bands. No commit falls inside the measured
window, which is shorter than the 30-second commit interval, so this is the cost of tracking, not of committing. It
allocates about 1,128 B/op at 0µs and 1,770 to 1,820 at 100 and 1000µs, 13% to 28% more than the raw loop, and still
less than Atleon's arm with the same ordering and less than CPC at every delay. Captures that used JMH 1.36 under-count
allocation for any arm whose consumer runs on threads that exit during the measurement, so their allocation comparisons,
including the 2026-07 ones, overstate KPipe's cost relative to the others. In the 2026-07 captures, the `KEY_ORDERED`
mode's advantage over CPC's equivalent at sub-millisecond workloads was machine-dependent (it lost on one of our two
test machines), and the virtual-thread-per-record advantage shrinks as per-record work approaches zero or becomes
CPU-bound, because there is nothing to overlap. The raw loop has no in-flight limit, no backpressure and no offset
tracking, so the gap to it measures all three.

Full tables with error bars, environments, methodology, DNF explanations, and every capture's raw data:
[`benchmarks/`](benchmarks).

The at-least-once claim is itself under test: every CI run gates on 13 [Fray](https://github.com/cmu-pasta/fray)
controlled-concurrency classes plus jqwik property suites over the offset lifecycle and chaos-rebalance/crash-restart
integration tests against a real broker. Fray explores thread schedules under its own scheduler rather than running racy
code repeatedly and hoping, and the suite is falsification-tested — deliberately breaking an invariant makes it fail.
Building it caught three real data-loss bugs before release ([docs/OFFSET-INVARIANTS.md](docs/OFFSET-INVARIANTS.md)).

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

Benchmarked against Atleon 0.47.1, Confluent Parallel Consumer 0.5.3.3 and Reactor Kafka 1.3.25. The 2026-10-22 capture
ran on kafka-clients 4.3.0, which [`benchmarks/build.gradle.kts`](benchmarks/build.gradle.kts) forces because Parallel
Consumer processes no records on 4.4.0, and earlier captures ran on the kafka-clients version the libraries used at the
time. The libraries build and test against the kafka-clients version in
[`gradle/libs.versions.toml`](gradle/libs.versions.toml). The Spring Kafka and Kafka Streams columns describe documented
behavior, not versions we test.

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

A `CrashRestartHarness` covers the harder question — does the resume window reprocess correctly after a crash — without
a broker involved. Add `testImplementation("io.github.eschizoid:kpipe-test")`; the full kit lives in
[`lib/kpipe-test`](lib/kpipe-test).

## Documentation

| Document                                               | Contents                                                                                                                                                                                    |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [docs/API.md](docs/API.md)                             | The fluent surface, grouped by type; multi-topic routing; built-in operators                                                                                                                |
| [docs/GUARANTEES.md](docs/GUARANTEES.md)               | Delivery guarantee boundary, failure matrix, ordering, backpressure                                                                                                                         |
| [docs/FORMATS.md](docs/FORMATS.md)                     | JSON / Avro / Protobuf, wire envelopes, Confluent Schema Registry modes                                                                                                                     |
| [docs/SINKS.md](docs/SINKS.md)                         | Custom, Kafka-producer, fan-out, and batch sinks                                                                                                                                            |
| [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)         | Metrics tables, outcome counters, tracing, local Grafana stack                                                                                                                              |
| [docs/MODULES.md](docs/MODULES.md)                     | Module catalog, BOM usage, JPMS                                                                                                                                                             |
| [docs/ESCAPE-HATCHES.md](docs/ESCAPE-HATCHES.md)       | The explicit builder API: custom offset managers, reporters, seams                                                                                                                          |
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

Issues and PRs welcome — see the test suites (`unit`, Fray under `src/frayTest`, integration under `examples/`) for the
bar contributions are held to.

## License

Apache 2.0. See [LICENSE](LICENSE).
