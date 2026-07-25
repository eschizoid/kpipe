<p align="center">
  <img src="img/kpipe.png" alt="kpipe — a Kafka consumer library for Java" width="320" />
</p>

# kpipe

A Kafka consumer library for the modern JVM: one virtual thread per record, a typed pipeline API, and at-least-once
delivery with parallel processing.

[![JVM 25+](https://img.shields.io/badge/JVM-25%2B-brightgreen.svg?&logo=openjdk)](https://openjdk.org/projects/jdk/25/)
[![Build](https://github.com/eschizoid/kpipe/actions/workflows/ci.yaml/badge.svg)](https://github.com/eschizoid/kpipe/actions/workflows/ci.yaml)
[![Codecov](https://codecov.io/gh/eschizoid/kpipe/graph/badge.svg?token=X50GBU4X7J)](https://codecov.io/gh/eschizoid/kpipe)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.eschizoid/kpipe-consumer.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.eschizoid/kpipe-consumer)
[![Javadoc](https://javadoc.io/badge2/io.github.eschizoid/kpipe-api/javadoc.svg?color=purple)](https://javadoc.io/doc/io.github.eschizoid/kpipe-api)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

KPipe replaces the hand-rolled `Consumer.poll` loop that most Kafka services grow around their business logic. You
declare a pipeline — deserialize, transform, sink — and KPipe runs it with:

- **A virtual thread per record.** I/O-bound work (DB writes, HTTP calls) overlaps without a worker pool to configure.
  You still bound in-flight work (KPipe's backpressure does this) and your downstream resources — connection pools and
  rate limits don't disappear because threads got cheap.
- **At-least-once delivery, kept honest under parallelism.** An offset is committed only after its record reaches a
  terminal state (sink completed, filtered, or parked in the dead-letter topic), and commits never pass a record that
  is still in flight. The exact guarantee boundary and failure matrix: [docs/GUARANTEES.md](docs/GUARANTEES.md).
- **One deserialize, one serialize.** Operators transform the typed payload (`Map`, `GenericRecord`, `Message`)
  between a single decode and a single encode, instead of re-serializing between steps.
- **Typed outcomes instead of nulls.** Every record ends as `Passed`, `Filtered`, or `Failed` — a sealed type, so
  "intentionally skipped" and "broken" cannot be confused, in code or in metrics.
- **The operational stack as configuration.** Retries, dead-letter routing, backpressure, a circuit breaker, metrics,
  and W3C trace propagation are fluent calls, not code you write.

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
gives you a bounded graceful shutdown (`close()` = drain in-flight work for up to 5 seconds, flush batch buffers,
commit synchronously), and `awaitShutdown()` keeps the process alive until you stop it. No JVM shutdown hook is
installed unless you opt in.

## Installation

```kotlin
implementation(platform("io.github.eschizoid:kpipe-bom:1.18.0"))
implementation("io.github.eschizoid:kpipe-api")
implementation("io.github.eschizoid:kpipe-format-json")   // or -avro / -protobuf — formats are opt-in
```

`kpipe-api` brings the consumer, producer, and core modules transitively; its only external runtime dependency is
`kafka-clients`. Maven snippets, the full module catalog, `platform` vs `enforcedPlatform`, and JPMS
(`module-info.java`) guidance: [docs/MODULES.md](docs/MODULES.md).

## How processing works

Each polled record flows through: **track offset → deserialize → operators → sink → mark processed**. The whole unit
runs on one virtual thread, and the payload object is confined to it — operators may freely mutate a JSON `Map` or
Avro `GenericRecord` in place (several built-in helpers do), because no other thread sees it and a retry
re-deserializes from the raw bytes rather than reusing a possibly-mutated object.

The pipeline *definition* is immutable: every fluent call returns a new `Stream<T>`, so branching two pipelines from a
shared prefix is safe.

A record's outcome is one of three sealed variants:

- **Passed** — the sink received it; the offset is marked processed.
- **Filtered** — a `filter` predicate returned false or an operator returned null. Deliberate: the sink is skipped and
  the offset still commits.
- **Failed** — an operator, the deserializer, or the sink threw. After configured retries, the record goes to the
  error handler and (if configured) the dead-letter topic.

Offsets commit on a 30-second cadence (synchronously, on the consumer thread), always at the **commit frontier** — the
lowest offset still in flight per partition. A fast record cannot commit past a slow one, which is what makes
at-least-once true with parallel processing. Full lifecycle, failure matrix, and rebalance/shutdown behavior:
[docs/GUARANTEES.md](docs/GUARANTEES.md).

## Processing modes

| Mode | Concurrency | Ordering |
| --- | --- | --- |
| `PARALLEL` (default) | One virtual thread per record | None — side effects for consecutive offsets may interleave |
| `SEQUENTIAL` | One record at a time, whole consumer | Poll order (per-partition offset order, partitions interleaved) |
| `KEY_ORDERED` | Serial per key, parallel across keys | Offset order within each key; `null` keys share one queue |

Ordering requires cooperation from your producer: same-key records are only ordered if they land on the same
partition, which is the producer's partitioner's job. Offset tracking alone does not order side effects — for
"Authorize before Capture per account", key the producer by account **and** run `KEY_ORDERED` (or `SEQUENTIAL`).
Details, including the `KEY_ORDERED` key cap and eviction behavior: [docs/GUARANTEES.md](docs/GUARANTEES.md#ordering).

## Operational features

Each is one fluent call; exact semantics are in [docs/API.md](docs/API.md).

- **Retries** — `withRetry(maxRetries, backoff)`: fixed backoff, `maxRetries` counts retries after the initial attempt.
- **Dead-letter topic** — `withDeadLetterTopic("orders.dlq")`: terminally-failed records are produced there with their
  original headers plus an `x-dlq-*` envelope (error, source topic/partition/offset, timestamp), making the DLQ
  replayable. A failed DLQ send leaves the offset uncommitted — a down DLQ applies backpressure rather than dropping
  records.
- **Backpressure** — on by default: pauses all assigned partitions at 10,000 in-flight records, resumes at 7,000
  (hysteresis prevents flapping). A paused consumer keeps polling so it is never evicted from the group.
- **Circuit breaker** — `withCircuitBreaker(threshold, window, openDuration)`: pauses consumption when the terminal
  failure rate trips, probes recovery after a cool-down. Off unless configured.
- **Metrics** — `withMetrics(...)`: OpenTelemetry-backed counters/histograms via the opt-in `kpipe-metrics-otel`, or
  the log-based reporters; `Handle.metrics()` for programmatic snapshots. [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md).
- **Tracing** — `withTracer(...)`: W3C `traceparent` extraction on consume, injection on produce and DLQ writes
  (opt-in `kpipe-tracing-otel`).
- **Graceful shutdown** — bounded in-flight drain, batch-buffer flush, then a final synchronous commit. Interrupted
  records are not marked processed, so they are redelivered rather than lost.

Multi-topic consumers (one consumer group, per-topic typed pipelines via `KPipe.multi(...)`), batch sinks, fan-out,
and producing to another topic: [docs/API.md](docs/API.md) and [docs/SINKS.md](docs/SINKS.md).

## Performance

Benchmarked against Confluent Parallel Consumer, Reactor Kafka, the KIP-932 share consumer, a raw
`KafkaConsumer` + virtual-threads loop, and a single-threaded baseline — same broker, same workload, JMH with
published raw JSON. Summary of the 2026-07 reference capture (one 6-core machine; read ratios, not absolutes): with
10–100ms of real work per record, KPipe `PARALLEL` sustained roughly 6.6× Confluent Parallel Consumer's throughput at
10ms and 41× at 100ms, tracking the machine's capacity where a fixed worker pool is bounded by `workers ÷ work-time`.

The costs, measured in the same captures: KPipe allocates more per record than any alternative tested (~1.7 KB/op,
mostly the per-record virtual thread, vs ~35 B/op for CPC); at sub-millisecond workloads the `KEY_ORDERED` mode's
advantage over CPC's equivalent is machine-dependent (it loses on one of our two test machines); and the raw-loop
baseline is faster than KPipe at every cell — it omits rebalance-safe offset tracking, so it is not comparable on
delivery guarantees.

Full tables with error bars, environments, methodology, DNF explanations, and every capture's raw data:
[`benchmarks/`](benchmarks/).

The at-least-once claim is tested, not only documented: every CI run gates on 21
[jcstress](https://openjdk.org/projects/code-tools/jcstress/) concurrency-stress classes plus jqwik property suites
over the offset lifecycle and chaos-rebalance/crash-restart integration tests against a real broker. Building that
suite caught three real data-loss bugs before release ([docs/OFFSET-INVARIANTS.md](docs/OFFSET-INVARIANTS.md)).

## Positioning

KPipe sits between a hand-rolled `KafkaConsumer` loop and a stream-processing framework: it is a **consumer runtime**
for transform/enrich/route services, not a stream processor. Honest comparison across current versions (verify
against the versions you run — competitors evolve):

| Concern | KPipe | Spring Kafka | Kafka Streams | Reactor Kafka |
| --- | --- | --- | --- | --- |
| Primary abstraction | Fluent record pipeline | Listener containers + templates | Stateful stream topology | Reactive `Flux` over Kafka |
| Runtime model | Plain `java -jar`; no DI container. JPMS modules | Usable without Spring Boot; designed around the Spring context, programmatic or annotation-driven | Library, no container | Library, no container |
| Record concurrency | Virtual thread per record; serial and per-key modes | Container concurrency (threads per container/partition); async and VT executors configurable | Stream threads / tasks per partition | Reactive schedulers |
| Ordering options | None / global serial / per-key | Per-partition via container model | Per-partition (task model) | Per-partition (reactive chains) |
| Offset tracking under parallelism | Lowest-pending-offset frontier, built in | Ack modes; out-of-order commit management is manual or container-managed | Managed by streams runtime | Manual or auto-ack variants |
| Retries + DLT | Built-in fixed-backoff retry + replayable DLQ | Extensive (error handlers, `@RetryableTopic`, `DeadLetterPublishingRecoverer`, backoff policies) | Application-level | Application-level |
| Transactions / EOS | **Not supported** | Supported (transactional templates, listeners) | Supported (EOS v2) | Sender transactions |
| Stateful processing (joins, windows, stores) | **Not supported** | Not built-in | Core feature | Not built-in |
| Ecosystem & maturity | Young, small API surface | Large ecosystem, long history | Large ecosystem, long history | Established in reactive stacks |

Choose Spring Kafka when you live in the Spring ecosystem or need transactions; Kafka Streams when you need stateful
processing; Reactor Kafka when your service is already reactive. KPipe's case is the plain-JVM consumer service doing
I/O-bound per-record work that wants parallelism, ordering options, and delivery guarantees without assembling them
by hand.

### Non-goals

KPipe is not: a stream processor (no windowing, joins, or state stores), an exactly-once framework (no Kafka
transactions), a workflow engine, an event store, a schema registry (it is a registry *client*), a reactive framework,
or a substitute for producer-side partitioning. It also cannot make your side effects idempotent — at-least-once means
your sink may see a record twice after a crash.

## Testing your pipeline

`kpipe-test` drives your pipeline through a real `KPipeConsumer` over an in-memory `MockConsumer` — no broker, no
Docker, milliseconds per test. `flush()` returns only once every sent record has fully settled, so assertions never
race the consumer:

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

A `CrashRestartHarness` covers the harder question — does the resume window reprocess correctly after a crash —
without a broker to flake on. Add `testImplementation("io.github.eschizoid:kpipe-test")`.

## Documentation

| Document | Contents |
| --- | --- |
| [docs/API.md](docs/API.md) | The fluent surface, grouped by type; multi-topic routing; built-in operators |
| [docs/GUARANTEES.md](docs/GUARANTEES.md) | Delivery guarantee boundary, failure matrix, ordering, backpressure |
| [docs/FORMATS.md](docs/FORMATS.md) | JSON / Avro / Protobuf, wire envelopes, Confluent Schema Registry modes |
| [docs/SINKS.md](docs/SINKS.md) | Custom, Kafka-producer, fan-out, and batch sinks |
| [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) | Metrics tables, outcome counters, tracing, local Grafana stack |
| [docs/MODULES.md](docs/MODULES.md) | Module catalog, BOM usage, JPMS |
| [docs/ESCAPE-HATCHES.md](docs/ESCAPE-HATCHES.md) | The explicit builder API: custom offset managers, reporters, seams |
| [docs/OFFSET-INVARIANTS.md](docs/OFFSET-INVARIANTS.md) | The machine-checked offset invariants |
| [benchmarks/](benchmarks/) | Methodology, raw results, dated capture snapshots |
| [examples/](examples/) | Runnable apps per format + a full demo with observability stack (`./scripts/run-demo.sh`) |

## Requirements

- **Java 25+** — the project's compilation target (`--release 25`). The runtime model is built on virtual threads,
  records, and sealed types; Java 25 is the supported baseline, not a hint that a specific 25-only API is required.
- **kafka-clients 4.x** (managed by the BOM).
- **Docker** only for the integration examples and benchmarks — the library and `kpipe-test` need none.

## Contributing

Issues and PRs welcome — see the test suites (`unit`, jcstress under `src/jcstress`, integration under `examples/`)
for the bar contributions are held to.

## License

Apache 2.0. See [LICENSE](LICENSE).
