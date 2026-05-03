# KPipe — Architectural Review & Refactor Plan

**Reviewed:** 2026-05-02
**Last updated:** 2026-05-02 (status reconciliation)
**Branch state at review:** `fix/grafana-emetrics`
**Scope:** module boundaries, public API surface, dependency direction, error semantics, build configuration.

---

## Priority Ladder (TL;DR)

| # | Finding                                                         | Severity    | Breaking?    | Effort | Status     |
|---|-----------------------------------------------------------------|-------------|--------------|--------|------------|
| 1 | `MessagePipeline.apply()` swallows all exceptions               | 🔴 Critical | No (bug fix) | S      | ✅ Done     |
| 2 | `kpipe-consumer` is a kitchen-sink module (all formats bundled) | 🟠 High     | Yes          | L      | ✅ Done     |
| 3 | OTel API leaks through `api()` chain — forced on every user     | 🟠 High     | Yes          | M      | ✅ Done     |
| 4 | `MessageSink` lives in the wrong module                         | 🟡 Medium   | Yes          | M      | ✅ Done     |
| 5 | `MessageProcessorRegistry` has too many responsibilities        | 🟡 Medium   | Yes          | M      | ✅ Done     |
| 6 | Build/test config smells (7G heap, Postgres deps, single fork)  | 🟢 Low      | No           | S      | ⏳ Pending  |

### Current 2.0 work order (active)

1. **#2 residual** — extract `kpipe-core` from `kpipe-consumer` (registry/pipeline/sink machinery). Format modules and
   producer rebase onto core.
2. **#4 residual** — once core exists, move `MessageSink` interface from `kpipe-producer` to `kpipe-core`. Producer
   becomes a peer of consumer, not a base.
3. **#5** — decompose `MessageProcessorRegistry` into focused classes (lands cleaner once it lives in `kpipe-core`).
4. **#6** — test infra cleanup, can ship anytime.

Items 2, 3, 4, 5 break the public API. Already bundled into the in-progress 2.0 branch.

---

## Strengths to Preserve

These design decisions are working well and should not be regressed during refactors:

- **Clean module dependency direction** — `kpipe-metrics ← kpipe-producer ← kpipe-consumer`. No cycles, no sideways
  leaks.
- **JPMS is real** — `module-info.java` is present, modular Javadoc and source jars are produced.
- **Single SerDe cycle** — documented in `README.md:196-208`. Genuinely good for throughput; avoid breaking it.
- **Virtual-thread friendliness** — command queues for cross-thread coordination, no obvious `synchronized` in hot
  paths.
- **Professional Maven publishing** — proper signing, POM metadata, separate artifacts per module.

---

## 1. 🔴 `MessagePipeline.apply()` swallows all exceptions — ✅ DONE

**File:** `lib/kpipe-consumer/src/main/java/org/kpipe/registry/MessagePipeline.java:41-51`

```java
default byte[] apply(byte[] data) {
    try {
        final var deserialized = deserialize(data);
        if (deserialized == null) return null;
        final var processed = process(deserialized);
        if (processed == null) return null;
        return serialize(processed);
    } catch (final Exception e) {
        return null;            // ← silent failure
    }
}
```

### Problem

`null` return is overloaded:

- `null` from `deserialize()` → "data was malformed" (error)
- `null` from `process()` → "intentionally filtered" (success)
- `null` from `catch` → "anything blew up" (error)

Downstream `KPipeConsumer.processTypedRecord` (
`lib/kpipe-consumer/src/main/java/org/kpipe/consumer/KPipeConsumer.java:895-917`) treats `processed == null` as *
*intentional filtering** and increments `messagesProcessed` — even when deserialization actually failed.

### Symptom We Hit

During the Grafana dashboard work (2026-05-01 session):

- Protobuf seed messages all failed deserialization (`skipBytes(5)` was wrong)
- Pipeline returned `null`
- Consumer counted them as "processed", not "errors"
- Sink invocationCount=0 was the only signal that anything was wrong
- 10 minutes wasted diagnosing a bug that should have been a single error log

### Fix Direction

Replace `null`-overloading with explicit semantics. Two viable shapes:

**Option A — `Result<T>` (preferred):**

```java
sealed interface Result<T> permits Ok, Filtered, Failed {
}

record Ok<T>(T value) implements Result<T> {
}

record Filtered<T>() implements Result<T> {
}

record Failed<T>(Throwable cause) implements Result<T> {
}
```

Pipeline returns `Result<byte[]>`; consumer pattern-matches and routes to the correct counter.

**Option B — Distinct exceptions:**

```java
class IntentionalFilterException extends RuntimeException { /* ... */
}
// Re-throw everything else; catch IntentionalFilterException specifically.
```

A is cleaner. Java 25 sealed interfaces + records make it ergonomic.

### Effort

**Small.** One interface, one switch in `KPipeConsumer.processTypedRecord`, update `JsonFormat`/`AvroFormat`/
`ProtobufFormat` to return `Result` shapes. ~1 day including tests.

### Why First

It's a correctness bug, not just an architecture concern. Every other refactor will be safer once errors are observable.

---

## 2. 🟠 `kpipe-consumer` is a kitchen-sink module

**File:** `lib/kpipe-consumer/build.gradle.kts:25-37`

```kotlin
implementation(libs.kafkaClients)
implementation(libs.dslJson)         // JSON
implementation(libs.avro)             // Avro
implementation(libs.protobufJava)     // Protobuf
implementation(libs.protobufUtil)
```

### Problem

A user who only consumes JSON drags in:

- **Avro runtime** (~2MB jar + Jackson + commons-compress)
- **Protobuf runtime** (~1.5MB + protobuf-util reflection)

That's roughly **3–4MB of dead weight** for the typical "I just want to read JSON off Kafka" use case.

The single-module structure also packs 8 packages into one artifact:

```
org.kpipe.consumer/         ← KPipeConsumer, KPipeRunner, BackpressureController
org.kpipe.consumer.config/
org.kpipe.consumer.enums/
org.kpipe.consumer.metrics/
org.kpipe.consumer.sink/    ← console sinks
org.kpipe.health/           ← HttpHealthServer (??)
org.kpipe.processor/        ← format-specific processors
org.kpipe.registry/         ← MessageFormat, MessagePipeline, registries
```

### Fix Direction

Split into:

| New module              | Contents                                                                                                             | Deps              |
|-------------------------|----------------------------------------------------------------------------------------------------------------------|-------------------|
| `kpipe-core`            | `KPipeConsumer`, `KPipeRunner`, `BackpressureController`, `MessageTracker`, `OffsetManager`, registry **interfaces** | Kafka only        |
| `kpipe-format-json`     | `JsonFormat`, `JsonMessageProcessor`, `JsonConsoleSink`                                                              | core + DSL-JSON   |
| `kpipe-format-avro`     | `AvroFormat`, `AvroMessageProcessor`, `AvroConsoleSink`                                                              | core + Avro       |
| `kpipe-format-protobuf` | `ProtobufFormat`, `ProtobufMessageProcessor`, `ProtobufConsoleSink`                                                  | core + Protobuf   |
| `kpipe-health`          | `HttpHealthServer`, `HealthConfig`                                                                                   | nothing JVM-extra |

Users opt-in to the formats they need.

### Effort

**Large.** Module split + JPMS `module-info.java` reorganization + republishing 5 artifacts instead of 1 + migration
guide. ~3–5 days.

### Why Second-Tier

Wait — actually do **#3 first** because the OTel coupling will make this split easier. With OTel optional, `kpipe-core`
doesn't drag in `opentelemetry-api`.

---

## 3. 🟠 OTel API leaks through the entire `api()` chain

**Files:**

- `lib/kpipe-metrics/build.gradle.kts:22` — `api(libs.opentelemetryApi)`
- `lib/kpipe-producer/build.gradle.kts:22` — `api(project(":lib:kpipe-metrics"))`
- `lib/kpipe-consumer/build.gradle.kts:22` — `api(project(":lib:kpipe-producer"))`

### Problem

```
opentelemetry-api ──(api)── kpipe-metrics ──(api)── kpipe-producer ──(api)── kpipe-consumer
```

Anyone using `kpipe-consumer` gets OTel API on the classpath whether they want it or not. The `noop()` factories help *
*runtime** cost (no actual recording happens) but the **compile-time** dep is unavoidable.

This blocks adoption in environments that:

- Ban OTel API on classpath (some enterprise security policies)
- Already use a different metrics library and don't want classpath confusion
- Want a minimal-footprint embedded use case

### Fix Direction

**Option A — Service Loader (cleanest):**

```java
// kpipe-core declares an SPI:
public interface ConsumerMetricsProvider {
    ConsumerMetrics create(String pipelineName);
}

// kpipe-metrics-otel registers an impl via META-INF/services
// If no impl on classpath, ConsumerMetrics.noop() is used.
```

**Option B — Two artifacts:**

- `kpipe-metrics-api` — interfaces only, no OTel dep. `kpipe-core` and `kpipe-producer` depend on this.
- `kpipe-metrics-otel` — OTel impl, optional. Users add it explicitly if they want telemetry.

Option B is mechanically simpler but creates a 2-module split where 1 should suffice. Option A is more elegant but
introduces SPI complexity.

**My recommendation: Option B.** Java/Kafka ecosystem norm; users expect this pattern from kafka-clients itself.

### Effort

**Medium.** ~2 days. Bulk of the work is restructuring `ConsumerMetrics` and `ProducerMetrics` to extract the API
surface from the OTel-specific impl.

---

## 4. 🟡 `MessageSink` lives in the wrong module

**Current locations:**

- Interface: `lib/kpipe-producer/src/main/java/org/kpipe/sink/MessageSink.java`
- Console impls: `lib/kpipe-consumer/src/main/java/org/kpipe/consumer/sink/`
- Registry: `lib/kpipe-consumer/src/main/java/org/kpipe/registry/MessageSinkRegistry.java`

### Problem

A sink is **more fundamental than producer or consumer** — it's the output abstraction. Today the dependency arrow is:

```
kpipe-consumer  needs  MessageSink  →  defined in  kpipe-producer
```

Which is why `kpipe-consumer` has `api(project(":lib:kpipe-producer"))` at all. Most consumer users **never produce**
anything; they're forced to depend on the producer module purely to satisfy the sink interface dependency.

### Fix Direction

After the module split (#2), put `MessageSink<T>` and `MessageSinkRegistry` in `kpipe-core`. Producer becomes a true
peer of consumer rather than a base.

```
kpipe-metrics-api
       ▲
       │
kpipe-core (sink interface, registries, KPipeConsumer, BackpressureController)
       ▲
   ┌───┴───┐
   │       │
producer  format-* (json/avro/protobuf)
```

### Effort

**Medium.** Bundled with #2 — natural during the module split.

---

## 5. 🟡 `MessageProcessorRegistry` has too many responsibilities

**File:** `lib/kpipe-consumer/src/main/java/org/kpipe/registry/MessageProcessorRegistry.java`

### Problem

Counted responsibilities in one class:

1. Processor registry (`registryMap`, `register`, `getOperator`, `unregister`, `clear`)
2. Sink registry (delegated to `sinkRegistry()` but exposed in the same class surface — `getSink`, `getMetrics` fall
   through)
3. Schema registry (`addSchema` proxies to `MessageFormat.addSchema`)
4. Format-aware default-processor seeding (`registerDefaultProcessors` checks `messageFormat == MessageFormat.JSON`)
5. Static error-handling helper (`withErrorHandling`, lines 203-205)
6. Metrics aggregation (`getMetrics(key)` — looks across both processor and sink registries)

Lines 33-38 hardcode **JSON-specific** constants (`JSON_ADD_SOURCE`, `JSON_ADD_TIMESTAMP`, `JSON_MARK_PROCESSED`) on a
supposedly format-agnostic registry. Adding a new format means editing this class.

### Fix Direction

Split into focused classes:

| Class                  | Responsibility                                                   |
|------------------------|------------------------------------------------------------------|
| `ProcessorRegistry<T>` | typed processor lookup only                                      |
| `SinkRegistry<T>`      | typed sink lookup only (already exists, drop the delegation)     |
| `SchemaRegistry`       | schema registration (or fold into `MessageFormat<T>` directly)   |
| `RegistryFunctions`    | static helpers like `withErrorHandling` (already exists, expand) |

Move JSON-specific constants to `JsonFormat` (or a new `JsonProcessors` class in `kpipe-format-json`).

### Effort

**Medium.** ~1.5 days. Mostly mechanical — the delegations already exist.

---

## 6. 🟢 Build/test configuration smells

**File:** `lib/kpipe-consumer/build.gradle.kts:64-67`

```kotlin
minHeapSize = "7g"
maxHeapSize = "7g"
maxParallelForks = 1
forkEvery = 200
```

### Problem

- **7GB heap** for tests is unusual and suggests memory leaks (probably from testcontainers not cleaning up Kafka
  brokers between tests).
- **`maxParallelForks = 1`** means tests run sequentially — slow CI.
- **`forkEvery = 200`** restarts the JVM every 200 tests — implies stateful test interdependencies.

Also (`build.gradle.kts:53` of producer):

```kotlin
testImplementation(libs.testcontainersPostgresql)
testImplementation(libs.postgresql)
```

**Postgres** in a Kafka library? Either leftover or hidden coupling. Investigate.

### Fix Direction

1. Profile tests to find actual memory usage. If reality is <2GB, drop the heap.
2. Audit testcontainers lifecycle — is each test creating a new Kafka container? Use shared container patterns.
3. Audit Postgres usage in tests — if leftover, remove. If real, document why a Kafka library has Postgres tests.
4. Try `maxParallelForks = N` (where N = CPU cores / 2) once tests are isolated.

### Effort

**Small.** ~0.5 day to investigate, 1–2 days to fix depending on what's found.

---

## Suggested Roadmap

### Sprint 1 (1.x patch release)

- ✅ #1 — `MessagePipeline` error semantics. Non-breaking, ship as 1.9.0.

### Sprint 2 (2.0 candidate, breaking)

- #3 — OTel optional split. Lays groundwork for #2.
- #2 — Module split (core + format-* + health).
- #4 — Move `MessageSink` to core. Falls out of #2.
- #5 — Decompose `MessageProcessorRegistry`. Bundle with format split.

Single 2.0 release with a migration guide. Provide a `kpipe-bom` BOM artifact so users get matching versions across
modules.

### Sprint 3 (independent of API changes)

- #6 — Test infrastructure cleanup. Can ship anytime.

---

## Open Questions for the Maintainer

1. **Is `HttpHealthServer` core, or a sample?** It's currently in `kpipe-consumer` — if it's example code, move to
   `examples/`. If it's part of the library contract, document it as such.
2. **Does Postgres testcontainers get used?** If yes, where? If no, remove.
3. **Is there an actual user demand for "consumer without OTel"?** This drives whether #3 is high or medium priority.
4. **Are there existing 1.x users in production?** Affects whether 2.0 should ship a compatibility shim package or
   hard-break.

---

## Completed Initiatives (merged from `.junie/PLAN.md`)

These shipped earlier. Listed for completeness so the roadmap stays a single source of truth.

### P1 — OTel Observability Dashboard ✅

- ✅ Local stack: `docker-compose.yaml` runs OpenTelemetry Collector + Prometheus + Grafana.
- ✅ Provisioned dashboard: `infra/observability/grafana/dashboards/kpipe-overview.json` covers consumer / backpressure /
  producer panels.
- ✅ README "Metrics Dashboard" section with `docker compose up` instructions.

### P2 — Full Demo Application ✅

- ✅ `examples/demo` module with `DemoApp.java` exercising JSON + Avro + Protobuf pipelines and demo sinks.
- ✅ Docker compose wiring + `scripts/run-demo.sh`.
- ✅ `examples/demo/src/test/java/org/kpipe/demo/DemoAppIntegrationTest.java` (Testcontainers).
- ✅ README "Running the Demo" section.

---

## P3 — Benchmark Improvements (pending)

Inherited from `.junie/PLAN.md`. Independent of the 2.0 module split; can ship anytime.

### B1 — Fairness Contract

- Align KPipe and Confluent Parallel Consumer benchmarks on concurrency, partitioning, key distribution, commit strategy,
  ordering guarantees, retry behavior.
- Add a side-by-side config table in benchmark sources so the comparison is reproducible.

### B2 — Parameterized Workload Matrix

- Add JMH parameters for payload size, partition count, processing cost profile, and concurrency levels.

### B3 — Latency Coverage

- Add latency metrics (p50/p95/p99) alongside throughput.

### B4 — Allocation and GC Profiling

- Dedicated profiling runs to capture allocation rate and GC behavior.

### B5 — Statistical Rigor and Reporting

- Increase iteration defaults for comparison runs.
- Export machine-readable outputs (CSV/JSON) for automated analysis.

### B6 — Real Kafka Validation

- Re-run a selected matrix on non-embedded Kafka to confirm the embedded results.

---

## P4 — Future Features (pending, post-2.0)

1. **Batch Sinks** — batching interface for `MessageSink` to improve throughput for database / HTTP targets.
2. **Multi-topic Support** — subscribe to multiple topics with topic-specific processors.
3. **Circuit Breaker** — circuit-breaker logic for sinks to prevent excessive retries when targets are down.

---

*Generated from architectural review session on 2026-05-02. Merged with `.junie/PLAN.md` content on the same date. Update
this file as items are completed; remove items rather than crossing them out so the priority ladder stays scannable.*
