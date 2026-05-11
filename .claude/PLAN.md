# KPipe — Roadmap & State of the Library

**Last updated:** 2026-05-09
**Current released line:** 1.11.0 (Maven Central)
**Active work:** none — `main` is clean. Backlog refreshed 2026-05-09 (this revision).

---

## Public API self-assessment (current)

| Dimension                              | Score       |
|----------------------------------------|-------------|
| Common-path ergonomics                 | 9/10        |
| Customization / escape hatches         | 9/10        |
| Discovery (does the API teach itself?) | 8.5/10      |
| Type safety                            | 9/10        |
| Error semantics                        | 10/10       |
| Module taxonomy                        | 9.5/10      |
| Documentation                          | 8/10        |
| Test coverage of the public surface    | 8.5/10      |
| Aggregate                              | **~8.7/10** |

API quality is solid; what separates KPipe from "niche tool" is **ecosystem reach** — Confluent SR support,
testing primitives, distributed tracing, Spring interop. The refreshed backlog leans toward those.

---

## Prioritized backlog (refreshed 2026-05-09)

| #  | Priority                      | Item                                                                                                                                                                                 | Type              | Effort    |
|----|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|-----------|
| 1  | ~~**P1 — Adoption blocker**~~ | ~~Multi-topic Phase 1 (homogeneous) + Phase 2 (heterogeneous via `KPipe.multi`)~~ — both landed 2026-05-07                                                                           | Feature           | shipped   |
| 2  | ~~**P1 — Adoption blocker**~~ | ~~Java baseline decision~~ — **stay on Java 25** (decided 2026-05-07)                                                                                                                | Strategy          | decided   |
| 3  | ~~**P2 — Bug surface**~~      | ~~Batch sinks (`BatchSink<T>` interface + size/time flush + per-record DLQ + parallel mode + multi-topic + JMH bench)~~ — landed 2026-05-08/09 across 8 commits                      | Feature           | shipped   |
| 4  | **P2 — Adoption**             | Confluent Schema Registry module (`kpipe-schema-registry-confluent`) — schema-by-ID lookup from wire envelope, local cache; absorbs HTTP-fetcher extraction from `kpipe-format-avro` | Feature           | ~3–4 days |
| 5  | **P2 — Productivity**         | Testing primitives (`kpipe-test`) — `TestStream<T>` with `.send().expect()`, no Testcontainers required (Kafka-Streams' `TopologyTestDriver` analogue)                               | Feature           | ~3–4 days |
| 6  | **P2 — Production-readiness** | W3C trace context propagation through Kafka headers + tracer integration (`kpipe-tracing-otel`)                                                                                      | Feature           | ~2–3 days |
| 7  | **P2 — Bug surface**          | Circuit breaker for sinks — hand-rolled inside `kpipe-consumer`, mirrors `BackpressureController` shape                                                                              | Feature           | ~3–4 days |
| 8  | **P3 — User-visible**         | `Format.INSTANCE` global mutable state hardening (real on paper, rarely hit in practice; documented as known footgun for now)                                                        | Hardening         | ~1 day    |
| 9  | **P3 — User-visible**         | `AppConfig` slimdown / split (real example infra; decide split or move)                                                                                                              | Refactor          | ~1 hr     |
| 10 | **P3 — User-visible**         | `HttpHealthServer` placement decision (lib contract or sample → `examples/`?)                                                                                                        | Refactor          | ~30 min   |
| 11 | **P3 — User-visible**         | Build/test config cleanup remaining (consumer parallelism, residual fork tuning)                                                                                                     | Refactor          | ~30 min   |
| 12 | **P3 — Audience**             | Spring Boot starter (`kpipe-spring-boot-starter`) — auto-wires `KPipeConsumer` from `application.yml`; opt-in, no Spring in core                                                     | Feature           | ~1 week   |
| 13 | **P5 — 2.0 candidates**       | `MessageTracker` collapse — add `KPipeConsumer.waitForInFlightDrain(Duration)`, delete the standalone class                                                                          | Breaking refactor | ~half day |
| 14 | **P5 — Speculative perf**     | Format serialization caches re-wire (only with JMH evidence)                                                                                                                         | Perf              | ~1–2 days |

**What's gone from the previous revision** (and why):

- ~~Type-name shortening (`MessageProcessorRegistry` → `Pipelines`)~~ — bikeshedding. Breaking change for cosmetics.
  Drop unless 2.0 ships for a real reason.
- ~~`Result<T>` sealed type for pipeline errors~~ — re-litigates the §12 doctrine (`null` = filter, throw = fail), which
  is settled and battle-tested. Drop unless evidence emerges.
- ~~Fold `KPipeRunner` into `KPipeConsumer`~~ — the split has architectural meaning (consumer = engine, runner =
  lifecycle host). Folding removes a clean boundary for nominal LOC. Drop.
- HTTP fetcher extraction (#4 in old plan) — folded into the new `kpipe-schema-registry-confluent` item; "extract and
  delete" loses functionality, "extract into a real SR module" turns it into an adoption feature for similar effort.
- `Format.INSTANCE` (was P2 #3) — downgraded to P3. Practical hit-rate is low; doc warning is the cheap mitigation.

**Active P2 ordering rationale:** schema-registry first because it's a day-1 adoption blocker for Confluent
shops; testing primitives second because every existing user benefits immediately; tracing third for production
observability; CB last because it's incident-time value, not an adoption driver. P3 items are cheap to close
in spare cycles.

### Recently shipped

- **2026-05-09 — batch sinks v1 → v2 → cleanup** (PR #100 + 7 main commits): single-session push that took
  the batch story end-to-end.
    1. **v1 — single-topic, sequential** (`5939d26`): `Stream.toBatch(BatchSink<T>, BatchPolicy)` facade
        + `BatchPipelineWrapper` (per-topic buffer, age-tick scheduler via virtual-thread
          `ScheduledExecutorService`, drain-on-close), `MessagePipeline.processToValue(byte[])` so the
          consumer can buffer the deserialized value alongside the originating record. Whole-batch DLQ on
          failure; offsets committed only after the sink returns.
    2. **`DefaultStream` Mut helper** (`c85a04e`): collapsed 13 hand-rolled 15-arg constructor calls
       in the wither methods to one `mutate(Consumer<Mut<T>>)` funnel; adding a field is a 4-spot edit
       contained in one file. **−63 lines** on a 353-line file.
    3. **PartialBatchSink for per-record DLQ** (`0b888e6`): `BatchResult(succeededIndexes, failedByIndex)`
       with `allSucceeded` / `allFailed` factories; `Stream.toBatchPartial`; `BatchPipelineWrapper`
       grew a `Flusher` strategy interface to host both whole-batch and partial-batch flushers. **+962
       / −29**, 8 unit tests + a Testcontainers integration test that drains the DLQ topic with a
       second consumer.
    4. **Test flake fix** (`52c10e1`): `StreamBatchIntegrationTest` now waits for `messagesReceived
       > = N` before shutdown so the trailing batch isn't drained before the consumer has finished
       polling. Pre-existing latent flake surfaced after the partial-batch flusher refactor reshuffled
       timing.
    5. **Parallel-mode batching** (`4459530`): dropped the v1 sequential-only restriction. `BatchPipeline-
       Wrapper.bufferedCount()` participates in the in-flight backpressure metric so a slow batch sink
       under parallel mode can't let the buffer grow unbounded; the wrapper's `ReentrantLock` was
       already concurrency-safe by design. **+565 / −27** including a multi-partition Testcontainers
       test and a backpressure-pause test with watermarks `(50, 25)` + 10ms-per-flush sink.
    6. **JMH bench** (`50066bb` + README update): `BatchSinkLatencyBenchmark` in the existing `benchmarks/`
       module, parameterised over `batchSize ∈ {1, 10, 100}` × `sinkLatencyMicros ∈ {10, 100, 1000}`.
       Drives the public facade through `MockConsumer` so `BatchPipelineWrapper` stays package-private.
       Headline: at `sinkLatencyMicros=1000` (≈ JDBC commit), `batchSize=100` yields **84× the
       throughput** of `batchSize=1`. Published in `benchmarks/README.md`.
    7. **Multi-topic batch routes** (PR #100): lifted single-topic restriction. `KPipeConsumer.Builder.batchSpec`
       → `Map<String, BatchSpec<?>>`; `withBatchPipeline` and `withPartialBatchPipeline` are multi-call.
       `MultiBuilder` accepts `DefaultBatchSink` / `DefaultPartialBatchSink` from the configurator —
       no new MultiBuilder methods, the existing `.json/.avro/...` routes compose with `.toBatch /
       .toBatchPartial`. Disjoint-topic invariant validated at `build()`.
    8. **`BatchSink` + `PartialBatchSink` unification** (PR #100, second commit): collapsed the two
       interfaces into one `BatchSink<T> extends Function<List<T>, BatchResult>`. `BatchSink.ofVoid(consumer)`
       wraps void-style consumers (success → `allSucceeded`, throw → `allFailed`). Deletes:
       `PartialBatchSink`, `DefaultPartialBatchSink`, `withPartialBatchPipeline`, the `Flusher` strategy
       interface, the `(sink == null) ^ (partialSink == null)` invariant in `BatchSpec`, the partial-
       batch unit test (its scenarios survive in the wrapper concurrency test). MultiBuilder's
       `instanceof` cascade goes 3-way → 2-way. **−762 / +274** across 21 files, 4 file deletions.
- **2026-05-08 — api module cleanup** (`refactor/api-cleanup`): `DefaultStream` converted to record (11
  field decls + 11 ctor assignments + 9 trivial accessors → auto-generated);
  `KPipeConsumer.Builder.enableMetrics(boolean)`
  deleted (only `@Deprecated` in the codebase, all 9 callers migrated); `ToConsoleDispatchTest` parameterized;
  redundant `KPipeFacadeBuildTest` factory tests dropped; `validateTopics` single-pass. Net **−160 lines**.
- **2026-05-07 — multi-topic** (`feature/multi-topic`): both phases landed in one branch. Homogeneous via
  `KPipe.json/avro/.../bytes/custom(Collection<String>, Properties)`; heterogeneous via
  `KPipe.multi(props).json(topic, configurator).avro(...).start()`. Topic-aware OTel metrics with
  `computeIfAbsent` `Attributes` cache. `Stream.skipBytes(int)` for Confluent envelope handling. Demo
  collapsed from 3 explicit-API runners (~250 lines) to one `KPipe.multi(...)` (~50 lines).
- **2026-05-07 — P4 internal polish backlog cleared**: `OffsetState`/`ConsumerState` out of the
  `enums` sub-package; vestigial `MessageProcessorRegistry.sourceAppName` deleted; `KPipeProducer.sendAsync`
  silent-metrics footgun fixed; `OffsetManager.createRebalanceListener()` made a `default` interface
  method; `kpipe-metrics-otel/module-info.java` docstring added. 8 audits/refactors total.

---

## Module taxonomy

```
kpipe-metrics ← kpipe-core ← kpipe-consumer
                          ← kpipe-producer
                          ← kpipe-format-{json, avro, protobuf}
                          ← kpipe-api  (← KPipe facade)
kpipe-metrics-otel ← kpipe-metrics                (opt-in OTel impl)
kpipe-bom                                          (BOM — pins versions)
```

| Module                  | What's in it                                                                                                                                         |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `kpipe-bom`             | Maven BOM — pins all `kpipe-*` artifacts to matching versions                                                                                        |
| `kpipe-core`            | Format-agnostic pipeline machinery: `MessageProcessorRegistry`, `MessageFormat` / `SchemaAwareFormat`, `MessageSink`, `Operators`, `MessagePipeline` |
| `kpipe-metrics`         | Metrics interfaces (`ConsumerMetrics`, `ProducerMetrics`) + log-based reporters; **no OTel API** on classpath                                        |
| `kpipe-metrics-otel`    | OpenTelemetry-backed implementation (opt-in)                                                                                                         |
| `kpipe-producer`        | Kafka producer wrapper, `KafkaMessageSink`, `KafkaProducerConfig`                                                                                    |
| `kpipe-consumer`        | `KPipeConsumer`, `KPipeRunner`, `BackpressureController`, `KafkaOffsetManager`, `HttpHealthServer`                                                   |
| `kpipe-format-json`     | `JsonFormat`, `JsonConsoleSink`                                                                                                                      |
| `kpipe-format-avro`     | `AvroFormat` (owns its own schema registry), `AvroConsoleSink`                                                                                       |
| `kpipe-format-protobuf` | `ProtobufFormat` (owns its own descriptor registry), `ProtobufConsoleSink`                                                                           |
| `kpipe-api`             | `KPipe` fluent facade — `Stream<T>`, `Sink<T>`, `Handle`                                                                                             |

**Planned new modules** (from the refreshed backlog):

| Module                            | What's in it                                                                                                                                                                     |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `kpipe-schema-registry-confluent` | Confluent Schema Registry client: schema-by-ID lookup from wire envelope, local schema cache, version migration. Absorbs the `http://` fetcher currently in `kpipe-format-avro`. |
| `kpipe-test`                      | `TestStream<T>` for unit-testing pipelines without Testcontainers; `.send(record).expect(output)` ergonomics.                                                                    |
| `kpipe-tracing-otel`              | W3C trace context propagation through Kafka headers; OTel tracer integration (opt-in, no API in `kpipe-core`).                                                                   |
| `kpipe-spring-boot-starter`       | Auto-wiring of `KPipeConsumer` from `application.yml`. Opt-in; no Spring on `kpipe-core`'s classpath.                                                                            |

---

## Production-readiness roadmap (detail for active P2 items)

### P2 #4 — Confluent Schema Registry module

- **Today:** `kpipe-format-avro` carries `java.net.http` + `jackson-core` (`requires` in `module-info.java`)
  to support `AvroFormat.readSchemaFromLocation` with `http://` URLs. The `addSchema(key, schemaJson)` 2-arg
  overload covers inline / classpath cases without any HTTP. So `kpipe-format-avro` pays a transitive cost
  for a feature that's only used by Confluent users.
- **Target:** New module `kpipe-schema-registry-confluent` that:
    - Provides a real Confluent SR client (schema-by-ID lookup, local cache with TTL or revision-based
      invalidation, version migration helpers).
    - Hosts the `http://` fetching code currently in `kpipe-format-avro`, so the format module can drop
      `java.net.http` and `jackson-core` from `requires`.
    - Plugs into both `AvroFormat` and `ProtobufFormat` via a small SPI in `kpipe-core`
      (e.g., `interface SchemaResolver { Schema lookup(int schemaId); }`).
    - User wires it via builder: `.withSchemaResolver(new ConfluentSchemaResolver(srUrl, ttl))`.
- **Why P2 adoption:** Confluent SR is the de-facto standard for Avro/Protobuf at scale. Without first-class
  support, every shop running Confluent has to either hand-roll fetching or use the `http://` URL hack.
  Day-1 friction.
- **Scope guard:** stays a *consumer* of SR. No schema *publishing* in v1 (that's a producer-side feature
  and Confluent's own producer client already does it well).
- **Effort:** ~3–4 days including unit tests, a Testcontainers integration test against
  `confluentinc/cp-schema-registry`, and the format-module dependency cleanup.

### P2 #5 — Testing primitives (`kpipe-test`)

- **Today:** writing a unit test for a KPipe pipeline means either spinning up Testcontainers (slow,
  Docker-dependent, 10s+ per test) or hand-rolling a `MockConsumer`-based harness. The hand-rolled path
  is what the JMH bench does for `BatchSinkLatencyBenchmark`, but that's not packaged for users.
- **Target:** A `kpipe-test` module mirroring Kafka Streams' `TopologyTestDriver` ergonomics:
  ```java
  final var driver = TestStream.<Map<String, Object>>builder()
      .pipe(addTimestamp)
      .filter(active)
      .toCustom(captureSink)
      .build();
  driver.send(record1);
  driver.send(record2);
  driver.flush();
  assertEquals(List.of(...), captureSink.captured());
  ```
- **What it ships:**
    - `TestStream<T>` — drives a single pipeline without Kafka (`MockConsumer` + the existing pipeline
      machinery + a synchronous executor so the test thread sees ordering immediately).
    - `CapturingSink<T>` — first-class instead of hand-rolled per test (matches the §Testing Strategy
      convention).
    - Helpers for `BatchSink` testing (verify flush at size / age / shutdown).
- **Why P2 productivity:** every existing and future user benefits. Cuts test feedback loops from 10s+
  to milliseconds. Removes the Docker-required friction for CI environments without Docker (matches
  the "Codecov 38% patch coverage" gap discovered during the batch sink work).
- **Effort:** ~3–4 days. Most of the machinery already exists (`MockConsumer` plumbing in the JMH
  bench is ~80% of what a `TestStream` needs).

### P2 #6 — W3C trace context propagation

- **Today:** OTel **metrics** are wired via `kpipe-metrics-otel` (`kpipe.consumer.*` instruments). OTel
  **traces** are not. A record produced by service A with a `traceparent` header lands in KPipe and the
  span context is dropped. Downstream services see a brand-new trace, breaking distributed correlation
  across the Kafka boundary.
- **Target:** New module `kpipe-tracing-otel`:
    - On record receive, extract W3C `traceparent` from Kafka headers.
    - Start a span for the pipeline with the upstream as parent.
    - Inject `traceparent` into outbound producer records (DLQ + `KafkaMessageSink`).
    - Tracer is configured opt-in: `.withTracer(new OtelTracer(tracerProvider))`.
- **Why P2 production-readiness:** distributed tracing is table stakes for prod observability. Without
  it, KPipe is invisible in Jaeger / Tempo / Honeycomb traces — a cliff between upstream and downstream
  spans.
- **Scope guard:** v1 does W3C propagation only. B3, Datadog headers, and custom propagators come later
  if asked.
- **Effort:** ~2–3 days including a Testcontainers integration test that asserts the parent span ID is
  preserved end-to-end.

### P2 #7 — Circuit breaker for sinks (hand-rolled)

- **Today:** if a sink fails (DB down, HTTP 503), retries are unbounded and per-record. Backpressure helps
  but doesn't stop the bleeding. Every record during an outage runs `maxRetries+1` attempts and lands in
  the DLQ.
- **Target:** `CircuitBreakerController` record next to `BackpressureController`. Same architectural
  pattern: pure-function record, takes thresholds + window + strategy, returns `Action {OPEN, CLOSE,
  PROBE, NONE}`. Wired into `KPipeConsumer.checkBackpressure`-style hook so trip → `internalPause()`,
  half-open → `internalResume()` and the next record acts as the probe.
    - Sliding window: `AtomicLongArray` ring buffer (success / failure counters per bucket) — ~30 lines.
    - State machine: CLOSED / OPEN / HALF_OPEN with single-read CAS transitions per §11 — ~50 lines.
    - Half-open timer: virtual-thread `ScheduledExecutorService` mirroring the `batchScheduler` precedent.
    - Facade: `Stream.withCircuitBreaker(double failureThreshold, int windowSize, Duration openDuration)`.
    - Metrics: `recordCircuitBreakerTrip`, `recordCircuitBreakerState(State)`, `recordCircuitBreakerTimeOpen(ms)`
      on `ConsumerMetrics` + OTel impl.
- **Why hand-rolled, not Resilience4j:** Resilience4j duplicates infrastructure already in `kpipe-consumer`
  (state machine, command queue serialization onto consumer thread, scheduled executor for deferred work,
  pause/resume choreography via `manualPause` + `LockSupport.park/unpark`). Adopting it for CB-only would
  mean running parallel state machines or reducing Resilience4j to a thin wrapper using ~10% of its API.
  If KPipe later grows resilience features (rate limit, bulkhead, slow-call detection, exponential
  backoff with jitter), revisit; one-time migration is mechanical. Hand-rolling here is the *reversible*
  decision.
- **Effort:** ~3–4 days including virtual-thread concurrency tests using `Thread.ofVirtual()` +
  `CountDownLatch` per the §Testing Strategy convention.

---

## P3 items (cheap to close in spare cycles)

### P3 #8 — `Format.INSTANCE` hardening (downgraded from P2)

JVM-global mutable singleton holding schema/descriptor maps. Theoretical risk: two pipelines in the same
process step on each other via `AvroFormat.INSTANCE.addSchema(...)`. Practical hit-rate is low — most
users have one app with one consumer. Multi-tenant in a single JVM with conflicting registrations is
unusual.

**Cheap mitigation already in place:** docs carry an explicit "Footgun warning."

**If this ever bites someone:** make `addSchema` available only on `new AvroFormat()` instances and route
the fluent facade through a per-stream format instance (preserves API), or delete `INSTANCE` entirely
(clean break, requires migration). Both are breaking. Defer until someone hits it.

### P3 #9 — `AppConfig` slimdown

Real example infra; likely belongs in `examples/` rather than crossing module boundaries.

### P3 #10 — `HttpHealthServer` placement

Currently in `kpipe-consumer`. Decide: lib contract (document and own) or sample (move to `examples/`).
~30 minutes either way once the call is made.

### P3 #11 — Build/test config cleanup

Residual fork tuning, consumer parallelism settings. Cleanup pass; no functional change.

### P3 #12 — Spring Boot starter

Audience-multiplier feature. Auto-wire `KPipeConsumer` from `application.yml`, expose `@KPipeListener`
or similar declarative entry point, hook health probes to Spring Boot Actuator. The starter is the opt-in;
no Spring on `kpipe-core`'s classpath. ~1 week — non-trivial because Spring's lifecycle and
config-binding semantics are their own learning curve.

**Why P3 not P2:** higher effort than the P2 items and the audience overlap is real but not 100%
(Spring Boot Kafka users already have Spring Kafka; the win is for users who picked KPipe but use Spring
Boot for everything else). Promote to P2 if a Spring-shop user explicitly asks.

---

## P5 candidates (2.0 / speculative)

### P5 #13 — `MessageTracker` collapse

Add `KPipeConsumer.waitForInFlightDrain(Duration)` and delete the standalone `MessageTracker` class.
Fewer types, same capability. Breaking but trivial migration.

### P5 #14 — Format serialization caches

Only revisit when allocation rate or GC pressure shows up in profiling.

To resurrect as a real optimization:

1. Move the `inScopedCaches` boundary from per-format-call to per-poll-batch (consumer wraps the record loop).
2. Have each format's serialize / deserialize read `OUTPUT_STREAM_CACHE.get()` (with `.reset()` for buffer reuse)
   and pass the cached encoder / decoder to `EncoderFactory.binaryEncoder(out, prev)` for stateful reuse.
3. Handle the multi-schema corner case carefully — `BinaryEncoder` and `Schema.Parser` carry state that doesn't
   transfer cleanly across schemas. With multi-topic Phase 2 (per-topic dispatch, heterogeneous schemas) this
   becomes a per-schema lookup, not a single ScopedValue.
4. Land **only with JMH evidence** that the cache reuse exceeds `ScopedValue` lookup overhead.

**Format-specific note:** Protobuf does not benefit from the same pattern — `Message` is immutable (must
allocate per record), `CodedOutputStream` / `CodedInputStream` are thin wrappers (cheap to allocate). Scope
any caching work to Avro and JSON only.

---

## Strengths to preserve

Don't regress these in any future refactor:

- **Byte boundary at the consumer entry point** — `KPipeConsumer<K>` operates on `byte[]` values. Format SerDe lives in
  the pipeline.
- **Single SerDe cycle** — one deserialize, many transforms, one serialize. Genuinely good for throughput.
- **Virtual threads + ScopedValue** — thread-per-record without ThreadLocal scalability traps.
- **Lowest-pending-offset commits** — at-least-once safety even with parallel processing.
- **Strategy-based backpressure with hysteresis** — in-flight for parallel, lag for sequential.
- **Clean module dependency direction** — no cycles, no sideways leaks, no split packages.
- **OTel as opt-in interface, not transitive dep** — `kpipe-metrics` ships interfaces only.
- **Concurrency safety in `KPipeConsumer`** — single-read CAS, error-handler-throw safety, nested-finally shutdown,
  `LockSupport.park` for paused state.
- **Explicit error semantics in `MessagePipeline`** — no silent null swallowing.
- **Immutable `DefaultStream`** — branching from a common root is safe.
- **Professional Maven publishing** — proper signing, POM metadata, separate artifacts, BOM for version unification.

---

## Open questions for the maintainer

1. **Confluent SR scope in v1** — read-only client (lookup by ID), or also include schema validation /
   compatibility checks? Read-only is a tight ~3–4 day scope; full validation adds a week.
2. **`kpipe-test` test runner integration** — JUnit 5 first-class (provide a `@KPipeTest` extension), or
   plain `TestStream` and let users wire it themselves?
3. **Tracing default-on or opt-in?** — `kpipe-tracing-otel` is opt-in by module choice. Should the
   facade auto-enable trace propagation when the module is on the classpath, or stay explicit
   (`.withTracer(...)`)?
4. **Spring starter audience confirmation** — has a Spring-shop user actually asked, or is this
   speculative? Promote to P2 if real demand exists.
5. **Are there 1.x users in production?** Affects whether 2.0 ships a compat shim package or hard-breaks.
