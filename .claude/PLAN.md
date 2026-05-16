# KPipe — Roadmap & State of the Library

**Last updated:** 2026-05-15
**Current released line:** 1.12.0 (Maven Central) — batch sinks, Confluent SR, W3C tracing, circuit breaker, JUnit 6,
`enableMetrics` removal
**Active branch:** `feature/docs-sweep`

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
| Documentation                          | 7/10        |
| Test coverage of the public surface    | 8.5/10      |
| Aggregate                              | **~8.6/10** |

API quality is solid; the gap is **adoption-side documentation**: 1.12's batch / SR / tracing / CB
features are released but the README still describes the 1.11 surface. The backlog leads with
closing that gap, not adding more features.

---

## Prioritized backlog

| # | Priority               | Item                                                                                                                                 | Type      | Effort    |
|---|------------------------|--------------------------------------------------------------------------------------------------------------------------------------|-----------|-----------|
| 1 | **P1 — Adoption**      | Docs sweep — README sections for circuit breaker, tracing, Confluent SR, multi-topic, batch sinks; example apps for each new module  | Docs      | ~1–2 days |
| 2 | **P2 — Productivity**  | Testing primitives (`kpipe-test`) — `TestStream<T>` analogue of `TopologyTestDriver`, no Testcontainers required                     | Feature   | ~3–4 days |
| 3 | **P3 — User-visible**  | `Format.INSTANCE` hardening — JVM-global mutable singleton, real on paper, rarely hit; documented footgun for now                    | Hardening | ~1 day    |
| 4 | **P3 — User-visible**  | `AppConfig` slimdown / split — example infra; decide whether to split or move into `examples/`                                       | Refactor  | ~1 hr     |
| 5 | **P3 — User-visible**  | `HttpHealthServer` placement — lib contract or sample → `examples/`?                                                                 | Refactor  | ~30 min   |
| 6 | **P3 — User-visible**  | Residual build/test config cleanup (consumer parallelism, fork tuning)                                                               | Refactor  | ~30 min   |
| 7 | **P3 — Audience**      | Spring Boot starter — opt-in module, `@KPipeListener`, auto-wires from `application.yml`. **Only build if a Spring-shop user asks.** | Feature   | ~1 week   |
| 8  | **P5 — 2.0 candidate** | `MessageTracker` collapse — add `KPipeConsumer.waitForInFlightDrain(Duration)`, delete the standalone class                                                                | Breaking | ~half day |
| 9  | **P5 — 2.0 candidate** | `Result<T>` sealed type for pipeline outcomes — make filter vs fail explicit at the *type* level instead of through the §12 null/throw convention. See §P5 detail below.   | Breaking | ~1–2 days |
| 10 | **P5 — Speculative**   | Format serialization caches re-wire — only with JMH evidence of allocation/GC pressure                                                                                     | Perf     | ~1–2 days |

**Why this ordering:**

- **Docs first.** 1.12 is on Maven Central, but the README still describes the 1.11 surface — zero
  examples for CB, tracing, SR, batch sinks. Adoption decisions are made on docs; the doc score
  dropped because the codebase moved faster than the README. Cheap to close.
- **kpipe-test second.** Helps users who already adopted KPipe write faster tests. Real value,
  but it's a productivity multiplier — most useful after the new features are visible to them.
- **Spring starter held until asked.** ~1 week of work for an audience that already has Spring
  Kafka. Build it when a Spring-shop user actively asks, not speculatively.

---

## P1 — Docs sweep detail

README is currently 1.11-shaped. What's missing:

- **Circuit breaker section** — `Stream.withCircuitBreaker(0.5, 100, Duration.ofSeconds(30))` example,
  state machine diagram, OTel metric names.
- **Tracing section** — `Stream.withTracer(new OtelTracer(otel))`, headers-in-headers-out diagram,
  how downstream consumers pick up the parent span.
- **Confluent SR section** — `new ConfluentSchemaResolver(httpClient, baseUrl)` + `lookupBySubjectVersion`
  at startup, link to the examples app.
- **Batch sink section** — `Stream.toBatch(sink, BatchPolicy.of(100, Duration.ofSeconds(5)))`,
  `BatchSink.ofVoid(...)` convenience, the JMH headline (84× throughput at 1ms latency, batch 100).
- **Multi-topic section** — `KPipe.multi(props).json(t1, ...).avro(t2, ...).start()` example.
- **Example apps:** at minimum one app per new module (`examples/circuit-breaker`,
  `examples/tracing`, `examples/schema-registry`). Existing `examples/avro` already migrated to SR.

Side benefit: writing these examples flushes out doc gaps in the API itself.

---

## P2 — kpipe-test module detail

**Today:** writing a unit test for a KPipe pipeline means either Testcontainers (10s+ per test,
Docker-dependent) or hand-rolling a `MockConsumer` harness. The JMH bench has the second; users don't.

**Target ergonomics:**

```
final var captured = new CapturingSink<Map<String, Object>>();
final var driver = TestStream.<Map<String, Object>>builder(JsonFormat.INSTANCE)
    .pipe(addTimestamp)
    .filter(active)
    .toCustom(captured)
    .build();
driver.send(record1);
driver.send(record2);
driver.flush();
assertEquals(List.of(...), captured.captured());
```

**Surface decisions (open):**

1. **Drive model.** Option (a) Kafka-free pipeline-direct invocation; option (b) MockConsumer-backed
   `KPipeConsumer` on a synchronous executor. Recommendation: **(b)** — exercises real code paths,
   stays in sync as the consumer evolves. Cost is ~5–20ms/test vs ~1ms.
2. **JUnit integration.** v1 plain API only; `@KPipeTest` extension is a follow-up.
3. **Assertion API.** Just `captured()` returning `List<T>` — let users use AssertJ / Hamcrest /
   plain `assertEquals`. Don't invent a DSL.
4. **Multi-topic.** Single-topic v1, multi-topic follow-up.
5. **Batch helpers.** Size-flush + shutdown-flush v1; virtual-clock age-flush is a follow-up
   (interaction with `ScheduledFuture` needs care).
6. **Module type.** Full `compile` artifact (not `test`-classifier) — it's a runtime tool for users'
   test suites.

**Why P2 productivity:** every existing and future user benefits. Cuts test feedback loops 100×.
Removes the Docker-required friction for CI environments without Docker.

---

## Module taxonomy

```
kpipe-metrics ← kpipe-core ← kpipe-consumer
                          ← kpipe-producer
                          ← kpipe-format-{json, avro, protobuf}
                          ← kpipe-api  (← KPipe facade)
kpipe-metrics-otel              ← kpipe-metrics              (opt-in OTel metrics)
kpipe-tracing-otel              ← kpipe-producer             (opt-in OTel tracing)
kpipe-schema-registry-confluent ← kpipe-core                 (opt-in Confluent SR client)
kpipe-bom                                                    (BOM — pins versions)
```

| Module                            | What's in it                                                                                                                                                               |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `kpipe-bom`                       | Maven BOM — pins all `kpipe-*` artifacts to matching versions                                                                                                              |
| `kpipe-core`                      | Format-agnostic pipeline machinery: `MessageProcessorRegistry`, `MessageFormat` / `SchemaAwareFormat`, `MessageSink`, `Operators`, `MessagePipeline`, `SchemaResolver` SPI |
| `kpipe-metrics`                   | Metrics interfaces (`ConsumerMetrics`, `ProducerMetrics`) + log-based reporters; **no OTel API** on classpath                                                              |
| `kpipe-metrics-otel`              | OpenTelemetry-backed metrics implementation (opt-in)                                                                                                                       |
| `kpipe-tracing-otel`              | W3C trace context propagation through Kafka headers; OTel tracer implementation (opt-in)                                                                                   |
| `kpipe-schema-registry-confluent` | Confluent Schema Registry client (`ConfluentSchemaResolver`); schema-by-ID + by-subject-version lookup (opt-in)                                                            |
| `kpipe-producer`                  | Kafka producer wrapper, `KafkaMessageSink`, `KafkaProducerConfig`, `Tracer` SPI                                                                                            |
| `kpipe-consumer`                  | `KPipeConsumer`, `KPipeRunner`, `BackpressureController`, `CircuitBreakerController`, `PauseCoordinator`, `KafkaOffsetManager`, `HttpHealthServer`                         |
| `kpipe-format-json`               | `JsonFormat`, `JsonConsoleSink`                                                                                                                                            |
| `kpipe-format-avro`               | `AvroFormat` (owns its own schema registry), `AvroConsoleSink`                                                                                                             |
| `kpipe-format-protobuf`           | `ProtobufFormat` (owns its own descriptor registry), `ProtobufConsoleSink`                                                                                                 |
| `kpipe-api`                       | `KPipe` fluent facade — `Stream<T>`, `Sink<T>`, `Handle`, `MultiBuilder`                                                                                                   |

**Still planned:**

| Module                      | What's in it                                                                                           |
|-----------------------------|--------------------------------------------------------------------------------------------------------|
| `kpipe-test`                | `TestStream<T>` for unit-testing pipelines without Testcontainers; `.send(record).flush()` ergonomics. |
| `kpipe-spring-boot-starter` | Auto-wiring of `KPipeConsumer` from `application.yml`. Opt-in; no Spring on `kpipe-core`'s classpath.  |

---

## P3 detail

### `Format.INSTANCE` hardening

JVM-global mutable singleton holding schema/descriptor maps. Two pipelines in the same process can
step on each other via `AvroFormat.INSTANCE.addSchema(...)`. Practical hit-rate is low — most users
have one app with one consumer. Docs carry an explicit footgun warning.

**If this ever bites someone:** make `addSchema` available only on `new AvroFormat()` instances and
route the facade through a per-stream format instance (preserves API), or delete `INSTANCE` entirely
(clean break, requires migration). Both are breaking. Defer until someone hits it.

### `AppConfig` slimdown

Real example infra; likely belongs in `examples/` rather than crossing module boundaries.

### `HttpHealthServer` placement

Currently in `kpipe-consumer`. Decide: lib contract (document and own) or sample (move to
`examples/`). ~30 minutes either way once the call is made.

### Build/test config cleanup

Residual fork tuning, consumer parallelism settings. Cleanup pass; no functional change.

### Spring Boot starter

Audience-multiplier feature. Auto-wire `KPipeConsumer` from `application.yml`, expose
`@KPipeListener` or similar declarative entry point, hook health probes to Spring Boot Actuator.
The starter is the opt-in; no Spring on `kpipe-core`'s classpath. ~1 week — non-trivial because
Spring's lifecycle and config-binding semantics are their own learning curve.

**Why P3:** higher effort than the P2 items, audience overlap is real but not 100% (Spring Boot
Kafka users already have Spring Kafka). Promote to P2 if a Spring-shop user explicitly asks.

---

## P5 candidates (2.0 / speculative)

### `MessageTracker` collapse

Add `KPipeConsumer.waitForInFlightDrain(Duration)` and delete the standalone `MessageTracker` class.
Fewer types, same capability. Breaking but trivial migration.

### `Result<T>` sealed type for pipeline outcomes

§12 settled "null = filter, throw = fail" as a hard rule and the protobuf `skipBytes(5)` burn
proved the value. The convention works because it's enforced *by discipline* — readers know null
means filter, but the compiler doesn't. Move that enforcement to the type system:

```java
sealed interface Result<T> {
  record Passed<T>(T value) implements Result<T> {}
  record Filtered<T>() implements Result<T> {}
  record Failed<T>(Throwable cause) implements Result<T> {}
}
```

`MessagePipeline.apply()` returns `Result<T>` instead of `T` (or null, or throwing). Operators in
`Operators` and user `UnaryOperator`s stay typed; only the pipeline-runtime boundary upgrades. The
consumer pattern-matches on the result:

```java
switch (pipeline.apply(record)) {
  case Result.Passed<T> p -> sink.accept(p.value());
  case Result.Filtered<T> _ -> markOffsetProcessed(record);
  case Result.Failed<T> f -> handleProcessingError(record, f.cause());
}
```

Wins:
- Filter and fail become distinct *types*, not overloaded channels. A reader can't accidentally
  conflate them, and the compiler enforces exhaustive handling.
- The §12 doctrine becomes machine-checked. Future refactors can't accidentally introduce a
  null-swallow path because there's no null to swallow.
- Operators can opt into structured filtering via a `Filtered.fromPredicate(...)` helper, making
  the "intentional filter" intent explicit instead of "return null and trust the caller".

Tradeoffs:
- Every operator call site allocates a `Result.Passed` wrapper. For the hot path that's one
  allocation per record per stage — measurable in JMH. Mitigation: cache `Filtered` (it's empty
  anyway) and consider a value-class form once Valhalla lands.
- Public API break — every existing `UnaryOperator<T>` continues to work, but `MessagePipeline`'s
  return type changes. Per §16, all callers migrate in the 2.0 PR.

**Decision was deferred in 1.x**: the PLAN previously rejected this as "re-litigates the §12
doctrine" — but §12 is a convention atop nullable returns, not a type-level guarantee. The 2.0
window is the right place to upgrade convention → type. Wait for evidence (a real bug that
typed Results would have caught at compile time) or for a 2.0 budget before pulling the trigger.

### Format serialization caches

Only revisit when allocation rate or GC pressure shows up in profiling.

To resurrect as a real optimization:

1. Move the `inScopedCaches` boundary from per-format-call to per-poll-batch (consumer wraps the record loop).
2. Have each format's serialize / deserialize read `OUTPUT_STREAM_CACHE.get()` (with `.reset()` for buffer reuse)
   and pass the cached encoder / decoder to `EncoderFactory.binaryEncoder(out, prev)` for stateful reuse.
3. Handle the multi-schema corner case carefully — `BinaryEncoder` and `Schema.Parser` carry state that doesn't
   transfer cleanly across schemas. With multi-topic Phase 2 (per-topic dispatch, heterogeneous schemas) this
   becomes a per-schema lookup, not a single ScopedValue.
4. Land **only with JMH evidence** that the cache reuse exceeds `ScopedValue` lookup overhead.

**Format-specific note:** Protobuf does not benefit — `Message` is immutable (must allocate per
record), `CodedOutputStream` / `CodedInputStream` are thin wrappers. Scope any caching work to Avro
and JSON only.

---

## Strengths to preserve

Don't regress these in any future refactor:

- **Byte boundary at the consumer entry point** — `KPipeConsumer<K>` operates on `byte[]` values. Format SerDe lives in
  the pipeline.
- **Single SerDe cycle** — one deserialize, many transforms, one serialize. Genuinely good for throughput.
- **Virtual threads + ScopedValue** — thread-per-record without ThreadLocal scalability traps.
- **Lowest-pending-offset commits** — at-least-once safety even with parallel processing.
- **Strategy-based backpressure with hysteresis** — in-flight for parallel, lag for sequential.
- **PauseCoordinator unification** — multiple pause sources (manual, backpressure, CB) can't auto-resume each other.
- **Clean module dependency direction** — no cycles, no sideways leaks, no split packages.
- **OTel as opt-in interface, not transitive dep** — `kpipe-metrics` ships interfaces only.
- **Concurrency safety in `KPipeConsumer`** — single-read CAS, error-handler-throw safety, nested-finally shutdown,
  `LockSupport.park` for paused state.
- **Explicit error semantics in `MessagePipeline`** — no silent null swallowing.
- **Immutable `DefaultStream`** — branching from a common root is safe.
- **Professional Maven publishing** — proper signing, POM metadata, separate artifacts, BOM for version unification.
- **§16 no-deprecation policy** — delete and migrate, don't carry dead overloads.

---

## Open questions

1. **`kpipe-test` test runner integration** — JUnit 5/6 first-class (`@KPipeTest` extension), or
   plain `TestStream` and let users wire it themselves? Recommendation: plain v1, extension follow-up.
2. **Tracing default-on or opt-in?** — `kpipe-tracing-otel` is opt-in by module choice. Should the
   facade auto-enable trace propagation when the module is on the classpath, or stay explicit
   (`.withTracer(...)`)?
3. **Confluent SR scope beyond v1** — TTL cache for `lookupById` (hot-path) and wire-envelope
   auto-lookup in `AvroFormat`/`ProtobufFormat` decode — both deferred from the initial SR module.
   Schedule for 1.13?
4. **Spring starter audience confirmation** — has a Spring-shop user actually asked, or is this
   speculative? Promote to P2 if real demand exists.
5. **Are there 1.x users in production?** Affects whether 2.0 ships a compat shim package or hard-breaks.
