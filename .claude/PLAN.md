# KPipe — Roadmap & State of the Library

**Last updated:** 2026-05-16
**Current released line:** 1.12.0 (Maven Central) — batch sinks, Confluent SR, W3C tracing, circuit breaker, JUnit 6,
`enableMetrics` removal
**Queued for release:** 1.13.0 on `main` — three breaking changes (`Result<T>` sealed type, Avro/Protobuf codec /
catalog split, `ConsumerHealthController` facade collapsing `PauseCoordinator` + `CircuitBreakerStats`) plus Codecov
test-results upload and CI/release wiring for `kpipe-schema-registry-confluent` + `kpipe-tracing-otel`.
**Active branch:** main (clean)

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

API quality is solid; the 1.13 cycle closed three breaking-change items that had been deferred to a future major
(Result<T>, INSTANCE singleton removal, pause/CB/backpressure consolidation) and brought README + Javadoc back in
sync with the 1.12 + 1.13 surface. The remaining gap is **productivity tooling for adopters** (`kpipe-test`) — the
backlog now leads with that.

---

## Prioritized backlog

| # | Priority                   | Item                                                                                                                                                                                                                                                                                                | Type     | Effort    |
|---|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-----------|
| 1 | **P2 — Productivity**      | Testing primitives (`kpipe-test`) — `TestStream<T>` analogue of `TopologyTestDriver`, no Testcontainers required                                                                                                                                                                                    | Feature  | ~3–4 days |
| 2 | **P3 — User-visible**      | `AppConfig` slimdown / split — example infra; decide whether to split or move into `examples/`                                                                                                                                                                                                      | Refactor | ~1 hr     |
| 3 | **P3 — User-visible**      | `HttpHealthServer` placement — lib contract or sample → `examples/`?                                                                                                                                                                                                                                | Refactor | ~30 min   |
| 4 | **P3 — User-visible**      | Residual build/test config cleanup (consumer parallelism, fork tuning)                                                                                                                                                                                                                              | Refactor | ~30 min   |
| 5 | **P3 — Audience**          | Spring Boot starter — opt-in module, `@KPipeListener`, auto-wires from `application.yml`. **Only build if a Spring-shop user asks.**                                                                                                                                                                | Feature  | ~1 week   |
| 6 | ~~**P3 — User-visible**~~  | ~~`Format.INSTANCE` hardening — JVM-global mutable singleton~~ — shipped in 1.13.0. `AvroFormat` / `ProtobufFormat` are now stateless codecs constructed with `new AvroFormat(schema)` / `new ProtobufFormat(descriptor)`; keyed lookup moves to `AvroSchemaCatalog` / `ProtobufDescriptorCatalog`. | Breaking | shipped   |
| 7 | ~~**P5 — 2.0 candidate**~~ | ~~`MessageTracker` collapse — add `KPipeConsumer.waitForInFlightDrain(Duration)`~~ — shipped in 1.12.0; `MessageTracker` was deleted, the public method lives on `KPipeConsumer`. (Already marked in §17 / capability table.)                                                                       | Breaking | shipped   |
| 8 | ~~**P5 — 2.0 candidate**~~ | ~~`Result<T>` sealed type for pipeline outcomes~~ — shipped in 1.13.0. `MessagePipeline.process()` returns `Result<T>` with `Passed \| Filtered \| Failed`; byte-level entry points (`apply` / `processToSink` / `processToValue`) preserve their pre-1.13 contracts by unwrapping internally.      | Breaking | shipped   |
| 9 | **P5 — Speculative**       | Format serialization caches re-wire — only with JMH evidence of allocation/GC pressure                                                                                                                                                                                                              | Perf     | ~1–2 days |

**Why this ordering:**

- **kpipe-test first.** Docs caught up in 1.13 (README + Javadoc audit landed alongside the breaking changes);
  the next leverage point is letting adopters write unit tests without Testcontainers. Productivity multiplier for
  every existing and future user, and CI-environment-friendly for shops without Docker.
- **P3 refactors next.** Cheap (~2 hours total) but worth doing before they accumulate.
- **Spring starter held until asked.** ~1 week of work for an audience that already has Spring
  Kafka. Build it when a Spring-shop user actively asks, not speculatively.

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

| Module                            | What's in it                                                                                                                                                                                                 |
|-----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `kpipe-bom`                       | Maven BOM — pins all `kpipe-*` artifacts to matching versions                                                                                                                                                |
| `kpipe-core`                      | Format-agnostic pipeline machinery: `MessageProcessorRegistry`, `MessageFormat`, `MessageSink`, `Operators`, `MessagePipeline` (returns `Result<T>` — `Passed \| Filtered \| Failed`), `SchemaResolver` SPI  |
| `kpipe-metrics`                   | Metrics interfaces (`ConsumerMetrics`, `ProducerMetrics`) + log-based reporters; **no OTel API** on classpath                                                                                                |
| `kpipe-metrics-otel`              | OpenTelemetry-backed metrics implementation (opt-in)                                                                                                                                                         |
| `kpipe-tracing-otel`              | W3C trace context propagation through Kafka headers; OTel tracer implementation (opt-in)                                                                                                                     |
| `kpipe-schema-registry-confluent` | Confluent Schema Registry client (`ConfluentSchemaResolver`); schema-by-ID + by-subject-version lookup (opt-in)                                                                                              |
| `kpipe-producer`                  | Kafka producer wrapper, `KafkaMessageSink`, `KafkaProducerConfig`, `Tracer` SPI                                                                                                                              |
| `kpipe-consumer`                  | `KPipeConsumer` (hosts lifecycle + metrics-reporter thread), `BackpressureController`, `CircuitBreakerController`, `ConsumerHealthController` (pkg-private façade), `KafkaOffsetManager`, `HttpHealthServer` |
| `kpipe-format-json`               | `JsonFormat`, `JsonConsoleSink`                                                                                                                                                                              |
| `kpipe-format-avro`               | `AvroFormat` (stateless codec, one schema per instance), `AvroSchemaCatalog` (keyed lookup), `AvroConsoleSink`                                                                                               |
| `kpipe-format-protobuf`           | `ProtobufFormat` (stateless codec, one descriptor per instance), `ProtobufDescriptorCatalog` (keyed lookup), `ProtobufConsoleSink`                                                                           |
| `kpipe-api`                       | `KPipe` fluent facade — `Stream<T>`, `Sink<T>`, `Handle`, `MultiBuilder`                                                                                                                                     |

**Still planned:**

| Module                      | What's in it                                                                                           |
|-----------------------------|--------------------------------------------------------------------------------------------------------|
| `kpipe-test`                | `TestStream<T>` for unit-testing pipelines without Testcontainers; `.send(record).flush()` ergonomics. |
| `kpipe-spring-boot-starter` | Auto-wiring of `KPipeConsumer` from `application.yml`. Opt-in; no Spring on `kpipe-core`'s classpath.  |

---

## P3 detail

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

## P5 candidates (speculative)

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
- **ConsumerHealthController unification** — multiple pause sources (manual, backpressure, CB) can't auto-resume each
  other; CB state machine + backpressure decision + pause arbitration live behind a single façade in `kpipe-consumer`.
- **Clean module dependency direction** — no cycles, no sideways leaks, no split packages.
- **OTel as opt-in interface, not transitive dep** — `kpipe-metrics` ships interfaces only.
- **Concurrency safety in `KPipeConsumer`** — single-read CAS, error-handler-throw safety, nested-finally shutdown,
  `LockSupport.park` for paused state.
- **Explicit error semantics in `MessagePipeline`** — `process()` returns `Result<T>` (Passed/Filtered/Failed) so the
  filter-vs-fail distinction is enforced by the compiler, not by convention.
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
   auto-lookup in `AvroFormat` / `ProtobufFormat` decode — both still deferred. With the 1.13
   codec/catalog split, wire-envelope auto-lookup now has a clean place to live (the catalog can
   own a `SchemaResolver`-backed loader). Schedule for 1.14?
4. **Spring starter audience confirmation** — has a Spring-shop user actually asked, or is this
   speculative? Promote to P2 if real demand exists.
5. **Are there 1.x users in production?** Affects whether 2.0 ships a compat shim package or hard-breaks.
