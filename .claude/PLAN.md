# KPipe — Roadmap & State of the Library

**Last updated:** 2026-05-08
**Current released line:** 1.10.0 (Maven Central)
**Active work:** `refactor/api-cleanup` (DefaultStream record + test dedup + deprecated removal)

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

**Honest "would I adopt this in prod?" delta:** API quality is high, but ecosystem-level features (multi-topic,
batch sinks) are the gap between "polished niche tool" and "Spring Kafka alternative." Both committed on the
roadmap below.

---

## Prioritized refactor backlog

Single source of truth for what's left across the whole library.

| #  | Priority                      | Item                                                                                                                         | Type              | Effort    |
|----|-------------------------------|------------------------------------------------------------------------------------------------------------------------------|-------------------|-----------|
| 1  | ~~**P1 — Adoption blocker**~~ | ~~Multi-topic Phase 1 (homogeneous) + Phase 2 (heterogeneous via `KPipe.multi`)~~ — both landed 2026-05-07                   | Feature           | shipped   |
| 2  | ~~**P1 — Adoption blocker**~~ | ~~Java baseline decision~~ — **stay on Java 25** (decided 2026-05-07)                                                        | Strategy          | decided   |
| 3  | **P2 — Real footgun**         | `Format.INSTANCE` global mutable state (Avro/Protobuf)                                                                       | Hardening         | ~1 day    |
| 4  | **P2 — Real footgun**         | HTTP fetcher remaining extraction from `kpipe-format-avro` (drop `java.net.http` + `jackson.core`; dsl-json already removed) | Cleanup           | ~2–3 hr   |
| 5  | **P2 — Bug surface**          | Batch sinks (`BatchSink<T>` interface + size/time flush + partial-failure semantics)                                         | Feature           | ~5–7 days |
| 6  | **P2 — Bug surface**          | Circuit breaker for sinks                                                                                                    | Feature           | ~3–4 days |
| 7  | **P3 — User-visible**         | `AppConfig` slimdown / split (real example infra; decide split or move)                                                      | Refactor          | ~1 hr     |
| 8  | **P3 — User-visible**         | `HttpHealthServer` placement decision (lib contract or sample → `examples/`?)                                                | Refactor          | ~30 min   |
| 9  | **P3 — User-visible**         | Build/test config cleanup remaining (consumer parallelism, residual fork tuning)                                             | Refactor          | ~30 min   |
| 10 | **P5 — 2.0 candidates**       | Type-name shortening (`MessageProcessorRegistry` → `Pipelines`, etc.)                                                        | Breaking refactor | ~1 day    |
| 11 | **P5 — 2.0 candidates**       | `Result<T>` sealed type for pipeline errors                                                                                  | Breaking refactor | ~1 day    |
| 12 | **P5 — 2.0 candidates**       | Fold `KPipeRunner` into `KPipeConsumer`                                                                                      | Breaking refactor | ~half day |
| 13 | **P5 — 2.0 candidates**       | `MessageTracker` collapse — add `KPipeConsumer.waitForInFlightDrain(Duration)`, delete the standalone class                  | Breaking refactor | ~half day |
| 14 | **P5 — Speculative perf**     | Format serialization caches re-wire (only with JMH evidence)                                                                 | Perf              | ~1–2 days |

**Recommendation:** P1 (#1, #2) shipped. Next adoption needle is **P2 #5 — batch sinks**; that's the
"can I batch-insert into Postgres?" feature Spring Kafka users evaluate against. Circuit breaker (#6) and
HTTP-fetcher cleanup (#4) compose on top. P3 items are correct fixes but invisible to users — defer until
something forces them. **No further internal polish** — diminishing returns.

### Recently shipped

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

---

## Production-readiness roadmap (detail for the P1 / P2 items above)

### P1 #1 — Multi-topic consumer support (SHIPPED 2026-05-07)

Both phases landed in a single branch:

- **Homogeneous (Phase 1):** `KPipe.json/avro/protobuf/bytes/custom(Collection<String>, Properties)` overloads
  and `KPipeConsumer.Builder.withTopics(Collection<String>)` + `withTopics(String...)`. One pipeline, N
  topics, same payload type.
- **Heterogeneous (Phase 2):** `KPipe.multi(props).json(topic, configurator).avro(...).bytes(...).start()`.
  Per-topic pipelines through one consumer / one offset manager / one consumer-group; `KPipeConsumer`
  dispatches by `record.topic()`. Unrouted topics drop+log at WARNING and commit (no infinite retry on
  config error).
- **Coverage:** `KPipeFacadeIntegrationTest.endToEndMultiTopicJsonStream` (homogeneous, 3 topics) and
  `endToEndHeterogeneousMulti` (JSON + bytes through one consumer).

### P1 #2 — Java baseline decision (DECIDED 2026-05-07: stay on Java 25)

Stay on Java 25. Trade-off accepted: smaller initial audience (most prod teams still on 17/21) in exchange
for `///` markdown javadoc, stable `ScopedValue` if we ever re-introduce thread-local-like state, modern
pattern matching, and unnamed variables (`_`) in switch patterns. Re-evaluate if adoption hits a wall purely
because of the baseline.

### P2 #3 — `Format.INSTANCE` singletons remain a footgun

Each format's `INSTANCE` is a JVM-global mutable singleton holding the schema/descriptor map. Any caller can
`AvroFormat.INSTANCE.addSchema("user", schemaA)` and silently overwrite a sibling pipeline's registration. The
fix is either (a) make `addSchema` only available on `new AvroFormat()` instances and route the fluent facade
through a per-stream format instance, or (b) delete `INSTANCE` entirely and make construction explicit. Both are
breaking changes. Documentation already carries an explicit "Footgun warning" — that's the cheap mitigation.

### P2 #4 — `kpipe-format-avro` HTTP fetcher remaining extraction

**Done already:** dsl-json dependency removed; envelope parsing rewritten to Jackson `JsonFactory` streaming
(reuses the jackson-core that Avro already pulls transitively, so no new dep was added).

**What's left:** `AvroFormat.readSchemaFromLocation` still supports `http://` URLs hitting a Confluent Schema
Registry, which keeps two `requires` declarations on `kpipe-format-avro`'s `module-info.java`: `java.net.http`
and `com.fasterxml.jackson.core`. The 2-arg `addSchema(key, schemaJson)` overload covers the "I have the JSON"
case directly, so HTTP fetching could move to a separate optional module (`kpipe-schema-registry-confluent`)
or be deleted in favor of user-side fetching. Wins: smaller transitive footprint for users who only consume
inline / classpath schemas.

### P2 #5 — Batch sinks

- **Today:** every `MessageSink<T>` consumes one record at a time. DB/HTTP targets bottleneck on per-record
  round-trips.
- **Target:** `BatchSink<T>` interface with `accept(List<T>)` plus configurable batch size + flush interval.
  The pipeline buffers, flushes on size or time, handles partial-batch failures (DLQ entire batch vs. per-record).
- **Design tension:** offset commit semantics with batches — you can only commit batch_min - 1 until the
  batch fully succeeds. `KafkaOffsetManager` already handles this via lowest-pending-offset, but batch lifetime
  needs to integrate cleanly.
- **Effort:** medium-large. ~5–7 days including failure-mode tests.

### P2 #6 — Circuit breaker for sinks

- **Today:** if a sink fails (DB down, HTTP 503), retries are unbounded and per-record. Backpressure helps
  but doesn't stop the bleeding.
- **Target:** `CircuitBreaker` wrapper around `MessageSink<T>` with the standard three states (closed / open /
  half-open), failure-rate threshold, and trip duration. Open state pauses the consumer (existing pause
  machinery) until half-open probe succeeds.
- **Implementation note:** consider Resilience4j vs. hand-roll. Hand-roll keeps zero-dep posture but adds
  surface area. Resilience4j is a clean opt-in via a separate `kpipe-resilience` module.

---

## Speculative perf — format serialization caches (#22)

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

These properties of the library should NOT be regressed in any future refactor:

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

1. **Is `HttpHealthServer` core, or a sample?** Currently in `kpipe-consumer`. If it's example code, move to
   `examples/`. If part of the library contract, document as such. (Tracked as #8 in the backlog.)
2. **Are there 1.x users in production?** Affects whether 2.0 ships a compat shim package or hard-breaks.
3. **Is there demand for "consumer without OTel even at compile time"?** Current state: OTel is a pure runtime
   opt-in via `kpipe-metrics-otel`; `kpipe-metrics` itself has no OTel deps. Sufficient for most needs.
