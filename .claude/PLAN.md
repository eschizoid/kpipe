# KPipe — Roadmap & State of the Library

**Last updated:** 2026-05-07
**Current released line:** 1.10.0 (Maven Central)
**Active branch state:** `feature/1.11.1-test-coverage` (PR #94) plus `fix/critical-bugs` cleanup branch

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

| #  | Priority                  | Item                                                                                                                         | Type              | Effort    |
|----|---------------------------|------------------------------------------------------------------------------------------------------------------------------|-------------------|-----------|
| 1  | **P1 — Adoption blocker** | Multi-topic Phase 1 (homogeneous topics, single shared pipeline)                                                             | Feature           | ~3 days   |
| 2  | **P1 — Adoption blocker** | Java baseline decision (stay on 25 vs drop to 21 LTS)                                                                        | Strategy          | decision  |
| 3  | **P2 — Real footgun**     | `Format.INSTANCE` global mutable state (Avro/Protobuf)                                                                       | Hardening         | ~1 day    |
| 4  | **P2 — Real footgun**     | HTTP fetcher remaining extraction from `kpipe-format-avro` (drop `java.net.http` + `jackson.core`; dsl-json already removed) | Cleanup           | ~2–3 hr   |
| 5  | **P2 — Bug surface**      | Batch sinks (`BatchSink<T>` interface + size/time flush + partial-failure semantics)                                         | Feature           | ~5–7 days |
| 6  | **P2 — Bug surface**      | Circuit breaker for sinks                                                                                                    | Feature           | ~3–4 days |
| 7  | **P3 — User-visible**     | `AppConfig` slimdown / split (real example infra; decide split or move)                                                      | Refactor          | ~1 hr     |
| 8  | **P3 — User-visible**     | `HttpHealthServer` placement decision (lib contract or sample → `examples/`?)                                                | Refactor          | ~30 min   |
| 9  | **P3 — User-visible**     | Build/test config cleanup remaining (consumer parallelism, residual fork tuning)                                             | Refactor          | ~30 min   |
| 10 | **P5 — 2.0 candidates**   | Type-name shortening (`MessageProcessorRegistry` → `Pipelines`, etc.)                                                        | Breaking refactor | ~1 day    |
| 11 | **P5 — 2.0 candidates**   | `Result<T>` sealed type for pipeline errors                                                                                  | Breaking refactor | ~1 day    |
| 12 | **P5 — 2.0 candidates**   | Fold `KPipeRunner` into `KPipeConsumer`                                                                                      | Breaking refactor | ~half day |
| 13 | **P5 — 2.0 candidates**   | `MessageTracker` collapse — add `KPipeConsumer.waitForInFlightDrain(Duration)`, deprecate the standalone class               | Breaking refactor | ~half day |
| 14 | **P5 — Speculative perf** | Format serialization caches re-wire (only with JMH evidence)                                                                 | Perf              | ~1–2 days |

**Recommendation:** ship P1 (#1, #2) and P2 (#3–#6) — those move the adoption needle. P3 items are correct
fixes but invisible to users. P4 internal-polish backlog was cleared on 2026-05-07 (8 audits/refactors landed
in one branch — see "Recently completed" below). Multi-topic Phase 1 is the next focused effort.

### Recently completed (P4 internal polish — 2026-05-07)

- Moved `OffsetState` and `ConsumerState` out of the `org.kpipe.consumer.enums` sub-package; deleted the
  one-package-deep folder.
- Added a discoverable docstring to `kpipe-metrics-otel/module-info.java`. (`kpipe-bom` is a `java-platform`
  BOM with no `module-info.java` — its description in `build.gradle.kts` is the appropriate doc.)
- **Audited and removed** `MessageProcessorRegistry.sourceAppName` — confirmed vestigial (set by 17 callers,
  read by zero). Deleted the field, both `String`-taking constructors, the getter, and `withSourceAppName`.
  Updated all 17 call sites.
- **Audited** `KafkaConsumerConfig` — confirmed canonical (parallels `KafkaProducerConfig`, used by all 4
  example apps + own test). No change.
- **Audited** `MessageTracker` — keep as-is. Well-tested public class; potential 2.0 collapse target moved
  to P5.
- **Fixed** `KPipeProducer.sendAsync` silent-metrics footgun: previously returned the raw producer Future
  with no instrumentation; now wraps with a callback that increments `messages.sent` / `messages.failed`.
- **Tightened** `OffsetManager.createRebalanceListener()` — moved the no-op default into the interface as a
  `default` method. Custom external offset stores no longer need to implement Kafka rebalance plumbing.
- **Audited** `ConsumerCommand` sealed hierarchy — keep as-is. `CommitOffsets` is the cross-thread bridge
  for both scheduled commits and `close()` path; `withOffsets` has a real callsite in `RebalanceListener`.
- **Audited** `BackpressureController` public surface — already minimal. All instance methods are used,
  factories are intentional escape hatches, `calculateTotalLag` is a useful static utility.

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

### P1 #1 — Multi-topic consumer support (PHASED — hardest of the four)

- **Today:** `KPipeConsumer<K>` subscribes to a single topic. Real apps consume from N topics; users have to
  spin up N consumer instances, each with its own thread/poll loop, which inflates rebalance traffic and breaks
  the "one consumer group, many partitions" model Kafka was designed for.

- **Why it's hard (be honest with future-you):**
    1. The fluent facade is built around a single typed pipeline — `KPipe.json("topic")` returns
       `Stream<Map<String,Object>>`. Multi-topic forces a decision: all topics share one pipeline (and one
       payload type), or each topic gets its own pipeline. Sharing works for homogeneous topics; per-topic
       typing breaks the current `Stream<T>` shape.
    2. `KafkaOffsetManager` is already per-topic-partition internally, but everything else in the consumer
       treats topic as implicit. `topic` needs to be plumbed through `MessagePipeline.apply`, the in-flight
       tracker, the metrics labels, and `processRecord`. None hard alone; the breadth bites.
    3. Rebalance semantics worsen — `RebalanceListener` is invoked for the union of partitions across topics,
       and partial revocations (lose 2 partitions of topic-a, keep all of topic-b) need correct routing in the
       per-topic dispatcher.
    4. The Testcontainers harness assumes one topic. Most existing tests need new fixtures.

- **Phase 1 — homogeneous multi-topic (target 1.11.0)**
    - API: `KPipe.json(Set.of("topic-a", "topic-b", "topic-c"))` or varargs. Single shared pipeline. All topics
      must produce the same payload type.
    - Covers the ~60% real-world case ("partitioned-by-region versions of the same event").
    - Touches: `KPipeConsumer.subscribe`, `KafkaOffsetManager` plumbing review, facade signature additions,
      metrics topic-label propagation, Testcontainers fixture for multi-topic.
    - Effort: ~3 days.

- **Phase 2 — per-topic dispatch (target 1.13.0 or later, defer if Phase 1 covers demand)**
    - API: `KPipe.multi().on("topic-a", jsonStream).on("topic-b", avroStream).build()`. Different payload
      types per topic, separate pipelines.
    - Requires a new facade entry point and a routing layer on top of the consumer poll loop.
    - Effort: ~5–7 days, mostly facade redesign.

- **Trap to avoid:** don't try to land both phases in one release. Ship Phase 1 standalone, gather feedback,
  decide if Phase 2 is needed at all.

### P1 #2 — Java baseline decision (THINKING)

- **Status:** under consideration, not committed. Java 25 floor locks out ~95% of production Java teams (most
  on 17 or 21).
- **Option A — stay on 25:** ideologically pure (full `ScopedValue` stable, /// markdown javadoc, latest
  pattern matching), niche audience, slow adoption. Fine if positioning is "for teams already on 25."
- **Option B — drop to 21 LTS + ScopedValue preview:** keeps the §3 design (VT + ScopedValue, no
  ThreadLocal). Costs users `--enable-preview`, awkward for some shops. VT itself is stable in 21.
- **Option C — drop to 21 LTS stable, ScopedValue → ThreadLocal in fallback path:** broadest reach, but
  regresses §3. Considered a regression.
- **Recommendation:** Option B if we want adoption now; Option A if we wait for 25 to age into prod (~2027).
  Multi-topic / batch sinks / circuit breaker should ship regardless; baseline choice is orthogonal.

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
