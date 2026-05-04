# KPipe — Roadmap & State of the Library

**Last updated:** 2026-05-03
**Current released line:** 1.10.0 (tagged today, Maven Central)
**Active branch state:** `feature/1.11.1-test-coverage` open as PR #94 (test-coverage hardening for the 1.10.x line;
branch name predates the version cut)

---

## TL;DR — Where the library stands

The architectural review of 2026-05-02 identified 7 priorities. As of 1.10.0:

| #  | Finding                                                                                                   | Status     | Shipped in     |
|----|-----------------------------------------------------------------------------------------------------------|------------|----------------|
| 1  | `MessagePipeline.apply()` swallowed all exceptions (silent failure)                                       | ✅ Done     | 1.9.0          |
| 2  | `kpipe-consumer` was a kitchen-sink module (all formats bundled)                                          | ✅ Done     | 1.9.0          |
| 3  | OTel API leaked through `api()` chain — forced on every user                                              | ✅ Done     | 1.9.0          |
| 4  | `MessageSink` lived in the wrong module                                                                   | ✅ Done     | 1.9.0          |
| 5  | `MessageProcessorRegistry` had too many responsibilities                                                  | ✅ Done     | 1.9.0 / 1.10.0 |
| 6  | Build/test config smells (7G heap, Postgres deps, single fork)                                            | ⏳ Pending  | —              |
| 7  | Common-path API ergonomics — fluent facade                                                                | ✅ Done     | 1.10.0         |
| 8  | **Multi-topic consumer support** — single-topic ceiling blocks prod                                       | 🎯 Roadmap | 1.11.0 target  |
| 9  | **Batch sinks** — per-record round-trips bottleneck DB/HTTP targets                                       | 🎯 Roadmap | 1.12.0 target  |
| 10 | **Format-module slimdown** — drop forced operator helpers, dead caches, PojoFormat, standalone registries | ✅ Done     | 1.10.1         |

**Public API self-assessment (post-1.10.0):**

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

The remaining gap to "perfect" is mostly Java's syntactic ceiling and long enterprise-y type names that would require a
2.0 break to rename. **Shipped as 1.10.0.**

**Honest "would I adopt this in prod?" delta:** API quality is high, but ecosystem-level features (#8 multi-topic,
#9 batch sinks) are the gap between "polished niche tool" and "Spring Kafka alternative." Both are now committed
on the roadmap. Circuit breaker (P3) and Java baseline decision (P4 — currently thinking Java 21 LTS) are tracked
under "Production-readiness roadmap" below but not yet committed to specific releases.

---

## Module taxonomy (post-1.10.0)

```
kpipe-metrics ← kpipe-core ← kpipe-consumer
                          ← kpipe-producer
                          ← kpipe-format-{json, avro, protobuf}
                          ← kpipe-api  (← KPipe facade)
kpipe-metrics-otel ← kpipe-metrics                (opt-in OTel impl)
kpipe-bom                                          (BOM — pins versions)
```

| Module                  | What's in it                                                                                                                                                                              | First shipped |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `kpipe-bom`             | Maven BOM — pins all `kpipe-*` artifacts to matching versions                                                                                                                             | 1.9.0         |
| `kpipe-core`            | Format-agnostic pipeline machinery: `MessageProcessorRegistry`, `MessageFormat`, `MessageSink`, `Operators`, `MessagePipeline`                                                            | 1.9.0         |
| `kpipe-metrics`         | Metrics interfaces (`ConsumerMetrics`, `ProducerMetrics`) + log-based reporters; **no OTel API** on classpath                                                                             | 1.9.0         |
| `kpipe-metrics-otel`    | OpenTelemetry-backed implementation (opt-in)                                                                                                                                              | 1.9.0         |
| `kpipe-producer`        | Kafka producer wrapper, `KafkaMessageSink`                                                                                                                                                | 1.9.0         |
| `kpipe-consumer`        | `KPipeConsumer`, `KPipeRunner`, `BackpressureController`, `KafkaOffsetManager`, `HttpHealthServer`                                                                                        | 1.9.0         |
| `kpipe-format-json`     | `JsonFormat`, `JsonConsoleSink` (slimmed in 1.10.1 — operator helpers, `JsonMessageProcessor`, dead ScopedValue caches, and `PojoFormat` removed)                                         | 1.9.0         |
| `kpipe-format-avro`     | `AvroFormat` (owns its own schema registry instance-side), `AvroConsoleSink` (slimmed in 1.10.1 — operator helpers, dead caches, and `AvroSchemaRegistry` standalone class removed)       | 1.9.0         |
| `kpipe-format-protobuf` | `ProtobufFormat` (owns its own descriptor registry instance-side), `ProtobufConsoleSink` (slimmed in 1.10.1 — operator helpers and `ProtobufDescriptorRegistry` standalone class removed) | 1.9.0         |
| `kpipe-api`             | `KPipe` fluent facade — `Stream<T>`, `Sink<T>`, `Handle`                                                                                                                                  | 1.10.0        |

---

## Bugs fixed during the architectural review (running tally)

The audit passes that produced the last version found and fixed real bugs, not just refactors:

1. **`MessagePipeline.apply()` swallowed all exceptions** — `null` return overloaded as both "filtered" and "failed".
   Now: `null` means filter, real failures throw.
2. **`KPipeProducer.build()` `new Properties(parent)` silently shadowed user serializers** — `Properties(parent)` makes
   parent a fallback for `getProperty()` only; `putIfAbsent` on the new instance ignores parent entries. Switched to
   `(Properties) source.clone()`.
3. **`KafkaOffsetManager.markOffsetProcessed` non-atomic cleanup race** — concurrent `trackOffset` could be dropped when
   the partition entry was removed mid-track. Switched to `computeIfPresent` for atomic remove-if-empty.
4. **`KafkaOffsetManager.prepareOffsetsToCommit` `pending.first()` race** — `ConcurrentSkipListSet.first()` throws
   `NoSuchElementException` if emptied between `isEmpty()` and `first()`. Wrapped in `safeFirst`.
5. **`KafkaOffsetManager.getPartitionState` same race** — same fix.
6. **`RebalanceListener.onPartitionsRevoked` same race** — same fix.
7. **`KafkaOffsetManager.scheduler` / `scheduledCommitTask` JLS visibility** — written after the start CAS, read after
   the close CAS. The CAS-to-CAS happens-before edge does NOT extend to post-CAS field writes. Marked `volatile`.
8. **`createMessageTracker()` returned `null` when metrics disabled** — silent-failure anti-pattern. Now throws
   `IllegalStateException`.
9. **`KPipeProducer.sendToDlq` exposed leaky `AtomicLong` parameter** — internal counter leaked through public API.
   Returns `boolean` instead.
10. **`BackpressureController.calculateTotalLag` used 60-second default Kafka timeout** — broker hiccup could stall the
    consumer thread. Bounded to 2 seconds.
11. **`BackpressureController.calculateTotalLag` swallowed `InterruptException` without restoring flag** — masked
    shutdown signals. Now restored.
12. **`BackpressureController.calculateTotalLag` silently returned 0 on broker errors** — backpressure decisions were
    quietly wrong. Now logged at WARNING.

All twelve are test-locked in `feature/1.11.1-test-coverage` (PR #94).

---

## Format-module slimdown (landed in 1.10.1, on the same branch)

A single editorial pass removed code that didn't earn its place:

- **15 operator helpers deleted** (5 each on `JsonMessageProcessor`, `AvroMessageProcessor`,
  `ProtobufMessageProcessor`). Diagnosis: most were 2-line wrappers over the format's native API,
  several encouraged anti-patterns (mutating schema-bound Avro/Protobuf payloads in the consumer),
  and one — `JsonMessageProcessor.removeFieldsOperator` — duplicated `Operators.removeFields`.
  Users now use `Operators` (generic + `Map<String, Object>`-typed) or write inline lambdas using
  the format's native API. Two-line lambdas over `record.put` / `msg.toBuilder()` are honest;
  helpers that hide them while pretending the formats are symmetric were not.
- **Dead ScopedValue cache infrastructure deleted** — `OUTPUT_STREAM_CACHE`, `ENCODER_CACHE`,
  `DECODER_CACHE`, `SCHEMA_PARSER` and `inScopedCaches`. Diagnosis: written by `inScopedCaches`,
  never read by any caller; `ENCODER_CACHE` and `DECODER_CACHE` were even bound to `null`. The §3
  "ScopedValue for high-performance caching" claim was infrastructure-on-spec, not implemented.
  See "Cosmetic / discoverability items" below for the conditions under which to revisit.
- **`PojoFormat` deleted.** No real users; half the class was fake schema plumbing existing only
  to satisfy the `MessageFormat<T>` interface; not exposed via the fluent facade. Users wanting
  typed JSON write a 3-line projection lambda over `KPipe.json` — same as if `PojoFormat` had
  existed.
- **`JsonMessageProcessor` deleted entirely.** After the operator helpers and dead caches went,
  the class was empty. `JsonFormat` now serializes inline.
- **`AvroSchemaRegistry` and `ProtobufDescriptorRegistry` inlined into formats.** The standalone
  registry classes were redundant indirection — `AvroFormat` already had instance state for
  schema metadata (`Map<String, SchemaInfo>`); the static registry just held the parsed `Schema`
  separately. Now `AvroFormat` owns both via `parsedSchemas` and exposes `getSchema(key)` /
  `addSchema(key, schemaJson)`. Same shape for `ProtobufFormat` (which already had its own
  descriptor map; the static registry was pure write-only side effect).

**Net effect on the public surface (per-format module):**

| Before                                                                | After                                   |
|-----------------------------------------------------------------------|-----------------------------------------|
| `JsonFormat`, `JsonConsoleSink`, `JsonMessageProcessor`, `PojoFormat` | `JsonFormat`, `JsonConsoleSink`         |
| `AvroFormat`, `AvroConsoleSink`, `AvroMessageProcessor`               | `AvroFormat`, `AvroConsoleSink`         |
| `ProtobufFormat`, `ProtobufConsoleSink`, `ProtobufMessageProcessor`   | `ProtobufFormat`, `ProtobufConsoleSink` |

**Migration for 1.10.x users:**

- `JsonMessageProcessor.removeFieldsOperator(...)` → `Operators.removeFields(...)` (no-op import change)
- `JsonMessageProcessor.addFieldOperator/addTimestampOperator/transformFieldOperator/mergeWithOperator` → inline lambda
- `AvroMessageProcessor.registerSchema(key, json)` → `AvroFormat.INSTANCE.addSchema(key, json)`
- `AvroMessageProcessor.getSchema(key)` → `AvroFormat.INSTANCE.getSchema(key)`
- `AvroMessageProcessor.clearSchemaRegistry()` → `AvroFormat.INSTANCE.clearSchemas()`
- `ProtobufMessageProcessor.registerDescriptor/getDescriptor/clearDescriptorRegistry` → same on
  `ProtobufFormat.INSTANCE`
- `JsonFormat.pojo(MyClass.class)` → use `KPipe.json(...)` and project to your domain object inside `.pipe(...)`

---

## Open work (post-1.10.0)

### #6 — Build/test config cleanup (non-breaking, ship anytime)

`lib/kpipe-consumer/build.gradle.kts` (and similar in `kpipe-producer`):

```kotlin
minHeapSize = "7g"
maxHeapSize = "7g"
maxParallelForks = 1
forkEvery = 200
```

Plus stray `testcontainersPostgresql` + `postgresql` deps in `kpipe-producer/build.gradle.kts`. Targets:

1. Profile tests to find actual memory usage. If <2GB, drop the heap.
2. Audit testcontainers lifecycle — shared container patterns instead of per-test.
3. Remove or document the Postgres deps.
4. `maxParallelForks = N` (CPU/2) once tests are isolated.

**Effort:** small. ~0.5 day investigate, 1–2 days fix depending on findings.

### Cosmetic / discoverability items not yet addressed

- **Long enterprise-y type names** in the explicit API (`MessageProcessorRegistry`, `MessageSinkRegistry`, etc.).
  Cleaner aliases would require a 2.0 break — left alone for backwards compat.
- **`BackpressureController.calculateTotalLag` performance** — hits Kafka on every consumer-loop iteration (~10/sec)
  when lag strategy is in use. A small TTL cache would cut broker load. Honestly: not an emergency, brokers handle it
  fine. Flagged for if/when it shows up in profiling.
- **`DefaultStream` vs immutability** — currently fully immutable (each fluent method returns a new instance). Foot gun
  resolved.
- **`AvroFormat` HTTP / Confluent Schema Registry fetching — extract to its own concern.** Today
  `AvroFormat.readSchemaFromLocation` supports `http://` URLs that hit a Confluent Schema
  Registry, which forces three deps onto `kpipe-format-avro`: `requires java.net.http` (the
  fetch), `requires dsl.json` (one-line use to parse the `{"schema":"..."}` envelope), and
  `requires com.fasterxml.jackson.core` (declared but currently unused — likely leftover from a
  previous implementation). The 2-arg `addSchema(key, schemaJson)` overload added in 1.10.1
  covers the "I have the JSON" case directly, so HTTP fetching could move to a separate optional
  module (`kpipe-schema-registry-confluent`) or be deleted in favor of user-side fetching. Wins:
  drop three deps from the avro module, smaller transitive footprint for users who only consume
  inline / classpath schemas. Effort: small (~1 day) once decided.
- **`Format.INSTANCE` singletons remain a footgun — eliminate or harden.** After the registry
  inlining, each format's INSTANCE owns the schema/descriptor map. That's *one* global per format
  instead of two, but the global is still loaded: any caller can do
  `AvroFormat.INSTANCE.addSchema("user", schemaA)` and silently overwrite a sibling pipeline's
  registration. The fix is either (a) make `addSchema` only available on `new AvroFormat()`
  instances and route the fluent facade through a per-stream format instance, or (b) delete
  `INSTANCE` entirely and make construction explicit. Both are breaking changes — defer until a
  real user files a collision bug or until 2.0. Documentation already carries an explicit
  "Footgun warning"; that's the cheap mitigation.
- **Format serialization caches — revisit when allocation rate matters.** The original design (CLAUDE.md §3) called
  for `ScopedValue`-backed reuse of `ByteArrayOutputStream` / `BinaryEncoder` / `BinaryDecoder` / `Schema.Parser` in
  the Avro and JSON serialize / deserialize hot paths. Investigation in 1.10.1 found the infrastructure was in place
  but **never wired up** — the caches were written but no caller ever read them, and `ENCODER_CACHE` / `DECODER_CACHE`
  were even bound to `null`. Dead infrastructure was deleted; serialize / deserialize now allocate per-record. To
  resurrect this as a real optimization:
    1. Move the `inScopedCaches` boundary from per-format-call to per-poll-batch (consumer wraps the record loop).
    2. Have each format's serialize / deserialize read `OUTPUT_STREAM_CACHE.get()` (with `.reset()` for buffer reuse)
       and pass the cached encoder / decoder to `EncoderFactory.binaryEncoder(out, prev)` for stateful reuse.
    3. Handle the multi-schema corner case carefully — `BinaryEncoder` and `Schema.Parser` carry state that doesn't
       transfer cleanly across schemas. With multi-topic Phase 2 (per-topic dispatch, heterogeneous schemas) this
       becomes a per-schema lookup, not a single ScopedValue.
    4. Land **only with JMH evidence** that the cache reuse exceeds `ScopedValue` lookup overhead. Don't add
       complexity speculatively.
       Format-specific note: **Protobuf does not benefit from the same pattern** — `Message` is immutable (must allocate
       per record), `CodedOutputStream` / `CodedInputStream` are thin wrappers (cheap to allocate). The original
       `ProtobufMessageProcessor` correctly had no cache infrastructure; if caching is revisited, scope it to Avro and
       JSON only.

### Production-readiness roadmap (PROMOTED — closes the "would adopt for prod" delta)

The honest reality: kpipe today is a polished niche tool, not yet a Spring Kafka alternative. To become one,
four items must ship. Promoted out of "2.0 candidates" because they unblock real-world adoption.

#### P1 — Multi-topic consumer support (PHASED — this is the hardest of the four)

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

#### P2 — Batch sinks

- **Today:** every `MessageSink<T>` consumes one record at a time. DB/HTTP targets bottleneck on per-record
  round-trips.
- **Target:** `BatchSink<T>` interface with `accept(List<T>)` plus configurable batch size + flush interval.
  The pipeline buffers, flushes on size or time, handles partial-batch failures (DLQ entire batch vs. per-record).
- **Design tension:** offset commit semantics with batches — you can only commit batch_min - 1 until the
  batch fully succeeds. `KafkaOffsetManager` already handles this via lowest-pending-offset, but batch lifetime
  needs to integrate cleanly.
- **Effort:** medium-large. ~5–7 days including failure-mode tests.

#### P3 — Circuit breaker for sinks

- **Today:** if a sink fails (DB down, HTTP 503), retries are unbounded and per-record. Backpressure helps
  but doesn't stop the bleeding.
- **Target:** `CircuitBreaker` wrapper around `MessageSink<T>` with the standard three states (closed / open /
  half-open), failure-rate threshold, and trip duration. Open state pauses the consumer (existing pause
  machinery) until half-open probe succeeds.
- **Implementation note:** consider Resilience4j vs. hand-roll. Hand-roll keeps zero-dep posture but adds
  surface area. Resilience4j is a clean opt-in via a separate `kpipe-resilience` module.
- **Effort:** medium. ~3–4 days.

#### P4 — Java baseline decision (THINKING)

- **Status:** under consideration, not committed. Java 25 floor locks out ~95% of production Java teams (most
  on 17 or 21).
- **Option A — stay on 25:** ideologically pure (full `ScopedValue` stable, /// markdown javadoc, latest
  pattern matching), niche audience, slow adoption. Fine if positioning is "for teams already on 25."
- **Option B — drop to 21 LTS + ScopedValue preview:** keeps the §3 design (VT + ScopedValue, no
  ThreadLocal). Costs users `--enable-preview`, awkward for some shops. VT itself is stable in 21.
- **Option C — drop to 21 LTS stable, ScopedValue → ThreadLocal in fallback path:** broadest reach, but
  regresses §3 (the whole "no ThreadLocal scalability traps" design rationale). Considered a regression.
- **Recommendation:** Option B if we want adoption now; Option A if we wait for 25 to age into prod (~2027).
  Multi-topic / batch sinks / circuit breaker should ship regardless; baseline choice is orthogonal.

### 2.0 candidates (genuinely deferred)

- **Drop `Message*` prefix** on registry / format / sink types. Cleaner reads, but breaks every 1.x import.
- **`Result<T>` sealed type** for pipeline errors instead of throw + filter-via-null. Already partially achieved by
  1.9.0's exception-throwing path; full sealed-type would be a 2.0.

---

## Strengths to preserve

These properties of the library should NOT be regressed in any future refactor:

- **Byte boundary at the consumer entry point** — `KPipeConsumer<K>` operates on `byte[]` values. Format SerDe lives in
  the pipeline. (CLAUDE.md §1)
- **Single SerDe cycle** — one deserialize, many transforms, one serialize. Genuinely good for throughput. (CLAUDE.md
  §2)
- **Virtual threads + ScopedValue** — thread-per-record without ThreadLocal scalability traps. (CLAUDE.md §3)
- **Lowest-pending-offset commits** — at-least-once safety even with parallel processing. (CLAUDE.md §4)
- **Strategy-based backpressure with hysteresis** — in-flight for parallel, lag for sequential. (CLAUDE.md §5)
- **Clean module dependency direction** — no cycles, no sideways leaks, no split packages. (CLAUDE.md §9)
- **OTel as opt-in interface, not transitive dep** — `kpipe-metrics` ships interfaces only. (CLAUDE.md §10)
- **Concurrency safety in `KPipeConsumer`** — single-read CAS, error-handler-throw safety, nested-finally shutdown,
  `LockSupport.park` for paused state. (CLAUDE.md §11)
- **Explicit error semantics in `MessagePipeline`** — no silent null swallowing. (CLAUDE.md §12)
- **Immutable `DefaultStream`** — branching from a common root is safe.
- **Professional Maven publishing** — proper signing, POM metadata, separate artifacts, BOM for version unification.

---

## Open questions for the maintainer

1. **Is `HttpHealthServer` core, or a sample?** Currently in `kpipe-consumer`. If it's example code, move to
   `examples/`. If part of the library contract, document as such.
2. **Are there 1.x users in production?** Affects whether 2.0 ships a compat shim package or hard-breaks.
3. **Is there demand for "consumer without OTel even at compile time"?** Current state: OTel is a pure runtime opt-in
   via `kpipe-metrics-otel`; `kpipe-metrics` itself has no OTel deps. Sufficient for most needs.

---

## Historical reference

This document used to be the original architectural-review report from 2026-05-02. The full priority-ladder writeup, fix
directions, and effort estimates for items #1-7 are preserved in git history at `.claude/PLAN.md` from before this
rewrite — see `git log --follow .claude/PLAN.md`. The current document is the post-shipping state.

The 2026-05-02 review session produced 12 fixed bugs, 1 new module (`kpipe-api`), 8 published artifacts, and ~9.7/10
internal correctness coverage as of 1.10.0.
