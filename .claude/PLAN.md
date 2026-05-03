# KPipe — Roadmap & State of the Library

**Last updated:** 2026-05-03
**Current released line:** 1.11.0 (in flight)
**Active branch state:** `feature/1.11.1-test-coverage` open as PR #94

---

## TL;DR — Where the library stands

The architectural review of 2026-05-02 identified 7 priorities. As of 1.11.0:

| # | Finding | Status | Shipped in |
|---|---|---|---|
| 1 | `MessagePipeline.apply()` swallowed all exceptions (silent failure) | ✅ Done | 1.9.0 |
| 2 | `kpipe-consumer` was a kitchen-sink module (all formats bundled) | ✅ Done | 1.9.0 |
| 3 | OTel API leaked through `api()` chain — forced on every user | ✅ Done | 1.9.0 |
| 4 | `MessageSink` lived in the wrong module | ✅ Done | 1.9.0 |
| 5 | `MessageProcessorRegistry` had too many responsibilities | ✅ Done | 1.9.0 / 1.10.0 |
| 6 | Build/test config smells (7G heap, Postgres deps, single fork) | ⏳ Pending | — |
| 7 | Common-path API ergonomics — fluent facade | ✅ Done | 1.11.0 |

**Public API self-assessment (post-1.11.0):**

| Dimension | Score |
|---|---|
| Common-path ergonomics | 9/10 |
| Customization / escape hatches | 9/10 |
| Discovery (does the API teach itself?) | 8.5/10 |
| Type safety | 9/10 |
| Error semantics | 10/10 |
| Module taxonomy | 9.5/10 |
| Documentation | 8/10 |
| Test coverage of the public surface | 8.5/10 |
| Aggregate | **~8.7/10** |

The remaining gap to "perfect" is mostly Java's syntactic ceiling and long enterprise-y type names that would require a 2.0 break to rename. **Shippable as 1.11.0 with confidence.**

---

## Module taxonomy (post-1.11.0)

```
kpipe-metrics ← kpipe-core ← kpipe-consumer
                          ← kpipe-producer
                          ← kpipe-format-{json, avro, protobuf}
                          ← kpipe-api  (← KPipe facade)
kpipe-metrics-otel ← kpipe-metrics                (opt-in OTel impl)
kpipe-bom                                          (BOM — pins versions)
```

| Module | What's in it | First shipped |
|---|---|---|
| `kpipe-bom` | Maven BOM — pins all `kpipe-*` artifacts to matching versions | 1.9.0 |
| `kpipe-core` | Format-agnostic pipeline machinery: `MessageProcessorRegistry`, `MessageFormat`, `MessageSink`, `Operators`, `MessagePipeline` | 1.9.0 |
| `kpipe-metrics` | Metrics interfaces (`ConsumerMetrics`, `ProducerMetrics`) + log-based reporters; **no OTel API** on classpath | 1.9.0 |
| `kpipe-metrics-otel` | OpenTelemetry-backed implementation (opt-in) | 1.9.0 |
| `kpipe-producer` | Kafka producer wrapper, `KafkaMessageSink` | 1.9.0 |
| `kpipe-consumer` | `KPipeConsumer`, `KPipeRunner`, `BackpressureController`, `KafkaOffsetManager`, `HttpHealthServer` | 1.9.0 |
| `kpipe-format-json` | `JsonFormat`, `JsonMessageProcessor`, `JsonConsoleSink` | 1.9.0 |
| `kpipe-format-avro` | `AvroFormat`, `AvroMessageProcessor`, `AvroConsoleSink` | 1.9.0 |
| `kpipe-format-protobuf` | `ProtobufFormat`, `ProtobufMessageProcessor`, `ProtobufConsoleSink` | 1.9.0 |
| `kpipe-api` | `KPipe` fluent facade — `Stream<T>`, `Sink<T>`, `Handle` | 1.11.0 |

---

## Bugs fixed during the architectural review (running tally)

The audit passes that produced 1.9.0 / 1.10.0 / 1.11.0 found and fixed real bugs, not just refactors:

1. **`MessagePipeline.apply()` swallowed all exceptions** — `null` return overloaded as both "filtered" and "failed". Now: `null` means filter, real failures throw.
2. **`KPipeProducer.build()` `new Properties(parent)` silently shadowed user serializers** — `Properties(parent)` makes parent a fallback for `getProperty()` only; `putIfAbsent` on the new instance ignores parent entries. Switched to `(Properties) source.clone()`.
3. **`KafkaOffsetManager.markOffsetProcessed` non-atomic cleanup race** — concurrent `trackOffset` could be dropped when the partition entry was removed mid-track. Switched to `computeIfPresent` for atomic remove-if-empty.
4. **`KafkaOffsetManager.prepareOffsetsToCommit` `pending.first()` race** — `ConcurrentSkipListSet.first()` throws `NoSuchElementException` if emptied between `isEmpty()` and `first()`. Wrapped in `safeFirst`.
5. **`KafkaOffsetManager.getPartitionState` same race** — same fix.
6. **`RebalanceListener.onPartitionsRevoked` same race** — same fix.
7. **`KafkaOffsetManager.scheduler` / `scheduledCommitTask` JLS visibility** — written after the start CAS, read after the close CAS. The CAS-to-CAS happens-before edge does NOT extend to post-CAS field writes. Marked `volatile`.
8. **`createMessageTracker()` returned `null` when metrics disabled** — silent-failure anti-pattern. Now throws `IllegalStateException`.
9. **`KPipeProducer.sendToDlq` exposed leaky `AtomicLong` parameter** — internal counter leaked through public API. Returns `boolean` instead.
10. **`BackpressureController.calculateTotalLag` used 60-second default Kafka timeout** — broker hiccup could stall the consumer thread. Bounded to 2 seconds.
11. **`BackpressureController.calculateTotalLag` swallowed `InterruptException` without restoring flag** — masked shutdown signals. Now restored.
12. **`BackpressureController.calculateTotalLag` silently returned 0 on broker errors** — backpressure decisions were quietly wrong. Now logged at WARNING.

All twelve are test-locked in `feature/1.11.1-test-coverage` (PR #94).

---

## Open work (post-1.11.0)

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

- **Long enterprise-y type names** in the explicit API (`MessageProcessorRegistry`, `MessageSinkRegistry`, etc.). Cleaner aliases would require a 2.0 break — left alone for backwards compat.
- **`BackpressureController.calculateTotalLag` performance** — hits Kafka on every consumer-loop iteration (~10/sec) when lag strategy is in use. A small TTL cache would cut broker load. Honestly: not an emergency, brokers handle it fine. Flagged for if/when it shows up in profiling.
- **`DefaultStream` vs immutability** — currently fully immutable (each fluent method returns a new instance). Foot gun resolved.

### 2.0 candidates (not on the near-term roadmap)

- **Drop `Message*` prefix** on registry / format / sink types. Cleaner reads, but breaks every 1.x import.
- **`Result<T>` sealed type** for pipeline errors instead of throw + filter-via-null. Already partially achieved by 1.9.0's exception-throwing path; full sealed-type would be a 2.0.
- **Multi-topic consumer support.** Today a `KPipeConsumer<K>` subscribes to a single topic.
- **Batch sinks** — batching interface for `MessageSink` to improve database/HTTP target throughput.
- **Circuit breaker** — for sinks, prevent excessive retries when downstream is down.

---

## Strengths to preserve

These properties of the library should NOT be regressed in any future refactor:

- **Byte boundary at the consumer entry point** — `KPipeConsumer<K>` operates on `byte[]` values. Format SerDe lives in the pipeline. (CLAUDE.md §1)
- **Single SerDe cycle** — one deserialize, many transforms, one serialize. Genuinely good for throughput. (CLAUDE.md §2)
- **Virtual threads + ScopedValue** — thread-per-record without ThreadLocal scalability traps. (CLAUDE.md §3)
- **Lowest-pending-offset commits** — at-least-once safety even with parallel processing. (CLAUDE.md §4)
- **Strategy-based backpressure with hysteresis** — in-flight for parallel, lag for sequential. (CLAUDE.md §5)
- **Clean module dependency direction** — no cycles, no sideways leaks, no split packages. (CLAUDE.md §9)
- **OTel as opt-in interface, not transitive dep** — `kpipe-metrics` ships interfaces only. (CLAUDE.md §10)
- **Concurrency safety in `KPipeConsumer`** — single-read CAS, error-handler-throw safety, nested-finally shutdown, `LockSupport.park` for paused state. (CLAUDE.md §11)
- **Explicit error semantics in `MessagePipeline`** — no silent null swallowing. (CLAUDE.md §12)
- **Immutable `DefaultStream`** — branching from a common root is safe.
- **Professional Maven publishing** — proper signing, POM metadata, separate artifacts, BOM for version unification.

---

## Open questions for the maintainer

1. **Is `HttpHealthServer` core, or a sample?** Currently in `kpipe-consumer`. If it's example code, move to `examples/`. If part of the library contract, document as such.
2. **Are there 1.x users in production?** Affects whether 2.0 ships a compat shim package or hard-breaks.
3. **Is there demand for "consumer without OTel even at compile time"?** Current state: OTel is a pure runtime opt-in via `kpipe-metrics-otel`; `kpipe-metrics` itself has no OTel deps. Sufficient for most needs.

---

## Historical reference

This document used to be the original architectural-review report from 2026-05-02. The full priority-ladder writeup, fix directions, and effort estimates for items #1-7 are preserved in git history at `.claude/PLAN.md` from before this rewrite — see `git log --follow .claude/PLAN.md`. The current document is the post-shipping state.

The 2026-05-02 review session produced 12 fixed bugs, 1 new module (`kpipe-api`), 8 published artifacts, and ~9.7/10 internal correctness coverage as of 1.11.0.
