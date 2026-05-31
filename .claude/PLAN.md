# KPipe — Roadmap & State of the Library

- **Last updated:** 2026-05-30
- **Current released line:** 1.15.0 (Maven Central) — `Dispatcher` abstraction with `ProcessingMode` enum (SEQUENTIAL /
  PARALLEL / KEY_ORDERED), per-key serial processing via `KeyOrderedDispatcher`, KEY_ORDERED operability bundle
  (stall-WARN, `topKeyQueueDepths`, builder warn-log). Per-release detail lives in git history + CLAUDE.md §19/§20; this
  file tracks what's next, not what's done.

---

## Public API self-assessment (current)

| Dimension                              | Score       |
| -------------------------------------- | ----------- |
| Common-path ergonomics                 | 9/10        |
| Customization / escape hatches         | 9/10        |
| Discovery (does the API teach itself?) | 9/10        |
| Type safety                            | 9.5/10      |
| Error semantics                        | 10/10       |
| Module taxonomy                        | 9.5/10      |
| Documentation                          | 8/10        |
| Test coverage of the public surface    | 8.5/10      |
| Aggregate                              | **~8.8/10** |

API quality is solid. The remaining gap is **productivity tooling for adopters** (`kpipe-test`) — the backlog leads with
that. Discovery + Type safety nudged up in the architecture-deepening pass after the `MessagePipeline` byte-level entry
points were removed — the `Result<T>` switch is now the only way to get a pipeline outcome, enforced by the compiler
instead of by doctrine.

---

## Prioritized backlog

| #   | Priority              | Item                                                                                                                                 | Type     | Effort    |
| --- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | -------- | --------- |
| 1   | **P2 — Productivity** | Testing primitives (`kpipe-test`) — `TestStream<T>` analogue of `TopologyTestDriver`, no Testcontainers required                     | Feature  | ~3–4 days |
| 2   | **P3 — User-visible** | `AppConfig` slimdown / split — example infra; decide whether to split or move into `examples/`                                       | Refactor | ~1 hr     |
| 3   | **P3 — User-visible** | `HttpHealthServer` placement — lib contract or sample → `examples/`?                                                                 | Refactor | ~30 min   |
| 4   | **P3 — User-visible** | Residual build/test config cleanup (consumer parallelism, fork tuning)                                                               | Refactor | ~30 min   |
| 5   | **P3 — Audience**     | Spring Boot starter — opt-in module, `@KPipeListener`, auto-wires from `application.yml`. **Only build if a Spring-shop user asks.** | Feature  | ~1 week   |
| 6   | **P5 — Speculative**  | Format serialization caches re-wire — only with JMH evidence of allocation/GC pressure                                               | Perf     | ~1–2 days |

### Deliberately deferred (still not committed)

These came up in earlier brainstorms and were dropped on the merits; recorded here so they don't get re-proposed without
new evidence:

- `Stream.strict()` / `.lenient()` toggle for malformed-record routing — conflates two API decisions.
- `KPipe.from(props).json().topic(...)` short-form — current `KPipe.json("topic", props)` is already one call.
- **Protobuf SR auto-lookup** — needs runtime `.proto` text compilation (protoc-jar); doctrine in CLAUDE.md §19.
- `just new-format <name>` scaffold — cheap, but ungated; pick up if a real format addition lands.
- Fold `HttpHealthServer.fromEnv(...)` into `Handle` — overlaps with backlog #3; decide alongside that move.

---

## P2 — kpipe-test module detail

**Today:** writing a unit test for a KPipe pipeline means either Testcontainers (10s+ per test, Docker-dependent) or
hand-rolling a `MockConsumer` harness. The JMH bench has the second; users don't.

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

1. **Drive model.** Option (a) Kafka-free pipeline-direct invocation; option (b) MockConsumer-backed `KPipeConsumer` on
   a synchronous executor. Recommendation: **(b)** — exercises real code paths, stays in sync as the consumer evolves.
   Cost is ~5–20ms/test vs ~1ms.
2. **JUnit integration.** v1 plain API only; `@KPipeTest` extension is a follow-up.
3. **Assertion API.** Just `captured()` returning `List<T>` — let users use AssertJ / Hamcrest / plain `assertEquals`.
   Don't invent a DSL.
4. **Multi-topic.** Single-topic v1, multi-topic follow-up.
5. **Batch helpers.** Size-flush + shutdown-flush v1; virtual-clock age-flush is a follow-up (interaction with
   `ScheduledFuture` needs care).
6. **Module type.** Full `compile` artifact (not `test`-classifier) — it's a runtime tool for users' test suites.

**Why P2 productivity:** every existing and future user benefits. Cuts test feedback loops 100×. Removes the
Docker-required friction for CI environments without Docker.

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

Per-module contents are recoverable from `settings.gradle` + the `build.gradle` per module, and the package-ownership
rules live in CLAUDE.md §9. Not duplicated here.

**Still planned:**

| Module                      | What's in it                                                                                           |
| --------------------------- | ------------------------------------------------------------------------------------------------------ |
| `kpipe-test`                | `TestStream<T>` for unit-testing pipelines without Testcontainers; `.send(record).flush()` ergonomics. |
| `kpipe-spring-boot-starter` | Auto-wiring of `KPipeConsumer` from `application.yml`. Opt-in; no Spring on `kpipe-core`'s classpath.  |

---

## P3 detail

### `AppConfig` slimdown

Real example infra; likely belongs in `examples/` rather than crossing module boundaries.

### `HttpHealthServer` placement

Currently in `kpipe-consumer`. Decide: lib contract (document and own) or sample (move to `examples/`). ~30 minutes
either way once the call is made.

### Build/test config cleanup

Residual fork tuning, consumer parallelism settings. Cleanup pass; no functional change.

### Spring Boot starter

Audience-multiplier feature. Auto-wire `KPipeConsumer` from `application.yml`, expose `@KPipeListener` or similar
declarative entry point, hook health probes to Spring Boot Actuator. The starter is the opt-in; no Spring on
`kpipe-core`'s classpath. ~1 week — non-trivial because Spring's lifecycle and config-binding semantics are their own
learning curve.

**Why P3:** higher effort than the P2 items, audience overlap is real but not 100% (Spring Boot Kafka users already have
Spring Kafka). Promote to P2 if a Spring-shop user explicitly asks.

---

## P5 candidates (speculative)

### Format serialization caches

Only revisit when allocation rate or GC pressure shows up in profiling.

To resurrect as a real optimization:

1. Move the `inScopedCaches` boundary from per-format-call to per-poll-batch (consumer wraps the record loop).
2. Have each format's serialize / deserialize read `OUTPUT_STREAM_CACHE.get()` (with `.reset()` for buffer reuse) and
   pass the cached encoder / decoder to `EncoderFactory.binaryEncoder(out, prev)` for stateful reuse.
3. Handle the multi-schema corner case carefully — `BinaryEncoder` and `Schema.Parser` carry state that doesn't transfer
   cleanly across schemas. With multi-topic Phase 2 (per-topic dispatch, heterogeneous schemas) this becomes a
   per-schema lookup, not a single ScopedValue.
4. Land **only with JMH evidence** that the cache reuse exceeds `ScopedValue` lookup overhead.

**Format-specific note:** Protobuf does not benefit — `Message` is immutable (must allocate per record),
`CodedOutputStream` / `CodedInputStream` are thin wrappers. Scope any caching work to Avro and JSON only.

---

## Open questions

1. **`kpipe-test` test runner integration** — plain `TestStream` v1, `@KPipeTest` extension as follow-up.
2. **Tracing default-on or opt-in?** — auto-enable when `kpipe-tracing-otel` is on the classpath, or stay explicit
   (`.withTracer(...)`)?
3. **Spring starter demand check** — promote to P2 only on real user ask.
