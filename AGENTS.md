# KPipe Architecture and Design Principles

This file is the long-term memory for working in this repo: the non-obvious invariants, the footguns that have already
burned us, and the architectural decisions that are not recoverable by reading the code. It is the agent-instructions
file for this repository — `.claude/CLAUDE.md` is a one-line pointer at it, so there is one copy and it is the one under
version control and open to review.

Things that _are_ recoverable from the code — type signatures, who calls what, package layout — are omitted on purpose,
because a second copy drifts from the first and the copy is the one that lies. Where a section restates something the
build already declares, the build is the authority and the section is a reading aid; the package ownership list below is
the standing example, and it had drifted in three places before anyone checked it against `module-info.java`.

When a claim here disagrees with the code, the code is right and this file is a bug. Fix it in the same change.

## Core invariants

- **Byte boundary at the consumer entry.** `KPipeConsumer` operates on `byte[]` values. Format SerDe lives inside the
  pipeline. Mixed wire formats and Confluent magic-byte prefixes are handled inside operators (`skipBytes(n)`), not at
  the consumer level. The key type parameter is gone — it was vestigial ceremony carrying a
  witness/deserializer-mismatch footgun, and it was deleted rather than deprecated. Keys are routing metadata
  (partitioning, KEY_ORDERED dispatch), and `KeyOrderedDispatcher` normalizes them internally (`byte[]` → `ByteBuffer`).
- **Single SerDe cycle per record.** Deserialize once → chain `UnaryOperator<T>` transforms on the typed object →
  serialize once. Never compose `Function<byte[], byte[]>` chains; that's the "SerDe tax" the typed pipeline was built
  to eliminate.
- **Lowest-pending-offset commits.** `PendingOffsetSet` (sorted primitive-`long` window; replaced the earlier
  `ConcurrentSkipListSet`) per partition; offset 102 is not committed until 101 also finishes. This is what makes
  "at-least-once with parallel processing" honest.

## Coding standards

- **Java 25 triple-slash Javadoc** (`///`), markdown code blocks (`java ... `). No legacy `/** ... */` or HTML
  `<pre>{@code ... }</pre>`.
- **`final var` for locals** wherever possible. Exception: accumulator patterns inside loops
  (`var total = ...; for (var x : xs) total += x.foo();`) — the reassignment is the point.
- **`java.lang.System.Logger`** — not SLF4J, not direct Log4j. Keeps the core library dependency-free.
- **No `e.printStackTrace()`** — always use the logger.
- **No fully-qualified class names in code** — if you have to write `java.util.concurrent.ConcurrentHashMap` instead of
  just `ConcurrentHashMap`, add an import and reformat the code.

## Testing strategy

- **Unit tests**: `lib/{module}/src/test/java`. Focus on pure function transformations.
- **Integration tests**: `examples/{format}/src/test/java`.
- **Testcontainers** for end-to-end with Kafka / Schema Registry. Use a `CapturingSink` to verify the actual payload
  transformation.
- **Virtual-thread tests** use `Thread.ofVirtual()` directly (not `CompletableFuture`) with `CountDownLatch` for sync.
  Exercises real VT behaviour.

---

## §3 Virtual Threads + ScopedValue doctrine

VT is used everywhere (consumer poll → record processing). **`ScopedValue` is NOT currently used** — earlier per-format
SerDe caches were torn out as dead infrastructure. The doctrine still stands for any future thread-local-like state we
might add (tenant context, span propagation): prefer `ScopedValue` over `ThreadLocal` to avoid the
inheritable-thread-local scalability trap on a thread-per-record consumer.

## §5 Strategy-based backpressure

`BackpressureController` encapsulates the decision logic. Two strategies, always with hysteresis (high/low watermarks)
to prevent thrashing:

- **In-flight (parallel)** — counts active virtual threads.
- **Lag (sequential)** — `Σ (end_offset - position)`, the only meaningful metric for one-at-a-time processing.

## §7 Unified Registry (1.13.0, refined 1.14.0)

**One registry type, two namespaces.** `MessageProcessorRegistry` holds operators (`UnaryOperator<T>`) and sinks
(`MessageSink<T>`) in separate internal maps, keyed identically by `RegistryKey<T>`. The registration entry points are
split by namespace — `registerOperator(key, op)` / `registerSink(key, sink)` — and the static error-handling helpers
follow the same naming: `withOperatorErrorHandling(UnaryOperator<T>)` / `withSinkErrorHandling(MessageSink<T>)`. Lookups
are `getOperator(key)` / `getSink(key)`. Since 1.19.0 the namespaces are a perfect 7×2 mirror — every method carries its
namespace (`unregisterOperator`/`unregisterSink`, `clearOperators`/`clearSinks`, `getOperatorKeys`/`getSinkKeys`,
`getAllOperators`/`getAllSinks`, `getOperatorMetrics`/`getSinkMetrics`); the bare `unregister`/`clear`/`getKeys`/
`getMetrics` operator forms are gone (they invited `registry.clear()`-wipes-only-operators surprises).

**Historical note:** through 1.13 these were `register` / `withErrorHandling` overloaded by argument type. Bare lambdas
like `x -> x` were ambiguous and needed an explicit cast. 1.14 renamed both pairs per §16 (delete + migrate) to kill the
overload-ambiguity footgun.

## §8 Centralized error handling

Public face: `MessageProcessorRegistry.withOperatorErrorHandling(UnaryOperator<T>)` and
`MessageProcessorRegistry.withSinkErrorHandling(MessageSink<T>)`. Both wrap user code so one failing component can't
crash the pipeline. Implementation note: the sink helper delegates to `RegistryFunctions.withConsumerErrorHandling`
internally — that mismatched name is a historical artifact and not part of the surface users should call.

## §9 Modular architecture

`kpipe-metrics` ← `kpipe-producer` ← `kpipe-consumer`. Each module is published individually to Maven Central;
`kpipe-consumer` transitively pulls the other two.

**Split-package rule (JPMS):** no two modules may export the same Java package. When a class references types from
another module, it stays in the module that owns those types.

Package ownership, as declared by the `exports` clauses in each `module-info.java`:

- `io.github.eschizoid.kpipe` → `kpipe-api`
- `io.github.eschizoid.kpipe.registry` → `kpipe-core`
- `io.github.eschizoid.kpipe.sink` → `kpipe-core` (this is where `MessageSink` and `CompositeMessageSink` live)
- `io.github.eschizoid.kpipe.metrics` → `kpipe-metrics`
- `io.github.eschizoid.kpipe.metrics.otel` → `kpipe-metrics-otel`
- `io.github.eschizoid.kpipe.producer`, `.producer.config`, `.producer.sink` → `kpipe-producer`
- `io.github.eschizoid.kpipe.consumer`, `.consumer.config`, `.consumer.metrics` → `kpipe-consumer`
- `io.github.eschizoid.kpipe.health` → `kpipe-consumer`
- `io.github.eschizoid.kpipe.format.json` → `kpipe-format-json`
- `io.github.eschizoid.kpipe.format.avro` → `kpipe-format-avro`
- `io.github.eschizoid.kpipe.format.protobuf` → `kpipe-format-protobuf` (incl. the `ProtobufDescriptorCompiler` SPI)
- `io.github.eschizoid.kpipe.schemaregistry.confluent` → `kpipe-schema-registry-confluent`
- `io.github.eschizoid.kpipe.tracing` → `kpipe-tracing` (the `Tracer` SPI module — a root like `kpipe-metrics`)
- `io.github.eschizoid.kpipe.tracing.otel` → `kpipe-tracing-otel`
- `io.github.eschizoid.kpipe.test` → `kpipe-test`

`kpipe-format-protobuf-confluent` ships the shaded Confluent compiler impl and is an automatic module, so it declares no
`exports` and does not appear above. `kpipe-bom` publishes no packages.

**Facade opt-in (1.18.0):** `kpipe-api` `requires static` the JSON/Avro/Protobuf format modules — formats are opt-in, so
a consumer only pulls the format it actually uses (§19).

## §10 OpenTelemetry metrics

`kpipe-metrics` carries no telemetry dependency — `build.gradle.kts` declares nothing beyond test libraries and
`module-info` has no `requires` clause at all, so it is interfaces plus a no-op default and nothing else. On the metrics
side `opentelemetry-api` arrives only with `kpipe-metrics-otel`, which brings the API along, so the user adds just an
SDK (`opentelemetry-sdk`) and an exporter (Prometheus, OTLP, Jaeger). Tracing has its own at `kpipe-tracing-otel`.
`ConsumerMetrics` / `ProducerMetrics` default to `ConsumerMetrics.noop()` / `ProducerMetrics.noop()` (zero cost when not
configured) and wire via `.withMetrics(...)`.

| Component         | Instrument                                     | Type      |
| ----------------- | ---------------------------------------------- | --------- |
| `ProducerMetrics` | `kpipe.producer.messages.sent`                 | counter   |
|                   | `kpipe.producer.messages.failed`               | counter   |
|                   | `kpipe.producer.dlq.sent`                      | counter   |
|                   | `kpipe.producer.dlq.failed`                    | counter   |
| `ConsumerMetrics` | `kpipe.consumer.messages.received`             | counter   |
|                   | `kpipe.consumer.messages.processed`            | counter   |
|                   | `kpipe.consumer.messages.errors`               | counter   |
|                   | `kpipe.consumer.processing.duration`           | histogram |
|                   | `kpipe.consumer.messages.inflight`             | gauge     |
|                   | `kpipe.consumer.backpressure.pauses`           | counter   |
|                   | `kpipe.consumer.backpressure.time`             | counter   |
|                   | `kpipe.consumer.circuit_breaker.trips`         | counter   |
|                   | `kpipe.consumer.circuit_breaker.state_changes` | counter   |
|                   | `kpipe.consumer.circuit_breaker.time_open`     | counter   |

Log-based fallback for users who don't run OTel: `ConsumerMetricsReporter` (consumer-wide snapshot) +
`EntryMetricsReporter` (per-entry, namespace-aware via `forProcessors(...)` / `forSinks(...)`).

## §11 KPipeConsumer concurrency & safety patterns

- **State machine.** All transitions use single-read `compareAndSet` — read once into a local, decide, CAS. Never two
  sequential CAS calls (double-CAS window). Shared helper `transitionToClosing()` is used by `close()` and
  `uncaughtExceptionHandler`.
- **Error handler safety.** Every `errorHandler.accept()` call site is wrapped in try-catch. A throwing user callback
  must never crash the consumer thread, leak in-flight counts, or skip offset marking. When `markOffsetProcessed()` is
  called on the per-record error paths (`handleProcessingError`, batch callback) it is always called **before**
  `errorHandler.accept()`. Exceptions where it is NOT called at all: (1) `handleParallelRejection` (executor rejected a
  record during shutdown) skips both — the record never started processing, and at shutdown re-fetch on restart is the
  right recovery, not a poisoned mark; (2) **DLQ send failure** — when a DLQ is configured but `sendToDlq` returns
  `false`, the record is neither processed nor durably parked, so the offset is left pending (reprocessed on restart)
  and the `dlqFailed` counter is incremented. A down DLQ applies backpressure rather than silently dropping. The
  no-DLQ-configured path still marks (log-and-advance is the caller's explicit opt-in).
- **Shutdown guarantee.** `state.set(CLOSED)` lives in a nested `finally` inside the consumer thread's outer `finally`.
  Even if `kafkaConsumer.close()` throws, the consumer always reaches terminal state.
- **Paused = keep polling (rewritten with the lag-park-forever fix, PR #233; previously LockSupport.park).** A paused
  consumer no longer parks: each iteration flushes commands, defensively re-issues `pause(assignment())` (per-partition
  pause doesn't survive revoke/assign, and this closes the state-flipped-before-command-queued race), then polls
  normally. Paused partitions fetch nothing, but the poll (a) keeps group membership alive — Kafka's pause+poll
  contract, which kills the silent `max.poll.interval.ms` eviction for ALL pause sources, and (b) bounds the iteration
  to `pollTimeout` so `tickBackpressure` re-evaluates on a fixed cadence (external lag drops — reassignment, retention
  truncation, offset reset — now resume the consumer). Records slipping through a mid-poll rebalance are processed,
  never dropped (positions already advanced; dropping would lose data on commit). Resume latency is ≤ `pollTimeout`
  (100ms default) rather than an instant unpark; the remaining unparks (`internalResume`, `close`,
  `afterRecordComplete`) are best-effort latency nudges for the teardown-drain `parkNanos` path only. History: the old
  indefinite `park()` deadlocked forever under the lag strategy (SEQUENTIAL has no async completions to unpark it, and
  lag is monotonically non-decreasing while parked) and got the consumer evicted from the group after 5 minutes.
- **Pipeline null handling.** Null record value and null deserialization throw specific `IllegalStateException` messages
  (retryable). Null `process()` result is intentional filtering — mark offset processed, count as success, no error.
- **Metrics immutability.** `getMetrics()` returns `Collections.unmodifiableMap()`.
- **Thread-safety boundaries.** `processCommands()` is package-private — external callers can't invoke
  `kafkaConsumer.pause()`/`resume()`/`commitSync()` from arbitrary threads. `isRunning()` snapshots `state.get()` into a
  local before comparing.

## §12 Explicit pipeline error semantics — no silent failures

**Doctrine:** `MessagePipeline.process()` returns sealed `Result<T>` (`Passed | Filtered | Failed`). The three outcomes
are distinct compiler-enforced types — the §12 rule moved from "convention" to "guarantee enforced by exhaustive pattern
matching" in 1.13.0.

**Real-world burn:** before 1.9, `apply()` caught all exceptions and returned `null`, and the downstream consumer
treated `processed == null` as intentional filtering and incremented `messagesProcessed` — masking deserialization
errors as successes. Discovered during a Grafana session: protobuf seed messages all failed deserialization
(`skipBytes(5)` was wrong) but were reported as processed. The only signal was `messagesProcessed` rising while
`sinkInvocationCount` stayed at 0.

**Invariants:**

- Null record value or null deserialization → throw `IllegalStateException` (retryable). Format implementations must
  throw, not return null.
- **Read-path failures throw `IllegalStateException`, uniformly (1.19.0).** Every format's deserialize / wire-envelope
  parse / descriptor-index resolution failure throws `IllegalStateException` — JSON, Avro, Protobuf (envelope + decode),
  and `ConfluentProtobufDescriptorCompiler`'s message-index errors were unified from a JSON/Avro→`RuntimeException`,
  Protobuf-envelope→`IllegalStateException`, confluent-index→`IllegalArgumentException` divergence. Two boundaries are
  deliberate: a `SchemaResolver` that throws propagates its own exception unchanged (a transiently-down registry is not
  a malformed-envelope condition to reclassify), and **write-path** `serialize` failures stay `RuntimeException` (a
  can't-encode-this-object failure is a distinct mode from bad-input-bytes). Not a correctness hazard (no consumer path
  switches on exception type — retry catches `Exception` uniformly); this is log/reader consistency.
- `process()` returns `Result.Passed` / `Result.Filtered` / `Result.Failed(cause)`. `Result.filtered()` is a **shared
  singleton** to avoid per-record allocation; `Passed` and `Failed` allocate one record per call.
- Operators (`UnaryOperator<T>`) still return `T` or null-for-filter — `Result` wrapping lives at the pipeline level.
  `TypedPipelineBuilder.build()` catches `RuntimeException` from operators → `Result.failed(...)`; null returns →
  `Result.filtered()`.
- **No byte-level convenience entry points.** Callers do `deserializeOrFail(bytes)` then `switch (process(value))`
  directly — the architecture-deepening pass removed the legacy `apply` / `processToSink` / `processToValue` shims that
  unwrapped `Result` back to null-for-filter / rethrow-for-failure. Recreating that overloaded-null shape would defeat
  the type-level enforcement §12 exists for.

**Generalizable rule:** when composing `Function<T, R>` chains, never use `null`/`Optional.empty()` to signal both
"filtered" and "failed." Model them as distinct types or distinct exceptions. **Triage heuristic:** when a "processed"
counter rises but downstream invocations stay at 0, suspect overloaded null semantics first.

## §13 Fluent facade (1.10.0+)

**Layered API.** Two public surfaces:

1. **`io.github.eschizoid.kpipe.KPipe.json/avro/protobuf/bytes/custom(...)`** — fluent, immutable, returns `Stream<T>` →
   `Sink<T>` → `Handle`. The 80% path. The `custom(topic, props, format)` overload accepts any user-supplied
   `MessageFormat<T>` for payload types not covered by the bundled format modules.
2. **`MessageProcessorRegistry` + `KPipeConsumerBuilder`** — explicit, multi-step. The 20% path.

**Immutability contract.** `DefaultStream` is a Java record; every fluent method returns a NEW instance carrying updated
config. Operators are stored as `List.copyOf(...)`. Branching from a common root is safe: `s.pipe(a)` and `s.pipe(b)`
produce independent streams. Adding a new fluent setter is a one-place change: declare the component, add the `with*`
method, reference in `DefaultSink.buildPipeline` if it affects pipeline construction.

`Handle` is `AutoCloseable` with a default `close()` that calls `shutdownGracefully(Duration.ofSeconds(5))`. `metrics()`
returns `Map<String, Long>` (typed).

## §14 Audit-derived concurrency patterns

These came out of deep internal audit work — preserve across the codebase.

- **Atomic remove-if-empty on `ConcurrentHashMap` of mutable collections.** Never do
  `if (set.isEmpty()) map.remove(key)` after a separate `set.remove(value)` — non-atomic, a concurrent `computeIfAbsent`
  can repopulate the set between the check and the map removal. Use
  `map.computeIfPresent(key, (k, v) -> { v.remove(value); return v.isEmpty() ? null : v; })`. The bucket lock makes the
  whole sequence atomic.
- **`ConcurrentSkipListSet.first()` is a check-then-act trap.** `if (!set.isEmpty()) set.first()` can throw
  `NoSuchElementException` under contention. Wrap in `safeFirst` returning `null` on `NoSuchElementException`. Same for
  `last()`.
- **`(Properties) parent.clone()`, NOT `new Properties(parent)`.** The latter makes parent a _fallback for
  `getProperty()`_, not a copy. `putIfAbsent` operates on the new instance only and silently shadows parent entries.
  Always `clone()` when deriving a copy.
- **CAS-to-CAS happens-before doesn't extend to post-CAS field writes.** If `start()` does `state.compareAndSet(...)`
  then assigns `scheduler = ...`, a concurrent `close()` doing its own CAS is NOT guaranteed to see `scheduler`. Mark
  such fields `volatile`, or move the assignment inside the CAS-protected critical section.
- **`Kafka.endOffsets(...)` without a `Duration`** uses `default.api.timeout.ms` (60s default). On a hot path, a slow
  broker stalls the entire consumer. Always pass a bounded timeout.
- **`catch (Exception) { return defaultValue; }` is a silent-failure trap.** Never swallow without logging at WARNING.
  If you catch `InterruptException` (Kafka) or `InterruptedException`, **always** call
  `Thread.currentThread().interrupt()` to restore the flag.
- **Don't expose internal counters through public APIs.** A method taking `AtomicLong` for "optionally update this
  counter on success" leaks implementation detail. Return `boolean` and let the caller update its own counter.

## §15 Multi-topic dispatch (1.11.0)

One `KPipeConsumer`, one Kafka consumer, one consumer-group, one offset manager. Either a single shared pipeline across
N topics (homogeneous, `Builder.withPipeline(...)`) or a per-topic pipeline map (heterogeneous,
`Builder.withPipelines(Map)`).

**Unrouted-topic policy.** If a record arrives for a topic with no registered pipeline (rebalance race, config error):
**drop + log at WARNING + mark offset processed.** Never throw (would crash the consumer thread for a config error) and
never DLQ (no per-topic DLQ exists). The "mark processed" part is critical — without it, the same record gets re-fetched
forever.

**Config-error rejection.** Mixing `withTopic`/`withTopics` with `withPipelines` is rejected as a config error (silent
override would mask user mistakes).

## §16 No-deprecation policy

When a public API has to go: **delete it and migrate all callers in the same PR.** No `@Deprecated`, no `@deprecated`
Javadoc, no `since = "..."`.

**Reasoning:** deprecation cycles bloat the surface, train users to ignore warnings, and rot in place when the promised
"removal in next major" never happens. The PR description mentions the removal explicitly so callers always have a
working reference.

**This rule expires at 2.0.** It favours the codebase over its users, which is right while the API is still finding its
shape and wrong once people depend on it. `docs/VERSIONING.md` is the written promise: minors may remove public API
through 1.x with a migration table in the release notes; 2.0 onward is strict semver with a deprecation cycle. That
document also fixes the public / SPI / internal boundary. Note what does **not** draw it: every package in every module
here is exported and there are no qualified exports anywhere, so the `exports` clauses hide nothing and cannot be read
as the API surface. Package-private is the real internal boundary — which is why types like `KeyOrderedDispatcher`,
`OffsetLedger` and `PendingOffsetSet` carry no guarantees despite the modules around them being fully open. Keep that
document current when the surface moves; it is the only page an evaluator reads before deciding whether to depend on
this.

## §17 Core ↔ Facade capability mapping

Source of truth for which capabilities are on the fluent facade (`KPipe.X(...)` → `Stream<T>` → `Sink<T>` → `Handle`,
plus `KPipe.multi(props)` → `MultiBuilder`) versus only via the explicit API (`MessageProcessorRegistry` +
`KPipeConsumerBuilder`).

When adding a new consumer/builder feature: decide whether it belongs on the 80% path (add to `Stream<T>` and
`MultiBuilder`) or stays as an escape hatch (explicit-only). Update this table in the same PR. Rows in **bold** are
deliberately escape-hatch-only.

| Capability                                     | Source module                                                                                   | Facade path                                                                                                                            | Explicit-API path                                                                                                                   | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Format selection                               | `kpipe-format-*`                                                                                | `KPipe.json/avro/protobuf/bytes/custom(...)`                                                                                           | `new MessageProcessorRegistry()` + `registry.pipeline(format)`                                                                      | Custom format: pass any `MessageFormat<T>` to `KPipe.custom(topic, props, format)` for the fluent path, or wire it through `MessageProcessorRegistry` directly for the explicit path.                                                                                                                                                                                                                                                                                        |
| Topic subscription (homogeneous)               | `kpipe-consumer`                                                                                | `KPipe.X(topic, props)`                                                                                                                | `Builder.withTopic(s) / withTopics(...)`                                                                                            | One topic-set, one shared pipeline.                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| Topic subscription (heterogeneous)             | `kpipe-consumer`                                                                                | `KPipe.multi(props).json(...).avro(...)...start()`                                                                                     | `Builder.withPipelines(Map<String, MessagePipeline<?>>)`                                                                            | Per-topic dispatch (§15).                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Operator chain (`pipe/filter/peek/when`)       | `kpipe-core`                                                                                    | `Stream.pipe / filter / peek / when`                                                                                                   | `MessageProcessorRegistry.pipeline(format).add(...)`                                                                                | Identical operator semantics.                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Custom terminal sink                           | `kpipe-core`                                                                                    | `Stream.toCustom(MessageSink<T>)`                                                                                                      | `registry.registerSink(key, sink)`                                                                                                  | Any `MessageSink<T>`; facade also offers `.toConsole()`.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| Multi-sink fanout                              | `kpipe-core`                                                                                    | `Stream.toMulti(sinks...)`                                                                                                             | `new CompositeMessageSink<>(...)`                                                                                                   | Best-effort delivery; per-sink errors logged + suppressed.                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Batch sink (size + age flush)                  | `kpipe-core`                                                                                    | `Stream.toBatch(BatchSink<T>, BatchPolicy)`                                                                                            | `Builder.withBatchPipeline(topic, pipeline, sink, policy)` (multi-call for heterogeneous batch)                                     | `BatchSink<T>` returns `BatchResult` for per-record DLQ; `BatchSink.ofVoid(consumer)` wraps void-style sinks (whole-batch DLQ on throw). Both sequential and parallel modes. See §18.                                                                                                                                                                                                                                                                                        |
| Batch sink (multi-topic via `MultiBuilder`)    | `kpipe-api`                                                                                     | `KPipe.multi(props).json(topic, s -> s.toBatch(sink, policy))...start()`                                                               | `Builder.withBatchPipeline(...)` × N                                                                                                | One consumer-group, mixed batch + non-batch routes.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| Skip wire-format prefix                        | `kpipe-core`                                                                                    | `Stream.skipBytes(int)`                                                                                                                | `TypedPipelineBuilder.skipBytes(int)`                                                                                               | Confluent envelope: 5 (Avro) / 6 (Proto single-msg).                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Confluent SR per-record auto-lookup (Avro)     | `kpipe-format-avro` + `kpipe-schema-registry-confluent`                                         | `KPipe.avro(topic, props, resolver)` / `MultiBuilder.avro(topic, resolver, cfg)` / `Stream.withSchemaRegistry(SchemaResolver)`         | `AvroFormat.withRegistry(SchemaResolver)`                                                                                           | Reads the 5-byte wire envelope, caches schemas by ID (immutable in SR; no TTL). Schema parser bundled in `kpipe-format-avro`, so no `-confluent` module needed. See §19.                                                                                                                                                                                                                                                                                                     |
| Confluent SR per-record auto-lookup (Protobuf) | `kpipe-format-protobuf` + `kpipe-format-protobuf-confluent` + `kpipe-schema-registry-confluent` | `KPipe.protobuf(topic, props, resolver)` / `MultiBuilder.protobuf(topic, resolver, cfg)` / `Stream.withSchemaRegistry(SchemaResolver)` | `ProtobufFormat.withRegistry(SchemaResolver)`                                                                                       | Mirror of the Avro row. Reads the 6-byte envelope (magic + id + message-index varints), compiles `.proto` text → `Descriptor` via the `ProtobufDescriptorCompiler` discovered from the opt-in shaded `kpipe-format-protobuf-confluent` (protobuf-java has no `.proto` parser). Proven wire-compatible with Confluent's `KafkaProtobufSerializer`. See §19.                                                                                                                   |
| Pipeline outcome counters (OTel)               | `kpipe-metrics-otel`                                                                            | `Stream.peekResult(PipelineMetricsObserver)`                                                                                           | hand a `Consumer<Result<T>>` to `peekResult`                                                                                        | Emits `kpipe.pipeline.passed/filtered/failed` counters. Observer-only — doesn't suppress or reroute (see §11 observer-wrap layer).                                                                                                                                                                                                                                                                                                                                           |
| Retry policy                                   | `kpipe-consumer`                                                                                | `Stream.withRetry(int, Duration)` / `MultiBuilder.withRetry(...)`                                                                      | `Builder.withRetry(int, Duration)`                                                                                                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Circuit breaker                                | `kpipe-consumer`                                                                                | `Stream.withCircuitBreaker(CircuitBreakerController)` / `MultiBuilder.withCircuitBreaker(...)`                                         | `Builder.withCircuitBreaker(CircuitBreakerController)`                                                                              | Rolling-window failure-rate trip. CLOSED → OPEN once both `windowSize` samples and `failureThreshold` rate are met. OPEN → HALF_OPEN via a one-shot scheduled task after `openDuration` (no periodic tick — the delay encodes the threshold). HALF_OPEN → CLOSED on next success, → OPEN on next failure (window restarts). End-to-end coverage in `KPipeCircuitBreakerIntegrationTest`.                                                                                     |
| Backpressure (default watermarks)              | `kpipe-consumer`                                                                                | `Stream.withBackpressure()` / `MultiBuilder.withBackpressure()`                                                                        | `Builder.withBackpressure(BackpressureController)`                                                                                  | Defaults: pause 10k, resume 7k.                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Backpressure (custom watermarks)               | `kpipe-consumer`                                                                                | `Stream.withBackpressure(high, low)` / `MultiBuilder.withBackpressure(high, low)`                                                      | `Builder.withBackpressure(BackpressureController)`                                                                                  | High/low strategy auto-derived from `ProcessingMode`.                                                                                                                                                                                                                                                                                                                                                                                                                        |
| Processing mode                                | `kpipe-consumer`                                                                                | `Stream.withProcessingMode(ProcessingMode)`                                                                                            | `Builder.withProcessingMode(ProcessingMode)`                                                                                        | Three modes: `SEQUENTIAL` (lag-based BP §5), `PARALLEL` (default, in-flight BP), `KEY_ORDERED` (per-key serial via `KeyOrderedDispatcher`, in-flight BP). See §20.                                                                                                                                                                                                                                                                                                           |
| Key-ordered LRU cap                            | `kpipe-consumer`                                                                                | `Stream.withKeyOrderedMaxKeys(int)`                                                                                                    | `Builder.withKeyOrderedMaxKeys(int)`                                                                                                | Default 10,000 distinct keys held in memory. Only meaningful for `KEY_ORDERED`. Null-keyed records share a single sentinel queue.                                                                                                                                                                                                                                                                                                                                            |
| OTel/custom metrics                            | `kpipe-metrics(-otel)`                                                                          | `Stream.withMetrics(ConsumerMetrics)` / `MultiBuilder.withMetrics(...)`                                                                | `Builder.withMetrics(ConsumerMetrics)`                                                                                              | Single-format and multi-topic both supported.                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Tracing                                        | `kpipe-tracing-otel` + `kpipe-tracing`                                                          | `Stream.withTracer(Tracer)` / `MultiBuilder.withTracer(Tracer)`                                                                        | `Builder.withTracer(Tracer)`                                                                                                        | `Tracer` is the SPI (its own module, `kpipe-tracing`, package `io.github.eschizoid.kpipe.tracing` — since 1.19.0); `OtelTracer` is the OpenTelemetry-backed implementation in `kpipe-tracing-otel` that wires W3C `traceparent` propagation across the Kafka boundary. Default is `Tracer.noop()` — zero cost when not configured.                                                                                                                                           |
| Custom error handler                           | `kpipe-consumer`                                                                                | `Stream.withErrorHandler(...)` / `MultiBuilder.withErrorHandler(...)`                                                                  | `Builder.withErrorHandler(ErrorHandler<K>)`                                                                                         | Default logs at WARNING (§11). Facade handler is `Consumer<KPipeConsumer.ProcessingError>`.                                                                                                                                                                                                                                                                                                                                                                                  |
| Dead-letter topic                              | `kpipe-consumer`                                                                                | `Stream.withDeadLetterTopic(String)` / `MultiBuilder.withDeadLetterTopic(String)`                                                      | `Builder.withDeadLetterTopic(String)` / `Builder.withDeadLetterQueue(String, KPipeProducer)`                                        | One DLQ for the whole consumer (no per-route DLQ). The two-arg builder form pairs the topic and a pre-built producer atomically — preferred over calling `withDeadLetterTopic` and `withKafkaProducer` separately because it makes the producer's role (DLQ-specific, not generic) explicit and stops the two settings from drifting out of sync.                                                                                                                            |
| Poll timeout                                   | `kpipe-consumer`                                                                                | `Stream.withPollTimeout(Duration)` / `MultiBuilder.withPollTimeout(Duration)`                                                          | `Builder.withPollTimeout(Duration)`                                                                                                 | Default 100ms.                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Lifecycle handle                               | `kpipe-api`                                                                                     | `Handle.isHealthy / metrics / awaitShutdown / close`                                                                                   | `KPipeConsumer.start / awaitShutdown / shutdownGracefully / waitForInFlightDrain`                                                   | `Handle` is `AutoCloseable` (5s graceful default). Since 1.13: consumer hosts the lifecycle directly — no separate `KPipeRunner`.                                                                                                                                                                                                                                                                                                                                            |
| **Custom `OffsetManager`**                     | `kpipe-consumer`                                                                                | — (escape hatch only)                                                                                                                  | `Builder.withOffsetManager(OffsetManager<K>)` / `Builder.withOffsetManagerProvider(Function<Consumer<K,byte[]>, OffsetManager<K>>)` | E.g. Postgres/Redis-backed manager. The `Provider` form gets the live Kafka consumer so it can wire `commitSync` callbacks.                                                                                                                                                                                                                                                                                                                                                  |
| **Custom Kafka `Consumer` factory**            | `kpipe-consumer`                                                                                | —                                                                                                                                      | `Builder.withConsumer(Supplier<Consumer<K,byte[]>>)`                                                                                | Test seam / SSL configurators.                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| **Custom DLQ `KafkaProducer`**                 | `kpipe-producer`                                                                                | —                                                                                                                                      | `Builder.withKafkaProducer(Producer<K,byte[]>)` / `Builder.withKafkaProducer(KPipeProducer<K,byte[]>)`                              | Override default producer for the DLQ. Use the `Producer` overload to hand in a raw Kafka producer; use the `KPipeProducer` overload to share an already-wrapped instance (e.g. one with custom metrics already attached).                                                                                                                                                                                                                                                   |
| **Rebalance listener**                         | `kpipe-consumer`                                                                                | —                                                                                                                                      | Owned by `OffsetManager` — override `OffsetManager.createRebalanceListener()` in a custom manager                                   | No standalone `withRebalanceListener` — the Builder sets it from whatever `OffsetManager` is registered. External offset commit hooks live alongside that manager.                                                                                                                                                                                                                                                                                                           |
| **Custom command queue**                       | `kpipe-consumer`                                                                                | —                                                                                                                                      | `Builder.withCommandQueue(Queue<ConsumerCommand>)`                                                                                  | Test seam (rarely needed).                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| **Thread/executor termination**                | `kpipe-consumer`                                                                                | —                                                                                                                                      | `Builder.withThreadTerminationTimeout / withWaitForMessagesTimeout`                                                                 | Shutdown tuning. The executor-drain timeout is non-tunable from the Builder — it stays at `AppConfig.DEFAULT_EXECUTOR_TERMINATION`.                                                                                                                                                                                                                                                                                                                                          |
| **Periodic metrics reporting + shutdown hook** | `kpipe-consumer`                                                                                | —                                                                                                                                      | `Builder.withMetricsReporters(...) / withMetricsInterval(...) / withShutdownHook(true)`                                             | Folded into `KPipeConsumerBuilder` in 1.13. Reporter thread is daemon, doesn't keep the JVM up.                                                                                                                                                                                                                                                                                                                                                                              |
| **Health endpoint**                            | `kpipe-consumer`                                                                                | —                                                                                                                                      | `HttpHealthServer.fromEnv(...)`                                                                                                     | Run alongside `Handle` in the host process.                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| **In-flight drain**                            | `kpipe-consumer`                                                                                | `Handle.shutdownGracefully(Duration)`                                                                                                  | `KPipeConsumer.waitForInFlightDrain(Duration) / shutdownGracefully(Duration)`                                                       | Replaces the deleted `MessageTracker` class. Waits on `dispatcher.activeCount()` (records a worker is actively processing), NOT `totalInFlight()`. Buffered batch records are excluded — they never flush mid-drain, only on size/age trigger or `BatchPipelineWrapper.close()` at teardown, so waiting on them just burns the timeout; teardown flushes + commits them right after. Backpressure still uses `totalInFlight()` (buffered records count for memory pressure). |
| **Custom `MessageProcessorRegistry`**          | `kpipe-core`                                                                                    | —                                                                                                                                      | `new MessageProcessorRegistry()` + `register*(...)` + `pipeline(format)`                                                            | Pre-shared pipelines across consumers, multi-format orchestrators. The registry is format-agnostic — the format is supplied per pipeline call, not on construction.                                                                                                                                                                                                                                                                                                          |

## §18 Batch sink architecture (1.12.0)

- **One `BatchSink<T> extends Function<List<T>, BatchResult>`.** Implementations that report per-record outcomes return
  `BatchResult` directly; void-style consumers wrap with `BatchSink.ofVoid(consumer::accept)` — normal return →
  `BatchResult.allSucceeded(size)`, throw → `BatchResult.allFailed(size, e)`. **No separate `PartialBatchSink`** — the
  void shape is just a special case (collapsed in 1.12.0).
- **Coverage contract enforced.** A `BatchResult` whose `succeededIndexes` ∪ `failedByIndex.keys()` doesn't cover every
  `[0, batchSize)` is a contract violation. `BatchPipelineWrapper` flags missing indexes with a synthetic
  `IllegalStateException` and routes them to the DLQ rather than silently marking them processed (§12). Out-of-range
  indexes are logged at WARNING; a `null` `BatchResult` is treated as whole-batch failure.
- **`BatchPipelineWrapper` owns buffer + lock + gauge + age-tick.** One wrapper per topic; a single `ReentrantLock`
  serializes `enqueue` / `tick` / `close` / `flushLocked`, and one flush per topic is in flight at a time. Note how much
  runs under that lock: `flushLocked` calls the user's `BatchSink` while holding it, and does **not** release when the
  sink returns — the per-record outcome dispatch runs there too, including `markProcessed` (which reaches a
  user-supplied `OffsetManager`, possibly Postgres- or Redis-backed) and `onBatchFailure` (which reaches a synchronous
  DLQ produce that waits for the broker ack). `failAll` does that produce once per record, serially, so a whole-batch
  failure against an unavailable DLQ holds the topic's lock for the batch size times the per-record produce timeout —
  `max.block.ms` or `delivery.timeout.ms` depending on whether DLQ topic metadata is cached, per the refuted-claims
  entry below. An interrupt collapses that: the producer restores the interrupt flag, so every later send in the loop
  fails on entry and the hold falls to roughly one timeout rather than N. The sink is arbitrary user code of unbounded
  duration — that, not any assumption that it performs I/O, is why holding the lock across it matters.

  The age tick adds a second dimension: the scheduler is a **single** thread shared by every topic's tick and by the
  circuit-breaker probe, so an age-triggered flush that blocks also delays age flushes on every other topic and the
  breaker's OPEN → HALF_OPEN transition. Size-triggered flushes run wherever the dispatcher placed the record — a worker
  virtual thread under PARALLEL and KEY_ORDERED, but the **consumer thread itself under SEQUENTIAL**, where a blocking
  sink stalls the poll loop and risks `max.poll.interval.ms` eviction, the same end state the paused loop keeps polling
  to avoid. Whether one-flush-at-a-time is a guarantee worth keeping or an accident of lock placement is tracked in
  #313. Constructed in the consumer ctor, started in `start()`, drained in `close()`.

- **Backpressure participation in parallel mode.** `inFlightCount` is decremented as soon as `processRecord` returns —
  for batch paths that's "the record was buffered," which would make buffered records invisible to the in-flight
  watermark. The wrapper's `bufferedCount()` is added to `KPipeConsumer.totalInFlight()` to close that gap.
- **Offset commits use `OffsetManager` directly, not the command queue.** The command queue retains its real job:
  serializing Kafka-consumer calls (`pause` / `resume` / `commitSync`) that genuinely need the consumer thread.
  Originally batch-only (1.12.0); the architecture-deepening pass extended this to ALL offset bookkeeping — worker
  virtual threads on every processing mode now call `offsetManager.markOffsetProcessed` / `trackOffset` directly. The
  manager is thread-safe (atomic ops on `ConcurrentHashMap` / `ConcurrentSkipListSet`), so the queue was pure ceremony
  plus a hop-to-consumer-thread latency penalty on every record. Bypassing it also means shutdown drain works even after
  the consumer thread has exited.
- **Whole-batch shutdown drain happens BEFORE `offsetManager.close()`.** The consumer's `close()` is split into four
  named phases — `closeNeverStarted` (fast path), `initiateShutdown` (queue Close + signal dispatcher + wait for
  in-flight drain), `waitForConsumerThreadToJoin` (wakeup + unpark + join), `finalizeAfterThreadJoined` (idempotent
  safety net). `releaseConstructedResources` runs in order: `dispatcher.close()` → drain all batch wrappers (flushes
  remaining buffers, marks their offsets via the now-direct `OffsetManager` call) → health shutdown → scheduler →
  `offsetManager.close()` (final commitSync) → producer close → state = CLOSED. CAS-guarded so it runs exactly once
  whether triggered by external `close()` or by a self-terminating consumer thread.
- **Multi-topic batching is heterogeneous-only via `MultiBuilder`.** A consumer can host any mix of regular and batch
  routes, but a single topic can only appear in one or the other — the disjoint-set check fires in `Builder.build()`.
- **Bench harness** lives in `benchmarks/`. `BatchSinkLatencyBenchmark` drives the public facade through `MockConsumer`
  so `BatchPipelineWrapper` stays package-private.

## §19 Confluent SR auto-lookup (1.14.0)

**Two modes, one format class.** `AvroFormat` now operates in either static mode (`new AvroFormat(schema)` — single
schema for the lifetime of the codec) or registry mode (`AvroFormat.withRegistry(SchemaResolver)` — per-record envelope
read + schema lookup). The mode is decided at construction and never changes. Registry mode rejects `serialize` with
`UnsupportedOperationException` — KPipe is consumer-first; if you need writer-side SR, construct the format in static
mode with the writer schema you want.

**Why per-record lookup matters even with FORWARD-compatible evolution.** A static-fetch-at-startup codec reads the
schema once and treats it as both writer and reader. When the producer rolls v2, v2 bytes hit the consumer and the
static reader decodes them against v1 — silently corrupting the output (extra v2 fields read as part of the next field,
or v1 fields offset wrong if a field was removed). FORWARD compatibility at the SR level allows non-append evolution
(field removal with defaults, type promotion, union reordering) which the static path can't decode safely. Per-record
auto-lookup uses the actual writer schema for each record, then projects to the reader schema via
`GenericDatumReader(writerSchema, readerSchema)` — Avro's schema-resolution rules handle the evolution.

**Cache design.** `CachedSchemaResolver` in `kpipe-schema-registry-confluent` wraps any `SchemaResolver` with a
`ConcurrentHashMap<Integer, String>` cache. No TTL, no LRU eviction — Confluent SR schema IDs are immutable, so
cache-by-ID is trivially correct and cardinality is naturally bounded (typically tens of distinct schemas across the
lifetime of a topic). `computeIfAbsent` atomicizes load+store so concurrent misses on the same ID collapse to one HTTP
call. The format itself caches _parsed_ `Schema` instances by ID on top of the resolver — two-level cache, both correct
because IDs never reassign.

**No `skipBytes(5)` when registry-backed.** The format reads the 5-byte envelope itself. Combining
`.withSchemaRegistry(...)` with `.skipBytes(5)` would strip the envelope before the format sees it, leaving the schema
ID unreadable. Users who set both get a decode error; documented in `Stream.skipBytes` Javadoc.

**Protobuf SR now ships (two-module design).** Confluent Protobuf SR returns `.proto` _source text_ (not a binary
descriptor), and `protobuf-java` has **no `.proto`-text parser** (it only builds a `Descriptor` from a binary
`FileDescriptorProto`). This is the key asymmetry with Avro: Avro compiles its JSON schema text with its **own** bundled
parser (`Schema.Parser`), so `AvroFormat.withRegistry` needs no extra module; Protobuf's compiler is heavy and
JPMS-hostile, so it lives in a **separate module**. The split mirrors `kpipe-metrics` → `kpipe-metrics-otel` and the
`Tracer` → `kpipe-tracing-otel` SPI/impl pattern:

- **`kpipe-format-protobuf`** (base, stays a tiny real JPMS module, ~12K): the `ProtobufDescriptorCompiler` SPI +
  `ProtobufFormat.withRegistry(SchemaResolver)` — **single-param, an exact mirror of `AvroFormat.withRegistry`**. The
  compiler is discovered via `ServiceLoader` (`uses ProtobufDescriptorCompiler` in module-info); no Confluent/Wire dep.
- **`kpipe-format-protobuf-confluent`** (impl, opt-in): `ConfluentProtobufDescriptorCompiler` using Confluent's
  `ProtobufSchema` (which parses `.proto` text → `Descriptor` and knows the message-index → message mapping + schema
  references). Ships a `META-INF/services` entry so `ServiceLoader` finds it even though the jar is an **automatic
  module** — the unavoidable JPMS tradeoff of **shading** Square Wire (its `wire-schema-jvm`/`wire-runtime-jvm` split
  `com.squareup.wire`, illegal on the module path, so it's relocated to `io.github.eschizoid.kpipe.shaded.wire` and
  bundled → ~17M). That weight + the automatic-module status fall **only on opt-in SR users**; JSON/Avro/static-Protobuf
  users carry nothing.

The wire envelope (magic + 4-byte schema id + a **message-index varint array** — Kafka `ByteUtils` zig-zag varints, with
the `0x00`→`[0]` single-message shorthand) is parsed in `ProtobufFormat.deserializeFromEnvelope`, the
`(schemaId, message-index)` pair keys a `ConcurrentHashMap` descriptor cache (mirroring Avro's two-level cache), then
`DynamicMessage.parseFrom`. **Proven wire-compatible against Confluent's own `KafkaProtobufSerializer`** (gold-standard
round-trip test in the `-confluent` module). Registry mode rejects `serialize` (`UnsupportedOperationException`, §19
consumer-first); combining registry mode with `.skipBytes(6)` is an error (double-strip) — same rules as Avro. The
static `new ProtobufFormat(descriptor)` + `.skipBytes(6)` path is unchanged for the descriptor-in-hand case.

**Facade formats are opt-in.** `kpipe-api` now `requires static` the JSON/Avro/Protobuf format modules (not
`requires transitive`), so a consumer only pulls the format it uses. This was made possible by the two-module split (the
base protobuf module is tiny); it also means a `KPipe.avro(...)` / `KPipe.protobuf(...)` caller must add that format
module to their own build.

**Generalizable rule:** when SR returns by-ID, cache forever in-process — the immutability of the key removes every
cache-coherence concern that would otherwise need TTLs or invalidation protocols.

## §20 Dispatcher abstraction (1.15.0)

**Three-way dispatch.** `KPipeConsumer` no longer branches on a `sequentialProcessing` boolean. Instead, a sealed
`Dispatcher<K>` interface has three implementations selected at construction time from a `ProcessingMode` enum:

- `SequentialDispatcher` — runs each record inline on the consumer thread. `activeCount()` returns 0 or 1 (incremented
  around the inline `processTask.run()`) so `inFlight` metrics and `shutdownGracefully(timeout)` drain reporting stay
  accurate. Lag-based backpressure doesn't consult the value.
- `ParallelDispatcher` — owns the virtual-thread executor and an `AtomicLong` in-flight counter. Submits per-record.
- `KeyOrderedDispatcher` — LRU map keyed by record key (null → single sentinel). Each key gets its own serial queue
  drained by a virtual-thread worker. Workers exit when the queue empties; new records for the same key start a fresh
  worker. Cap on distinct keys is configurable (`withKeyOrderedMaxKeys`, default 10,000); eviction walks LRU from
  oldest, skipping non-empty queues. If all queues at cap are non-empty, dispatch holds (releases lock, yields, retries)
  — implicit backpressure for the KEY_ORDERED path.

**In-flight ownership.** Pre-1.15 `KPipeConsumer` held `AtomicLong inFlightCount` directly and called
`incrementAndGet()` in `processRecords` and `decrementAndGet()` in `processRecord`'s finally. 1.15 moved that ownership
into each dispatcher: all three now own their own `activeCount()`. `SequentialDispatcher` tracks a 0/1 counter
incremented around the inline `processTask.run()` — lag-based backpressure doesn't read it, but `inFlight` metrics and
`shutdownGracefully(timeout)` drain reporting do, and a hardcoded 0 would lie to both. `ParallelDispatcher` and
`KeyOrderedDispatcher` each own a real counter exposed via `activeCount()`. `KPipeConsumer.totalInFlight()` is now
`dispatcher.activeCount() + Σ batchWrappers.bufferedCount()`.

**Post-record callback.** `processRecord` used to unpark the consumer thread when backpressure was held. That logic
moved to `KPipeConsumer.afterRecordComplete()`, which the dispatcher invokes via the `onComplete` argument to
`dispatch()`. This makes the dispatcher mode-agnostic about the consumer's internal state. (Since PR #233 the paused
consumer keeps polling rather than parking — see §11 — so the unpark-on-completion is a best-effort latency nudge, no
longer a liveness invariant.)

**Migration (§16 delete + migrate).** `withSequentialProcessing(boolean)` deleted from both `KPipeConsumerBuilder` and
the fluent `Stream` facade. Callers migrate to `withProcessingMode(ProcessingMode.SEQUENTIAL)` /
`withProcessingMode(ProcessingMode.PARALLEL)`. Every call site across lib, tests, benchmarks, examples and docs was
updated in the same PR per the no-deprecation policy.

**KEY_ORDERED operability bundle.** Three observability/diagnostic additions ship alongside the dispatcher:

- **One-shot stall WARN.** `KeyOrderedDispatcher.allocateNewQueue` logs a WARNING the first time the LRU cap saturates
  with every queue non-empty (guarded by `AtomicBoolean.compareAndSet`). Repeating per stall would flood logs under
  sustained saturation; one signal is enough to tell an operator to raise `withKeyOrderedMaxKeys`.
- **`topKeyQueueDepths(int n)`** on `Dispatcher` + `KPipeConsumer` + `Handle`. Snapshot of the deepest `n` per-key
  queues for ad-hoc diagnostics (heap-dump replacement, JMX, REPL). Default returns empty list for SEQUENTIAL /
  PARALLEL. Deliberately NOT wired to OTel — per-key cardinality is unbounded.
- **Builder warn-log on silent-ignore.** `KPipeConsumerBuilder.build()` logs WARNING when `withKeyOrderedMaxKeys` was
  set but mode != KEY_ORDERED. Java builders silent-ignore by convention; this surfaces the misconfig without breaking
  chaining.

**Lock contention — resolved in the v2 dispatcher (2026-07-21).** `benchmarks/KeyOrderedDispatchBenchmark` measures
`KEY_ORDERED` vs PARALLEL throughput via MockConsumer (no Docker), parametrized over key cardinality. v1 used a single
`ReentrantLock` for all LRU + queue mutations and flatlined ~380k ops/s regardless of cardinality; v2 replaced it with
`ConcurrentHashMap` + one monitor per key queue (eviction via `computeIfPresent` + a `dead` tombstone set atomically
with removal; a dispatcher holding a stale queue reference observes the tombstone under the monitor and retries).
Interleaved A/B with the PARALLEL arm as drift canary: **+122% at 10k keys, +112% at 100, +18% at 1, control flat** —
`benchmarks/results/2026-07-21-keyordered-dispatch-ab.md`. Key insight: LRU _ordering_ was never a correctness
requirement (only empty+idle queues are ever evicted), so v2 drops the coldest-first preference. Future dispatcher work:
same rule — re-run the bench and demonstrate a measurable win before landing.

**Generalizable rule:** when adding a third mode to a binary switch, do the rename in the same PR. Trying to keep the
old boolean alongside a new enum creates "which one wins?" footguns that always come back as bugs.

## §21 kpipe-test module (Docker-free test kit)

`lib/kpipe-test` publishes `TestStream<T>` + `CapturingSink<T>` (package and JPMS module
`io.github.eschizoid.kpipe.test`) so users can unit-test pipelines without Testcontainers. Full compile artifact, not a
test-classifier jar — it's a runtime tool for users' test suites.

- **Drive model: real consumer, mock transport.** `TestStream` builds a real `KPipeConsumer<String>` over a seeded
  `MockConsumer` (subscribe stubbed to a no-op so the manual `assign` survives — same pattern as
  `ProcessingModeSinkDlqMatrixTest` and the JMH harness). Chosen over direct pipeline invocation so tests exercise the
  production dispatcher / offset / sink code paths and stay in sync as the consumer evolves (~5–20ms/test vs ~1ms).
  Default mode is SEQUENTIAL so capture order equals send order; `withProcessingMode` opts into PARALLEL / KEY_ORDERED
  (assertions must then be order-insensitive).
- **Deterministic `flush()` contract.** Returns only when every sent record is (1) polled (`messagesReceived` ≥ sent),
  (2) accounted for (`messagesProcessed + processingErrors + inFlight` ≥ sent), and (3) drained (`waitForInFlightDrain`,
  then a re-read of (1)+(2)). Records move strictly forward (unpolled → in-flight → buffered/terminal), so this check
  order cannot false-positive. On timeout (default 10s) it throws `AssertionError` carrying the metrics snapshot.
- **Batch v1 semantics: size + shutdown flush only.** `toBatch(sink, maxSize)` flushes on the size trigger while running
  and drains the trailing partial batch on `close()` (the production shutdown-drain guarantee). The age trigger is
  deliberately disabled (1-day age cap) — a virtual-clock age-flush is a follow-up because it has to interact with the
  batch wrapper's scheduled tick.
- **`CapturingSink` has no assertion DSL, on purpose.** `captured()` returns an immutable snapshot `List<T>`; users
  assert with JUnit / AssertJ / Hamcrest. A bespoke DSL was considered and rejected — the value is the deterministic
  drive + capture, not a new assertion vocabulary.
- **`CrashRestartHarness` — deterministic resume-window verification (also public).**
  `CrashRestartHarness.builder(fmt).pipe(op).seed(values).commitUpTo(k).crashAfter(P).restart()` →
  `CrashRestartResult<T>` (`firstRun` / `secondRun` / `committedOffset` / `uncommittedTail`, all immutable, no assertion
  DSL; single-use — `restart()` throws on re-invocation). Two real consumer phases: A processes `[0,P)` and commits `k`,
  "crashes", then B is driven over the resume window `[k,N)`. **Honest scope (a 4-lens review sharpened this — the
  naming originally overclaimed).** B is a _separate_ `MockConsumer` **seeded** with `[k,N)`, so the harness _supplies_
  the resume window rather than making the consumer _seek_ to a committed offset — it therefore pins the resume-window
  **delivery** path (poll→dispatch→operator→sink over `[k,N)`, deterministically, where a live broker is flaky), NOT
  that the consumer resumes from the right offset. That seek + the commit-frontier math (lowest-pending,
  no-commit-ahead) stay covered by the offset Fray/property suites and the broker E2E
  `CrashRestartReprocessingIntegrationTest`. **No-op offset manager on purpose:** a deterministic uncommitted tail
  requires controlling the commit point out-of-band; a real-manager partial commit is the mid-processing timing race
  #220 tripped on. `uncommittedTail()` applies the operators to the seeded `[k,P)` (filter-aware) so it matches the
  sink's post-pipeline shape under any mode — computing it from `firstRun.subList` would be wrong under PARALLEL
  (capture order ≠ offset order). A genuinely load-bearing resume-seek assertion (consumer skips `[0,k)` on its own) is
  tracked in the verification epic, #312.
- **Spotless has three footguns worth knowing before you touch a file.** The Java block sets
  `ratchetFrom("origin/main")`, so it judges only files a change actually touches — `main` therefore reports clean while
  individual files still carry violations, and the first edit to such a file drags the whole file's reformatting into
  your diff. Expect unrelated hunks and a `codecov/patch` drop when that happens. Second, the markdown formatter
  deliberately does **not** load `prettier-plugin-java`: it reformats Java inside fenced code blocks and disagrees with
  google-java-format about lambda parameters, writing `(order) ->` where the Java formatter writes `order ->`, which
  silently rewrote the README quickstart out of step with the compiled `ReadmeQuickstart.java` that
  `scripts/check-docs.sh` compares line by line.
- **The prose formatter is not idempotent, and the second run is the one that corrupts.** Given a bare
  underscore-bearing word in plain prose, followed anywhere later in the same block by a single-delimiter emphasis span,
  pass one is clean and pass two destroys the word: `KEY_ORDERED *behind*` becomes `KEY_ORDERED _behind_`, which on the
  next run becomes `KEY*ORDERED \_behind*` and renders as `KEYORDERED _behind`. Pass three is stable — the corruption
  escapes the underscores and flips the delimiters, leaving nothing further to pair — so **running `spotlessApply` twice
  and diffing is a complete check** for any tree, and needs no heuristics.
- **`spotlessCheck` is the defense against that, which is why CI runs it rather than `spotlessApply`.** The gate passes
  only when `apply(f) == f`, and a file carrying the latent `KEY_ORDERED _behind_` form is by definition not yet at its
  fixed point — so the gate fails it and prints the hunk that would corrupt. What the gate cannot see is text that
  arrives **already** corrupted from outside it, which is how these notes rotted: they lived untracked in `.claude/`,
  spotless rewrote them on disk across many local runs, and #318 copied the corrupted result into a tracked file. Under
  the old `spotlessApply` step CI would silently advance a latent file to the corrupted fixed point and discard the
  result, leaving the bomb in the repo to detonate on somebody's laptop.
- **Backtick identifiers in prose; escaping them is worse than doing nothing.** A code span is never re-parsed for
  emphasis, so `` `KEY_ORDERED` `` is inert. A manual backslash escape is not a remedy — `KEY\_ORDERED *behind*`
  corrupts on pass **one**, a pass earlier than the unescaped form, because the formatter strips the escape and re-pairs
  in the same run. Underscores are also safe inside link text, autolink URLs, and nested strong or emphasis, and the
  hazard needs a _single_-delimiter target: `KEY_ORDERED **bold**` is fine. A `__dunder__` in prose is a separate,
  pass-one effect — it silently becomes bold.
- **`///` Javadoc + google-java-format footgun.** spotless (google-java-format) wraps any `///` doc line **>100
  columns** into a `//` continuation — which the IDE then flags as _dangling Javadoc_ (a real, recurring paper-cut).
  Keep every `///` line ≤ ~95 cols; hand-joining a long line is silently reverted on the next `spotlessApply`. This is
  why some test-tree dangling-Javadoc kept reappearing.

---

## Deliberately deferred

Things decided against, with the reason. Re-proposing any of these needs new evidence, not a new argument — the argument
was already had. Moved here from an untracked roadmap file so the decisions are reviewable; the forward-looking roadmap
lives in the GitHub epics instead (verification in #312, architecture in #313), where it is visible.

- **`Stream.strict()` / `.lenient()` toggle, `KPipe.from(props)` short-form, a `just new-format` scaffold.** Surface
  area without a demonstrated need.
- **Transient-vs-permanent DLQ-send classification.** A failed DLQ send increments a counter, logs at ERROR, and leaves
  the offset pending so the record is reprocessed on restart. A down DLQ applies backpressure rather than silently
  dropping.
- **Extracting a shared DLQ-or-mark helper.** The per-path asymmetries are deliberate, and `DlqTerminalContractTest`
  enforces the lockstep a shared helper would have provided — a 2×3 matrix over both paths, with both production sites
  carrying `LOCKSTEP:` comments naming it.
- **A unified metrics collector / `MetricsContext` bundle, and a `RegistryModeFormat` base class.** Extract when a third
  implementation arrives, not before.
- **`Tracer.isEnabled()`.** `Tracer.noop()` already makes the guard free, so the method would buy nothing.
- **Further dispatcher performance work.** Measured in `benchmarks/results/2026-07-21-keyordered-dispatch-ab.md`:
  dispatch is a rounding error at the broker level once per-record work reaches a millisecond, and the v2 broker verify
  came back flat. Re-open only with evidence of a dispatch-bound production workload, and re-run
  `KeyOrderedDispatchBenchmark` before landing anything.
- **Three refuted audit claims.** `send()` cannot hang forever — both of its phases are bounded, and they add rather
  than substitute; the javadoc on `KPipeProducer.send` carries which knob governs which phase. There is no `Pattern`
  subscription, so no per-topic cardinality bomb. Protobuf message-index over-allocation is rejected before the
  allocation. Do not re-raise without new facts.
- **Header-based schema envelopes** (Azure SR style, schema id in Kafka headers). `MessageFormat.deserialize(byte[])`
  cannot see headers, so this needs a contract extension, not an implementation. Only on real demand.
- **A Spring Boot starter.** Only on a real ask from a Spring shop — that trigger is the whole decision.
- **An aggregated NOTICE for the shaded `-confluent` jar.** Shadow's default merge keeps the first-seen LICENSE and
  NOTICE. A hand-curated aggregate would be better attribution hygiene, not a correctness fix — and if the entry count
  matters, assert it in the build rather than in prose here.

**Still open, and not deferred:** whether tracing should default on when `kpipe-tracing-otel` is present on the
classpath, or stay explicit.

**Two standing facts about this repo, recorded because nothing else holds them.** Copilot code review is effectively
off: adding `copilot-pull-request-reviewer[bot]` as a requested reviewer returns success and attaches nobody, and the
`reviews` array stays empty, so a Copilot done-signal never arrives and any workflow waiting for one will not terminate.
Re-enabling it is a repository setting.
