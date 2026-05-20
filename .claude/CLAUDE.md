# KPipe Architecture and Design Principles

This file is the long-term memory for working in this repo. It records the non-obvious invariants, the footguns that
have already burned us, and the architectural decisions that aren't recoverable just by reading the code. Things that
_are_ recoverable from the code (package layout, type signatures, who calls what) are deliberately omitted — read the
code for those.

## Core invariants

- **Byte boundary at the consumer entry.** `KPipeConsumer<K>` operates on `byte[]` values. Format SerDe lives inside the
  pipeline. Mixed wire formats and Confluent magic-byte prefixes are handled inside operators (`skipBytes(n)`), not at
  the consumer level.
- **Single SerDe cycle per record.** Deserialize once → chain `UnaryOperator<T>` transforms on the typed object →
  serialize once. Never compose `Function<byte[], byte[]>` chains; that's the "SerDe tax" the typed pipeline was built
  to eliminate.
- **Lowest-pending-offset commits.** `ConcurrentSkipListSet` per partition; offset 102 is not committed until 101 also
  finishes. This is what makes "at-least-once with parallel processing" honest.

## Coding standards

- **Java 25 triple-slash Javadoc** (`///`), markdown code blocks (`java ... `). No legacy `/** ... */` or HTML
  `<pre>{@code ... }</pre>`.
- **`final var` for locals** wherever possible.
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
are `getOperator(key)` / `getSink(key)`, and per-namespace utilities carry the `Sink` suffix (`getSinkKeys`,
`getAllSinks`, `getSinkMetrics`, `unregisterSink`, `clearSinks`, `compositeSink`).

**Historical note:** through 1.13 these were `register` / `withErrorHandling` overloaded by argument type. Bare lambdas
like `x -> x` were ambiguous and needed an explicit cast. 1.14 renamed both pairs per §16 (delete + migrate) to kill the
overload-ambiguity footgun.

## §8 Centralized error handling

`RegistryFunctions.withOperatorErrorHandling` (for `UnaryOperator<T>`) and `withConsumerErrorHandling` (for
`MessageSink<T>`) wrap user code so one failing component can't crash the pipeline.

## §9 Modular architecture

`kpipe-metrics` ← `kpipe-producer` ← `kpipe-consumer`. Each module is published individually to Maven Central;
`kpipe-consumer` transitively pulls the other two.

**Split-package rule (JPMS):** no two modules may export the same Java package. When a class references types from
another module, it stays in the module that owns those types.

Package ownership:

- `org.kpipe.metrics` → `kpipe-metrics`
- `org.kpipe.producer.*` → `kpipe-producer`
- `org.kpipe.sink.*` → `kpipe-producer`
- `org.kpipe.consumer.*` → `kpipe-consumer`
- `org.kpipe.consumer.metrics` → `kpipe-consumer` (reporters that depend on registry types)
- `org.kpipe.consumer.sink.*` → `kpipe-consumer`

## §10 OpenTelemetry metrics

`kpipe-metrics` ships interfaces against `opentelemetry-api` only — no SDK. Users bring their own SDK (Prometheus, OTLP,
Jaeger). Both `ConsumerMetrics` / `ProducerMetrics` default to `OpenTelemetry.noop()` (zero cost when not configured)
and wire via `.withMetrics(...)`.

| Component         | Instrument                           | Type      |
| ----------------- | ------------------------------------ | --------- |
| `ProducerMetrics` | `kpipe.producer.messages.sent`       | counter   |
|                   | `kpipe.producer.messages.failed`     | counter   |
|                   | `kpipe.producer.dlq.sent`            | counter   |
| `ConsumerMetrics` | `kpipe.consumer.messages.received`   | counter   |
|                   | `kpipe.consumer.messages.processed`  | counter   |
|                   | `kpipe.consumer.messages.errors`     | counter   |
|                   | `kpipe.consumer.processing.duration` | histogram |
|                   | `kpipe.consumer.messages.inflight`   | gauge     |
|                   | `kpipe.consumer.backpressure.pauses` | counter   |
|                   | `kpipe.consumer.backpressure.time`   | counter   |

Log-based fallback for users who don't run OTel: `ConsumerMetricsReporter` (consumer-wide snapshot) +
`EntryMetricsReporter` (per-entry, namespace-aware via `forProcessors(...)` / `forSinks(...)`).

## §11 KPipeConsumer concurrency & safety patterns

- **State machine.** All transitions use single-read `compareAndSet` — read once into a local, decide, CAS. Never two
  sequential CAS calls (double-CAS window). Shared helper `transitionToClosing()` is used by `close()` and
  `uncaughtExceptionHandler`.
- **Error handler safety.** Every `errorHandler.accept()` call site is wrapped in try-catch. A throwing user callback
  must never crash the consumer thread, leak in-flight counts, or skip offset marking. `markOffsetProcessed()` is always
  called **before** `errorHandler.accept()`.
- **Shutdown guarantee.** `state.set(CLOSED)` lives in a nested `finally` inside the consumer thread's outer `finally`.
  Even if `kafkaConsumer.close()` throws, the consumer always reaches terminal state.
- **LockSupport park/unpark.** Consumer thread uses `LockSupport.park()` (not `Thread.sleep()`) when paused. Unpark
  sources: `internalResume()`, `close()`, and `processRecord()` finally block when `backpressurePaused` is true.
  Commands are flushed via `processCommands()` **before** parking so `kafkaConsumer.pause()` executes immediately.
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

**Real-world burn:** before 1.9, `apply()` caught all exceptions and returned `null`. The downstream
`KPipeConsumer.processTypedRecord` treated `processed == null` as intentional filtering and incremented
`messagesProcessed` — masking deserialization errors as successes. Discovered during a Grafana session: protobuf seed
messages all failed deserialization (`skipBytes(5)` was wrong) but were reported as processed. The only signal was
`messagesProcessed` rising while `sinkInvocationCount` stayed at 0.

**Invariants:**

- Null record value or null deserialization → throw `IllegalStateException` (retryable). Format implementations must
  throw, not return null.
- `process()` returns `Result.Passed` / `Result.Filtered` / `Result.Failed(cause)`. `Result.filtered()` is a **shared
  singleton** to avoid per-record allocation; `Passed` and `Failed` allocate one record per call.
- Operators (`UnaryOperator<T>`) still return `T` or null-for-filter — `Result` wrapping lives at the pipeline level.
  `TypedPipelineBuilder.build()` catches `RuntimeException` from operators → `Result.failed(...)`; null returns →
  `Result.filtered()`.
- Byte-level entry points (`apply` / `processToSink` / `processToValue`) preserve pre-1.13 contracts by unwrapping the
  Result internally: null for filters, re-throw `Failed.cause` for failures.

**Generalizable rule:** when composing `Function<T, R>` chains, never use `null`/`Optional.empty()` to signal both
"filtered" and "failed." Model them as distinct types or distinct exceptions. **Triage heuristic:** when a "processed"
counter rises but downstream invocations stay at 0, suspect overloaded null semantics first.

## §13 Fluent facade (1.10.0+)

**Layered API.** Two public surfaces:

1. **`org.kpipe.KPipe.json/avro/protobuf/bytes/custom(...)`** — fluent, immutable, returns `Stream<T>` → `Sink<T>` →
   `Handle`. The 80% path.
2. **`MessageProcessorRegistry` + `KPipeConsumer.Builder`** — explicit, multi-step. The 20% path.

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

## §17 Core ↔ Facade capability mapping

Source of truth for which capabilities are on the fluent facade (`KPipe.X(...)` → `Stream<T>` → `Sink<T>` → `Handle`,
plus `KPipe.multi(props)` → `MultiBuilder`) versus only via the explicit API (`MessageProcessorRegistry` +
`KPipeConsumer.Builder`).

When adding a new consumer/builder feature: decide whether it belongs on the 80% path (add to `Stream<T>` and
`MultiBuilder`) or stays as an escape hatch (explicit-only). Update this table in the same PR. Rows in **bold** are
deliberately escape-hatch-only.

| Capability                                     | Source module                                           | Facade path                                                              | Explicit-API path                                                                                    | Notes                                                                                                                                                                                 |
| ---------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Format selection                               | `kpipe-format-*`                                        | `KPipe.json/avro/protobuf/bytes/custom(...)`                             | `new MessageProcessorRegistry(format)`                                                               | Custom format: pass any `MessageFormat<T>` to `KPipe.custom`.                                                                                                                         |
| Topic subscription (homogeneous)               | `kpipe-consumer`                                        | `KPipe.X(topic, props)`                                                  | `Builder.withTopic(s) / withTopics(...)`                                                             | One topic-set, one shared pipeline.                                                                                                                                                   |
| Topic subscription (heterogeneous)             | `kpipe-consumer`                                        | `KPipe.multi(props).json(...).avro(...)...start()`                       | `Builder.withPipelines(Map<String, MessagePipeline<?>>)`                                             | Per-topic dispatch (§15).                                                                                                                                                             |
| Operator chain (`pipe/filter/peek/when`)       | `kpipe-core`                                            | `Stream.pipe / filter / peek / when`                                     | `MessageProcessorRegistry.pipeline(format).add(...)`                                                 | Identical operator semantics.                                                                                                                                                         |
| Custom terminal sink                           | `kpipe-core`                                            | `Stream.toCustom(MessageSink<T>)`                                        | `registry.registerSink(key, sink)`                                                                   | Any `MessageSink<T>`; facade also offers `.toConsole()`.                                                                                                                              |
| Multi-sink fanout                              | `kpipe-core`                                            | `Stream.toMulti(sinks...)`                                               | `new CompositeMessageSink<>(...)`                                                                    | Best-effort delivery; per-sink errors logged + suppressed.                                                                                                                            |
| Batch sink (size + age flush)                  | `kpipe-core`                                            | `Stream.toBatch(BatchSink<T>, BatchPolicy)`                              | `Builder.withBatchPipeline(topic, pipeline, sink, policy)` (multi-call for heterogeneous batch)      | `BatchSink<T>` returns `BatchResult` for per-record DLQ; `BatchSink.ofVoid(consumer)` wraps void-style sinks (whole-batch DLQ on throw). Both sequential and parallel modes. See §18. |
| Batch sink (multi-topic via `MultiBuilder`)    | `kpipe-api`                                             | `KPipe.multi(props).json(topic, s -> s.toBatch(sink, policy))...start()` | `Builder.withBatchPipeline(...)` × N                                                                 | One consumer-group, mixed batch + non-batch routes.                                                                                                                                   |
| Skip wire-format prefix                        | `kpipe-core`                                            | `Stream.skipBytes(int)`                                                  | `TypedPipelineBuilder.skipBytes(int)`                                                                | Confluent envelope: 5 (Avro) / 6 (Proto single-msg).                                                                                                                                  |
| Confluent SR per-record auto-lookup            | `kpipe-format-avro` + `kpipe-schema-registry-confluent` | `Stream.withSchemaRegistry(SchemaResolver)`                              | `AvroFormat.withRegistry(SchemaResolver)`                                                            | Reads wire envelope, caches schemas by ID (immutable in SR; no TTL). Avro-only — Protobuf needs runtime `.proto` compilation. See §19.                                                |
| Pipeline outcome counters (OTel)               | `kpipe-metrics-otel`                                    | `Stream.peekResult(PipelineMetricsObserver)`                             | hand a `Consumer<Result<T>>` to `peekResult`                                                         | Emits `kpipe.pipeline.passed/filtered/failed` counters. Observer-only — doesn't suppress or reroute (see §11 observer-wrap layer).                                                    |
| Retry policy                                   | `kpipe-consumer`                                        | `Stream.withRetry(int, Duration)`                                        | `Builder.withRetry(int, Duration)`                                                                   |                                                                                                                                                                                       |
| Backpressure (default watermarks)              | `kpipe-consumer`                                        | `Stream.withBackpressure()`                                              | `Builder.withBackpressure(BackpressureController)`                                                   | Defaults: pause 10k, resume 7k.                                                                                                                                                       |
| Backpressure (custom watermarks)               | `kpipe-consumer`                                        | `Stream.withBackpressure(high, low)`                                     | `Builder.withBackpressure(BackpressureController)`                                                   | High/low strategy auto-derived from `sequentialProcessing`.                                                                                                                           |
| Sequential processing                          | `kpipe-consumer`                                        | `Stream.withSequentialProcessing(boolean)`                               | `Builder.withSequentialProcessing(boolean)`                                                          | Switches backpressure to lag-based (§5).                                                                                                                                              |
| OTel/custom metrics                            | `kpipe-metrics(-otel)`                                  | `Stream.withMetrics(ConsumerMetrics)` / `MultiBuilder.withMetrics(...)`  | `Builder.withMetrics(ConsumerMetrics)`                                                               | Single-format and multi-topic both supported.                                                                                                                                         |
| Custom error handler                           | `kpipe-consumer`                                        | `Stream.withErrorHandler(Consumer<ProcessingError<byte[]>>)`             | `Builder.withErrorHandler(ErrorHandler<K>)`                                                          | Default logs at WARNING (§11).                                                                                                                                                        |
| Dead-letter topic                              | `kpipe-consumer`                                        | `Stream.withDeadLetterTopic(String)`                                     | `Builder.withDeadLetterTopic(String)` / `withDeadLetterBundle(...)`                                  | Bundle form pairs DLQ + producer for advanced wiring.                                                                                                                                 |
| Poll timeout                                   | `kpipe-consumer`                                        | `Stream.withPollTimeout(Duration)`                                       | `Builder.withPollTimeout(Duration)`                                                                  | Default 100ms.                                                                                                                                                                        |
| Lifecycle handle                               | `kpipe-api`                                             | `Handle.isHealthy / metrics / awaitShutdown / close`                     | `KPipeConsumer.start / awaitShutdown / shutdownGracefully / waitForInFlightDrain`                    | `Handle` is `AutoCloseable` (5s graceful default). Since 1.13: consumer hosts the lifecycle directly — no separate `KPipeRunner`.                                                     |
| **Custom `OffsetManager`**                     | `kpipe-consumer`                                        | — (escape hatch only)                                                    | `Builder.withOffsetManager(...)` / `withOffsetManager(Provider)`                                     | E.g. Postgres/Redis-backed manager.                                                                                                                                                   |
| **Custom Kafka `Consumer` factory**            | `kpipe-consumer`                                        | —                                                                        | `Builder.withConsumerProvider(Supplier<Consumer<K,byte[]>>)`                                         | Test seam / SSL configurators.                                                                                                                                                        |
| **Custom DLQ `KafkaProducer`**                 | `kpipe-producer`                                        | —                                                                        | `Builder.withKafkaProducer(...)` / `withDeadLetterBundle(...)`                                       | Override default producer for the DLQ.                                                                                                                                                |
| **Rebalance listener**                         | `kpipe-consumer`                                        | —                                                                        | `Builder.withRebalanceListener(ConsumerRebalanceListener)`                                           | External offset commit hooks, log routing.                                                                                                                                            |
| **Custom command queue**                       | `kpipe-consumer`                                        | —                                                                        | `Builder.withCommandQueue(Queue<ConsumerCommand>)`                                                   | Test seam (rarely needed).                                                                                                                                                            |
| **Thread/executor termination**                | `kpipe-consumer`                                        | —                                                                        | `Builder.withThreadTerminationTimeout / withExecutorTerminationTimeout / withWaitForMessagesTimeout` | Shutdown tuning.                                                                                                                                                                      |
| **Periodic metrics reporting + shutdown hook** | `kpipe-consumer`                                        | —                                                                        | `Builder.withMetricsReporters(...) / withMetricsInterval(...) / withShutdownHook(true)`              | Folded into `KPipeConsumer.Builder` in 1.13. Reporter thread is daemon, doesn't keep the JVM up.                                                                                      |
| **Health endpoint**                            | `kpipe-consumer`                                        | —                                                                        | `HttpHealthServer.fromEnv(...)`                                                                      | Run alongside `Handle` in the host process.                                                                                                                                           |
| **In-flight drain**                            | `kpipe-consumer`                                        | `Handle.shutdownGracefully(Duration)`                                    | `KPipeConsumer.waitForInFlightDrain(Duration) / shutdownGracefully(Duration)`                        | Replaces the deleted `MessageTracker` class. Uses the live in-flight counter (includes buffered batch records), not metric-derived.                                                   |
| **Custom `MessageProcessorRegistry`**          | `kpipe-core`                                            | —                                                                        | `new MessageProcessorRegistry(...)` + manual wiring                                                  | Pre-shared pipelines across consumers, multi-format orchestrators.                                                                                                                    |

## §18 Batch sink architecture (1.12.0)

- **One `BatchSink<T> extends Function<List<T>, BatchResult>`.** Implementations that report per-record outcomes return
  `BatchResult` directly; void-style consumers wrap with `BatchSink.ofVoid(consumer::accept)` — normal return →
  `BatchResult.allSucceeded(size)`, throw → `BatchResult.allFailed(size, e)`. **No separate `PartialBatchSink`** — the
  void shape is just a special case (collapsed in 1.12.0).
- **Coverage contract enforced.** A `BatchResult` whose `succeededIndexes` ∪ `failedByIndex.keys()` doesn't cover every
  `[0, batchSize)` is a contract violation. `BatchPipelineWrapper` flags missing indexes with a synthetic
  `IllegalStateException` and routes them to the DLQ rather than silently marking them processed (§12). Out-of-range
  indexes are logged at WARNING; a `null` `BatchResult` is treated as whole-batch failure.
- **`BatchPipelineWrapper` owns buffer + lock + gauge + age-tick.** One wrapper per topic; `ReentrantLock` protects
  `enqueue` / `tick` / `close` / `flushLocked` so parallel-mode workers can enqueue concurrently while a flush is
  mid-flight. Constructed in the consumer ctor, started in `start()`, drained in `close()`.
- **Backpressure participation in parallel mode.** `inFlightCount` is decremented as soon as `processRecord` returns —
  for batch paths that's "the record was buffered," which would make buffered records invisible to the in-flight
  watermark. The wrapper's `bufferedCount()` is added to `KPipeConsumer.totalInFlight()` to close that gap.
- **Offset commits use `OffsetManager` directly, not the command queue.** The command queue serializes Kafka-consumer
  calls (pause/resume/commitSync) on the consumer thread; `OffsetManager.markOffsetProcessed` is already thread-safe.
  Bypassing the queue means shutdown drain works even after the consumer thread has exited.
- **Whole-batch shutdown drain happens BEFORE `offsetManager.close()`.** Order in `close()`: pause + Close-command +
  wait-for-in-flight + join consumer thread + shutdown executor → drain all batch wrappers → close offset manager →
  close producer → state = CLOSED.
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

**Protobuf is deferred.** Confluent Protobuf SR returns `.proto` source text (not a binary descriptor), so runtime
auto-lookup needs `.proto` compilation — out of scope for 1.14. The static `new ProtobufFormat(descriptor)` +
`.skipBytes(6)` path remains for the single-top-level-message case.

**Generalizable rule:** when SR returns by-ID, cache forever in-process — the immutability of the key removes every
cache-coherence concern that would otherwise need TTLs or invalidation protocols.
