# KPipe Architecture and Design Principles

## Core Architectural Decisions

### 1. The Byte Boundary Strategy

- **Decision**: The top-level `KPipeConsumer` and its pipelines always operate on `byte[]` at their entry and exit
  points.
- **Reasoning**: Kafka is natively a byte-array storage and transport system. Keeping the boundary at the `byte[]` level
  allows for:
    - Transparent handling of wire formats (e.g., Confluent Magic Bytes).
    - Flexibility to handle mixed formats in the same application.
    - Decoupling of Kafka's low-level client configuration from the application's SerDe logic.
    - Centralized error handling for deserialization failures.

### 2. High-Performance "Single SerDe Cycle" Pipeline

- **Decision**: Modernize all message transformations from `Function<byte[], byte[]>` to `UnaryOperator<Object>` (e.g.,
  `Map<String, Object>` for JSON, `GenericRecord` for Avro).
- **Reasoning**: Chaining byte-to-byte functions incurs a massive "SerDe tax" by re-serializing data at every step.
- **Implementation**: The `TypedPipelineBuilder<T>` (via `MessageProcessorRegistry.pipeline()`) acts as the
  high-performance bridge. It:
    1. Receives `byte[]`.
    2. Deserializes **once** into an object of type `T`.
    3. Applies a chain of `UnaryOperator<T>` transformations on the same object (avoiding copies).
    4. Optionally sends the typed object to a terminal `MessageSink<T>`.
    5. Serializes **once** back to `byte[]`.

### 3. Java 25 Virtual Threads and Scoped Values

- **Decision**: Use Virtual Threads (Project Loom) for parallel processing. When per-record context-sharing is needed,
  prefer `ScopedValue` over `ThreadLocal`.
- **Reasoning**: Virtual Threads replace complex thread pool management with a simple "thread-per-record" model.
  `ScopedValue` is the VT-friendly alternative to `ThreadLocal`, avoiding the scalability traps of inheritable
  thread-locals on a thread-per-record consumer.
- **Current state**: VT is used everywhere (consumer poll → record processing). `ScopedValue` is **not** currently
  used — earlier per-format SerDe caches were torn out as dead infrastructure. The doctrine still stands for any
  future thread-local-like state we might add (e.g. tenant context, span propagation).

### 4. Reliable Offset Management ("Lowest Pending Offset")

- **Decision**: Use a `ConcurrentSkipListSet` to track all in-flight offsets per partition.
- **Reasoning**: Parallel processing leads to out-of-order completions. To ensure "at-least-once" delivery and avoid "
  gaps" in committed data:
    - Only the **lowest pending offset** for each partition is eligible for commitment.
    - Ensures that even if message 102 finishes before message 101, 102 will not be committed until 101 is also
      finished.

## Coding Standards and Patterns

### Modern Javadoc Format

- Use Java 25 triple-slash (///) comments instead of legacy `/** ... */`.
- Use Markdown-style code blocks (```java ... ```) instead of HTML `<pre>{@code ... }</pre>`.

### Clean Code Principles

- Always use `final var` for local variable declarations whenever possible.
- Use `java.lang.System.Logger` (Standard Java System Logger) instead of SLF4J or direct Log4j to keep the core library
  dependency-free.
- Avoid `e.printStackTrace()`; always use the logger for error reporting.

### Testing Strategy

- **Unit Tests**: Place in `lib/{module}/src/test/java`. Focus on pure function transformations.
- **Integration Tests**: Place in `examples/{format}/src/test/java`.
- **Testcontainers**: Use for end-to-end validation involving Kafka and Schema Registry. Always use a `CapturingSink` to
  verify the actual payload transformation.
- **Virtual Thread Tests**: Concurrency tests should use `Thread.ofVirtual()` directly rather than `CompletableFuture`
  to exercise real virtual thread behavior. Use `CountDownLatch` for synchronization between threads.

### 5. Strategy-Based Backpressure

- **Decision**: Decouple backpressure monitoring from the consumer core using a strategy pattern.
- **In-Flight Strategy (Parallel)**: Monitors active virtual threads to prevent memory exhaustion.
- **Lag Strategy (Sequential)**: Monitors total consumer lag (`Σ (end_offset - position)`) as the only meaningful metric
  when processing messages one-by-one.
- **Hysteresis**: Always uses high and low watermarks to prevent "thrashing" (rapidly toggling between pause and resume
  states).
- **Implementation**: `BackpressureController` encapsulates the decision logic, providing static factory methods (
  `lagStrategy()`, `inFlightStrategy()`) to the `KPipeConsumer`.

### 6. Refinement of Core Components

- **KPipeConsumer Simplicity**: Minimized internal fields and delegated lifecycle management (initialization, shutdown,
  backpressure) to specialized controllers (`BackpressureController`, `OffsetManager`).
- **Composition over Inheritance**: Message transformations are composed using functional operators (Stream.reduce) in
  `MessageProcessorRegistry` rather than deep inheritance or manual chaining.

### 7. Unified Registry Architecture

- **Decision**: Consolidate `MessageProcessorRegistry` and `MessageSinkRegistry` into a unified management system.
- **Reasoning**: Processors (`UnaryOperator<T>`) and Sinks (`MessageSink<T>`) share common needs for lifecycle
  management, metrics, and type-safe identification.
- **Implementation**:
    - `MessageProcessorRegistry` acts as a primary facade that delegates sink-related operations to an internal
      `MessageSinkRegistry`.
    - Both use a shared `RegistryEntry<T>` to centralize metrics tracking.
    - Standardized API: Both registries support `register`, `get`, `getAll`, `getMetrics`, and `unregister` with
      identical semantics.

### 8. Centralized Error Handling

- **Decision**: Standardize error suppression and logging logic in `RegistryFunctions`.
- **Reasoning**: Consistent error handling prevents a single failing component from crashing the entire pipeline.
- **Implementation**:
    - `withOperatorErrorHandling`: Specifically for `UnaryOperator<T>`.
    - `withConsumerErrorHandling`: Specifically for `MessageSink<T>`.

### 9. Modular Architecture and Split-Module Strategy

- **Decision**: Split the library into three submodules: `kpipe-metrics`, `kpipe-producer`, `kpipe-consumer`.
- **Dependency chain**: `kpipe-metrics` ← `kpipe-producer` ← `kpipe-consumer`
- **Reasoning**:
    - **Reduced Surface Area**: Each module carries only the dependencies it needs.
    - **JPMS Compliance**: Avoid split packages by clearly defining package ownership between modules.
    - **No Aggregate Artifact**: Each module is published individually to Maven Central. `kpipe-consumer` transitively
      includes `kpipe-producer` and `kpipe-metrics`, so users only need a single dependency.
- **Publishing**: Each submodule applies `maven-publish` and `signing`. JReleaser (configured in `:lib`) deploys all
  three staging directories to Maven Central in a single release.
- **Package Convention**:
    - `org.kpipe.metrics` — owned by `kpipe-metrics` (OTel instruments, `KPipeMetricsReporter`,
      `ConsumerMetricsReporter`).
    - `org.kpipe.producer.*` — owned by `kpipe-producer`.
    - `org.kpipe.sink.*` — owned by `kpipe-producer`.
    - `org.kpipe.consumer.*` — owned by `kpipe-consumer`.
    - `org.kpipe.consumer.metrics` — owned by `kpipe-consumer` (reporters that depend on registry types).
    - `org.kpipe.consumer.sink.*` — owned by `kpipe-consumer`.
- **Split Package Rule**: No two modules may export the same Java package. When a class references types from another
  module (e.g., `ProcessorMetricsReporter` referencing `MessageProcessorRegistry`), it stays in the module that owns
  those types.

### 10. OpenTelemetry Metrics Strategy

- **Decision**: Provide first-class OTel support via `kpipe-metrics` using `opentelemetry-api` only (no SDK).
- **Reasoning**: Libraries should instrument with the API; users bring their own SDK (Prometheus, OTLP, Jaeger, etc.).
- **Implementation**:
    - `ProducerMetrics` — counters: `kpipe.producer.messages.sent`, `kpipe.producer.messages.failed`,
      `kpipe.producer.dlq.sent`.
    - `ConsumerMetrics` — counters: `kpipe.consumer.messages.received`, `kpipe.consumer.messages.processed`,
      `kpipe.consumer.messages.errors`; histogram: `kpipe.consumer.processing.duration`; gauge:
      `kpipe.consumer.messages.inflight`; counters: `kpipe.consumer.backpressure.pauses`,
      `kpipe.consumer.backpressure.time`.
    - Both default to `OpenTelemetry.noop()` — zero cost when not configured.
    - Wired via builder: `.withMetrics(new ConsumerMetrics(otel))` / `.withMetrics(new ProducerMetrics(otel))`.
- **Log-based reporters** (`ConsumerMetricsReporter`, `ProcessorMetricsReporter`, `SinkMetricsReporter`) remain
  available as a lightweight fallback for users who do not use OTel.

### 11. KPipeConsumer Concurrency and Safety Patterns

- **State Machine**: All state transitions use single-read `compareAndSet` — read once into a local, decide, CAS. Never
  use two sequential CAS calls (double-CAS window). Shared helper `transitionToClosing()` is used by `close()` and
  `uncaughtExceptionHandler`.
- **Error Handler Safety**: Every `errorHandler.accept()` call site is wrapped in try-catch. A throwing user callback
  must never crash the consumer thread, leak in-flight counts, or skip offset marking. `markOffsetProcessed()` is always
  called **before** `errorHandler.accept()`.
- **Shutdown Guarantee**: `state.set(CLOSED)` lives in a nested `finally` inside the consumer thread's outer `finally`.
  Even if `kafkaConsumer.close()` throws, the consumer always reaches terminal state.
- **LockSupport Park/Unpark**: The consumer thread uses `LockSupport.park()` (not `Thread.sleep()`) when paused. This
  avoids CPU waste and wakes instantly. Unpark sources:
    - `internalResume()` — manual or backpressure resume.
    - `close()` — shutdown path.
    - `processRecord()` finally block — when `backpressurePaused` is true (in-flight drain triggers re-evaluation).
      Commands are flushed via `processCommands()` **before** parking so `kafkaConsumer.pause()` executes immediately.
- **Pipeline Null Handling**: Null record value and null deserialization throw specific `IllegalStateException` messages
  (retryable via normal exception path). Null `process()` result is intentional filtering — mark offset processed, count
  as success, no error.
- **Metrics Immutability**: `getMetrics()` returns `Collections.unmodifiableMap()`. Callers cannot mutate the snapshot.
- **Thread Safety Boundaries**: `processCommands()` is package-private — external callers cannot invoke
  `kafkaConsumer.pause()`/`resume()`/`commitSync()` from arbitrary threads. `isRunning()` snapshots `state.get()` into a
  local variable before comparing.

### 12. Explicit Pipeline Error Semantics — No Silent Failures

- **Decision (1.9.0)**: `MessagePipeline.apply()` no longer overloads `null` as both "filtered" and "failed". Failures
  throw; `null` means exactly one thing — intentional filtering by `process()`.
- **Reasoning**: The pre-1.9.0 implementation caught all exceptions in `apply()` and returned `null`. The downstream
  `KPipeConsumer.processTypedRecord` then treated `processed == null` as intentional filtering and incremented
  `messagesProcessed` — masking deserialization errors as successes. The only signal something was wrong was a gauge
  mismatch (`messagesProcessed` rising while `sinkInvocationCount` stayed at 0).
- **Real-world burn**: Discovered during the Grafana dashboard session — protobuf seed messages all failed
  deserialization because `skipBytes(5)` was wrong, but the consumer reported them as processed. ~10 minutes wasted on
  a bug that should have been a single error log.
- **Implementation invariants**:
    - Null record value throws `IllegalStateException` with a specific message (retryable via the normal exception
      path).
    - Null deserialization result throws `IllegalStateException` (implementations must throw, not return null).
    - Null `process()` result is the *only* legitimate filter signal — mark offset processed, count as success, no
      error.
- **Generalizable rule**: When composing `Function<T, R>` chains, never use `null`/`Optional.empty()` to signal both
  "filtered" and "failed". If both states exist, model them as distinct types (sealed `Result<T>` or distinct
  exceptions). Triage heuristic: when a "processed" counter rises but downstream invocations stay at 0, suspect
  overloaded null semantics first.

### 13. KPipe Fluent Facade (1.10.0) — Layered API Strategy

- **Decision (1.10.0)**: ship a top-level `KPipe` fluent facade in a new `kpipe-api` module that delegates to the
  existing `MessageProcessorRegistry` / `KPipeConsumer.Builder` / `KPipeRunner.Builder` stack. The facade is purely
  additive — no public 1.x API was changed.
- **Reasoning**: The 1.10.0 ergonomics pass smoothed surface friction (no-arg ctors, format helpers, DLQ bundle, etc.)
  but left the typical "build a JSON consumer" path at ~10 lines of registry + builder + runner. The fluent facade
  gets the common case to 5 lines without breaking anyone.
- **Layered API rule**: the codebase now exposes TWO public surfaces:
    1. **`org.kpipe.KPipe.json/avro/protobuf/bytes/custom(...)`** — fluent, immutable, returns `Stream<T>` →
       `Sink<T>` → `Handle`. The 80% path. Discoverable via IDE auto-complete after the first `.`.
    2. **`MessageProcessorRegistry` + `KPipeConsumer.Builder` + `KPipeRunner.Builder`** — explicit, multi-step,
       supports custom registries, shared pipelines, programmatic runner config. The 20% path.
- **Module placement (JPMS):** `Stream<T>`, `Sink<T>`, `Handle` interfaces live in `kpipe-api` (package
  `org.kpipe`). Private impls (`DefaultStream`, `DefaultSink`, `DefaultHandle`) also in `org.kpipe`, package-private.
  This keeps the user-facing import path clean (`import org.kpipe.KPipe;`) and avoids the `org.kpipe.facade` split-
  package cost while still letting `kpipe-core` own the registry/sink/format types in `org.kpipe.registry` and
  `org.kpipe.sink`.
- **Immutability contract on `Stream<T>`**: `DefaultStream` is a Java record. Every fluent method returns a NEW
  `DefaultStream` instance carrying the updated configuration. The original is never mutated. Branching from a
  common root is safe — `s.pipe(a)` and `s.pipe(b)` produce independent streams. Operators are stored as an
  immutable `List.copyOf(...)`. The record form keeps "adding a fluent setter is a one-place change" honest:
  declare the component, add the `with*` method, reference it in `DefaultSink.buildPipeline` if it affects pipeline
  construction.
- **`Handle` is `AutoCloseable`** with a default `close()` that performs `shutdownGracefully(Duration.ofSeconds(5))`.
  Use try-with-resources to guarantee cleanup. `metrics()` returns `Map<String, Long>` (typed), not `Map<String,
  Object>`.
- **`toConsole()` dispatch** uses a per-factory `Function<T, MessageSink<T>>` lambda baked into the `DefaultStream`
  at construction time. No `instanceof MessageFormat` checks. Avro's factory throws `IllegalStateException` if no
  default schema is registered.
- **Test contract**: facade has both unit tests (composition + dispatch) and a Testcontainers end-to-end test that
  spins up a real broker and verifies the entire chain through `start()` / `Handle.metrics()` / `shutdownGracefully()`.

### 14. Audit-Derived Concurrency Patterns

These patterns came out of the deep internal audit work and should be preserved across the codebase:

- **Atomic remove-if-empty on `ConcurrentHashMap` of mutable collections**: NEVER do
  `if (set.isEmpty()) map.remove(key)` after a separate `set.remove(value)` — the check-then-act is non-atomic and
  a concurrent `computeIfAbsent` can repopulate the set between the check and the map removal. Always use
  `map.computeIfPresent(key, (k, v) -> { v.remove(value); return v.isEmpty() ? null : v; })`. The bucket lock makes
  the whole sequence atomic.
- **`ConcurrentSkipListSet.first()` is a check-then-act trap**: pattern `if (!set.isEmpty()) set.first()` can throw
  `NoSuchElementException` under contention. Wrap in a `safeFirst` helper that returns `null` on
  `NoSuchElementException`. Apply the same pattern for `last()`.
- **`(Properties) parent.clone()`, NOT `new Properties(parent)`**: `new Properties(parent)` makes the parent a
  *fallback for `getProperty()`*, not a copy. `putIfAbsent` operates on the new instance only and silently shadows
  parent entries. Always `clone()` when you intend to derive a copy.
- **CAS-to-CAS happens-before doesn't extend to post-CAS field writes**: if `start()` does
  `state.compareAndSet(...)` then assigns `scheduler = ...`, a concurrent `close()` that does its own
  `state.compareAndSet(...)` is NOT guaranteed to see `scheduler` (the assignment is after the CAS in program order).
  Mark such fields `volatile`, or move the assignment into the CAS-protected critical section.
- **`Kafka.endOffsets(...)` without a `Duration`** uses the consumer's `default.api.timeout.ms` (60s default). On a
  hot path (every consumer-loop iteration), a slow broker stalls the entire consumer. Always pass a bounded timeout.
- **`catch (Exception) { return defaultValue; }` is a silent-failure trap**: never swallow without logging at
  WARNING. If you catch `InterruptException` (Kafka) or `InterruptedException`, **always** call
  `Thread.currentThread().interrupt()` to restore the flag for downstream code.
- **Don't expose internal counters through public APIs**: if a method takes an `AtomicLong` parameter for
  "optionally update this counter on success," it's leaking implementation detail. Return a `boolean` and let the
  caller update its own counter.

### 15. Multi-Topic Dispatch (1.11.0)

- **Decision**: One `KPipeConsumer` can host either one shared pipeline across N topics (homogeneous) or a
  per-topic pipeline map (heterogeneous), but always through one Kafka consumer / one consumer-group / one
  offset manager.
- **Internal model**: `KPipeConsumer` stores `Map<String, MessagePipeline<?>> pipelines`. The homogeneous path
  (`Builder.withPipeline(...)`) replicates the same pipeline reference across every subscribed topic at construction
  time; the heterogeneous path (`Builder.withPipelines(Map)`) takes the map directly and derives the topic set from
  its keys. `tryProcessRecord` does a single `pipelines.get(record.topic())` per record. The map is built via
  `Map.copyOf(...)`, so for typical small route counts it's `Map1`/`MapN` (identity-fast `String#equals` lookup),
  not `HashMap`.
- **Unrouted-topic policy**: if a record arrives for a topic with no registered pipeline (rebalance race, config
  error), **drop + log at WARNING + mark offset processed**. Never throw (would crash the consumer thread for a
  config error) and never DLQ (we don't have a per-topic DLQ). The "mark processed" part is critical — without
  it, the same record gets re-fetched forever.
- **Facade split**: homogeneous via `KPipe.json/avro/protobuf/bytes/custom(Collection<String>, Properties)`;
  heterogeneous via `KPipe.multi(props).json(topic, configurator).avro(...)...start()`. Single-string overloads
  remain unchanged. Mixing `withTopic`/`withTopics` with `withPipelines` is rejected as a config error (silent
  override would mask user mistakes).
- **Metrics**: OTel `kpipe.consumer.*` instruments now carry both a `pipeline` attribute (consumer-wide) and a
  `topic` attribute (per record). Per-topic `Attributes` are cached via `computeIfAbsent` so the per-record metric
  path is allocation-free after each topic has been seen once.

### 16. No-Deprecation Policy

- **Decision**: When a public API has to go, **delete it and migrate all callers**. Do not deprecate, do not add
  `@Deprecated`, do not add `since = "..."` annotations.
- **Reasoning**: Deprecation cycles bloat the surface, train users to ignore warnings, and rot in place when the
  promised "removal in next major" never happens. The library is pre-1.x-stable enough that hard breaks with a
  release-note migration are cleaner than carrying dead overloads. Tests and examples are migrated in the same PR
  that deletes the method, so callers always have a working reference.
- **Enforcement**: zero `@Deprecated` annotations and zero `@deprecated` Javadoc tags should ever land in the
  codebase. If something needs to be removed, the PR description mentions the removal explicitly.

### 17. Core ↔ Facade Capability Mapping

This table is the source of truth for which `kpipe-consumer` / `kpipe-core` / `kpipe-format-*` / `kpipe-metrics`
capabilities are reachable through the **fluent facade** (`KPipe.json/avro/protobuf/bytes/custom(...)` →
`Stream<T>` → `Sink<T>` → `Handle`, plus `KPipe.multi(props)` → `MultiBuilder`) versus only via the **explicit API**
(`MessageProcessorRegistry` + `KPipeConsumer.Builder` + `KPipeRunner.Builder`).

When adding a new consumer/builder feature: decide whether it belongs on the 80% path (add to `Stream<T>` and
`MultiBuilder`) or whether it stays as an escape hatch (explicit-only). Update this table in the same PR.

| Capability                               | Source module          | Facade path                                                             | Explicit-API path                                                                                    | Notes                                                              |
|------------------------------------------|------------------------|-------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| Format selection                         | `kpipe-format-*`       | `KPipe.json/avro/protobuf/bytes/custom(...)`                            | `new MessageProcessorRegistry(format)`                                                               | Custom format: pass any `MessageFormat<T>` to `KPipe.custom`.      |
| Topic subscription (homogeneous)         | `kpipe-consumer`       | `KPipe.X(topic, props)`                                                 | `Builder.withTopic(s) / withTopics(...)`                                                             | One topic-set, one shared pipeline.                                |
| Topic subscription (heterogeneous)       | `kpipe-consumer`       | `KPipe.multi(props).json(...).avro(...)...start()`                      | `Builder.withPipelines(Map<String, MessagePipeline<?>>)`                                             | Per-topic dispatch (§15).                                          |
| Operator chain (`pipe/filter/peek/when`) | `kpipe-core`           | `Stream.pipe / filter / peek / when`                                    | `MessageProcessorRegistry.pipeline(format).add(...)`                                                 | Identical operator semantics.                                      |
| Custom terminal sink                     | `kpipe-core`           | `Stream.toCustom(MessageSink<T>)`                                       | `sinkRegistry.register(key, sink)`                                                                   | Any `MessageSink<T>`; facade also offers `.toConsole()`.           |
| Multi-sink fanout                        | `kpipe-core`           | `Stream.toMulti(sinks...)`                                              | `new CompositeMessageSink<>(...)`                                                                    | Best-effort delivery; per-sink errors logged + suppressed.         |
| Skip wire-format prefix                  | `kpipe-core`           | `Stream.skipBytes(int)`                                                 | `TypedPipelineBuilder.skipBytes(int)`                                                                | Confluent envelope: 5 (Avro) / 6 (Proto single-msg).               |
| Retry policy                             | `kpipe-consumer`       | `Stream.withRetry(int, Duration)`                                       | `Builder.withRetry(int, Duration)`                                                                   |                                                                    |
| Backpressure (default watermarks)        | `kpipe-consumer`       | `Stream.withBackpressure()`                                             | `Builder.withBackpressure(BackpressureController)`                                                   | Defaults: pause 10k, resume 7k.                                    |
| Backpressure (custom watermarks)         | `kpipe-consumer`       | `Stream.withBackpressure(high, low)`                                    | `Builder.withBackpressure(BackpressureController)`                                                   | High/low strategy auto-derived from `sequentialProcessing`.        |
| Sequential processing                    | `kpipe-consumer`       | `Stream.withSequentialProcessing(boolean)`                              | `Builder.withSequentialProcessing(boolean)`                                                          | Switches backpressure to lag-based (§5).                           |
| OTel/custom metrics                      | `kpipe-metrics(-otel)` | `Stream.withMetrics(ConsumerMetrics)` / `MultiBuilder.withMetrics(...)` | `Builder.withMetrics(ConsumerMetrics)`                                                               | Single-format and multi-topic both supported.                      |
| Custom error handler                     | `kpipe-consumer`       | `Stream.withErrorHandler(Consumer<ProcessingError<byte[]>>)`            | `Builder.withErrorHandler(ErrorHandler<K>)`                                                          | Default logs at WARNING (§11).                                     |
| Dead-letter topic                        | `kpipe-consumer`       | `Stream.withDeadLetterTopic(String)`                                    | `Builder.withDeadLetterTopic(String)` / `withDeadLetterBundle(...)`                                  | Bundle form pairs DLQ + producer for advanced wiring.              |
| Poll timeout                             | `kpipe-consumer`       | `Stream.withPollTimeout(Duration)`                                      | `Builder.withPollTimeout(Duration)`                                                                  | Default 100ms.                                                     |
| Lifecycle handle                         | `kpipe-api`            | `Handle.isHealthy / metrics / awaitShutdown / close`                    | `KPipeRunner.start / awaitShutdown / shutdownGracefully`                                             | `Handle` is `AutoCloseable` (5s graceful default).                 |
| **Custom `OffsetManager`**               | `kpipe-consumer`       | — (escape hatch only)                                                   | `Builder.withOffsetManager(...)` / `withOffsetManager(Provider)`                                     | E.g. Postgres/Redis-backed manager.                                |
| **Custom Kafka `Consumer` factory**      | `kpipe-consumer`       | —                                                                       | `Builder.withConsumerProvider(Supplier<Consumer<K,byte[]>>)`                                         | Test seam / SSL configurators.                                     |
| **Custom DLQ `KafkaProducer`**           | `kpipe-producer`       | —                                                                       | `Builder.withKafkaProducer(...)` / `withDeadLetterBundle(...)`                                       | Override default producer for the DLQ.                             |
| **Rebalance listener**                   | `kpipe-consumer`       | —                                                                       | `Builder.withRebalanceListener(ConsumerRebalanceListener)`                                           | External offset commit hooks, log routing.                         |
| **Custom command queue**                 | `kpipe-consumer`       | —                                                                       | `Builder.withCommandQueue(Queue<ConsumerCommand>)`                                                   | Test seam (rarely needed).                                         |
| **Thread/executor termination**          | `kpipe-consumer`       | —                                                                       | `Builder.withThreadTerminationTimeout / withExecutorTerminationTimeout / withWaitForMessagesTimeout` | Shutdown tuning.                                                   |
| **`KPipeRunner` lifecycle hooks**        | `kpipe-consumer`       | —                                                                       | `KPipeRunner.Builder.withStartAction / withMetricsReporters / withShutdownAction`                    | Start/stop side-effects, periodic metric logging.                  |
| **Health endpoint**                      | `kpipe-consumer`       | —                                                                       | `HttpHealthServer.fromEnv(...)`                                                                      | Run alongside `Handle` in the host process.                        |
| **Message tracker**                      | `kpipe-consumer`       | —                                                                       | `KPipeConsumer.createMessageTracker(...)`                                                            | In-flight visibility for tests/diagnostics.                        |
| **Disable metrics**                      | `kpipe-consumer`       | —                                                                       | `Builder.disableMetrics()`                                                                           | Required only for the no-API-export footprint.                     |
| **Custom `MessageProcessorRegistry`**    | `kpipe-core`           | —                                                                       | `new MessageProcessorRegistry(...)` + manual wiring                                                  | Pre-shared pipelines across consumers, multi-format orchestrators. |

**Reading the table:** rows in **bold** are deliberately escape-hatch-only. The 80%-path features (everything not in
bold) are discoverable via the facade's IDE auto-complete after the first `.`. If a user reaches for an explicit-API
escape hatch, they're either operating an advanced topology or building a backend that should live in its own module
(e.g. a Postgres `OffsetManager`).
