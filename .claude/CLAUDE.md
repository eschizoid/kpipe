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

- **Decision**: Use Virtual Threads (Project Loom) for parallel processing and `ScopedValue` for context sharing and
  high-performance caching.
- **Reasoning**: Virtual Threads replace complex thread pool management with a simple "thread-per-record" model.
  `ScopedValue` provides a modern, lightweight alternative to `ThreadLocal` that is specifically optimized for large
  numbers of virtual threads.
- **Result**: Massive concurrency with minimal memory overhead, avoiding `ThreadLocal` scalability issues.

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
    - Null record value throws `IllegalStateException` with a specific message (retryable via the normal exception path).
    - Null deserialization result throws `IllegalStateException` (implementations must throw, not return null).
    - Null `process()` result is the *only* legitimate filter signal — mark offset processed, count as success, no error.
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
- **Immutability contract on `Stream<T>`**: every fluent method returns a NEW `DefaultStream` instance carrying the
  updated configuration. The original is never mutated. Branching from a common root is safe — `s.pipe(a)` and
  `s.pipe(b)` produce independent streams. Operators are stored as an immutable `List.copyOf(...)`. This matches the
  Java Stream API's functional style; the alternative (mutate-and-return-this, like `StringBuilder`) is a foot gun
  for users who reuse the root reference.
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
