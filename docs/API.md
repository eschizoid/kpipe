# Fluent API reference

The fluent surface, grouped by the type each method actually lives on. Chain shape:
`KPipe.<format>(...)` → `Stream<T>` (build the pipeline) → terminal → `Sink<T>` → `start()` → `Handle` (lifecycle).
This catalog is verified against the source of `kpipe-api` 1.18.0; the
[Javadoc](https://javadoc.io/doc/io.github.eschizoid/kpipe-api) is the normative reference.

Code snippets on this page use placeholder values (`kafkaProps`, `enrich`, `mySink`, ...) — they show shape, not a
runnable program. For a complete runnable example see
[`examples/json/.../ReadmeQuickstart.java`](../examples/json/src/main/java/io/github/eschizoid/kpipe/ReadmeQuickstart.java).

## `KPipe` — entry points

| Factory | Returns | Notes |
| --- | --- | --- |
| `json(topic, props)` | `Stream<Map<String, Object>>` | also `json(Collection<String>, props)` for many topics, one pipeline |
| `avro(topic, props, AvroFormat)` | `Stream<GenericRecord>` | static single-schema mode; `Collection` overload exists |
| `avro(topic, props, SchemaResolver)` | `Stream<GenericRecord>` | Confluent SR per-record lookup; no `Collection` overload; `toConsole()` unsupported in this mode |
| `protobuf(topic, props, ProtobufFormat)` | `Stream<Message>` | static descriptor mode; `Collection` overload exists |
| `protobuf(topic, props, SchemaResolver)` | `Stream<Message>` | SR mode; requires `kpipe-format-protobuf-confluent` at runtime; no `Collection` overload; `toConsole()` unsupported |
| `bytes(topic, props)` | `Stream<byte[]>` | passthrough; `Collection` overload exists |
| `custom(topic, props, MessageFormat<T>)` | `Stream<T>` | your own format; `Collection` overload exists |
| `multi(props)` | `MultiBuilder` | heterogeneous per-topic routes on one consumer |

## `Stream<T>` — building the pipeline

Every method returns a **new immutable** `Stream<T>`; branching two pipelines from one prefix is safe.

### Transforms

| Method | Semantics |
| --- | --- |
| `pipe(UnaryOperator<T>)` | Append an operator. Returning `null` filters the record: downstream operators and the sink are skipped, the offset is still marked processed. |
| `filter(Predicate<T>)` | Predicate false → the record is filtered (same semantics as a null-returning operator: offset still commits, `onFiltered` observers fire). |
| `peek(Consumer<T>)` | Observe without replacing the value. |
| `when(Predicate, ifTrue, ifFalse)` | Conditional operator. Both branches are required — there is no drop-on-else shape; use `filter` for dropping. |
| `skipBytes(int n)` | Strip a fixed-length leading prefix before deserialization (the payload is copied once, minus the prefix). A low-level escape hatch for fixed-size envelopes — see the framing warning in [FORMATS.md](FORMATS.md#wire-envelopes-and-skipbytes). Not allowed together with `withSchemaRegistry`. |

### Observers (visibility only — never change the record's outcome)

Calling the same observer method twice replaces the previous observer. A throwing observer is logged at WARNING and
swallowed.

| Method | Semantics |
| --- | --- |
| `onFiltered(Runnable)` | Fires when a record is intentionally filtered. No record argument. |
| `onFailed(Consumer<Throwable>)` | Fires **per attempt** — with `withRetry(2, ...)` a persistently failing record invokes it up to 3 times. It observes pipeline (operator) failures only; a deserialization or sink throw takes the error path without invoking it. For one callback per record after retries, use `withErrorHandler`. |
| `peekResult(Consumer<Result<T>>)` | Lowest-level hook: sees every `Passed` / `Filtered` / `Failed` outcome. Feeds `PipelineMetricsObserver` ([OBSERVABILITY.md](OBSERVABILITY.md)). |

### Configuration

| Method | Semantics |
| --- | --- |
| `withRetry(int maxRetries, Duration backoff)` | `maxRetries` counts retries **after** the initial attempt (`withRetry(3, ...)` = up to 4 executions). Backoff is fixed, not exponential. Any `Exception` is retried except interruption, which aborts immediately with the flag restored. Default 0: a failure goes straight to the error handler / DLQ. |
| `withBackpressure()` / `withBackpressure(high, low)` | Backpressure is **on by default**; call these only to restore/override the watermarks (defaults: pause at 10,000 in-flight, resume at 7,000). See [GUARANTEES.md](GUARANTEES.md#backpressure) for what "paused" means. |
| `withProcessingMode(ProcessingMode)` | `PARALLEL` (default), `SEQUENTIAL`, `KEY_ORDERED` — ordering semantics in [GUARANTEES.md](GUARANTEES.md#ordering). |
| `withKeyOrderedMaxKeys(int)` | Cap on distinct keys tracked in `KEY_ORDERED` (default 10,000). Ignored (with a build-time WARNING) in other modes. |
| `withCircuitBreaker(double threshold, int window, Duration open)` | Rolling-failure-rate breaker. The window sees one terminal outcome per record (after retries) — deserialization, operator, and sink failures all count. A second overload, `withCircuitBreaker(CircuitBreakerController)`, accepts a pre-built/shared controller. |
| `withTracer(Tracer)` | W3C trace-context propagation; default `Tracer.noop()`. |
| `withDeadLetterTopic(String)` | Route terminally-failed records to a DLQ topic; the producer is derived from the consumer properties. |
| `withErrorHandler(Consumer<KPipeConsumer.ProcessingError>)` | Invoked once per record after retries are exhausted. `ProcessingError` carries the raw `ConsumerRecord<byte[], byte[]>`, the exception, and the retry count. |
| `withMetrics(ConsumerMetrics)` | Default is `ConsumerMetrics.noop()`. |
| `withPollTimeout(Duration)` | Kafka poll timeout, default 100ms. |
| `withSchemaRegistry(SchemaResolver)` | Switch an Avro/Protobuf stream to per-record SR lookup. Throws for other formats; not allowed together with `skipBytes`. |

### Terminals — each returns `Sink<T>`

| Method | Semantics |
| --- | --- |
| `toConsole()` | Format-appropriate console sink. Throws `IllegalStateException` on registry-mode Avro/Protobuf streams (no fixed schema to format with). |
| `toCustom(MessageSink<T>)` | Your own sink — `MessageSink` is a `Consumer<T>`-shaped functional interface. |
| `toMulti(MessageSink<T>...)` | Fan-out, **best effort**: each sink is invoked; a throwing sink is logged at WARNING and suppressed, the record still counts as processed and its offset commits, and the DLQ is not invoked. Do not use it when every sink must durably receive every record — see [GUARANTEES.md](GUARANTEES.md#sinks). |
| `toBatch(BatchSink<T>, BatchPolicy)` | Buffered sink with size/age flush and per-record failure reporting — details in [SINKS.md](SINKS.md#batch-sinks). |

## `Sink<T>` — one method

`start()` returns a `Handle`. It is single-shot: a second `start()` on the same `Sink` throws `IllegalStateException`.

## `Handle` — lifecycle

| Method | Semantics |
| --- | --- |
| `isHealthy()` | Liveness snapshot. |
| `metrics()` | Unmodifiable `Map<String, Long>` snapshot; empty when metrics are disabled. |
| `awaitShutdown()` | Blocks indefinitely until the consumer stops (throws `InterruptedException`). |
| `awaitShutdown(Duration)` | Bounded wait; returns whether the consumer stopped. |
| `shutdownGracefully(Duration)` | Initiate shutdown with a bounded in-flight drain; returns whether the drain finished cleanly. |
| `close()` | `shutdownGracefully(Duration.ofSeconds(5))`. `Handle` is `AutoCloseable`, so try-with-resources gives you a bounded graceful shutdown by default. |
| `topKeyQueueDepths(int n)` | `KEY_ORDERED` diagnostics: the `n` deepest per-key queues, deepest first (empty list in other modes; a `null` Kafka key appears as a `null` entry key). |

`start()` does not block; the consumer runs on its own (non-daemon) thread, so the process stays alive until you close
the handle or the JVM receives a shutdown signal. Keep the `Handle` — the recommended shape is try-with-resources plus
`awaitShutdown()`, as in the README's quickstart.

## `MultiBuilder` — heterogeneous multi-topic routing

One consumer, one consumer group, one offset manager; each topic gets its own typed pipeline:

```java
KPipe.multi(kafkaProps)
    .json("events-json", s -> s.pipe(addTimestamp).toCustom(jsonSink))
    .avro("events-avro", avroFormat, s -> s.filter(active).toCustom(avroSink))
    .bytes("events-raw", s -> s.toCustom(rawSink))
    .start();
```

- Route methods: `json(topic, configurator)`, `avro(topic, AvroFormat | SchemaResolver, configurator)`,
  `protobuf(topic, ProtobufFormat | SchemaResolver, configurator)`, `bytes(topic, configurator)`,
  `custom(topic, MessageFormat<T>, configurator)`. Registering the same topic twice throws.
- Unlike `Stream`, `MultiBuilder` is a mutable builder (`with*` methods return `this`).
- Consumer-wide settings (`withRetry`, `withBackpressure`, `withProcessingMode`, `withKeyOrderedMaxKeys`,
  `withMetrics`, `withTracer`, `withCircuitBreaker(CircuitBreakerController)`, `withDeadLetterTopic`,
  `withErrorHandler`, `withPollTimeout`) live on the builder. Setting one inside a route configurator throws
  `IllegalArgumentException` pointing you at the builder-level mirror — per-route values would silently fight over one
  consumer otherwise.
- The circuit breaker observes outcomes across **all** routes; one trip pauses the whole consumer.
- Records for topics with no registered route are dropped at WARNING and their offsets are committed (a config error
  must not wedge the consumer into infinite refetch).
- `start()` with zero routes throws.

## Built-in operators

`io.github.eschizoid.kpipe.registry.Operators` provides ready-made operators for `.pipe(...)`: `filter`, `drop`,
`peek`, `map`, `compose`, `safe`, `requireField`, `rename`, `removeFields`, `addField`. The `Map`-typed helpers
(`rename`, `removeFields`, `addField`, `requireField`) **mutate the payload map in place and return it** — they are
convenient, not pure functions. That is safe under KPipe's threading model (one record's payload is only ever touched
by the one worker processing it) but matters if your own code shares payload references beyond the pipeline.

## The explicit API

Everything above is a facade over `MessageProcessorRegistry` + `KPipeConsumerBuilder`. Drop down when you need custom
offset managers, pre-shared registries, custom Kafka consumer factories, periodic metrics reporters, or shutdown
tuning — the capability map and worked examples are in [ESCAPE-HATCHES.md](ESCAPE-HATCHES.md).
