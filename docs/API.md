# Fluent API reference

The fluent surface, grouped by the type each method actually lives on. Chain shape: `KPipe.<format>(...)` → `Stream<T>`
(build the pipeline) → terminal → `Sink<T>` → `start()` → `Handle` (lifecycle). This catalog is verified against the
`kpipe-api` source; the [Javadoc](https://javadoc.io/doc/io.github.eschizoid/kpipe-api) is the normative reference.

Code snippets on this page use placeholder values (`kafkaProps`, `enrich`, `mySink`, ...) — they show shape, not a
runnable program. For a complete runnable example see
[`examples/json/.../ReadmeQuickstart.java`](../examples/json/src/main/java/io/github/eschizoid/kpipe/ReadmeQuickstart.java).

## `KPipe` — entry points

| Factory                                  | Returns                       | Notes                                                                                                                   |
| ---------------------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `json(topic, props)`                     | `Stream<Map<String, Object>>` | also `json(Collection<String>, props)` for many topics, one pipeline                                                    |
| `avro(topic, props, AvroFormat)`         | `Stream<GenericRecord>`       | static single-schema mode; `Collection` overload exists                                                                 |
| `avro(topic, props, SchemaResolver)`     | `Stream<GenericRecord>`       | Confluent SR per-record lookup; `Collection` overload exists; `toConsole()` unsupported in this mode                    |
| `protobuf(topic, props, ProtobufFormat)` | `Stream<Message>`             | static descriptor mode; `Collection` overload exists                                                                    |
| `protobuf(topic, props, SchemaResolver)` | `Stream<Message>`             | SR mode; requires `kpipe-format-protobuf-confluent` at runtime; `Collection` overload exists; `toConsole()` unsupported |
| `bytes(topic, props)`                    | `Stream<byte[]>`              | passthrough; `Collection` overload exists                                                                               |
| `custom(topic, props, MessageFormat<T>)` | `Stream<T>`                   | your own format; `Collection` overload exists                                                                           |
| `multi(props)`                           | `MultiBuilder`                | heterogeneous per-topic routes on one consumer                                                                          |

## `Stream<T>` — building the pipeline

Every method returns a **new immutable** `Stream<T>`, so two pipelines can branch from one prefix without affecting each
other.

### Transforms

| Method                                  | Semantics                                                                                                                                                                                                                                                                                        |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `pipe(UnaryOperator<T>)`                | Append an operator. Returning `null` filters the record: downstream operators and the sink are skipped, the offset is still marked processed.                                                                                                                                                    |
| `filter(Predicate<T>)`                  | Predicate false → the record is filtered (same semantics as a null-returning operator: the offset still commits and the `filtered` metric counts it).                                                                                                                                            |
| `peek(Consumer<T>)`                     | Observe without replacing the value.                                                                                                                                                                                                                                                             |
| `when(Predicate, ifTrue, ifFalse)`      | Conditional operator. Both branches are required — there is no drop-on-else shape; use `filter` for dropping.                                                                                                                                                                                    |
| `rekey(Function<T, byte[]>)`            | Replace the record's key for the rest of the attempt. Later operators and sinks read it from `RecordContext.current().key()`, and `toKafka` writes it. `KEY_ORDERED` dispatch, the error handler and the dead-letter topic use the consumed key, and a retry starts from it again.               |
| `rekeyString(Function<T, String>)`      | `rekey` with the key given as a string and encoded as UTF-8.                                                                                                                                                                                                                                     |
| `withHeader(name, Function<T, byte[]>)` | Add a header for the rest of the attempt. `toKafka` writes it after the consumed record's headers; the dead-letter topic and a retry do not see it.                                                                                                                                              |
| `skipBytes(int n)`                      | Strip a fixed-length leading prefix before deserialization (the payload is copied once, minus the prefix). A low-level escape hatch for fixed-size envelopes — see the framing warning in [FORMATS.md](FORMATS.md#wire-envelopes-and-skipbytes). Not allowed together with `withSchemaRegistry`. |

### Failure hooks

Two hooks, both consumer-wide and both handed a `KPipeConsumer.ProcessingError` (the consumed record, the cause and the
retry count):

| Method                                        | Semantics                                                                                                                                                                                                                                                                    |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `withErrorHandler(Consumer<ProcessingError>)` | Once per record that fails for good: after its retries, or at once for a `NonRetryableException`. Default logs at WARNING.                                                                                                                                                   |
| `onAttemptFailed(Consumer<ProcessingError>)`  | Once per failed attempt, at every stage (deserialize, operator, sink, batch flush), so it also sees the transient failures a retry recovers from. `retryCount` is the retries before that attempt. Watching only: it cannot stop a retry, and a throwing observer is logged. |

Neither has a current record; read the record from the `ProcessingError`. For filtered records, use the `filtered`
metric, or log from the filter itself, where `RecordContext.current()` gives the record.

### Configuration

| Method                                                            | Semantics                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `withRetry(int maxRetries, Duration backoff)`                     | `maxRetries` counts retries **after** the initial attempt (`withRetry(3, ...)` = up to 4 executions). Backoff is fixed, not exponential. Any `Exception` is retried except interruption, which aborts immediately with the flag restored, and a `NonRetryableException` (or a failure caused by one), which goes straight to the error handler / DLQ. Default 0: a failure goes straight to the error handler / DLQ.                                                                                                  |
| `withBackpressure()` / `withBackpressure(high, low)`              | Backpressure is **on by default**; call these only to restore/override the watermarks (defaults: pause at 10,000 in-flight, resume at 7,000). See [GUARANTEES.md](GUARANTEES.md#backpressure) for what "paused" means.                                                                                                                                                                                                                                                                                                |
| `withProcessingMode(ProcessingMode)`                              | `PARALLEL` (default), `SEQUENTIAL`, `KEY_ORDERED` — ordering semantics in [GUARANTEES.md](GUARANTEES.md#ordering).                                                                                                                                                                                                                                                                                                                                                                                                    |
| `withKeyOrderedMaxKeys(int)`                                      | Cap on distinct keys tracked in `KEY_ORDERED` (default 10,000). Ignored (with a build-time WARNING) in other modes.                                                                                                                                                                                                                                                                                                                                                                                                   |
| `withCircuitBreaker(double threshold, int window, Duration open)` | Rolling-failure-rate breaker. The window sees one terminal outcome per record (after retries) — deserialization, operator, and sink failures all count, except a `NonRetryableException`, which says the record is bad rather than that a downstream system is down. A failed dead-letter send always counts. A second overload, `withCircuitBreaker(CircuitBreakerController)`, accepts a pre-built/shared controller.                                                                                               |
| `withTracer(Tracer)`                                              | W3C trace-context propagation; default `Tracer.noop()`.                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `withDeadLetterTopic(String)`                                     | Route terminally-failed records to a DLQ topic; the producer is derived from the consumer properties.                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `withErrorHandler(Consumer<KPipeConsumer.ProcessingError>)`       | Invoked once per record after retries are exhausted, or at once for a `NonRetryableException`. `ProcessingError` carries the raw `ConsumerRecord<byte[], byte[]>`, the exception, and the retry count.                                                                                                                                                                                                                                                                                                                |
| `withMetrics(MetricsExporter)`                                    | Exports the consumer's metrics, e.g. `OtelMetrics.of(otel)`. Without it the metrics are still counted, just not exported.                                                                                                                                                                                                                                                                                                                                                                                             |
| `withPollTimeout(Duration)`                                       | Kafka poll timeout, default 100ms.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `withCommitInterval(Duration)`                                    | How often the offset frontier is committed, default 30 seconds. Offsets are also committed on rebalance and at shutdown; a shorter interval narrows what a crash reprocesses.                                                                                                                                                                                                                                                                                                                                         |
| `withOffsetStore(OffsetStore)`                                    | Keeps committed offsets in an external store, such as a database table, instead of Kafka. The consumer still commits the offset frontier; on assignment it seeks each partition to the stored offset. See [ESCAPE-HATCHES.md](ESCAPE-HATCHES.md#offsets-in-an-external-store).                                                                                                                                                                                                                                        |
| `withMetricsLog(Duration)`                                        | Logs one `key=value` line of the consumer's metrics per interval to the `io.github.eschizoid.kpipe.metrics.log` logger, plus a final line at close after the last flush. Needs no monitoring backend; independent of `withMetrics`.                                                                                                                                                                                                                                                                                   |
| `withShutdownHook(boolean)`                                       | Registers a JVM shutdown hook to close the consumer. Try-with-resources on `Handle` already covers the normal exit; the hook covers SIGTERM. With default timeouts a shutdown whose teardown hangs can take up to 40 seconds in `PARALLEL` mode with a dead-letter queue (the 5-second in-flight wait plus the 35-second join budget), so give the process a longer grace period, such as Kubernetes' `terminationGracePeriodSeconds`.                                                                                |
| `withThreadTerminationTimeout(Duration)`                          | How long shutdown waits for the consumer thread to join. The default depends on the processing mode and covers the waits that mode's teardown runs, through the final commit and the Kafka client's 5-second close: 30 seconds for `PARALLEL`, 25 for `KEY_ORDERED`, 15 for `SEQUENTIAL`, and 5 more when the consumer has a producer for a dead-letter queue or Kafka sinks, whose close is bounded the same way. A shorter value can interrupt the final commit when a record is stuck, leaving its result unknown. |
| `withWaitForMessagesTimeout(Duration)`                            | How long close waits for dispatched records to finish. Also applies inside `Handle.shutdownGracefully(Duration)`: its close waits this long again for records still dispatched when its own timeout ends.                                                                                                                                                                                                                                                                                                             |
| `withSchemaRegistry(SchemaResolver)`                              | Switch an Avro/Protobuf stream to per-record SR lookup. Throws for other formats; not allowed together with `skipBytes`.                                                                                                                                                                                                                                                                                                                                                                                              |

### Terminals — each returns `Sink<T>`

| Method                               | Semantics                                                                                                                                                                                                                                                                                                                                                                 |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `toConsole()`                        | Format-appropriate console sink. Throws `IllegalStateException` on registry-mode Avro/Protobuf streams (no fixed schema to format with).                                                                                                                                                                                                                                  |
| `toCustom(MessageSink<T>)`           | Your own sink — `MessageSink` is a `Consumer<T>`-shaped functional interface.                                                                                                                                                                                                                                                                                             |
| `toKafka(String topic)`              | Produce each value to `topic`, serialized with the stream's format, keeping each record's key and headers, after any `rekey` or `withHeader`. The producer is built from the stream's Kafka properties and shared with the dead-letter topic. Throws `IllegalStateException` on registry-mode streams — details in [SINKS.md](SINKS.md#producing-to-another-kafka-topic). |
| `toMulti(MessageSink<T>...)`         | Fan-out, **best effort**: each sink is invoked; a throwing sink is logged at WARNING and suppressed, the record still counts as processed and its offset commits, and the DLQ is not invoked. Do not use it when every sink must durably receive every record — see [GUARANTEES.md](GUARANTEES.md#sinks).                                                                 |
| `toBatch(BatchSink<T>, BatchPolicy)` | Buffered sink with size/age flush and per-record failure reporting — details in [SINKS.md](SINKS.md#batch-sinks).                                                                                                                                                                                                                                                         |

### Reading the record

Operators and sinks receive the value. To read the Kafka record it came from, call `RecordContext.current()` (in
`io.github.eschizoid.kpipe.record`) inside the operator or sink:

```java
import static io.github.eschizoid.kpipe.record.RecordContext.current;

KPipe.json("orders", props)
    .filter(order -> current().headerString("tenant").isPresent())
    .toCustom(order -> db.put(current().keyString(), order));
```

| Method                                                 | Returns                                                                           |
| ------------------------------------------------------ | --------------------------------------------------------------------------------- |
| `key()` / `keyString()`                                | The key as bytes, or decoded as UTF-8, after any `rekey`; `null` for no key.      |
| `originalKey()`                                        | The key the record was consumed with.                                             |
| `header(name)` / `headerString(name)`                  | The header's last value, including one added with `withHeader`, as an `Optional`. |
| `forEachHeader(action)`                                | Every header, in order.                                                           |
| `topic()` / `partition()` / `offset()` / `timestamp()` | Where the record came from, and its timestamp in epoch milliseconds.              |

- The consumer binds the record for each processing attempt, so a retry reads the record as it was consumed, without the
  changes `rekey` and `withHeader` made in an earlier attempt.
- `current()` works on the thread that runs the operator or sink. On any other thread, such as an executor task, it
  throws `IllegalStateException`. A context reads the consumed record and keeps no per-thread state, so capture it first
  (`var ctx = current();`) and use `ctx` there. Do not modify the arrays it returns; a retry reads the same ones.
- A batch sink covers many records, so `current()` throws inside one. It reads them with `RecordContext.currentBatch()`,
  index-aligned with its values and including any `rekey` or `withHeader`.
- The error handler has no current record; it gets the record in its `ProcessingError`, as it was consumed.
- To unit-test an operator that reads the record, build its context with `TestRecord.of(value).withKey("k").context()`
  from `kpipe-test`, or extend `RecordContext` with fixed values, and run the operator with
  `RecordContext.callWith(context, () -> op.apply(value))`.

## `Sink<T>` — one method

`start()` returns a `Handle`. It is single-shot: a second `start()` on the same `Sink` throws `IllegalStateException`.

## `Handle` — lifecycle

| Method                         | Semantics                                                                                                                                                                                |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `health()`                     | A `HealthSnapshot`: whether the consumer runs, which `PauseSource`s hold a pause, and the circuit breaker state, each read once. `health().healthy()` is the liveness answer; see below. |
| `metrics()`                    | A `MetricsSnapshot`: record counts in total and per topic, in-flight, backpressure and circuit breaker figures.                                                                          |
| `awaitShutdown()`              | Blocks indefinitely until the consumer stops (throws `InterruptedException`).                                                                                                            |
| `awaitShutdown(Duration)`      | Bounded wait; returns whether the consumer stopped.                                                                                                                                      |
| `shutdownGracefully(Duration)` | Pauses, waits up to the timeout for dispatched records, then closes, which flushes batch buffers. Returns `true` only when nothing is left in flight after close.                        |
| `close()`                      | `shutdownGracefully(Duration.ofSeconds(5))`. `Handle` is `AutoCloseable`, so try-with-resources gives you a bounded graceful shutdown by default.                                        |
| `topKeyQueueDepths(int n)`     | `KEY_ORDERED` diagnostics: the `n` deepest per-key queues, deepest first (empty list in other modes; a `null` Kafka key appears as a `null` entry key).                                  |

`HealthSnapshot` answers the probe question; how much work is in flight is a load figure and lives in
`MetricsSnapshot.inFlight()`.

| Component / method | Meaning                                                                                                     |
| ------------------ | ----------------------------------------------------------------------------------------------------------- |
| `running()`        | The consumer has started and is not shutting down, paused or not.                                           |
| `pauseSources()`   | The `PauseSource`s holding a pause: `MANUAL`, `BACKPRESSURE`, `CIRCUIT_BREAKER`; empty when not paused.     |
| `paused()`         | At least one source holds a pause.                                                                          |
| `circuitBreaker()` | `CLOSED`, `OPEN` or `HALF_OPEN`; `CLOSED` when no breaker is configured.                                    |
| `healthy()`        | `running()` and the breaker is not `OPEN`. A manual or backpressure pause is normal flow and stays healthy. |

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
- Consumer-wide settings (`withRetry`, `withBackpressure`, `withProcessingMode`, `withKeyOrderedMaxKeys`, `withMetrics`,
  `withTracer`, `withCircuitBreaker(CircuitBreakerController)`, `withDeadLetterTopic`, `withErrorHandler`,
  `withPollTimeout`, `withMetricsLog`, `withShutdownHook`, `withThreadTerminationTimeout`, `withWaitForMessagesTimeout`,
  `withCommitInterval`, `withOffsetStore`) live on the builder. Setting one inside a route configurator throws
  `IllegalArgumentException` pointing you at the builder-level mirror — per-route values would silently fight over one
  consumer otherwise.
- The circuit breaker observes outcomes across **all** routes; one trip pauses the whole consumer.
- Records for topics with no registered route are dropped at WARNING and their offsets are committed (a config error
  must not wedge the consumer into infinite refetch).
- `start()` with zero routes throws.

## Built-in operators

`io.github.eschizoid.kpipe.pipeline.Operators` provides operators for any payload type: `filter`, `drop`, `peek` and
`compose`.

`io.github.eschizoid.kpipe.format.json.JsonFields` changes the fields of a JSON record:

| Operator                         | Does                                                       | When the field is missing                  |
| -------------------------------- | ---------------------------------------------------------- | ------------------------------------------ |
| `set(field, value)`              | Sets the field to a value fixed when the pipeline is built | Adds it                                    |
| `setIfAbsent(field, value)`      | Sets the field only if the record has no value for it      | Adds it                                    |
| `derive(field, record -> value)` | Sets the field to a value computed from each record        | Adds it                                    |
| `remove(fields...)`              | Removes the fields                                         | Ignores it                                 |
| `keep(fields...)`                | Removes every other field                                  | Ignores it                                 |
| `rename(from, to)`               | Moves the value of `from` to `to`                          | Fails the record; so does an existing `to` |
| `require(fields...)`             | Passes the record on unchanged                             | Fails the record                           |

A field whose value is JSON `null` counts as missing, because `JsonFormat` leaves null-valued keys out when it writes a
record; for the same reason `set` rejects a null value. Field names are top-level keys: `"user.id"` names a key with a
dot in it. An empty field list, or a rename of a field to itself, is rejected when the operator is created. To replace
an existing field with a rename, remove it first: `.pipe(remove("timestamp")).pipe(rename("ts", "timestamp"))`.

A record that fails `require` or `rename` fails with `NonRetryableException`: the same record would fail again, so it
skips the retries and goes straight to the error handler and the dead-letter topic when one is configured, and it does
not count toward the circuit breaker unless its dead-letter send fails. To skip a record without a field instead, use
`filter(record -> record.get("id") != null)`.

The operators change the record's map in place and return it. That is safe in a pipeline, where a record's map belongs
to the attempt processing it and a retry decodes a fresh one, but it matters if your own code keeps a reference to the
map. `set` and `setIfAbsent` put the same value object into every record; use `derive` for a fresh one per record.

For Avro and Protobuf, write the change as a function on the record (`GenericRecord.put`, `Message.toBuilder()`). For
Protobuf records that are only forwarded, `ProtobufBytes` edits the encoded bytes on a `KPipe.bytes` stream without
decoding them; see [FORMATS.md](FORMATS.md#editing-encoded-bytes).

## The explicit API

Everything above is a facade over `Pipeline.from(format)` + `KPipeConsumerBuilder`. Drop down when you need custom
offset managers, one built pipeline shared by several consumers, custom Kafka consumer factories, or shutdown tuning.
The capability map and worked examples are in [ESCAPE-HATCHES.md](ESCAPE-HATCHES.md).
