# Sinks

Where processed messages go. `MessageSink<T>` is a `Consumer<T>`-shaped functional interface — any lambda works:

```java
final MessageSink<Map<String, Object>> databaseSink = processed -> databaseService.insert(processed);
```

On the fluent path you attach a sink with a terminal: `.toConsole()`, `.toCustom(sink)`, `.toMulti(sinks...)`, or
`.toBatch(sink, policy)` — semantics summarized in [API.md](API.md#terminals--each-returns-sinkt). This page covers
the sink building blocks themselves. Snippets use placeholder values; they show shape, not a runnable program.

## Registering sinks in the explicit API

`MessageProcessorRegistry` holds operators and sinks in two namespaces under the same key shape:
`registerOperator(key, op)` / `registerSink(key, sink)` to add, `getOperator(key)` / `getSink(key)` to look up, and
per-namespace utilities (`getAllSinks`, `getSinkMetrics`, `unregisterSink`, `clearSinks`, `compositeSink`). Console
sinks (`JsonConsoleSink`, `AvroConsoleSink`, `ProtobufConsoleSink`) live in their format modules and are not
auto-registered:

```java
final var registry = new MessageProcessorRegistry();

registry.registerSink(RegistryKey.json("jsonConsole"), new JsonConsoleSink<Map<String, Object>>());
registry.registerSink(RegistryKey.of("database", Map.class), databaseSink);

final var pipeline = registry
  .pipeline(JsonFormat.INSTANCE)
  .add(RegistryKey.json("sanitize"))
  .toSink(RegistryKey.of("database", Map.class))
  .build();
```

Wrap user sinks with `MessageProcessorRegistry.withSinkErrorHandling(sink)` when one failing sink must not take down
the pipeline; the failure is logged and contained at the sink boundary.

## Producing to another Kafka topic

`KafkaMessageSink` (in `io.github.eschizoid.kpipe.producer.sink`) serializes the processed value and produces it:

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import io.github.eschizoid.kpipe.metrics.ProducerMetrics;
import io.github.eschizoid.kpipe.producer.sink.KafkaMessageSink;
import io.github.eschizoid.kpipe.producer.tracing.Tracer;

final var producer = new KafkaProducer<byte[], byte[]>(producerProps);

final var pipeline = registry
  .pipeline(JsonFormat.INSTANCE)
  .add(RegistryKey.json("transform"))
  .toSink(KafkaMessageSink.of(
    producer, "output-topic", JsonFormat.INSTANCE::serialize, ProducerMetrics.noop(), Tracer.noop()))
  .build();
```

Pass a real `ProducerMetrics` / `Tracer` to count sends and propagate trace context; the noops cost nothing.

## Fan-out to multiple sinks

`.toMulti(sinkA, sinkB, ...)` (or `new CompositeMessageSink<>(...)` in the explicit API) delivers each record to every
sink, **best effort**: a sink that throws is logged at WARNING and suppressed, the other sinks still run, the record
counts as processed, its offset commits, and the DLQ is not involved. This is the right tool for "also mirror to a
debug sink"; it is the wrong tool when every sink must durably receive every record — for that, produce to a topic and
let each destination consume it, or use one sink that writes transactionally to both destinations.

## Batch sinks

Single-record sinks pay the destination's per-call cost on every message. When that cost is non-trivial — a JDBC
commit, an HTTP POST, an S3 PUT — batching amortizes it. `BatchSink<T>` is a `Function<List<T>, BatchResult>` that
flushes at a configurable size or age:

```java
KPipe.json("events", kafkaProps)
    .pipe(addTimestamp)
    .toBatch(
        BatchSink.ofVoid(batch -> jdbc.bulkInsert(batch)),     // void-style: success on return, fail on throw
        new BatchPolicy(100, Duration.ofSeconds(5)))           // flush at 100 records OR 5 seconds, whichever first
    .start();
```

`BatchSink.ofVoid(...)` wraps a void-style consumer: a normal return means the whole batch succeeded, a throw sends
the whole batch to the DLQ. For per-record outcomes — a bulk HTTP API that reports which rows failed — implement
`BatchSink` directly and return a `BatchResult` with the succeeded/failed indexes; only the failures route to the DLQ.

Semantics:

- **All processing modes.** Sequential and parallel both work; in parallel mode the buffer counts toward the in-flight
  backpressure metric, so a slow batch destination cannot grow the buffer unbounded.
- **Multi-topic.** Each `KPipe.multi(...)` route can choose `.toBatch(...)` independently.
- **Coverage contract enforced.** A `BatchResult` that does not account for every position `[0, batchSize)` is treated
  as a contract violation: the unaccounted records are routed to the DLQ rather than silently marked processed.
- **Shutdown drain.** A final flush runs before the offset manager closes, so partially-filled buffers are delivered
  and committed on graceful shutdown.

Throughput impact is measured by `BatchSinkLatencyBenchmark` — at a 1ms-per-call sink, batch=100 measured roughly 84×
the per-record control (2026-05 capture; raw JMH JSON in [`benchmarks/results/`](../benchmarks/results/)).
