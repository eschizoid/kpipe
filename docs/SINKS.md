# Sinks

Where processed messages go. `MessageSink<T>` is a `Consumer<T>`-shaped functional interface — any lambda works:

```java
final MessageSink<Map<String, Object>> databaseSink = processed -> databaseService.insert(processed);
```

On the fluent path you attach a sink with a terminal: `.toConsole()`, `.toCustom(sink)`, `.toKafka(topic)`,
`.toMulti(sinks...)`, or `.toBatch(sink, policy)` — semantics summarized in
[API.md](API.md#terminals--each-returns-sinkt). This page covers the sink building blocks themselves. Snippets use
placeholder values; they show shape, not a runnable program.

## Sinks in the explicit API

In the explicit API, `Pipeline.from(format)` returns a builder that takes a sink directly. A pipeline has one sink:
`toSink(sink)` sets it, and a second call throws `IllegalStateException`. To fan out to several, use `toMulti(...)`, on
a stream or on the pipeline builder; delivery is best effort, so a sink that throws is logged and the record still
succeeds. To retry the record when any destination fails, write one sink that calls each in turn. Console sinks
(`JsonConsoleSink`, `AvroConsoleSink`, `ProtobufConsoleSink`) live in their format modules, and you pass an instance to
`toSink` like any other sink:

```java
final var pipeline = Pipeline.from(JsonFormat.INSTANCE)
  .pipe(sanitize)
  .toSink(databaseSink)
  .build();
```

Outside `toMulti`, which is best effort by design, a sink that throws fails the record: it is retried, then handed to
the error handler and, when one is configured, the dead-letter topic. A sink that should carry on past a failure catches
it itself.

## Producing to another Kafka topic

On the fluent path, `.toKafka(topic)` produces each value to `topic`, serialized with the stream's format:

```java
KPipe.json("orders", kafkaProps)
    .pipe(enrich)
    .toKafka("orders-enriched")
    .start();
```

The producer is built from the stream's Kafka properties when the stream starts, the same way the dead-letter producer
is; the dead-letter topic writes through it too, and it closes with the consumer. A stream that reads through a Schema
Registry cannot use `toKafka`, because its format only reads; it throws `IllegalStateException`.

Each output record carries the key and headers of the record it came from, after any `rekey` or `withHeader`. With the
default partitioner, records with the same non-null key go to one output partition. Within one output partition, records
consumed with the same key arrive in the order they were consumed in `KEY_ORDERED` and `SEQUENTIAL` mode, and in
`SEQUENTIAL` mode every record does. With a tracer, each output record carries the span of the record it came from, and
its `traceparent` replaces the one copied from the input. Without a tracer, the copied headers pass through unchanged.

In the explicit API, `KafkaMessageSink` (in `io.github.eschizoid.kpipe.producer.sink`) does the same with a producer you
create and close. It reads the key and headers from `RecordContext.current()`. When no record is bound, which is the
case outside a pipeline, on any other thread and inside a batch sink, it writes a null key and copies no headers:

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import io.github.eschizoid.kpipe.producer.sink.KafkaMessageSink;

final var producer = new KafkaProducer<byte[], byte[]>(producerProps);

final var pipeline = Pipeline.from(JsonFormat.INSTANCE)
  .pipe(transform)
  .toSink(KafkaMessageSink.of(producer, "output-topic", JsonFormat.INSTANCE::serialize))
  .build();
```

Each record waits for the broker to acknowledge its send; what an acknowledgement guarantees depends on the producer's
`acks` (Kafka's default, `all`, waits for the in-sync replicas). A failed send fails the record, unless the sink is one
destination of `toMulti(...)`. The record is then retried if retries are configured, goes to the error handler and, when
configured, the dead-letter topic, and counts as failed in the consumer's metrics; its offset commits after that
handling, unless the dead-letter write also fails. Delivery is at least once: a send that times out may still have
reached the broker, so its retry can write the record twice. In SEQUENTIAL mode each record waits for its own ack,
including the producer's `linger.ms`, so set `linger.ms=0` for such pipelines; PARALLEL overlaps sends across records,
and KEY_ORDERED across keys. The sink takes no tracer: it writes the span of the record being processed, from
`TraceContext.current()`, so the consumer's `withTracer(...)` decides what the outbound headers carry.

## Fan-out to multiple sinks

`.toMulti(sinkA, sinkB, ...)` (or `new CompositeMessageSink<>(...)` in the explicit API) delivers each record to every
sink, **best effort**: a sink that throws is logged at WARNING and suppressed, the other sinks still run, the record
counts as processed, its offset commits, and the DLQ is not involved. This is the right tool for "also mirror to a debug
sink"; it is the wrong tool when every sink must durably receive every record. For that, pass one sink that calls each
destination in turn: a failure in any of them fails the record, and on a retry the destinations that already succeeded
receive it again, so each must be idempotent. Alternatively, produce to a topic and let each destination consume it.

## Batch sinks

Single-record sinks pay the destination's per-call cost on every message. When that cost is non-trivial — a JDBC commit,
an HTTP POST, an S3 PUT — batching amortizes it. `BatchSink<T>` is a `Function<List<T>, BatchResult>` that flushes at a
configurable size or age:

```java
KPipe.json("events", kafkaProps)
    .pipe(addTimestamp)
    .toBatch(
        BatchSink.ofVoid(batch -> jdbc.bulkInsert(batch)),     // void-style: success on return, fail on throw
        new BatchPolicy(100, Duration.ofSeconds(5)))           // flush at 100 records OR 5 seconds, whichever first
    .start();
```

A batch sink reads the records its values came from with `RecordContext.currentBatch()`, index-aligned with the values
and including any `rekey` or `withHeader` that ran before each was buffered; `RecordContext.current()` throws there,
because a flush covers many records. The sink must not modify `values`, since the contexts line up with it by index.
Each buffered record keeps its context, including any added header values, until its batch flushes:

```java
.toBatch(values -> {
    final var records = RecordContext.currentBatch();
    for (int i = 0; i < values.size(); i++) store.put(records.get(i).keyString(), values.get(i));
    return BatchResult.allSucceeded(values.size());
}, policy)
```

`BatchSink.ofVoid(...)` wraps a void-style consumer: a normal return means the whole batch succeeded, a throw sends the
whole batch to the DLQ. For per-record outcomes — a bulk HTTP API that reports which rows failed — implement `BatchSink`
directly and return a `BatchResult` with the succeeded/failed indexes; only the failures route to the DLQ.

Semantics:

- **All processing modes.** Sequential and parallel both work; in parallel mode the buffer counts toward the in-flight
  backpressure metric, so a slow batch destination cannot grow the buffer unbounded.
- **Multi-topic.** Each `KPipe.multi(...)` route can choose `.toBatch(...)` independently.
- **Coverage contract enforced.** A `BatchResult` that does not account for every position `[0, batchSize)` is treated
  as a contract violation: the unaccounted records are routed to the DLQ rather than silently marked processed.
- **Shutdown drain.** A final flush runs before the offset manager closes, so partially-filled buffers are delivered and
  committed on graceful shutdown. The flush calls your sink on the consumer thread and shutdown waits for it, so a slow
  sink holds shutdown until the thread-join budget runs out and `close()` interrupts the thread. A sink interrupted that
  way has not delivered its batch: its records stay unmarked and are redelivered after restart, rather than being
  treated as failures, which with no DLQ would be logged and committed.

Throughput impact is measured by `BatchSinkLatencyBenchmark` — at a 1ms-per-call sink, batch=100 measured roughly 84×
the per-record control (2026-05 capture; raw JMH JSON in [`benchmarks/results/`](../benchmarks/results)).
