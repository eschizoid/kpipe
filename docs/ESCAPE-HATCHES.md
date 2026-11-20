# Escape hatches — when the fluent facade isn't enough

Code snippets on this page use placeholder values (`kafkaProps`, `dataSource`, `pipeline`, ...) — they show shape, not a
runnable program.

The [`KPipe` fluent facade](API.md) covers the 80% path:

```java
try (var handle = KPipe.json("orders", props)
    .pipe(order -> enrich(order))
    .filter(order -> order.total() > 0)
    .withRetry(3, Duration.ofMillis(100))
    .withBackpressure()
    .withDeadLetterTopic("orders.dlq")
    .toCustom(WarehouseSink.create())
    .start()) {
  handle.awaitShutdown();
}
```

Some advanced use cases need the explicit `Pipeline.from(format)` + `KPipeConsumerBuilder` surface. This page is the
index — for each capability, it tells you whether it's on the fluent path, on the explicit path, or both, and how to
wire it up.

Rows in **bold** are deliberately escape-hatch-only (no fluent equivalent planned).

## Capability map

| Capability                                     | Facade path                                                                                                               | Explicit-API path                                                                                                          | Notes                                                                                                                                                                                                                                                                                                                                                                                |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Format selection                               | `KPipe.json/avro/protobuf/bytes/custom(...)`                                                                              | `Pipeline.from(format)`                                                                                                    | Pass any `MessageFormat<T>`, bundled or your own. The `KPipe.custom(...)` facade form gives user-supplied formats the same fluent surface as the bundled ones.                                                                                                                                                                                                                       |
| Topic subscription (homogeneous)               | `KPipe.X(topic, props)`                                                                                                   | `Builder.withTopic(s) / withTopics(...)`                                                                                   | One topic-set, one shared pipeline.                                                                                                                                                                                                                                                                                                                                                  |
| Topic subscription (heterogeneous)             | `KPipe.multi(props).json(...).avro(...)...start()`                                                                        | `Builder.withPipelines(Map<String, Pipeline<?>>)`                                                                          | Per-topic dispatch.                                                                                                                                                                                                                                                                                                                                                                  |
| Operator chain (`pipe/filter/peek/when`)       | `Stream.pipe / filter / peek / when`                                                                                      | `Pipeline.from(format).pipe / filter / peek / when`                                                                        | Same names and behaviour: `pipe`, `filter`, `peek`, `when`, `skipBytes`, and `toMulti` for best-effort fan-out.                                                                                                                                                                                                                                                                      |
| Failed-attempt observer                        | `Stream.onAttemptFailed` / `MultiBuilder.onAttemptFailed`                                                                 | `Builder.onAttemptFailed(...)`                                                                                             | Every failed attempt at every stage, before the error handler.                                                                                                                                                                                                                                                                                                                       |
| Custom terminal sink                           | `Stream.toCustom(MessageSink<T>)`                                                                                         | `Pipeline.from(format).toSink(MessageSink<T>)`                                                                             | Any `MessageSink<T>`; facade also offers `.toConsole()`.                                                                                                                                                                                                                                                                                                                             |
| Multi-sink fanout                              | `Stream.toMulti(sinks...)`                                                                                                | `Pipeline.from(format).toMulti(...)` or `new CompositeMessageSink<>(...)`                                                  | Best-effort delivery; per-sink errors logged + suppressed.                                                                                                                                                                                                                                                                                                                           |
| Batch sink (size + age flush)                  | `Stream.toBatch(BatchSink<T>, BatchPolicy)`                                                                               | `Builder.withBatchPipeline(topic, pipeline, sink, policy)`                                                                 | `BatchSink<T>` returns `BatchResult` for per-record DLQ; `BatchSink.ofVoid(consumer)` wraps void-style sinks (whole-batch DLQ on throw). Sequential + parallel modes both work.                                                                                                                                                                                                      |
| Batch sink (multi-topic)                       | `KPipe.multi(props).json(topic, s -> s.toBatch(sink, policy))...start()`                                                  | `Builder.withBatchPipeline(...)` × N                                                                                       | One consumer-group, mixed batch + non-batch routes.                                                                                                                                                                                                                                                                                                                                  |
| Skip wire-format prefix                        | `Stream.skipBytes(int)`                                                                                                   | `Pipeline.Builder.skipBytes(int)`                                                                                          | Confluent envelope: 5 (Avro) / 6 (Proto single-msg). Don't combine with `withSchemaRegistry(...)`.                                                                                                                                                                                                                                                                                   |
| Confluent SR per-record auto-lookup (Avro)     | `Stream.withSchemaRegistry(SchemaResolver)`                                                                               | `AvroFormat.withRegistry(SchemaResolver)`                                                                                  | Cached by ID, no TTL (SR IDs are immutable). Replaces the static `lookupBySubjectVersion("latest")` startup-fetch path.                                                                                                                                                                                                                                                              |
| Confluent SR per-record auto-lookup (Protobuf) | `Stream.withSchemaRegistry(SchemaResolver)`                                                                               | `ProtobufFormat.withRegistry(SchemaResolver)`                                                                              | Same shape as Avro; additionally needs the opt-in `kpipe-format-protobuf-confluent` module at runtime (the shaded `.proto`-text compiler). See [FORMATS.md](FORMATS.md#confluent-schema-registry-mode).                                                                                                                                                                              |
| Retry policy                                   | `Stream.withRetry(int, Duration)`                                                                                         | `Builder.withRetry(int, Duration)`                                                                                         |                                                                                                                                                                                                                                                                                                                                                                                      |
| Backpressure (default watermarks)              | `Stream.withBackpressure()`                                                                                               | `Builder.withBackpressure()`                                                                                               | Defaults: pause 10k, resume 7k.                                                                                                                                                                                                                                                                                                                                                      |
| Backpressure (custom watermarks)               | `Stream.withBackpressure(high, low)`                                                                                      | `Builder.withBackpressure(high, low)`                                                                                      | High/low strategy auto-derived from `ProcessingMode`.                                                                                                                                                                                                                                                                                                                                |
| Processing mode                                | `Stream.withProcessingMode(ProcessingMode)`                                                                               | `Builder.withProcessingMode(ProcessingMode)`                                                                               | `SEQUENTIAL` (per-partition serial, lag-based backpressure), `PARALLEL` (default, virtual-thread-per-record), `KEY_ORDERED` (per-key serial, see next row).                                                                                                                                                                                                                          |
| Key-ordered key cap                            | `Stream.withKeyOrderedMaxKeys(int)`                                                                                       | `Builder.withKeyOrderedMaxKeys(int)`                                                                                       | Default 10,000 distinct keys held in memory. Only meaningful for `KEY_ORDERED`. Records with `null` keys share a single sentinel queue.                                                                                                                                                                                                                                              |
| OTel/custom metrics                            | `Stream.withMetrics(metrics)` (one shared `KPipeOtel.metrics(otel)`) / `MultiBuilder.withMetrics(...)`                    | `Builder.withMetrics(MetricsExporter)`                                                                                     | Single-format and multi-topic both supported.                                                                                                                                                                                                                                                                                                                                        |
| Custom error handler                           | `Stream.withErrorHandler(Consumer<ProcessingError>)`                                                                      | `Builder.withErrorHandler(ErrorHandler)`                                                                                   | Default logs at WARNING. `ProcessingError.record()` is `ConsumerRecord<byte[], byte[]>` — decode a string key with `new String(record.key(), UTF_8)` (guarding null).                                                                                                                                                                                                                |
| Dead-letter topic                              | `Stream.withDeadLetterTopic(String)`                                                                                      | `Builder.withDeadLetterTopic(String)` / `Builder.withDeadLetterQueue(String, KPipeProducer)`                               | The two-arg form pairs the topic and a pre-built producer atomically — preferred when sharing a producer because it stops the topic + producer settings from drifting out of sync.                                                                                                                                                                                                   |
| Poll timeout                                   | `Stream.withPollTimeout(Duration)`                                                                                        | `Builder.withPollTimeout(Duration)`                                                                                        | Default 100ms.                                                                                                                                                                                                                                                                                                                                                                       |
| Offset commit interval                         | `Stream.withCommitInterval(Duration)` / `MultiBuilder.withCommitInterval(Duration)`                                       | `Builder.withCommitInterval(Duration)`                                                                                     | Default 30 seconds, at least 1 ms. Configures the default `KafkaOffsetManager`; rejected alongside a custom `OffsetManager`.                                                                                                                                                                                                                                                         |
| Lifecycle handle                               | `Handle.health / metrics / awaitShutdown / close`                                                                         | `KPipeConsumer.start / awaitShutdown / shutdownGracefully / awaitDispatchIdle`                                             | `Handle` is `AutoCloseable` (5s graceful default); the consumer hosts the lifecycle directly.                                                                                                                                                                                                                                                                                        |
| **External offset store**                      | `Stream.withOffsetStore(OffsetStore)` / `MultiBuilder.withOffsetStore(...)`                                               | `Builder.withOffsetStore(OffsetStore)`                                                                                     | Keeps committed offsets in a database instead of Kafka; the default manager still computes the frontier. Rejected alongside a custom `OffsetManager`.                                                                                                                                                                                                                                |
| **Custom `OffsetManager`**                     | — (escape hatch only)                                                                                                     | `Builder.withOffsetManager(OffsetManager)` / `withOffsetManagerProvider(Function<Consumer<byte[],byte[]>, OffsetManager>)` | Replaces frontier tracking too. The `Provider` form receives the Kafka consumer, to build a manager such as `KafkaOffsetManager` around it; only the consumer thread may call that consumer, so commit from a timer through `Builder.getCommitExecutor()`. Set one form, not both. A `withOffsetManager` instance serves one `build()`; use the provider to build several consumers. |
| **Custom Kafka `Consumer` factory**            | —                                                                                                                         | `Builder.withConsumer(Supplier<Consumer<byte[],byte[]>>)`                                                                  | Test seam / SSL configurators.                                                                                                                                                                                                                                                                                                                                                       |
| **Custom DLQ `KafkaProducer`**                 | —                                                                                                                         | `Builder.withKafkaProducer(Producer<byte[],byte[]>)` / `withKafkaProducer(KPipeProducer<byte[],byte[]>)`                   | Override default producer for the DLQ. The `KPipeProducer` overload shares an already-wrapped instance. Either way, each DLQ record carries the failed record's trace.                                                                                                                                                                                                               |
| **Rebalance listener**                         | —                                                                                                                         | Owned by the registered `OffsetManager` — override `OffsetManager.createRebalanceListener()`                               | No standalone `withRebalanceListener` builder; the consumer wires the listener from whatever manager you register.                                                                                                                                                                                                                                                                   |
| **Custom command queue**                       | —                                                                                                                         | `Builder.withCommandQueue(Queue<ConsumerCommand>)`                                                                         | Test seam (rarely needed).                                                                                                                                                                                                                                                                                                                                                           |
| Thread/executor termination tuning             | `Stream.withThreadTerminationTimeout(Duration)` / `.withWaitForMessagesTimeout(Duration)`, and the `MultiBuilder` mirrors | `Builder.withThreadTerminationTimeout / withWaitForMessagesTimeout`                                                        |
| Metrics log + shutdown hook                    | `Stream.withMetricsLog(Duration)` / `.withShutdownHook(boolean)`, and the `MultiBuilder` mirrors                          | `Builder.withMetricsLog(Duration) / withShutdownHook(true)`                                                                |
| **Health endpoint**                            | —                                                                                                                         | `HttpHealthServer.fromEnv(...)`                                                                                            | Run alongside `Handle`: `HttpHealthServer.fromEnv(handle::health, "app")`.                                                                                                                                                                                                                                                                                                           |
| In-flight drain                                | `Handle.shutdownGracefully(Duration)`                                                                                     | `KPipeConsumer.awaitDispatchIdle(Duration) / shutdownGracefully(Duration)`                                                 | Waits for dispatched records, then closes, which flushes buffered batch records; `true` only when nothing is left in flight.                                                                                                                                                                                                                                                         |
| **Pre-built `Pipeline`**                       | —                                                                                                                         | `Pipeline.from(format)...build()`, then `withPipeline(pipeline)` on each consumer                                          | One built pipeline passed to several consumers; see [the worked example](#one-pipeline-shared-across-consumers).                                                                                                                                                                                                                                                                     |

## Worked examples

### Offsets in an external store

Use case: keep committed offsets in a database, such as Postgres or a DynamoDB global table that more than one region
can read, instead of in Kafka.

```java
final var handle = KPipe.json("orders", kafkaProps)
  .withProcessingMode(ProcessingMode.PARALLEL)
  .withOffsetStore(new PostgresOffsetStore(dataSource, "kpipe_offsets", "payment-service"))
  .toCustom(this::handle)
  .start();
```

An `OffsetStore` has two methods: `load(partitions)` returns the stored next offset for each partition it has one for,
and `commit(offsets)` stores the next offset for each partition given.
[`examples/offset-stores`](../examples/offset-stores) has two to copy, `PostgresOffsetStore` and `DynamoDbOffsetStore`,
built the same way (connection, table, group) and checked by one contract test.

The default `KafkaOffsetManager` still decides what is safe to commit. The store receives the commit frontier, the
lowest offset that has not finished processing, on the commit interval, on revoke and at shutdown. On assignment the
consumer seeks each partition to the offset `load` returns, and retries a failed `load` until it succeeds or the
consumer closes; it does not poll while it retries. A `load` that returns a null map or a null or negative offset is
logged at ERROR and retried the same way, and an offset for a partition that was not requested is ignored with a
WARNING. A partition with no stored offset starts from the group's Kafka offset or `auto.offset.reset`; with a store,
set `auto.offset.reset=earliest`. `KPipeConsumer.builder()` takes a store through `withOffsetStore` as well.
`GlobalTableFailoverIntegrationTest` in the same example runs one group in two regions over a DynamoDB table and shows
the second region resuming from the offsets that replicated to it.

Storing the offset of each record as it finishes, as a hand-written `OffsetManager` might, can skip records in
`PARALLEL` and `KEY_ORDERED` mode: records finish out of order, so a later offset can be stored while an earlier record
is still running, and a restart seeks past it.

The offset write is separate from the sink's writes, so delivery stays at-least-once: a restart reprocesses the records
from the stored frontier on. The store is not fenced the way Kafka fences commits from a consumer that left the group,
so a store several regions write to, such as a DynamoDB global table, needs its own fencing if its offsets must only
move forwards; see [GUARANTEES.md](GUARANTEES.md#external-offset-stores-and-managers).

### A custom OffsetManager

`OffsetManager` has six methods, and the consumer calls them in this order:

1. `start()` once, before the consumer subscribes.
2. `trackOffset(record)` for each record, on the consumer thread, before the record is dispatched.
3. `markOffsetProcessed(record)` once the record is done for good: processed, filtered, unrouted, parked in the DLQ, or
   failed with no DLQ configured. Worker threads, batch flush threads and the consumer thread call it concurrently, so
   it must be thread-safe. It is not called for a record whose DLQ send failed.
4. The listener from `createRebalanceListener()`, on the consumer thread inside `poll`. The default does nothing. A
   manager that keeps per-partition state must commit and forget revoked partitions, and forget lost ones. Another
   consumer may already own a lost partition, so a write to a store that cannot fence it would move the new owner's
   offset backwards.
5. `close()`, once, normally on the consumer thread before the Kafka consumer closes, or on the caller's thread for a
   consumer that never started; it makes the final commit. When another thread closes the consumer, `signalClosing()`
   comes first, on that thread and concurrently with the consumer thread, so a manager blocking the consumer thread can
   stop. A poll loop that ends on its own goes straight to `close`.

Only the consumer thread may call the Kafka consumer. A manager that commits from a timer must go through the
`CommitExecutor` from `KPipeConsumerBuilder.getCommitExecutor()`, which runs the commit on that thread. A `trackOffset`
or `markOffsetProcessed` that throws is logged at ERROR and does not stop the consumer or re-run the record, but the
offset stays pending, so the commit point holds there until the next restart or rebalance.

A manager that replaces `KafkaOffsetManager` must compute the commit frontier itself; to change only where offsets are
kept, use an `OffsetStore`. `MetricsSnapshot.commits()` counts only the commits of a `KafkaOffsetManager` the consumer
was built with, so a custom manager reports its own commit health.

### A custom rebalance listener

Use case: flush an external buffer before partitions are revoked. There is no standalone `withRebalanceListener` on the
Builder — the rebalance listener is owned by whatever `OffsetManager` you register. The usual approach is to wrap the
default `KafkaOffsetManager` (the class is final) with a delegating `OffsetManager` implementation that forwards every
method to the delegate except `createRebalanceListener()`:

```java
final class FlushingOffsetManager implements OffsetManager {

  private final KafkaOffsetManager delegate;
  private final ExternalBuffer externalBuffer;

  FlushingOffsetManager(final KafkaOffsetManager delegate, final ExternalBuffer externalBuffer) {
    this.delegate = delegate;
    this.externalBuffer = externalBuffer;
  }

  @Override
  public void start() {
    delegate.start();
  }

  @Override
  public void trackOffset(final ConsumerRecord<byte[], byte[]> record) {
    delegate.trackOffset(record);
  }

  @Override
  public void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record) {
    delegate.markOffsetProcessed(record);
  }

  @Override
  public void signalClosing() {
    delegate.signalClosing();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public ConsumerRebalanceListener createRebalanceListener() {
    final var wrapped = delegate.createRebalanceListener();
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        externalBuffer.flush(partitions);
        wrapped.onPartitionsRevoked(partitions);
      }

      @Override
      public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        wrapped.onPartitionsAssigned(partitions);
      }

      @Override
      public void onPartitionsLost(final Collection<TopicPartition> partitions) {
        wrapped.onPartitionsLost(partitions);
      }
    };
  }
}

// Wire it up with the Provider form so the Kafka consumer is available when the manager is built.
// Without an OffsetStore, KafkaOffsetManager.Builder requires a CommitExecutor — the seam that runs its periodic
// commits on the consumer thread. Grab the one bound to this builder via getCommitExecutor().
final var builder = KPipeConsumer.builder()
  .withProperties(kafkaProps)
  .withTopic("orders")
  .withPipeline(pipeline);

final var consumer = builder
  .withOffsetManagerProvider((kafkaConsumer) ->
    new FlushingOffsetManager(
      KafkaOffsetManager.builder(kafkaConsumer).withCommitExecutor(builder.getCommitExecutor()).build(),
      externalBuffer
    )
  )
  .build();
```

The delegate-and-wrap shape keeps the default offset bookkeeping intact while letting you add behaviour at rebalance
time. Forward every method, `signalClosing` included, and call through to the wrapped listener in `onPartitionsRevoked`,
`onPartitionsAssigned` and `onPartitionsLost`. The default `KafkaOffsetManager` listener does real work in each: on
revoke it commits the frontier of the revoked partitions and forgets them, on assignment it clears stale state and, with
an `OffsetStore`, seeks to the stored offsets, and on loss it forgets the partitions. `ConsumerRebalanceListener`'s own
default for `onPartitionsLost` calls `onPartitionsRevoked`, so a wrapper that leaves it out commits partitions that
another consumer may already own. The consumer counts commits in `MetricsSnapshot.commits()` only for a
`KafkaOffsetManager` it was built with directly, so the wrapped manager's commits are not counted there.

### One pipeline shared across consumers

Use case: a single pipeline definition reused across consumer groups for A/B traffic split.

```java
final var pipeline = Pipeline.from(JsonFormat.INSTANCE)
    .pipe(enrichOp)
    .toSink(warehouseSink)
    .build();

final var groupA = KPipeConsumer.builder()
    .withProperties(propsForGroup("group-a"))
    .withTopic("orders")
    .withPipeline(pipeline)
    .build();

final var groupB = KPipeConsumer.builder()
    .withProperties(propsForGroup("group-b"))
    .withTopic("orders")
    .withPipeline(pipeline)
    .build();
```

Both consumers use the same pipeline instance. A built `Pipeline` keeps no state of its own between records, but both
consumers call `enrichOp` and `warehouseSink`, so those must be safe to call from several threads at once.

## Dropping back to the explicit API mid-stream

You don't have to commit to one surface. The fluent facade's `Handle` exposes the underlying `KPipeConsumer` via
`metrics()` and lifecycle methods; for richer access, build with the explicit API directly. The two surfaces are
interchangeable: anything you can write with the facade can be written with the builder, and the facade itself is just a
thin layer over the builder.

If you find yourself reaching for an escape hatch that isn't in the table above, open an issue — either it's missing
from this doc, or it's a real gap in the API.

## Advanced pipeline patterns

Patterns for pipelines built with `Pipeline.from(format)`.

**Operators chosen by name.** To let `PROCESSOR_PIPELINE=TIMESTAMP,SOURCE`-style configuration pick operators, keep your
own map from name to operator and look each name up while you build:

```java
final Map<String, UnaryOperator<Map<String, Object>>> operators = Map.of(
    "TIMESTAMP", JsonFields.derive("processedAt", record -> System.currentTimeMillis()),
    "SOURCE", JsonFields.set("source", "orders-service"));

final var builder = Pipeline.from(JsonFormat.INSTANCE);
for (final var name : System.getenv().getOrDefault("PROCESSOR_PIPELINE", "").split(",")) {
  if (name.isBlank()) continue;
  final var operator = operators.get(name.strip());
  if (operator == null) throw new IllegalArgumentException("Unknown operator: " + name);
  builder.pipe(operator);
}
final var pipeline = builder.toSink(sink).build();
```

**Conditional processing** — `Pipeline.Builder.when(predicate, ifTrue, ifFalse)` branches inside one pipeline; both
branches are required. For drop-on-condition, add `Operators.filter(predicate)` — a false predicate marks the record
`Filtered` (offset still commits, sink skipped), which is the deliberate-skip half of the sealed `Result` type. The
failure half (`Failed`) routes through retry / error handler / DLQ — the two are distinct types precisely so a filter
can never mask a bug.
