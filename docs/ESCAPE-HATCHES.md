# Escape hatches — when the fluent facade isn't enough

The [`KPipe` fluent facade](../README.md#the-fluent-facade) covers the 80% path:

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

Some advanced use cases need the explicit `MessageProcessorRegistry` + `KPipeConsumerBuilder` surface. This page is the
index — for each capability, it tells you whether it's on the fluent path, on the explicit path, or both, and how to
wire it up.

Rows in **bold** are deliberately escape-hatch-only (no fluent equivalent planned).

## Capability map

| Capability                                          | Facade path                                                              | Explicit-API path                                                                                                          | Notes                                                                                                                                                                                                                |
| --------------------------------------------------- | ------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Format selection                                    | `KPipe.json/avro/protobuf/bytes/custom(...)`                             | `new MessageProcessorRegistry()` + `registry.pipeline(format)`                                                             | The registry is format-agnostic; pass the `MessageFormat<T>` per pipeline. The `KPipe.custom(...)` facade form gives user-supplied formats the same fluent surface as the bundled ones.                              |
| Topic subscription (homogeneous)                    | `KPipe.X(topic, props)`                                                  | `Builder.withTopic(s) / withTopics(...)`                                                                                   | One topic-set, one shared pipeline.                                                                                                                                                                                  |
| Topic subscription (heterogeneous)                  | `KPipe.multi(props).json(...).avro(...)...start()`                       | `Builder.withPipelines(Map<String, MessagePipeline<?>>)`                                                                   | Per-topic dispatch.                                                                                                                                                                                                  |
| Operator chain (`pipe/filter/peek/when`)            | `Stream.pipe / filter / peek / when`                                     | `MessageProcessorRegistry.pipeline(format).add(...)`                                                                       | Identical operator semantics.                                                                                                                                                                                        |
| Result observers (`onFiltered/onFailed/peekResult`) | `Stream.onFiltered / onFailed / peekResult`                              | — (the fluent facade is the entry point; explicit API users pattern-match `Result<T>` directly)                            | Observers fire after the pipeline returns `Result.Passed/Filtered/Failed`. Side-effect only; they do not suppress or reroute.                                                                                        |
| Custom terminal sink                                | `Stream.toCustom(MessageSink<T>)`                                        | `registry.registerSink(key, sink)`                                                                                         | Any `MessageSink<T>`; facade also offers `.toConsole()`.                                                                                                                                                             |
| Multi-sink fanout                                   | `Stream.toMulti(sinks...)`                                               | `new CompositeMessageSink<>(...)`                                                                                          | Best-effort delivery; per-sink errors logged + suppressed.                                                                                                                                                           |
| Batch sink (size + age flush)                       | `Stream.toBatch(BatchSink<T>, BatchPolicy)`                              | `Builder.withBatchPipeline(topic, pipeline, sink, policy)`                                                                 | `BatchSink<T>` returns `BatchResult` for per-record DLQ; `BatchSink.ofVoid(consumer)` wraps void-style sinks (whole-batch DLQ on throw). Sequential + parallel modes both work.                                      |
| Batch sink (multi-topic)                            | `KPipe.multi(props).json(topic, s -> s.toBatch(sink, policy))...start()` | `Builder.withBatchPipeline(...)` × N                                                                                       | One consumer-group, mixed batch + non-batch routes.                                                                                                                                                                  |
| Skip wire-format prefix                             | `Stream.skipBytes(int)`                                                  | `TypedPipelineBuilder.skipBytes(int)`                                                                                      | Confluent envelope: 5 (Avro) / 6 (Proto single-msg). Don't combine with `withSchemaRegistry(...)`.                                                                                                                   |
| Confluent SR per-record auto-lookup (Avro)          | `Stream.withSchemaRegistry(SchemaResolver)`                              | `AvroFormat.withRegistry(SchemaResolver)`                                                                                  | Cached by ID, no TTL (SR IDs are immutable). Avro only — Protobuf needs runtime `.proto` compilation. Replaces the static `lookupBySubjectVersion("latest")` startup-fetch path.                                     |
| Pipeline outcome counters (OTel)                    | `Stream.peekResult(PipelineMetricsObserver)`                             | hand any `Consumer<Result<T>>` to `peekResult`                                                                             | `PipelineMetricsObserver` in `kpipe-metrics-otel` emits `kpipe.pipeline.passed/filtered/failed`. Optional `bindSchemaRegistryCache(...)` adds the SR cache counters.                                                 |
| Retry policy                                        | `Stream.withRetry(int, Duration)`                                        | `Builder.withRetry(int, Duration)`                                                                                         |                                                                                                                                                                                                                      |
| Backpressure (default watermarks)                   | `Stream.withBackpressure()`                                              | `Builder.withBackpressure(BackpressureController)`                                                                         | Defaults: pause 10k, resume 7k.                                                                                                                                                                                      |
| Backpressure (custom watermarks)                    | `Stream.withBackpressure(high, low)`                                     | `Builder.withBackpressure(BackpressureController)`                                                                         | High/low strategy auto-derived from `ProcessingMode`.                                                                                                                                                                |
| Processing mode                                     | `Stream.withProcessingMode(ProcessingMode)`                              | `Builder.withProcessingMode(ProcessingMode)`                                                                               | `SEQUENTIAL` (per-partition serial, lag-based backpressure), `PARALLEL` (default, virtual-thread-per-record), `KEY_ORDERED` (per-key serial, see next row).                                                          |
| Key-ordered key cap                                 | `Stream.withKeyOrderedMaxKeys(int)`                                      | `Builder.withKeyOrderedMaxKeys(int)`                                                                                       | Default 10,000 distinct keys held in memory. Only meaningful for `KEY_ORDERED`. Records with `null` keys share a single sentinel queue.                                                                              |
| OTel/custom metrics                                 | `Stream.withMetrics(ConsumerMetrics)` / `MultiBuilder.withMetrics(...)`  | `Builder.withMetrics(ConsumerMetrics)`                                                                                     | Single-format and multi-topic both supported.                                                                                                                                                                        |
| Custom error handler                                | `Stream.withErrorHandler(Consumer<ProcessingError>)`                     | `Builder.withErrorHandler(ErrorHandler)`                                                                                   | Default logs at WARNING. `ProcessingError.record()` is `ConsumerRecord<byte[], byte[]>` — decode a string key with `new String(record.key(), UTF_8)` (guarding null).                                                |
| Dead-letter topic                                   | `Stream.withDeadLetterTopic(String)`                                     | `Builder.withDeadLetterTopic(String)` / `Builder.withDeadLetterQueue(String, KPipeProducer)`                               | The two-arg form pairs the topic and a pre-built producer atomically — preferred when sharing a producer because it stops the topic + producer settings from drifting out of sync.                                   |
| Poll timeout                                        | `Stream.withPollTimeout(Duration)`                                       | `Builder.withPollTimeout(Duration)`                                                                                        | Default 100ms.                                                                                                                                                                                                       |
| Lifecycle handle                                    | `Handle.isHealthy / metrics / awaitShutdown / close`                     | `KPipeConsumer.start / awaitShutdown / shutdownGracefully / waitForInFlightDrain`                                          | `Handle` is `AutoCloseable` (5s graceful default); the consumer hosts the lifecycle directly.                                                                                                                        |
| **Custom `OffsetManager`**                          | — (escape hatch only)                                                    | `Builder.withOffsetManager(OffsetManager)` / `withOffsetManagerProvider(Function<Consumer<byte[],byte[]>, OffsetManager>)` | E.g. Postgres/Redis-backed manager. The `Provider` form receives the live Kafka consumer for wiring commit callbacks.                                                                                                |
| **Custom Kafka `Consumer` factory**                 | —                                                                        | `Builder.withConsumer(Supplier<Consumer<byte[],byte[]>>)`                                                                  | Test seam / SSL configurators.                                                                                                                                                                                       |
| **Custom DLQ `KafkaProducer`**                      | —                                                                        | `Builder.withKafkaProducer(Producer<byte[],byte[]>)` / `withKafkaProducer(KPipeProducer<byte[],byte[]>)`                   | Override default producer for the DLQ. The `KPipeProducer` overload shares an already-wrapped instance (e.g. one with custom metrics attached).                                                                      |
| **Rebalance listener**                              | —                                                                        | Owned by the registered `OffsetManager` — override `OffsetManager.createRebalanceListener()`                               | No standalone `withRebalanceListener` builder; the consumer wires the listener from whatever manager you register.                                                                                                   |
| **Custom command queue**                            | —                                                                        | `Builder.withCommandQueue(Queue<ConsumerCommand>)`                                                                         | Test seam (rarely needed).                                                                                                                                                                                           |
| **Thread/executor termination tuning**              | —                                                                        | `Builder.withThreadTerminationTimeout / withWaitForMessagesTimeout`                                                        | Shutdown tuning. The executor-drain timeout is non-tunable from the Builder — it stays at `ConsumerDefaults.EXECUTOR_TERMINATION`.                                                                                  |
| **Periodic metrics reporting + shutdown hook**      | —                                                                        | `Builder.withMetricsReporters(...) / withMetricsInterval(...) / withShutdownHook(true)`                                    | Folded into `KPipeConsumerBuilder` in 1.13. Reporter thread is daemon, doesn't keep the JVM up. Behavior pinned by `KPipeReporterThreadTest` so the no-callers-in-repo signal can't be misread as dead code (#145). |
| **Health endpoint**                                 | —                                                                        | `HttpHealthServer.fromEnv(...)`                                                                                            | Run alongside `Handle` in the host process.                                                                                                                                                                          |
| **In-flight drain**                                 | `Handle.shutdownGracefully(Duration)`                                    | `KPipeConsumer.waitForInFlightDrain(Duration) / shutdownGracefully(Duration)`                                              | Uses the live in-flight counter (includes buffered batch records), not metric-derived.                                                                                                                               |
| **Custom `MessageProcessorRegistry`**               | —                                                                        | `new MessageProcessorRegistry()` + `register*(...)` + `pipeline(format)`                                                   | Pre-shared pipelines across consumers, multi-format orchestrators. The registry is format-agnostic — the format is supplied per pipeline.                                                                            |

## Worked examples

### A custom OffsetManager

Use case: persist offsets in Postgres alongside business writes, so commit is atomic with the record's downstream
side-effect.

```java
final var offsetManager = new PostgresOffsetManager(dataSource);

final var consumer = KPipeConsumer.builder()
  .withProperties(kafkaProps)
  .withTopic("orders")
  .withPipeline(pipeline)
  .withOffsetManager(offsetManager)
  .build();
```

`OffsetManager` is a thin SPI — the load-bearing methods are `trackOffset` (a record entered the pipeline),
`markOffsetProcessed` (it finished), `notifyCommitComplete` (a queued commit landed), and `close`, plus the `start` /
`stop` / `isRunning` / `getState` / `getStatistics` lifecycle-and-introspection surface and the default
`createRebalanceListener()`. The consumer threads call into it concurrently; implementations must be thread-safe.

### A custom rebalance listener

Use case: flush an external buffer before partitions are revoked. There is no standalone `withRebalanceListener` on the
Builder — the rebalance listener is owned by whatever `OffsetManager` you register. The usual approach is to wrap the
default `KafkaOffsetManager` (its constructor is private; you can't extend it) with a delegating `OffsetManager`
implementation that forwards every method to the delegate except `createRebalanceListener()`:

```java
final class FlushingOffsetManager implements OffsetManager {

  private final KafkaOffsetManager delegate;
  private final ExternalBuffer externalBuffer;

  FlushingOffsetManager(final KafkaOffsetManager delegate, final ExternalBuffer externalBuffer) {
    this.delegate = delegate;
    this.externalBuffer = externalBuffer;
  }

  // Forward every other OffsetManager method to delegate — start/close/trackOffset/markOffsetProcessed/...
  // (Omitted here; one-line forwarders.)

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
    };
  }
}

// Wire it up with the Provider form so the Kafka consumer is available when the manager is built.
// Share a single command queue with the consumer — KafkaOffsetManager.Builder requires it, and the
// consumer needs to drain the same queue to actually run the commits the manager enqueues.
final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

final var consumer = KPipeConsumer.builder()
  .withProperties(kafkaProps)
  .withTopic("orders")
  .withPipeline(pipeline)
  .withCommandQueue(commandQueue)
  .withOffsetManagerProvider((kafkaConsumer) ->
    new FlushingOffsetManager(
      KafkaOffsetManager.builder(kafkaConsumer).withCommandQueue(commandQueue).build(),
      externalBuffer
    )
  )
  .build();
```

The delegate-and-wrap shape keeps the default offset bookkeeping intact while letting you inject behaviour at rebalance
time. Make sure to call through to the underlying listener in both `onPartitionsRevoked` and `onPartitionsAssigned` —
the default `KafkaOffsetManager` listener does real work there (e.g. flushing the safe-offsets buffer on revoke).

### Pre-shared registries across consumers

Use case: a single pipeline definition reused across consumer groups for A/B traffic split.

```java
final var registry = new MessageProcessorRegistry();
registry.registerOperator(RegistryKey.json("enrich"), enrichOp);
registry.registerSink(RegistryKey.json("warehouse"), warehouseSink);

final var pipeline = registry.pipeline(JsonFormat.INSTANCE)
    .add(RegistryKey.<Map<String, Object>>json("enrich"))
    .toSink(RegistryKey.<Map<String, Object>>json("warehouse"))
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

Both consumers share the same pipeline instance — operators and sinks are thread-safe.

## Dropping back to the explicit API mid-stream

You don't have to commit to one surface. The fluent facade's `Handle` exposes the underlying `KPipeConsumer` via
`metrics()` and lifecycle methods; for richer access, build with the explicit API directly. The two surfaces are
interchangeable: anything you can write with the facade can be written with the builder, and the facade itself is just a
thin layer over the builder.

If you find yourself reaching for an escape hatch that isn't in the table above, open an issue — either it's missing
from this doc, or it's a real gap in the API.
