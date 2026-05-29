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

Some advanced use cases need the explicit `MessageProcessorRegistry` + `KPipeConsumer.Builder` surface. This page is the
index — for each capability, it tells you whether it's on the fluent path, on the explicit path, or both, and how to
wire it up.

Rows in **bold** are deliberately escape-hatch-only (no fluent equivalent planned).

## Capability map

| Capability                                          | Facade path                                                              | Explicit-API path                                                                                    | Notes                                                                                                                                                                            |
| --------------------------------------------------- | ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Format selection                                    | `KPipe.json/avro/protobuf/bytes/custom(...)`                             | `new MessageProcessorRegistry(format)`                                                               | Custom format: pass any `MessageFormat<T>` to `KPipe.custom`.                                                                                                                    |
| Topic subscription (homogeneous)                    | `KPipe.X(topic, props)`                                                  | `Builder.withTopic(s) / withTopics(...)`                                                             | One topic-set, one shared pipeline.                                                                                                                                              |
| Topic subscription (heterogeneous)                  | `KPipe.multi(props).json(...).avro(...)...start()`                       | `Builder.withPipelines(Map<String, MessagePipeline<?>>)`                                             | Per-topic dispatch.                                                                                                                                                              |
| Operator chain (`pipe/filter/peek/when`)            | `Stream.pipe / filter / peek / when`                                     | `MessageProcessorRegistry.pipeline(format).add(...)`                                                 | Identical operator semantics.                                                                                                                                                    |
| Result observers (`onFiltered/onFailed/peekResult`) | `Stream.onFiltered / onFailed / peekResult`                              | — (the fluent facade is the entry point; explicit API users pattern-match `Result<T>` directly)      | Observers fire after the pipeline returns `Result.Passed/Filtered/Failed`. Side-effect only; they do not suppress or reroute.                                                    |
| Custom terminal sink                                | `Stream.toCustom(MessageSink<T>)`                                        | `registry.registerSink(key, sink)`                                                                   | Any `MessageSink<T>`; facade also offers `.toConsole()`.                                                                                                                         |
| Multi-sink fanout                                   | `Stream.toMulti(sinks...)`                                               | `new CompositeMessageSink<>(...)`                                                                    | Best-effort delivery; per-sink errors logged + suppressed.                                                                                                                       |
| Batch sink (size + age flush)                       | `Stream.toBatch(BatchSink<T>, BatchPolicy)`                              | `Builder.withBatchPipeline(topic, pipeline, sink, policy)`                                           | `BatchSink<T>` returns `BatchResult` for per-record DLQ; `BatchSink.ofVoid(consumer)` wraps void-style sinks (whole-batch DLQ on throw). Sequential + parallel modes both work.  |
| Batch sink (multi-topic)                            | `KPipe.multi(props).json(topic, s -> s.toBatch(sink, policy))...start()` | `Builder.withBatchPipeline(...)` × N                                                                 | One consumer-group, mixed batch + non-batch routes.                                                                                                                              |
| Skip wire-format prefix                             | `Stream.skipBytes(int)`                                                  | `TypedPipelineBuilder.skipBytes(int)`                                                                | Confluent envelope: 5 (Avro) / 6 (Proto single-msg). Don't combine with `withSchemaRegistry(...)`.                                                                               |
| Confluent SR per-record auto-lookup (Avro)          | `Stream.withSchemaRegistry(SchemaResolver)`                              | `AvroFormat.withRegistry(SchemaResolver)`                                                            | Cached by ID, no TTL (SR IDs are immutable). Avro only — Protobuf needs runtime `.proto` compilation. Replaces the static `lookupBySubjectVersion("latest")` startup-fetch path. |
| Pipeline outcome counters (OTel)                    | `Stream.peekResult(PipelineMetricsObserver)`                             | hand any `Consumer<Result<T>>` to `peekResult`                                                       | `PipelineMetricsObserver` in `kpipe-metrics-otel` emits `kpipe.pipeline.passed/filtered/failed`. Optional `bindSchemaRegistryCache(...)` adds the SR cache counters.             |
| Retry policy                                        | `Stream.withRetry(int, Duration)`                                        | `Builder.withRetry(int, Duration)`                                                                   |                                                                                                                                                                                  |
| Backpressure (default watermarks)                   | `Stream.withBackpressure()`                                              | `Builder.withBackpressure(BackpressureController)`                                                   | Defaults: pause 10k, resume 7k.                                                                                                                                                  |
| Backpressure (custom watermarks)                    | `Stream.withBackpressure(high, low)`                                     | `Builder.withBackpressure(BackpressureController)`                                                   | High/low strategy auto-derived from `ProcessingMode`.                                                                                                                            |
| Processing mode                                     | `Stream.withProcessingMode(ProcessingMode)`                              | `Builder.withProcessingMode(ProcessingMode)`                                                         | `SEQUENTIAL` (per-partition serial, lag-based backpressure), `PARALLEL` (default, virtual-thread-per-record), `KEY_ORDERED` (per-key serial, see next row).                      |
| Key-ordered LRU cap                                 | `Stream.withKeyOrderedMaxKeys(int)`                                      | `Builder.withKeyOrderedMaxKeys(int)`                                                                 | Default 10,000 distinct keys held in memory. Only meaningful for `KEY_ORDERED`. Records with `null` keys share a single sentinel queue.                                          |
| OTel/custom metrics                                 | `Stream.withMetrics(ConsumerMetrics)` / `MultiBuilder.withMetrics(...)`  | `Builder.withMetrics(ConsumerMetrics)`                                                               | Single-format and multi-topic both supported.                                                                                                                                    |
| Custom error handler                                | `Stream.withErrorHandler(Consumer<ProcessingError<byte[]>>)`             | `Builder.withErrorHandler(ErrorHandler<K>)`                                                          | Default logs at WARNING.                                                                                                                                                         |
| Dead-letter topic                                   | `Stream.withDeadLetterTopic(String)`                                     | `Builder.withDeadLetterTopic(String)` / `withDeadLetterBundle(...)`                                  | Bundle form pairs DLQ + producer for advanced wiring.                                                                                                                            |
| Poll timeout                                        | `Stream.withPollTimeout(Duration)`                                       | `Builder.withPollTimeout(Duration)`                                                                  | Default 100ms.                                                                                                                                                                   |
| Lifecycle handle                                    | `Handle.isHealthy / metrics / awaitShutdown / close`                     | `KPipeConsumer.start / awaitShutdown / shutdownGracefully / waitForInFlightDrain`                    | `Handle` is `AutoCloseable` (5s graceful default). Since 1.13: consumer hosts the lifecycle directly — no separate `KPipeRunner`.                                                |
| **Custom `OffsetManager`**                          | — (escape hatch only)                                                    | `Builder.withOffsetManager(...)` / `withOffsetManager(Provider)`                                     | E.g. Postgres/Redis-backed manager.                                                                                                                                              |
| **Custom Kafka `Consumer` factory**                 | —                                                                        | `Builder.withConsumerProvider(Supplier<Consumer<K,byte[]>>)`                                         | Test seam / SSL configurators.                                                                                                                                                   |
| **Custom DLQ `KafkaProducer`**                      | —                                                                        | `Builder.withKafkaProducer(...)` / `withDeadLetterBundle(...)`                                       | Override default producer for the DLQ.                                                                                                                                           |
| **Rebalance listener**                              | —                                                                        | `Builder.withRebalanceListener(ConsumerRebalanceListener)`                                           | External offset commit hooks, log routing.                                                                                                                                       |
| **Custom command queue**                            | —                                                                        | `Builder.withCommandQueue(Queue<ConsumerCommand>)`                                                   | Test seam (rarely needed).                                                                                                                                                       |
| **Thread/executor termination tuning**              | —                                                                        | `Builder.withThreadTerminationTimeout / withExecutorTerminationTimeout / withWaitForMessagesTimeout` | Shutdown tuning.                                                                                                                                                                 |
| **Periodic metrics reporting + shutdown hook**      | —                                                                        | `Builder.withMetricsReporters(...) / withMetricsInterval(...) / withShutdownHook(true)`              | Folded into `KPipeConsumer.Builder` in 1.13. Reporter thread is daemon, doesn't keep the JVM up.                                                                                 |
| **Health endpoint**                                 | —                                                                        | `HttpHealthServer.fromEnv(...)`                                                                      | Run alongside `Handle` in the host process.                                                                                                                                      |
| **In-flight drain**                                 | `Handle.shutdownGracefully(Duration)`                                    | `KPipeConsumer.waitForInFlightDrain(Duration) / shutdownGracefully(Duration)`                        | Uses the live in-flight counter (includes buffered batch records), not metric-derived.                                                                                           |
| **Custom `MessageProcessorRegistry`**               | —                                                                        | `new MessageProcessorRegistry(...)` + manual wiring                                                  | Pre-shared pipelines across consumers, multi-format orchestrators.                                                                                                               |

## Worked examples

### A custom OffsetManager

Use case: persist offsets in Postgres alongside business writes, so commit is atomic with the record's downstream
side-effect.

```java
final var offsetManager = new PostgresOffsetManager(dataSource);

final var consumer = KPipeConsumer.<byte[]>builder()
  .withProperties(kafkaProps)
  .withTopic("orders")
  .withPipeline(pipeline)
  .withOffsetManager(offsetManager)
  .build();
```

`OffsetManager` is a thin SPI — implement `markOffsetProcessed`, `getOffsetsToCommit`, and `close`. The consumer threads
call into it concurrently; implementations must be thread-safe.

### A custom rebalance listener

Use case: flush an external buffer before partitions are revoked.

```java
final var consumer = KPipeConsumer.<byte[]>builder()
  .withProperties(kafkaProps)
  .withTopic("orders")
  .withPipeline(pipeline)
  .withRebalanceListener(
    new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        externalBuffer.flush(partitions);
      }

      @Override
      public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        // no-op
      }
    }
  )
  .build();
```

### Pre-shared registries across consumers

Use case: a single pipeline definition reused across consumer groups for A/B traffic split.

```java
final var registry = new MessageProcessorRegistry(JsonFormat.INSTANCE);
registry.registerOperator(RegistryKey.json("enrich"), enrichOp);
registry.registerSink(RegistryKey.json("warehouse"), warehouseSink);

final var pipeline = registry.pipeline(JsonFormat.INSTANCE)
    .add(RegistryKey.<Map<String, Object>>json("enrich"))
    .toSink(RegistryKey.<Map<String, Object>>json("warehouse"))
    .build();

final var groupA = KPipeConsumer.<byte[]>builder()
    .withProperties(propsForGroup("group-a"))
    .withTopic("orders")
    .withPipeline(pipeline)
    .build();

final var groupB = KPipeConsumer.<byte[]>builder()
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
