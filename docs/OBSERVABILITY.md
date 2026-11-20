# Observability

Metrics, tracing, and the local dashboard stack. Snippets use placeholder values.

## Programmatic access

`Handle.metrics()` and `KPipeConsumer.metrics()` return a `MetricsSnapshot`, a typed record of the consumer's counters:

```java
final var m = handle.metrics();
m.records().processed();            // across every topic
m.records("orders").failed();       // one topic
m.records().averageProcessingTime();
m.inFlight();
m.backpressure().time();            // counts a pause still in progress
m.circuitBreaker().trips();
m.commits().failed();               // offset commits that failed or timed out
```

`records()` holds received, processed, filtered, failed, retries, dlqSent, dlqFailed and processingTime. Filtered
records (dropped by an operator, or for a topic with no pipeline) are counted apart from processed ones, so once
in-flight records finish, `received = processed + filtered + failed`, except for records a shutdown abandoned for
redelivery. Each snapshot reads the outcome counts before `received`, so it never shows more outcomes than records
received. Batch routes do not measure processing time; read `averageProcessingTime()` per topic when some topics are
batch routes.

`commits()` counts the offset commits the default `KafkaOffsetManager` makes, periodic, on revoke and at close, to Kafka
or to an `OffsetStore`: `succeeded()` and `failed()`. A failed periodic commit leaves the commit point where it was, so
the next one retries from there; a failed commit on revoke or at close is not retried. Without an `OffsetStore`, a lost
partition counts as a failure, because Kafka rejects that commit. A custom `OffsetManager` commits on its own, and its
commits are not counted.

`m.lag()` and `m.lag("orders")` give the offsets between the consumer's position and the end of the log (the high
watermark, or the last stable offset under `read_committed`), summed over its assigned partitions. The poll loop reads
them at most once a second. A fetching partition's lag comes from the client's fetch state; the Kafka client may ask the
broker for a partition's end offset when it does not know it yet, and logs that request at INFO. A paused partition is
not fetched, so its lag is read from the broker's end offsets instead, with a 100 ms timeout; a partition whose lag
cannot be read keeps its last value.

## Logging metrics

Without a monitoring backend, `withMetricsLog(Duration)` (on `Stream`, `MultiBuilder` and `KPipeConsumerBuilder`) writes
one line per interval to the `io.github.eschizoid.kpipe.metrics.log` logger at INFO, plus a final line when the consumer
closes, after its last records are counted. The keys follow the OpenTelemetry instrument names without the
`kpipe.consumer.` and `records.` prefixes; times are in milliseconds, `processing.avg` is the average processing time,
and `circuit_breaker.state` is the current state:

```text
kpipe metrics [orders group=billing] received=120 processed=117 filtered=1 failed=2 retries=3 dlq.sent=2 dlq.failed=0 in_flight=0 lag=14 processing.avg=4.2ms backpressure.pauses=0 backpressure.time=0.0ms circuit_breaker.state=closed circuit_breaker.trips=0 circuit_breaker.open_time=0.0ms offsets.commits=4 offsets.commit_failures=0
```

## OpenTelemetry metrics

Add `kpipe-metrics-otel` plus your own OTel SDK and exporter, then pass `OtelMetrics.of(openTelemetry)` to
`withMetrics(...)`. Create one `OtelMetrics` per `OpenTelemetry` and share it across every consumer: its callback stays
registered with the SDK for good. Without `withMetrics(...)`, the consumer still counts everything and
`Handle.metrics()` still works; nothing is exported and the OTel API is not needed.

```java
final var metrics = OtelMetrics.of(GlobalOpenTelemetry.get());   // one, shared by every consumer

try (var handle = KPipe.json("orders", kafkaProps)
    .pipe(enrich)
    .withMetrics(metrics)
    .toCustom(orderSink)
    .start()) {
  handle.awaitShutdown();
}
```

Every instrument except the processing-time histogram is observable. One batch callback per `OtelMetrics` reads every
running consumer's `MetricsSnapshot` at each collection and adds the final counts of consumers that have closed, so each
running consumer reports the numbers its `Handle.metrics()` returns; the state counter reads `Handle.health()`.
Processing time is a histogram recorded once per processed record on per-record routes.

| Instrument                                 | Type                     | Unit         | Description                                                        |
| ------------------------------------------ | ------------------------ | ------------ | ------------------------------------------------------------------ |
| `kpipe.consumer.records.received`          | Counter, per topic       | `{record}`   | Records the consumer started processing                            |
| `kpipe.consumer.records.processed`         | Counter, per topic       | `{record}`   | Records that passed every operator to the sink                     |
| `kpipe.consumer.records.filtered`          | Counter, per topic       | `{record}`   | Records dropped on purpose: filtered, or no pipeline for the topic |
| `kpipe.consumer.records.failed`            | Counter, per topic       | `{record}`   | Records that failed after retries, or at once                      |
| `kpipe.consumer.records.retries`           | Counter, per topic       | `{retry}`    | Retry attempts, not counting first attempts                        |
| `kpipe.consumer.dlq.sent`                  | Counter, per topic       | `{record}`   | Failed records written to the dead-letter topic                    |
| `kpipe.consumer.dlq.failed`                | Counter, per topic       | `{record}`   | Failed records whose dead-letter write failed                      |
| `kpipe.consumer.records.in_flight`         | UpDownCounter            | `{record}`   | Records the consumer holds: processing, queued, or buffered        |
| `kpipe.consumer.lag`                       | UpDownCounter, per topic | `{record}`   | Records between the consumer's position and the end of the log     |
| `kpipe.consumer.processing.duration`       | Histogram, per topic     | `s`          | Time from receiving a record to its sink, retries included         |
| `kpipe.consumer.backpressure.pauses`       | Counter                  | `{pause}`    | Times backpressure paused consumption                              |
| `kpipe.consumer.backpressure.time`         | Counter                  | `s`          | Time paused by backpressure                                        |
| `kpipe.consumer.circuit_breaker.trips`     | Counter                  | `{trip}`     | Times the circuit breaker opened                                   |
| `kpipe.consumer.circuit_breaker.open_time` | Counter                  | `s`          | Time the circuit breaker spent open                                |
| `kpipe.consumer.circuit_breaker.state`     | UpDownCounter, per state | `{consumer}` | Consumers whose circuit breaker is in each state                   |
| `kpipe.consumer.offsets.commits`           | Counter                  | `{commit}`   | Offset commits the default offset manager made                     |
| `kpipe.consumer.offsets.commit_failures`   | Counter                  | `{commit}`   | Offset commits that failed or did not finish in time               |

Every consumer instrument carries `messaging.consumer.group.name` (the consumer's `group.id`) when one is set. Per-topic
instruments also carry `messaging.destination.name`, and the state counter carries `kpipe.circuit_breaker.state`
(`closed`, `open` or `half_open`). Batch routes do not record processing time. Consumers that share a `group.id` and
topic add up into the same series.

When a consumer closes, its final counts, including records flushed during shutdown, stay in its counters for as long as
the `OtelMetrics` lives, so a counter never goes down when one of the consumers adding into it closes. The closed
consumer drops out of `in_flight`, the lag and the state counter. Close consumers before shutting down the OTel SDK, so
the SDK's final collection includes their final counts. Retired totals are kept per group and topic, so their size grows
only with the number of distinct groups and topics ever seen.

A rising `filtered` or `failed` count while the sink stays flat is the usual sign of a pipeline silently dropping
records, and it shows up here without any extra wiring.

Using Confluent SR auto-lookup? Export the resolver's cache as `kpipe.schema_registry.cache.hits`, `.misses` and
`.size`, with no attributes:

```java
final var cacheMetrics = OtelMetrics.exportSchemaRegistryCache(
  otel,
  resolver::hitCount,
  resolver::missCount,
  () -> (long) resolver.size()
);
// cacheMetrics.close() stops exporting it
```

Call it once per cache: each call reports the cache, so two calls over one cache double its counts. It takes
`LongSupplier`s rather than the resolver, so `kpipe-metrics-otel` does not depend on `kpipe-schema-registry-confluent`.

### Sends to Kafka

KPipe has no separate producer metrics. A dead-letter write counts as `kpipe.consumer.dlq.sent` or
`kpipe.consumer.dlq.failed`. A `KafkaMessageSink` send waits for the broker's ack, so it counts as `processed` once the
broker has the record, or as `failed` when the send fails (after retries, if configured).

## Distributed tracing (W3C trace context)

`withTracer(Tracer)` turns on tracing. The `Tracer` SPI is in `kpipe-tracing`, which `kpipe-api` brings in, so it needs
nothing in your build. `OtelTracer`, the OpenTelemetry implementation, is in the opt-in `kpipe-tracing-otel` module.
Without a tracer the consumer uses `Tracer.noop()`, which records nothing and needs no OpenTelemetry API.

```java
final var openTelemetry = AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();

KPipe.json("events", kafkaProps)
  .withTracer(new OtelTracer(openTelemetry))
  .pipe(enrich)
  .toKafka("events-enriched")
  .start();
```

`OtelTracer` takes only the `OpenTelemetry` instance. Its spans report under the instrumentation scope
`OtelTracer.INSTRUMENTATION_SCOPE`, which is `io.github.eschizoid.kpipe`. [`examples/tracing`](../examples/tracing) runs
this setup against Kafka.

### One span per record

The consumer starts one span for each record it consumes. When the record's headers carry a trace context, the span is
its child and joins the producer's trace. When they carry none, the span starts a new trace. `OtelTracer` names the span
`process <topic>`, gives it the kind `CONSUMER` and sets these attributes:

| Attribute                            | Value                                                          |
| ------------------------------------ | -------------------------------------------------------------- |
| `messaging.system`                   | `kafka`                                                        |
| `messaging.operation.type`           | `process`                                                      |
| `messaging.operation.name`           | `process`                                                      |
| `messaging.destination.name`         | The record's topic                                             |
| `messaging.destination.partition.id` | The record's partition, as a string                            |
| `messaging.kafka.offset`             | The record's offset                                            |
| `messaging.consumer.group.name`      | The consumer's `group.id`. Set only when the consumer has one. |

One span covers every attempt at the record. Each failed attempt adds a `kpipe.attempt.failed` event with the attributes
`kpipe.attempt` (the attempt number, from 0), `exception.type` and, when the exception has a message,
`exception.message`. A record that succeeds on a retry keeps those events and has no error status. When the record fails
for good, the span records the exception, sets `error.type` to the exception's class name and gets the status `ERROR`.

The span ends when the consumer is done with the record: its sink returned, it was filtered, or its failure was handled,
including the dead-letter write. On a batch route the span stays open while the record waits in the buffer, so its
duration includes the wait. The flush that holds the record ends the span. When that flush fails, the failure is
recorded on the span before the dead-letter write and the error handler run.

### Trace context on written records

Every record KPipe writes on behalf of a consumed record carries that record's span in its headers, so a consumer of the
written record continues the same trace:

- `toKafka(...)` and `KafkaMessageSink` write the span of the record being processed. They read it from
  `TraceContext.current()`, which the consumer binds around each record's processing, so the sink takes no tracer. The
  new `traceparent` replaces the one copied from the input record.
- A dead-letter record carries the failed record's span, whichever producer sends it. That includes a producer passed to
  `withKafkaProducer(...)` or `withDeadLetterQueue(...)` on `KPipeConsumerBuilder`.
- On a batch route, the dead-letter record written after a failed flush carries its own record's trace, even though the
  flush runs on a different thread from the one that processed the record.

`TraceContext.current()` returns `Tracer.SpanScope.noop()` when no record is being processed. Code of your own can call
`TraceContext.current().inject(headers)` to write the record's trace into a record it sends.

### The propagator

`OtelTracer` reads and writes headers through the text-map propagator of the `OpenTelemetry` instance, so that instance
needs one, such as `W3CTraceContextPropagator`. The SDK autoconfigure module sets up W3C trace context and baggage
unless `OTEL_PROPAGATORS` says otherwise. With no propagator, spans still start and end, but no context is read from or
written to the headers, and the `OtelTracer` constructor logs a warning. `GlobalOpenTelemetry.get()` has no propagator
until an SDK is registered as the global instance.

### Spans inside your code

While the consumer processes a record, the record's span is the current OpenTelemetry span on the record's thread.
OpenTelemetry instrumentation inside an operator or sink, such as an instrumented HTTP or JDBC client or a span you
start yourself, nests under the record's span. Work you hand to another thread sees the span only if you pass the
OpenTelemetry context along. A batch flush covers many records, so instrumentation inside a batch sink does not nest
under each record's span.

### A tracer that throws

The consumer guards every call to the tracer and to its spans. When a call throws anything other than a
`VirtualMachineError`, the consumer logs a warning, loses that span and still processes the record.

To trace with another system, implement `Tracer`. It is a functional interface with one method,
`startConsumerSpan(ConsumerRecord<?, byte[]> record, String consumerGroup)`, which returns a `Tracer.SpanScope`. The
consumer calls `activate()` on the span for the record's thread, `inject(headers)` for each written record,
`attemptFailed(attempt, failure)` for each failed attempt, `recordException(failure)` for a final failure and `close()`
when the record is done. `close()` may run on a different thread from the one that started the span, and must be
idempotent.

## Health endpoint

`HttpHealthServer` serves `Handle.health()` over HTTP for Kubernetes or any orchestrator that polls an endpoint:

```java
try (final var handle = KPipe.json("orders", props).toConsole().start();
     final var server = HttpHealthServer.fromEnv(handle::health).orElse(null)) {
  handle.awaitShutdown();
}
```

Each `GET` reads one snapshot and answers `200` when it is healthy, `503` otherwise:

```json
{ "healthy": true, "running": true, "pauseSources": ["backpressure"], "circuitBreaker": "closed" }
```

Enum values are lowercase, as in the OpenTelemetry attributes and the metrics log line. A snapshot supplier that throws
or returns null answers `503` with `{"healthy": false}`. `HEAD` answers like `GET` without a body; any other method
answers `405` with `Allow: GET, HEAD`. The `HEALTH_HTTP_ENABLED` (default `true`), `HEALTH_HTTP_HOST` (`0.0.0.0`),
`HEALTH_HTTP_PORT` (`8080`) and `HEALTH_HTTP_PATH` (`/health`) environment variables configure it.

## Local dashboard

A local observability stack under [`infra/observability/`](../infra/observability) runs via Docker Compose: OTel
Collector → Prometheus → Grafana, with a pre-provisioned "KPipe Overview" dashboard. The demo
([`examples/demo`](../examples/demo)) wires a multi-format consumer into it end-to-end; `./scripts/run-demo.sh` brings
the whole thing up with seeded data.
