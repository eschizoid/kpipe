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
```

`records()` holds received, processed, filtered, failed, retries, dlqSent, dlqFailed and processingTime. Filtered
records (dropped by an operator, or for a topic with no pipeline) are counted apart from processed ones, so once
in-flight records finish, `received = processed + filtered + failed`, except for records a shutdown abandoned for
redelivery. Each snapshot reads the outcome counts before `received`, so it never shows more outcomes than records
received. Batch routes do not measure processing time; read `averageProcessingTime()` per topic when some topics are
batch routes.

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
kpipe metrics [orders group=billing] received=120 processed=117 filtered=1 failed=2 retries=3 dlq.sent=2 dlq.failed=0 in_flight=0 lag=14 processing.avg=4.2ms backpressure.pauses=0 backpressure.time=0.0ms circuit_breaker.state=closed circuit_breaker.trips=0 circuit_breaker.open_time=0.0ms
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

KPipe propagates `traceparent` / `tracestate` Kafka headers: the upstream context is extracted on poll, a CONSUMER span
with `messaging.kafka.{topic,partition,offset}` attributes wraps processing (closed in a nested `finally` so a throwing
user callback cannot leak the scope), and the current context is injected into outbound headers on produce and on DLQ
writes. The `Tracer` SPI lives in `kpipe-tracing` (pulled transitively — nothing to add to your build); the
implementation is the opt-in `kpipe-tracing-otel` module. Without `.withTracer(...)`, `Tracer.noop()` is used and no
OTel API is needed.

```java
final OpenTelemetry otel = /* GlobalOpenTelemetry.get() or your SDK */;

KPipe.json("events", kafkaProps)
    .withTracer(new OtelTracer(otel, "events-consumer"))
    .pipe(enrich)
    .toCustom(producerSink)
    .start();
```

## Local dashboard

A local observability stack under [`infra/observability/`](../infra/observability) runs via Docker Compose: OTel
Collector → Prometheus → Grafana, with a pre-provisioned "KPipe Overview" dashboard. The demo
([`examples/demo`](../examples/demo)) wires a multi-format consumer into it end-to-end; `./scripts/run-demo.sh` brings
the whole thing up with seeded data.
