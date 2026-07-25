# Observability

Metrics, pipeline-outcome counters, tracing, and the local dashboard stack. Snippets use placeholder values.

## Programmatic access

`Handle.metrics()` (fluent path) and `KPipeConsumer.getMetrics()` (explicit path) return an unmodifiable
`Map<String, Long>` snapshot of the consumer counters — received, processed, errors, in-flight, backpressure pauses,
and friends. For periodic logging without an OTel backend, the log-based reporters in `kpipe-metrics`
(`ConsumerMetricsReporter`, `EntryMetricsReporter`) can be attached via
`KPipeConsumerBuilder.withMetricsReporters(...)` — see [ESCAPE-HATCHES.md](ESCAPE-HATCHES.md).

## OpenTelemetry metrics

`kpipe-metrics` ships interfaces against `opentelemetry-api` only — no SDK. Add `kpipe-metrics-otel` plus your own
OTel SDK at runtime and wire `.withMetrics(new OtelConsumerMetrics(openTelemetry, "my-pipeline"))`. When
`withMetrics(...)` is omitted, `ConsumerMetrics.noop()` / `ProducerMetrics.noop()` are used: zero allocation, no OTel
API needed on the classpath.

| Instrument                                     | Type      | Description                                        |
| ---------------------------------------------- | --------- | -------------------------------------------------- |
| `kpipe.consumer.messages.received`             | Counter   | Records polled from Kafka                          |
| `kpipe.consumer.messages.processed`            | Counter   | Records successfully processed                     |
| `kpipe.consumer.messages.errors`               | Counter   | Records that failed processing                     |
| `kpipe.consumer.processing.duration`           | Histogram | Per-record processing time (ms)                    |
| `kpipe.consumer.messages.inflight`             | Gauge     | Current number of in-flight messages               |
| `kpipe.consumer.backpressure.pauses`           | Counter   | Times backpressure paused the consumer             |
| `kpipe.consumer.backpressure.time`             | Counter   | Total time paused due to backpressure              |
| `kpipe.consumer.circuit_breaker.trips`         | Counter   | Times the breaker tripped CLOSED → OPEN            |
| `kpipe.consumer.circuit_breaker.state_changes` | Counter   | Any CB state transition (tagged with target state) |
| `kpipe.consumer.circuit_breaker.time_open`     | Counter   | Total time the breaker spent in OPEN (ms)          |
| `kpipe.producer.messages.sent`                 | Counter   | Records successfully produced                      |
| `kpipe.producer.messages.failed`               | Counter   | Records that failed to produce                     |
| `kpipe.producer.dlq.sent`                      | Counter   | Records sent to DLQ                                |
| `kpipe.producer.dlq.failed`                    | Counter   | DLQ sends that failed (record stays uncommitted)   |

## Pipeline outcome counters

`PipelineMetricsObserver` (also in `kpipe-metrics-otel`) implements `Consumer<Result<?>>`. Hand it to
`Stream.peekResult(observer)` and every `Passed` / `Filtered` / `Failed` outcome increments the matching
`kpipe.pipeline.*` counter. This makes the classic silent-failure signature — "processed" rising while the sink stays
flat, i.e. a flood of `Filtered` or `Failed` records — visible in metrics instead of only in logs.

```java
final var otel = GlobalOpenTelemetry.get();

try (var handle = KPipe.json("orders", kafkaProps)
    .pipe(enrich)
    .withMetrics(new OtelConsumerMetrics(otel, "orders-consumer"))   // kpipe.consumer.* metrics
    .peekResult(new PipelineMetricsObserver(otel, "orders"))         // kpipe.pipeline.* outcome counters
    .toCustom(orderSink)
    .start()) {
  handle.awaitShutdown();
}
```

Using Confluent SR auto-lookup? Bind the resolver's cache counters so hit rate is visible alongside the outcomes:

```java
final var observer = new PipelineMetricsObserver(otel, "orders").bindSchemaRegistryCache(
  resolver::hitCount,
  resolver::missCount,
  () -> (long) resolver.size()
);
```

The observer takes `LongSupplier`s rather than the resolver itself, so `kpipe-metrics-otel` has no dependency on
`kpipe-schema-registry-confluent`.

## Distributed tracing (W3C trace context)

KPipe propagates `traceparent` / `tracestate` Kafka headers: the upstream context is extracted on poll, a CONSUMER
span with `messaging.kafka.{topic,partition,offset}` attributes wraps processing (closed in a nested `finally` so a
throwing user callback cannot leak the scope), and the current context is injected into outbound headers on produce
and on DLQ writes. The implementation is the opt-in `kpipe-tracing-otel` module; without `.withTracer(...)`,
`Tracer.noop()` is used and no OTel API is needed.

```java
final OpenTelemetry otel = /* GlobalOpenTelemetry.get() or your SDK */;

KPipe.json("events", kafkaProps)
    .withTracer(new OtelTracer(otel, "events-consumer"))
    .pipe(enrich)
    .toCustom(producerSink)
    .start();
```

## Local dashboard

A local observability stack under [`infra/observability/`](../infra/observability/) runs via Docker Compose: OTel
Collector → Prometheus → Grafana, with a pre-provisioned "KPipe Overview" dashboard. The demo
([`examples/demo`](../examples/demo/)) wires a multi-format consumer into it end-to-end; `./scripts/run-demo.sh`
brings the whole thing up with seeded data.
