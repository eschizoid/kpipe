# Tracing Example

Reads JSON records from a topic, tags each one and writes it to another topic, with one OpenTelemetry span per record.

## What it does

`App` builds one stream:

```java
KPipe.json(config.topic(), props)
  .withTracer(new OtelTracer(openTelemetry))
  .withDeadLetterTopic(dlqTopic)
  .peek(msg -> LOGGER.log(Level.INFO, "received {0}", msg))
  .pipe(msg -> {
    msg.put("traced", true);
    return msg;
  })
  .toKafka(outputTopic)
  .start()
```

- Each record gets a `CONSUMER` span named `process <topic>`. When the record has a `traceparent` header, the span is a
  child of that trace. When it has none, the span starts a new trace.
- The app logs the record, sets `traced` to `true` and writes it to the output topic. The output record keeps the input
  record's key and headers, and its `traceparent` names the record's span, so a consumer of the output topic continues
  the same trace.
- A record that fails, such as one whose value is not a JSON object, goes to the dead-letter topic. The dead-letter
  record carries the failed record's trace, and the span records the failure.

OpenTelemetry is configured from the standard `OTEL_*` environment variables through the SDK autoconfigure module, with
`AutoConfiguredOpenTelemetrySdk.initialize()`. Autoconfigure uses W3C trace-context propagation unless
`OTEL_PROPAGATORS` says otherwise.
[docs/OBSERVABILITY.md](../../docs/OBSERVABILITY.md#distributed-tracing-w3c-trace-context) lists the span's attributes
and events.

## Configuration

| Variable                      | Default                 | Description                                |
| ----------------------------- | ----------------------- | ------------------------------------------ |
| `KAFKA_BOOTSTRAP_SERVERS`     | `kafka:9092`            | Kafka bootstrap servers                    |
| `KAFKA_CONSUMER_GROUP`        | `kpipe-group`           | Consumer group ID                          |
| `KAFKA_TOPIC`                 | `json-topic`            | Topic to read                              |
| `OUTPUT_TOPIC`                | `<KAFKA_TOPIC>-traced`  | Topic to write the tagged records to       |
| `DLQ_TOPIC`                   | `<KAFKA_TOPIC>-dlq`     | Dead-letter topic for records that fail    |
| `OTEL_SERVICE_NAME`           | `unknown_service:java`  | Service name on the spans                  |
| `OTEL_TRACES_EXPORTER`        | `otlp`                  | Where spans go; `none` turns export off    |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP endpoint, gRPC by default             |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc`                  | `grpc`, or `http/protobuf` for port `4318` |
| `OTEL_PROPAGATORS`            | `tracecontext,baggage`  | Header formats for trace context           |

The defaults of the `OTEL_*` variables come from the OpenTelemetry SDK autoconfigure module. It also exports metrics and
logs over OTLP unless `OTEL_METRICS_EXPORTER` and `OTEL_LOGS_EXPORTER` are `none`. The app sends neither, so set both to
`none` when nothing listens on the OTLP endpoint.

## Running with Docker Compose

From the repository root, build the jar and start the stack with this app:

```bash
./gradlew :examples:tracing:shadowJar
APP=tracing docker compose up --build -d
```

The root `docker-compose.yaml` starts Kafka, Schema Registry, the observability stack from `infra/observability` and the
app. It sets `KAFKA_TOPIC=tracing-topic`, so the app writes to `tracing-topic-traced` and dead-letters to
`tracing-topic-dlq`. The broker creates topics when they are first used.

Send a record with a known `traceparent`:

```bash
docker compose exec kafka sh -c \
  "printf 'traceparent:00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\t{\"id\":1,\"event\":\"hello\"}\n' | \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic tracing-topic \
  --reader-property parse.headers=true"
```

Read the output record with its headers:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic tracing-topic-traced --from-beginning --formatter-property print.headers=true
```

Its `traceparent` has the same trace ID, `0af7651916cd43dd8448eb211c80319c`, and a new span ID: the ID of the record's
span. Run the same producer command with `not-json` in place of the JSON value, and the dead-letter record on
`tracing-topic-dlq` carries that record's trace in the same way.

### Seeing the spans

`docker-compose.yaml` sets `OTEL_TRACES_EXPORTER=none` for every app, so no spans leave the app by default. To see them:

1. In the `app` service of the root `docker-compose.yaml`, change `OTEL_TRACES_EXPORTER=none` to
   `OTEL_TRACES_EXPORTER=otlp`. `OTEL_EXPORTER_OTLP_ENDPOINT` already points at `http://otel-collector:4317`, the OTLP
   gRPC receiver of the collector in `infra/observability`.
2. The collector in `infra/observability/otel-collector-config.yaml` already has a traces pipeline that sends spans to
   its `debug` exporter. Set that exporter's `verbosity` to `detailed` to see each span's attributes in the collector
   log; the setting applies to metrics too, so the log gets louder.
3. Recreate the two services and follow the collector's log:

   ```bash
   APP=tracing docker compose up -d --force-recreate otel-collector app
   docker compose logs -f otel-collector
   ```

Each record you send then shows up as a `process tracing-topic` span whose parent is the span ID from its `traceparent`.

## Running without Docker Compose

The compose broker advertises itself as `kafka:9092`, which only resolves inside the compose network. To run the app on
your machine, point it at a Kafka broker it can reach:

```bash
./gradlew :examples:tracing:shadowJar
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_TOPIC=orders \
  OTEL_SERVICE_NAME=kpipe-tracing OTEL_TRACES_EXPORTER=otlp OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  OTEL_METRICS_EXPORTER=none OTEL_LOGS_EXPORTER=none \
  java -jar examples/tracing/build/libs/kpipe-tracing-*.jar
```

## Running the tests

The tests need Docker. Testcontainers starts a Kafka broker, and each test class is skipped when Docker is not
available. They export spans to an in-memory exporter instead of a collector.

```bash
./gradlew :examples:tracing:test
```

- `AppIntegrationTest` sends a record with a known `traceparent` and checks that the record's span is a child of it.
- `DlqTracePropagationIntegrationTest` sends a record through a pipeline that always fails. It checks that the
  dead-letter record's `traceparent` has the inbound trace ID and the ID of the record's span, and that the span is a
  child of the inbound span.
