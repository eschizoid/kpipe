# Demo Example

A combined example that runs **JSON**, **Avro**, and **Protobuf** consumer pipelines concurrently in a single
application.

## What It Does

The demo application starts three independent KPipe pipelines, each consuming from its own Kafka topic and applying
format-specific processing:

| Pipeline | Topic         | Format   | Processors                                           |
| -------- | ------------- | -------- | ---------------------------------------------------- |
| JSON     | `json-topic`  | JSON     | addSource, markProcessed, addTimestamp, removeFields |
| Avro     | `avro-topic`  | Avro     | Schema Registry deserialization + console sink       |
| Protobuf | `proto-topic` | Protobuf | Descriptor-based deserialization + console sink      |

## Running

Start the full stack (Kafka, Schema Registry, observability, and the demo app):

```bash
cd examples/demo
docker compose up --build
```

This will:

1. Start Kafka and Schema Registry via the root `docker-compose.yaml`
2. Start the observability stack (OpenTelemetry Collector, etc.)
3. Register Avro and Protobuf schemas
4. Build and start the demo application

## Configuration

All configuration is via environment variables (see `docker-compose.yaml`):

| Variable                  | Default                       | Description                   |
| ------------------------- | ----------------------------- | ----------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092`                  | Kafka bootstrap servers       |
| `KAFKA_CONSUMER_GROUP`    | `kpipe-demo`                  | Consumer group ID prefix      |
| `SCHEMA_REGISTRY_URL`     | `http://schema-registry:8081` | Confluent Schema Registry URL |
| `JSON_TOPIC`              | `json-topic`                  | Topic for JSON messages       |
| `AVRO_TOPIC`              | `avro-topic`                  | Topic for Avro messages       |
| `PROTO_TOPIC`             | `proto-topic`                 | Topic for Protobuf messages   |
| `HEALTH_HTTP_PORT`        | `8080`                        | Health check HTTP port        |
