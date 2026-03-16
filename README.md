# <img src="img/kpipe_1.png" width="200" height="200">

# KPipe

**KPipe is a lightweight Kafka processing library for modern Java that lets you build safe, high‑performance message
pipelines using virtual threads and a functional API.**

[![GitHub release](https://img.shields.io/github/release/eschizoid/kpipe.svg?style=flat-square)](https://github.com/eschizoid/kpipe/releases/latest)
[![Codecov](https://codecov.io/gh/eschizoid/kpipe/graph/badge.svg?token=X50GBU4X7J)](https://codecov.io/gh/eschizoid/kpipe)
[![Build Status](https://github.com/eschizoid/kpipe/actions/workflows/ci.yaml/badge.svg)](https://github.com/eschizoid/kpipe/actions/workflows/ci.yaml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

- **Modern Java concurrency** (virtual threads)
- **Composable functional pipelines**
- **Safe at-least-once processing**
- **High throughput with minimal framework overhead**

It is designed for Kafka consumer services performing transformations, enrichment, or routing.

---

# Quick Example

Create a pipeline and start a Kafka consumer in a few lines:

```java
final var registry = new MessageProcessorRegistry("demo");

registry.registerJsonOperator(
    "sanitize",
    JsonMessageProcessor.removeFields("password")
);

registry.registerJsonOperator(
    "stamp",
    JsonMessageProcessor.addTimestamp("processedAt")
);

final var pipeline = registry.jsonPipeline("sanitize", "stamp");

final var consumer = new KPipeConsumer.<byte[], byte[]>builder()
  .withProperties(kafkaProps)
  .withTopic("users")
  .withProcessor(pipeline)
  .withRetry(3, Duration.ofSeconds(1))
  .build();

  // Use the consumer
  consumer.start();
  ```

KPipe handles:

- record processing
- retries
- metrics
- offset tracking
- safe commits

---

# When to Use KPipe

KPipe works well for:

- Kafka consumer microservices
- event enrichment pipelines
- lightweight transformation services
- I/O-bound processing (REST calls, database lookups)
- teams adopting **modern Java concurrency**

KPipe is not intended to replace large streaming frameworks. It focuses on **simple, composable Kafka consumer pipelines.**

---

# Why Not Just Use Kafka Streams?

Kafka Streams is powerful but introduces a full topology framework and state management layer that many services do not need.

KPipe focuses on **code-first pipelines with minimal infrastructure overhead.**

| Capability                       | Kafka Streams | Reactor Kafka | KPipe |
|----------------------------------|---------------|---------------|-------|
| Full stream processing framework | Yes           | No            | No    |
| Lightweight consumer pipelines   | Partial       | Yes           | Yes   |
| Virtual-thread friendly          | No            | No            | Yes   |
| Functional pipeline API          | Yes           | Yes           | Yes   |
| Minimal dependencies             | No            | Yes           | Yes   |

KPipe sits between **raw KafkaConsumer code and full streaming frameworks.**

---

# Core Design

### Single SerDe Cycle

KPipe optimizes transformation pipelines by avoiding repeated serialization.

Messages are:

1. Deserialized once
2. Processed through a chain of operators
3. Serialized once before output

This significantly reduces CPU overhead and GC pressure.

---

### Virtual Thread Processing

KPipe uses **Java Virtual Threads (Project Loom)** for record processing.

This allows:

- massive concurrency
- simpler code
- efficient I/O-bound workloads

Processing can run:

- sequentially (strict ordering)
- in parallel (high throughput)

---

### Safe Offset Management

KPipe guarantees safe offset commits using a **lowest pending offset strategy**.

Even if messages finish out of order:

```
message 101 -> still processing
message 102 -> finished
```

offset **102 will NOT be committed** until **101 completes**.

This ensures:

- no gaps
- safe recovery
- at-least-once delivery

---
## Installation

### Maven

  ```xml
  <dependency>
      <groupId>io.github.eschizoid</groupId>
      <artifactId>kpipe</artifactId>
      <version>1.0.0</version>
  </dependency>
  ```

### Gradle (Groovy)

  ```groovy
  implementation 'io.github.eschizoid:kpipe:1.0.0'
  ```

### Gradle (Kotlin)

  ```kotlin
  implementation("io.github.eschizoid:kpipe:1.0.0")
  ```

### SBT

  ```sbt
  libraryDependencies += "io.github.eschizoid" % "kpipe" % "1.0.0"
  ```

---

## Architecture and Reliability

KPipe is designed to be a lightweight, high-performance alternative to existing Kafka consumer libraries, focusing on
modern Java 25+ features and predictable behavior.

### 1. The "Single SerDe Cycle" Strategy

Unlike traditional pipelines that often perform `byte[] -> Object -> byte[]` at every transformation step, KPipe
optimizes for throughput:

- **Single Deserialization**: Messages are deserialized **once** into a mutable representation (e.g., `Map` for JSON,
  `GenericRecord` for Avro).
- **In-Place Transformations**: A chain of `UnaryOperator` functions is applied to the same object.
- **Single Serialization**: The final object is serialized back to `byte[]` only once before being sent to the sink.

This approach significantly reduces CPU overhead and GC pressure.

### 2. Virtual Threads

By default, KPipe uses **Java Virtual Threads** (Project Loom) to process messages. This provides a "thread-per-record"
model that excels at I/O-bound tasks (e.g., enrichment via REST/DB).

- **I/O Bound**: Use the default parallel mode with virtual threads for maximum concurrency.
- **Sequential**: Use `.withSequentialProcessing(true)` for stateful operations where strict FIFO ordering per
  partition is required.

### 3. Reliable "At-Least-Once" Delivery

KPipe implements a **Lowest Pending Offset** strategy to ensure reliability even with parallel processing:

- **In-Flight Tracking**: Every record's offset is tracked in a `ConcurrentSkipListSet` per partition.
- **No-Gap Commits**: Even if message 102 finishes before 101, offset 102 will **not** be committed until 101 is
  successfully processed.
- **Crash Recovery**: If the consumer crashes, it will resume from the last committed "safe" offset. While this may
  result in some records being re-processed (standard "at-least-once" behavior), it guarantees no message is ever
  skipped.

### 4. Error Handling & Retries

KPipe provides a robust, multi-layered error handling mechanism:

- **Built-in Retries**: Configure `.withRetry(maxRetries, backoff)` to automatically retry transient failures.
- **Dead Letter Handling**: Provide a `.withErrorHandler()` to redirect messages that fail after all retries to an
  error topic or database.
- **Safe Pipelines**: Use `MessageProcessorRegistry.withErrorHandling()` to wrap individual processors with default
  values or logging, preventing a single malformed message from blocking the partition.

### 5. Graceful Shutdown & Interrupt Handling

KPipe respects JVM signals and ensures timely shutdown without data loss:

- **Interrupt Awareness**: Interrupts trigger a coordinated shutdown sequence. They do **not** cause records to be
  skipped.
- **Reliable Redelivery**: If a record's processing is interrupted (e.g., during retry backoff or transformation),
  the offset is NOT marked as processed. This ensures it will be safely picked up by the next consumer instance,
  guaranteeing "at-least-once" delivery even during shutdown.

---

## Example: Add Custom Processor

Extend the registry like this:

  ```java
  // Create a registry
  final var registry = new MessageProcessorRegistry("myApp");

  // Register a custom JSON operator for field transformations
  registry.registerJsonOperator("uppercase", map -> {
      final var value = map.get("text");
      if (value instanceof String text) {
          map.put("text", text.toUpperCase());
      }
      return map;
  });

  // Built-in operators are also available
  registry.registerJsonOperator("addEnvironment",
      JsonMessageProcessor.addField("environment", "production"));

  // Create a high-performance pipeline (single SerDe cycle)
  final var pipeline = registry.jsonPipeline(
      "addEnvironment", "uppercase", "addTimestamp"
  );

  // Use the pipeline with a consumer
  final var consumer = new KPipeConsumer.<byte[], byte[]>builder()
      .withProperties(kafkaProps)
      .withTopic("events")
      .withProcessor(pipeline)
      .withRetry(3, Duration.ofSeconds(1))
      .build();

  // Start processing messages
  consumer.start();
  ```

---

## Built-in Metrics

Monitor your consumer with built-in metrics:

  ```java
  // Access consumer metrics
  final var metrics = consumer.getMetrics();
  final var log = System.getLogger("org.kpipe.metrics");
  log.log(Level.INFO, "Messages received: " + metrics.get("messagesReceived"));
  log.log(Level.INFO, "Successfully processed: " + metrics.get("messagesProcessed"));
  log.log(Level.INFO, "Processing errors: " + metrics.get("processingErrors"));
  ```

Configure automatic metrics reporting:

  ```java
  final var runner = ConsumerRunner.builder(consumer)
    .withMetricsReporters(List.of(new ConsumerMetricsReporter(consumer::getMetrics, () -> uptimeMs, null)))
    .withMetricsInterval(30_000)
    .build();

  runner.start();
  ```

---

## Graceful Shutdown
The consumer supports graceful shutdown with in-flight message handling:

  ```java
  final var log = System.getLogger("org.kpipe.app.Shutdown");

  // Initiate graceful shutdown with 5-second timeout
  boolean allProcessed = runner.shutdownGracefully(5000);
  if (allProcessed) log.log(Level.INFO, "All messages processed successfully before shutdown");
  else log.log(Level.WARNING, "Shutdown completed with some messages still in flight");

  // Register as JVM shutdown hook
  Runtime.getRuntime().addShutdownHook(
    new Thread(() -> runner.close())
  );
  ```

---

## Working with Messages

### JSON Processing

The JSON processors provide operators (`UnaryOperator<Map<String, Object>>`) that can be composed into high-performance
pipelines:

  ```java
  final var registry = new MessageProcessorRegistry("myApp");

  // Operators are pure functions that modify a Map
  registry.registerJsonOperator("addTimestamp", JsonMessageProcessor.addTimestamp("processedAt"));
  registry.registerJsonOperator("sanitize", JsonMessageProcessor.removeFields("password", "ssn"));

  // Metadata merging
  final var metadata = Map.of("version", "1.0", "env", "prod");
  registry.registerJsonOperator("addMetadata", JsonMessageProcessor.mergeWith(metadata));

  // Build an optimized pipeline (one deserialization -> many transformations -> one serialization)
  final var pipeline = registry.jsonPipeline("sanitize", "addTimestamp", "addMetadata");
  ```

### Avro Processing

The Avro processors provide operators (`UnaryOperator<GenericRecord>`) that work within optimized pipelines:

  ```java
  final var registry = new MessageProcessorRegistry("myApp");

  // Add schema (automatically registers addSource_user and addTimestamp_user)
  registry.addSchema("user", "com.kpipe.User", "schemas/user.avsc");

  // Register manual operators
  registry.registerAvroOperator("sanitize", 
      AvroMessageProcessor.removeFields("user", "password", "creditCard"));

  // Transform fields
  registry.registerAvroOperator("uppercaseName", 
      AvroMessageProcessor.transformField("user", "name", value -> {
          if (value instanceof String text) return text.toUpperCase();
          return value;
      }));

  // Build an optimized pipeline
  // This pipeline handles deserialization, all operators, and serialization in one pass
  final var pipeline = registry.avroPipeline("user", "sanitize", "uppercaseName", "addTimestamp_user");
  
  // For data with magic bytes (e.g., Confluent Wire Format), specify an offset:
  final var confluentPipeline = registry.avroPipeline("user", 5, "sanitize", "addTimestamp_user");
  ```

---

## Message Sinks

Message sinks provide destinations for processed messages. The `MessageSink` interface is a functional interface that
defines a single method:

```java
@FunctionalInterface
public interface MessageSink<K, V> {
  void send(final ConsumerRecord<K, V> record, final V processedValue);
}
```

### Built-in Sinks

KPipe provides several built-in sinks:

```java
// Create a JSON console sink
final var jsonConsoleSink = new JsonConsoleSink<>();

// Create an Avro console sink
final var avroConsoleSink = new AvroConsoleSink<>();

// Use a sink with a consumer
final var consumer = new KPipeConsumer.<String, byte[]>builder()
  .withProperties(kafkaProps)
  .withTopic("events")
  .withProcessor(pipeline)
  .withMessageSink(jsonConsoleSink)
  .build();
```

### Custom Sinks

You can create custom sinks using lambda expressions:

```java
// Create a custom sink that writes to a database
MessageSink<String, byte[]> databaseSink = (record, processedValue) -> {
  try {
    // Parse the processed value
    final var data = JsonMessageProcessor.parseJson().apply(processedValue);

    // Write to database
    databaseService.insert(data);

    // Log success
    log.log(Level.INFO, "Successfully wrote message to database: " + record.key());
  } catch (Exception e) {
    log.log(Level.ERROR, "Failed to write message to database", e);
  }
};

// Use the custom sink with a consumer
final var consumer = new KPipeConsumer.<String, byte[]>builder()
  .withMessageSink(databaseSink)
  .build();
```

### Message Sink Registry

The `MessageSinkRegistry` provides a centralized repository for registering and retrieving message sinks:

```java
// Create a registry
final var registry = new MessageSinkRegistry();

// Register sinks
registry.register("console", new JsonConsoleSink<>());
registry.register("database", databaseSink);
registry.register("metrics", (record, value) -> metricsService.recordMessage(record.topic(), value.length));

// Create a pipeline of sinks
final var sinkPipeline = registry.<String, byte[]>pipeline("console", "database", "metrics");

// Use the sink pipeline with a consumer
final var consumer = new KPipeConsumer.<String, byte[]>builder()
  .withMessageSink(sinkPipeline)
  .build();
```

### Error Handling in Sinks

The registry provides utilities for adding error handling to sinks:

```java
// Create a sink with error handling
final var safeSink = MessageSinkRegistry.withErrorHandling(riskySink);

// Register and use the wrapped sink
registry.register("safeDatabase", safeSink);
final var safePipeline = registry.<String, byte[]>pipeline("console", "safeDatabase", "metrics");
```

---

## Consumer Runner

The `ConsumerRunner` provides a high-level management layer for Kafka consumers, handling lifecycle, metrics, and
graceful shutdown:

```java
// Create a consumer runner with default settings
ConsumerRunner<KPipeConsumer<String, String>> runner = ConsumerRunner.builder(consumer)
  .build();

// Start the consumer
runner.start();

// Wait for shutdown
runner.awaitShutdown();
```

### Advanced Configuration

The `ConsumerRunner` supports extensive configuration options:

```java
// Create a consumer runner with advanced configuration
final var runner = ConsumerRunner.builder(consumer)
  // Configure metrics reporting
  .withMetricsReporters(List.of(
    new ConsumerMetricsReporter(consumer::getMetrics, () -> System.currentTimeMillis() - startTime, null)
  ))
  .withMetricsInterval(30000) // Report metrics every 30 seconds
  // Configure health checks
  .withHealthCheck(KPipeConsumer::isRunning)
  // Configure graceful shutdown
  .withShutdownTimeout(10000) // 10 seconds timeout for shutdown
  .withShutdownHook(true) // Register JVM shutdown hook
  // Configure custom start action
  .withStartAction(c -> {
    log.log(Level.INFO, "Starting consumer");
    c.start();
  })
  // Configure custom graceful shutdown
  .withGracefulShutdown((c, timeoutMs) -> {
      log.log(Level.INFO, "Initiating graceful shutdown with timeout: " + timeoutMs + "ms");
      return ConsumerRunner.performGracefulConsumerShutdown(c, timeoutMs);
  })
  .build();
```

### Lifecycle Management

The `ConsumerRunner` manages the complete lifecycle of a consumer:

```java
// Start the consumer (idempotent - safe to call multiple times)
runner.start();

// Check if the consumer is healthy
boolean isHealthy = runner.isHealthy();

// Wait for shutdown (blocks until shutdown completes)
boolean cleanShutdown = runner.awaitShutdown();

// Initiate shutdown
runner.close();
```

### Metrics Integration

The `ConsumerRunner` integrates with metrics reporting:

```java
// Add multiple metrics reporters
ConsumerRunner<KPipeConsumer<String, String>> runner = ConsumerRunner.builder(consumer)
  .withMetricsReporters(List.of(
    new ConsumerMetricsReporter(consumer::getMetrics, () -> System.currentTimeMillis() - startTime, null),
    new ProcessorMetricsReporter(registry)
  ))
  .withMetricsInterval(60000) // Report every minute
  .build();
```

### Using with AutoCloseable

The `ConsumerRunner` implements `AutoCloseable` for use with try-with-resources:

```java
try (ConsumerRunner<KPipeConsumer<String, String>> runner = ConsumerRunner.builder(consumer).build()) {
  runner.start();
  // Application logic here
  // Runner will be automatically closed when exiting the try block
}
```

---

## Application Example

Here's a concise example of a KPipe application:

```java
public class KPipeApp implements AutoCloseable {
  private static final System.Logger LOGGER = System.getLogger(KPipeApp.class.getName());
  private final ConsumerRunner<KPipeConsumer<byte[], byte[]>> runner;

  static void main() {
    // Load configuration from environment variables
    final var config = AppConfig.fromEnv();

    try (final var app = new KPipeApp(config)) {
      app.start();
      app.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in application", e);
      System.exit(1);
    }
  }

  public KPipeApp(final AppConfig config) {
    // Create processor and sink registries
    final var processorRegistry = new MessageProcessorRegistry(config.appName());
    final var sinkRegistry = new MessageSinkRegistry();
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    // Create the functional consumer
    final var functionalConsumer = KPipeConsumer.<byte[], byte[]>builder()
      .withProperties(KafkaConsumerConfig.createConsumerConfig(
        config.bootstrapServers(), config.consumerGroup()))
      .withTopic(config.topic())
      .withProcessor(processorRegistry.jsonPipeline(
        "addSource", "markProcessed", "addTimestamp"))
      .withMessageSink(sinkRegistry.<byte[], byte[]>pipeline("jsonLogging"))
      .withCommandQueue(commandQueue)
      .withOffsetManagerProvider(consumer -> 
         OffsetManager.builder(consumer)
          .withCommandQueue(commandQueue)
           .withCommitInterval(Duration.ofSeconds(30))
           .build())
      .withMetrics(true)
      .build();

    // Set up the consumer runner with metrics and shutdown hooks
    runner = ConsumerRunner.builder(functionalConsumer)
      .withMetricsInterval(config.metricsInterval().toMillis())
      .withShutdownTimeout(config.shutdownTimeout().toMillis())
      .withShutdownHook(true)
      .build();
  }

  public void start() { runner.start(); }
  public boolean awaitShutdown() { return runner.awaitShutdown(); }
  public void close() { runner.close(); }
}
```

**Key Components:**
- Configuration from environment variables
- Processor and sink registries for message handling
- Processing pipeline with error handling
- Metrics reporting and graceful shutdown

**To Run:**

```bash
# Set configuration
export HEALTH_HTTP_ENABLED=true
export HEALTH_HTTP_HOST=0.0.0.0
export HEALTH_HTTP_PORT=8080
export HEALTH_HTTP_PATH=/health
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_CONSUMER_GROUP=my-group
export KAFKA_TOPIC=json-events
export KAFKA_PROCESSORS=parseJson,validateSchema,addTimestamp
export METRICS_INTERVAL_MS=30000
export SHUTDOWN_TIMEOUT_MS=5000
```

---

## Requirements

- Java 25+
- Gradle (for building the project)
- [kcat](https://github.com/edenhill/kcat) (for testing)
- Docker (for local Kafka setup)

---

## Testing

Follow these steps to test the KPipe Kafka Consumer:

### Build and Run

  ```bash
  # Format code and build the library module
  ./gradlew clean :lib:spotlessApply :lib:build

  # Format code and build the applications module
  ./gradlew :app:clean :app:spotlessApply :app:build

  # Build the consumer app container and start all services
  docker compose build --no-cache --build-arg MESSAGE_FORMAT=<json|avro|protobuf>
  docker compose down -v
  docker compose up -d

  # Publish a simple JSON message to the json-topic
  echo '{"message":"Hello world"}' | kcat -P -b kafka:9092 -t json-topic

  # For complex JSON messages, use a file
  cat test-message.json | kcat -P -b kafka:9092 -t json-topic

  # Publish multiple test messages
  for i in {1..10}; do echo "{\"id\":$i,\"message\":\"Test message $i\"}" | \
    kcat -P -b kafka:9092 -t json-topic; done
  ```

### Working with the Schema Registry and Avro

If you want to use Avro with a schema registry, follow these steps:

  ```bash
  # Register an Avro schema
  curl -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"schema\": $(cat lib/src/test/resources/avro/customer.avsc | jq tostring)}" \
    http://localhost:8081/subjects/com.kpipe.customer/versions

  # Read registered schema 
  curl -s http://localhost:8081/subjects/com.kpipe.customer/versions/latest | jq -r '.schema' | jq --indent 2 '.'

  # Produce an Avro message using kafka-avro-console-producer
  cat <<'JSON' | docker run -i --rm --network kpipe_default \
  -v "$PWD/lib/src/test/resources/avro/customer.avsc:/tmp/customer.avsc:ro" \
  confluentinc/cp-schema-registry:8.2.0 \
  sh -ec 'kafka-avro-console-producer \
    --bootstrap-server kafka:9092 \
    --topic avro-topic \
    --property schema.registry.url=http://schema-registry:8081 \
    --property value.schema="$(cat /tmp/customer.avsc)"'
{"id":1,"name":"Mariano Gonzalez","email":{"string":"mariano@example.com"},"active":true,"registrationDate":1635724800000,"address":{"com.kpipe.customer.Address":{"street":"123 Main St","city":"Chicago","zipCode":"00000","country":"USA"}},"tags":["premium","verified"],"preferences":{"notifications":"email"}}
JSON
  ```

Kafka consumer will:

- Connect to `localhost:9092`
- Subscribe to `avro-topic|json-topic|protobuf-topic`
- Compose the processing pipeline from configured processors
- Process each message concurrently using virtual threads

---

## Best Practices & Advanced Patterns

### Composing Complex Processing Pipelines

For maintainable pipelines, you can compose multiple pipelines or operators:

  ```java
  final var registry = new MessageProcessorRegistry("myApp");

  // Create focused operator groups
  registry.registerJsonOperator("security", 
      JsonMessageProcessor.removeFields("password", "creditCard"));
  
  registry.registerJsonOperator("enrichment",
      JsonMessageProcessor.addTimestamp("processedAt"));

  // Compose them into an optimized pipeline
  final var fullPipeline = registry.jsonPipeline("security", "enrichment");
  ```

### Conditional Processing

The library provides a built-in `when()` method for conditional processing:

  ```java
  // Create a predicate that checks message type
  Predicate<byte[]> isOrderMessage = bytes -> {
      // Logic to check if it's an order
      return true; 
  };

  // Use the built-in conditional processor
  Function<byte[], byte[]> conditionalPipeline = MessageProcessorRegistry.when(
      isOrderMessage,
      registry.jsonPipeline("orderProcessor"),
      registry.jsonPipeline("defaultProcessor")
  );
  ```

### Thread-Safety Considerations
- Message processors should be stateless and thread-safe
- Avoid shared mutable state between processors
- Use immutable data structures where possible
- For processors with side effects (like database calls), consider using thread-local variables

### Performance Optimization
- Register frequently used processor combinations as single processors
- For very large messages, consider streaming JSON processors
- Profile your processor pipeline to identify bottlenecks

---

## Performance Notes

### High-Performance Architecture

KPipe is designed for high-throughput, low-overhead Kafka processing using modern Java features and pipeline
optimizations. Performance depends on workload shape (I/O vs CPU bound), partitioning, and message size.

- **Single SerDe Cycle**: Traditional pipelines often perform `byte[] -> Object -> byte[]` at every step. KPipe's
  optimized pipelines deserialize once at the entry, apply any number of `UnaryOperator` transformations on the object,
  and serialize once at the exit.
- **Zero-Copy Magic Byte Handling**: For Avro data (especially from Confluent Schema Registry), KPipe supports an
  `offset` parameter that allows skipping magic bytes and schema IDs without performing expensive `Arrays.copyOfRange`
  operations.
- **Virtual Threads (Project Loom)**: Every Kafka record can be processed in its own virtual thread. This allows for
  massive concurrency with simpler coordination than large platform-thread pools.
- **DslJson Integration**: Uses a high-performance JSON library to reduce parsing overhead and GC pressure.

Latest parallel benchmark snapshot (see `benchmarks/README.md`) shows a throughput edge for KPipe in that scenario,
with a higher allocation footprint than Confluent Parallel Consumer. Treat these as scenario-specific results, not
universal guarantees.

---

## Ordering & Reliability

KPipe provides configurable ordering guarantees to balance throughput and strictness.

### 1. Sequential Processing (Strict Ordering)

When `sequentialProcessing` is enabled, KPipe processes messages one by one in the order they
were received from Kafka. This ensures **strict FIFO (First-In-First-Out) ordering** per partition.

### 2. Parallel Processing (Virtual Threads)

When `sequentialProcessing` is `false` (the builder default), KPipe leverages Java 25 Virtual Threads to process
multiple records concurrently.

- **Execution Order**: Messages start in order but may finish out of order (e.g., a small message finishing before a
  large one).
- **At-Least-Once Delivery**: Even with parallel processing, KPipe's `OffsetManager` uses a **Lowest Pending Offset**
  strategy. It will never commit a higher offset until all lower offsets in that partition are successfully processed.
  This ensures **no gaps** in your committed data.

### 3. Key-Level Ordering for Critical Systems (e.g., Payments)

For systems like **payment processors** where the order of operations (Authorize -> Capture) is vital:

- **Consistent Partitioning**: Ensure your producer uses a consistent key (e.g., `transaction_id` or `customer_id`).
  Kafka guarantees all messages with the same key land in the same partition.
- **Safety**: KPipe manages each partition as an independent, ordered sequence of offsets. As long as related events
  share a key, KPipe's commitment strategy ensures they are handled reliably without skipping steps.

---

## Inspiration

This library is inspired by the best practices from:

- [Project Loom](https://openjdk.org/projects/loom/)
- [High-performance JSON libraries (DslJson)](https://github.com/ngs-doo/dsl-json)

---

## Contributing

If you're a team using this library, feel free to:

- Register custom processors
- Add metrics/observability hooks
- Share improvements or retry strategies

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---
