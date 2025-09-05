# <img src="img/kpipe_1.png" width="200" height="200">

# ☕ KPipe - A Modern Kafka Consumer

[![GitHub release](https://img.shields.io/github/release/eschizoid/kpipe.svg?style=flat-square)](https://github.com/eschizoid/kpipe/releases/latest)
[![Codecov](https://codecov.io/gh/eschizoid/kpipe/graph/badge.svg?token=X50GBU4X7J)](https://codecov.io/gh/eschizoid/kpipe)
[![Build Status](https://github.com/eschizoid/kpipe/actions/workflows/ci.yaml/badge.svg)](https://github.com/eschizoid/kpipe/actions/workflows/ci.yaml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A **modern, functional, and high-performance Kafka consumer** built using **Java 24** features like **virtual threads**,
**composable message processors**, and **DslJson** for JSON processing. Features robust error handling, configurable
retries, built-in metrics, and support for both parallel and sequential processing. Ideal for large-scale systems.

---

## 🚀 Why This Library?

### ✅ Modern Java Features

- **Virtual Threads** for massive concurrency with minimal overhead
- **Functional programming** patterns for clean, maintainable code
- **High-performance JSON processing** with DslJson

### 🧩 Functional Processing Pipeline

- Message processors are **pure functions** (`Function<V, V>`) that transform data without side effects
- Build complex pipelines through **function composition** using `Function::andThen` or the registry
- **Declarative processing** lets you describe *what* to do, not *how* to do it
- **Higher-order functions** enable conditional processing, retry logic, and error handling
- Teams can **register their own processors** in a central registry via:

  ```java
  // Register team-specific processors
  MessageProcessorRegistry.register("sanitizeData",
      JsonMessageProcessor.removeFields("password", "ssn"));

  // Create pipelines from registered processors
  final var pipeline = MessageProcessorRegistry.pipeline(
      "parseJson", "validateSchema", "sanitizeData", "addMetadata");

  // Apply transformations with built-in error handling and retry logic
  final var consumer = new FunctionalConsumer.<byte[], byte[]>builder()
    .withProcessor(pipeline)
    .withRetry(3, Duration.ofSeconds(1))
    .build();
  consumer.start();
  ```

  ```java
  // Register custom processors for your team's needs
  MessageProcessorRegistry.register("extractMetadata", message -> {
    // Custom extraction logic here
    return processedMessage;
  });

  // Load processors from configuration
  String[] configuredProcessors = config.getStringArray("message.processors");
  Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(configuredProcessors);

  // Create a consumer with team-specific processing pipeline
  final var consumer = new FunctionalConsumer.<byte[], byte[]>builder()
    .withProperties(kafkaProps)
    .withTopic("team-topic")
    .withProcessor(MessageProcessorRegistry.pipeline(
        "parseJson",
        "validateSchema",
        "extractMetadata",
        "addTeamIdentifier"))
  .withErrorHandler(error -> publishToErrorTopic(error))
  .withRetry(3, Duration.ofSeconds(1))
  .build();

  // Use the consumer
  consumer.start();
  ```

---

## 📦 Installation

### Maven

  ```xml
  <dependency>
      <groupId>io.github.eschizoid</groupId>
      <artifactId>kpipe</artifactId>
      <version>0.2.0</version>
  </dependency>
  ```

### Gradle (Groovy)

  ```groovy
  implementation 'io.github.eschizoid:kpipe:0.2.0'
  ```

### Gradle (Kotlin)

  ```kotlin
  implementation("io.github.eschizoid:kpipe:0.2.0")
  ```

### SBT

  ```sbt
  libraryDependencies += "io.github.eschizoid" % "kpipe" % "0.2.0"
  ```

---

## 📁 Project Structure

KPipe is organized into two main modules:

### Library Module (lib)

The core library that provides the Kafka consumer functionality:

```
├── src/main/java/org/kpipe/
│   ├── consumer/                         # Core consumer components
│   │   ├── FunctionalConsumer.java       # Main functional consumer implementation
│   │   ├── OffsetManager.java            # Manages Kafka offsets for reliable processing
│   │   ├── MessageTracker.java           # Tracks message processing state
│   │   ├── RebalanceListener.java        # Handles Kafka consumer rebalancing
│   │   └── enums/                        # Enums for consumer states and commands
│   │
│   ├── processor/                        # Message processors
│   │   ├── JsonMessageProcessor.java     # JSON processing with DslJson
│   │   └── AvroMessageProcessor.java     # Avro processing with Apache Avro
│   │
│   ├── registry/                         # Registry components
│   │   ├── MessageProcessorRegistry.java # Registry for processor functions
│   │   ├── MessageSinkRegistry.java      # Registry for message sinks
│   │   ├── MessageFormat.java            # Enum for message format types
│   │   └── RegistryFunctions.java        # Shared utilities for registries
│   │
│   ├── sink/                             # Message sink implementations
│   │   ├── JsonConsoleSink.java          # Console sink for JSON messages
│   │   ├── AvroConsoleSink.java          # Console sink for Avro messages
│   │   └── MessageSink.java              # Message sink interface
│   │
│   ├── config/                           # Configuration components
│   │   ├── AppConfig.java                # Application configuration
│   │   └── KafkaConsumerConfig.java      # Kafka consumer configuration
│   │
│   └── metrics/                          # Metrics components
│       ├── ConsumerMetricsReporter.java  # Reports consumer metrics
│       ├── MetricsReporter.java          # Metrics reporting interface
│       └── ProcessorMetricsReporter.java # Reports processor metrics
```

### Application Module (app)

A ready-to-use application that demonstrates the library:

```
├── src/main/java/org/kpipe/
│   |── json/
│   │   └── App.java # Main application class to demonostrate JSON integration
|   |
│   └── avro/
│       └── App.java # Main application class to demonostrate Avro integration
```

---

## ⚙️ Example: Add Custom Processor

Extend the registry like this:

  ```java
  // Register a processor for JSON field transformations
  MessageProcessorRegistry.register("uppercase", bytes ->
      JsonMessageProcessor.transformField("text", value -> {
          if (value instanceof String text) {
              return text.toUpperCase();
          }
          return value;
      }).apply(bytes)
  );

  // Register a processor that adds environment information
  MessageProcessorRegistry.register("addEnvironment",
      JsonMessageProcessor.addField("environment", "production"));

  // Create a reusable processor pipeline
  final var pipeline = MessageProcessorRegistry.pipeline(
      "parseJson", "validateSchema", "addEnvironment", "uppercase", "addTimestamp"
  );

  // Use the pipeline with a consumer
  final var consumer = new FunctionalConsumer.<byte[], byte[]>builder()
      .withProperties(kafkaProps)
      .withTopic("events")
      .withProcessor(pipeline)
      .withRetry(3, Duration.ofSeconds(1))
      .build();

  // Start processing messages
  consumer.start();
  ```

---

## 📊 Built-in Metrics

Monitor your consumer with built-in metrics:

  ```java
  // Access consumer metrics
  Map<String, Long> metrics = consumer.getMetrics();
  System.out.println("Messages received: " + metrics.get("messagesReceived"));
  System.out.println("Successfully processed: " + metrics.get("messagesProcessed"));
  System.out.println("Processing errors: " + metrics.get("processingErrors"));
  ```

Configure automatic metrics reporting:

  ```java
  new App(config)
    .withMetricsInterval(Duration.ofSeconds(30))
    .start();
  ```

---

## 🛡️ Graceful Shutdown
The consumer supports graceful shutdown with in-flight message handling:

  ```java
  // Initiate graceful shutdown with 5-second timeout
  boolean allProcessed = kafkaApp.shutdownGracefully(5000);
  if (allProcessed) {
    LOGGER.info("All messages processed successfully before shutdown");
  } else {
    LOGGER.warning("Shutdown completed with some messages still in flight");
  }

  // Register as JVM shutdown hook
  Runtime.getRuntime().addShutdownHook(
    new Thread(() -> app.shutdownGracefully(5000))
  );
  ```

---

## 🔧 Working with Messages

### JSON Processing

The JSON processors handle deserialization and transformation of JSON data:

  ```java
  // Add a timestamp field to messages
  final var addTimestampProcessor = JsonMessageProcessor.addTimestamp("processedAt");

  // Remove sensitive fields
  final var sanitizeProcessor = JsonMessageProcessor.removeFields("password", "ssn", "creditCard");

  // Transform specific fields
  final var uppercaseSubjectProcessor = JsonMessageProcessor.transformField("subject", value -> {
      if (value instanceof String text) {
          return text.toUpperCase();
      }
      return value;
  });

  // Add metadata to messages
  final var metadata = new HashMap<String, Object>();
  metadata.put("version", "1.0");
  metadata.put("environment", "production");
  var addMetadataProcessor = JsonMessageProcessor.mergeWith(metadata);

  // Combine processors into a pipeline
  Function<byte[], byte[]> pipeline = message -> addMetadataProcessor.apply(
      uppercaseSubjectProcessor.apply(
          sanitizeProcessor.apply(
              addTimestampProcessor.apply(message)
          )
      )
  );

  // Or use the registry to build pipelines
  final var registryPipeline = MessageProcessorRegistry.pipeline(
      "sanitize", "addTimestamp", "uppercaseSubject", "addMetadata"
  );
  ```

### Avro Processing

The Avro processors handle deserialization and transformation of Avro data:

  ```java
  // Register an Avro schema
  AvroMessageProcessor.registerSchema("userSchema", userSchemaJson);

  // Parse Avro messages
  final var parseProcessor = AvroMessageProcessor.parseAvro("userSchema");

  // Add a timestamp field to messages
  final var addTimestampProcessor = AvroMessageProcessor.addTimestamp("userSchema", "processedAt");

  // Remove sensitive fields
  final var sanitizeProcessor = AvroMessageProcessor.removeFields("userSchema", "password", "creditCard");

  // Transform specific fields
  final var uppercaseNameProcessor = AvroMessageProcessor.transformField(
    "userSchema", 
    "name", 
    value -> {
      if (value instanceof String text) {
        return text.toUpperCase();
      }
      return value;
    }
  );

  // Add multiple fields at once
  final var fieldsToAdd = Map.of(
    "version", "1.0",
    "environment", "production"
  );
  final var addFieldsProcessor = AvroMessageProcessor.addFields("userSchema", fieldsToAdd);

  // Compose processors into a pipeline
  final var pipeline = AvroMessageProcessor.compose(
    parseProcessor,
    addTimestampProcessor,
    sanitizeProcessor,
    uppercaseNameProcessor,
    addFieldsProcessor
  );

  // Or register in the registry and build pipelines
  MessageProcessorRegistry.register("parseAvro", parseProcessor);
  MessageProcessorRegistry.register("addTimestamp", addTimestampProcessor);
  MessageProcessorRegistry.register("sanitize", sanitizeProcessor);
  MessageProcessorRegistry.register("uppercaseName", uppercaseNameProcessor);
  MessageProcessorRegistry.register("addFields", addFieldsProcessor);

  final var registryPipeline = MessageProcessorRegistry.pipeline(
    "parseAvro", "addTimestamp", "sanitize", "uppercaseName", "addFields"
  );
  ```

---

## 📤 Message Sinks

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
// Create a JSON console sink that logs messages at INFO level
final var jsonConsoleSink = new JsonConsoleSink<>(
  System.getLogger("org.kpipe.sink.JsonConsoleSink"), 
  Level.INFO
);

// Create an Avro console sink that logs messages at INFO level
final var avroConsoleSink = new AvroConsoleSink<>(
  System.getLogger("org.kpipe.sink.AvroConsoleSink"), 
  Level.INFO
);

// Use a sink with a consumer
final var consumer = new FunctionalConsumer.<String, byte[]>builder()
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
    Map<String, Object> data = JsonMessageProcessor.parseJson().apply(processedValue);

    // Write to database
    databaseService.insert(data);

    // Log success
    logger.info("Successfully wrote message to database: " + record.key());
  } catch (Exception e) {
    logger.error("Failed to write message to database", e);
  }
};

// Use the custom sink with a consumer
final var consumer = new FunctionalConsumer.<String, byte[]>builder()
  .withMessageSink(databaseSink)
  .build();
```

### Message Sink Registry

The `MessageSinkRegistry` provides a centralized repository for registering and retrieving message sinks:

```java
// Create a registry
final var registry = new MessageSinkRegistry();

// Register sinks
registry.register("console", new JsonConsoleSink<>(logger, Level.INFO));
registry.register("database", databaseSink);
registry.register("metrics", (record, value) -> metricsService.recordMessage(record.topic(), value.length));

// Create a pipeline of sinks
final var sinkPipeline = registry.<String, byte[]>pipeline("console", "database", "metrics");

// Use the sink pipeline with a consumer
final var consumer = new FunctionalConsumer.<String, byte[]>builder()
  .withMessageSink(sinkPipeline)
  .build();
```

### Error Handling in Sinks

The registry provides utilities for adding error handling to sinks:

```java
// Create a sink with error handling
final var safeSink = MessageSinkRegistry.withErrorHandling(
  riskySink,
  (record, value, error) -> logger.error("Error in sink: " + error.getMessage())
);

// Or use the registry's error handling
final var safePipeline = registry.<String, byte[]>pipelineWithErrorHandling(
  "console", "database", "metrics",
  (record, value, error) -> errorService.reportError(record.topic(), error)
);
```

---

## 🔄 Consumer Runner

The `ConsumerRunner` provides a high-level management layer for Kafka consumers, handling lifecycle, metrics, and graceful shutdown:

```java
// Create a consumer runner with default settings
ConsumerRunner<FunctionalConsumer<String, String>> runner = ConsumerRunner.builder(consumer)
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
ConsumerRunner<FunctionalConsumer<String, String>> runner = ConsumerRunner.builder(consumer)
  // Configure metrics reporting
  .withMetricsReporter(new ConsumerMetricsReporter(
    consumer::getMetrics,
    () -> System.currentTimeMillis() - startTime
  ))
  .withMetricsInterval(30000) // Report metrics every 30 seconds

  // Configure health checks
  .withHealthCheck(c -> c.getState() == ConsumerState.RUNNING)

  // Configure graceful shutdown
  .withShutdownTimeout(10000) // 10 seconds timeout for shutdown
  .withShutdownHook(true) // Register JVM shutdown hook

  // Configure custom start action
  .withStartAction(c -> {
    logger.info("Starting consumer for topic: " + c.getTopic());
    c.start();
  })

  // Configure custom graceful shutdown
  .withGracefulShutdown((c, timeoutMs) -> {
      logger.info("Initiating graceful shutdown with timeout: " + timeoutMs + "ms");
      return c.shutdownGracefully(timeoutMs);
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
ConsumerRunner<FunctionalConsumer<String, String>> runner = ConsumerRunner.builder(consumer)
  .withMetricsReporters(List.of(
    new ConsumerMetricsReporter(consumer::getMetrics, () -> System.currentTimeMillis() - startTime),
    new ProcessorMetricsReporter(registry)
  ))
  .withMetricsInterval(60000) // Report every minute
  .build();
```

### Using with AutoCloseable

The `ConsumerRunner` implements `AutoCloseable` for use with try-with-resources:

```java
try (ConsumerRunner<FunctionalConsumer<String, String>> runner = ConsumerRunner.builder(consumer).build()) {
  runner.start();
  // Application logic here
  // Runner will be automatically closed when exiting the try block
}
```

---

## 🔍 Application Example

Here's a concise example of a KPipe application:

```java
public class KPipeApp implements AutoCloseable {
  private final ConsumerRunner<FunctionalConsumer<byte[], byte[]>> runner;

  public static void main(final String[] args) {
    // Load configuration from environment variables
    final var config = AppConfig.fromEnv();

    try (final var app = new MyKafkaApp(config)) {
      app.start();
      app.awaitShutdown();
    } catch (final Exception e) {
      System.getLogger(MyKafkaApp.class.getName())
        .log(System.Logger.Level.ERROR, "Fatal error in application", e);
      System.exit(1);
    }
  }

  public MyKafkaApp(final AppConfig config) {
    // Create processor and sink registries
    final var processorRegistry = new MessageProcessorRegistry(config.appName());
    final var sinkRegistry = new MessageSinkRegistry();

    // Create the functional consumer
    final var functionalConsumer = FunctionalConsumer.<byte[], byte[]>builder()
      .withProperties(KafkaConsumerConfig.createConsumerConfig(
        config.bootstrapServers(), config.consumerGroup()))
      .withTopic(config.topic())
      .withProcessor(processorRegistry.pipeline(
        "parseJson", "addSource", "markProcessed", "addTimestamp"))
      .withMessageSink(sinkRegistry.<byte[], byte[]>pipeline("logging"))
      .withOffsetManagerProvider(consumer -> 
        OffsetManager.builder(consumer)
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
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_CONSUMER_GROUP=my-group
export KAFKA_TOPIC=json-events

# Run the application
./gradlew run

# Test with a sample message
echo '{"message":"Hello from KPipe!"}' | kcat -P -b localhost:9092 -t json-events
```

---

## 🛠️ Requirements

- Java 24+
- Gradle (for building the project)
- [kcat](https://github.com/edenhill/kcat) (for testing)
- Docker (for local Kafka setup)

---

## ⚙️ Configuration

Configure via environment variables:

  ```bash
  export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  export KAFKA_CONSUMER_GROUP=my-consumer-group
  export KAFKA_TOPIC=json-events
  export KAFKA_PROCESSORS=parseJson,validateSchema,addTimestamp
  export METRICS_INTERVAL_MS=30000
  export SHUTDOWN_TIMEOUT_MS=5000
  ```

---

## 🧪 Testing

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
  echo '{"id":1,"name":"Mariano Gonzalez","email":{"string":"mariano@example.com"},"active":true,"registrationDate":1635724800000,"address":{"com.kpipe.customer.Address":{"street":"123 Main St","city":"Chicago","zipCode":"00000","country":"USA"}},"tags":["premium","verified"],"preferences":{"notifications":"email"}}' \
  | docker run -i --rm --network=host confluentinc/cp-schema-registry:latest \
      kafka-avro-console-producer \
      --broker-list localhost:9092 \
      --topic avro-topic \
      --property schema.registry.url=http://localhost:8081 \
      --property value.schema.id=1
  ```

Kafka consumer will:

- Connect to `localhost:9092`
- Subscribe to `avro-topic|json-topic|protobuf-topic`
- Compose the processing pipeline from configured processors
- Process each message concurrently using virtual threads

---

## 🔍 Best Practices & Advanced Patterns

### Composing Complex Processing Pipelines

For maintainable pipelines, group related processors:

  ```java
  // Create focused processor groups
  final var securityProcessors = MessageProcessorRegistry.pipeline(
      "sanitizeData", "encryptSensitiveFields", "addAuditTrail");

  final var enrichmentProcessors = MessageProcessorRegistry.pipeline(
      "addMetadata", "addTimestamp", "addEnvironment");

  // Compose them into a master pipeline
  final var fullPipeline = message -> enrichmentProcessors.apply(
      securityProcessors.apply(message));

  // Or register the composed pipeline
  MessageProcessorRegistry.register("standardProcessing", fullPipeline);
  ```

### Conditional Processing

The library provides a built-in `when()` method for conditional processing:

  ```java
  // Create a predicate that checks message type
  Predicate<byte[]> isOrderMessage = message -> {
      try {
          Map<String, Object> parsed = JsonMessageProcessor.parseJson().apply(message);
          return "ORDER".equals(parsed.get("type"));
      } catch (Exception e) {
          return false;
      }
  };

  // Use the built-in conditional processor
  Function<byte[], byte[]> conditionalProcessor = MessageProcessorRegistry.when(
      isOrderMessage,
      MessageProcessorRegistry.get("orderEnrichment"),
      MessageProcessorRegistry.get("defaultEnrichment")
  );

  // Register the conditional pipeline
  MessageProcessorRegistry.register("smartProcessing", conditionalProcessor);
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

## 📈 Performance Notes

- Virtual threads are **1:1 with Kafka records** — scales to 100k+ messages/sec
- Zero-GC JSON processing
- Safe and efficient memory model using modern Java features

---

## 📚 Inspiration

This library is inspired by the best practices from:

- [Project Loom](https://openjdk.org/projects/loom/)
- [High-performance JSON libraries (DslJson)](https://github.com/ngs-doo/dsl-json)

---

## 💬 Contributing

If you're a team using this library, feel free to:

- Register custom processors
- Add metrics/observability hooks
- Share improvements or retry strategies

---

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

## 🧠 Final Thoughts

This Kafka consumer is:

- **Functional**
- **Extensible**
- **Future-proof**

Use it to modernize your Kafka stack with **Java 24 elegance and performance**.
